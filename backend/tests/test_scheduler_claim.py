"""调度领取占槽与队列更新的回归测试。

覆盖需求：
- 满载先检查上限再占槽：队头与选择位置保留、可重试原因、不计成功派发；
- 真正受理（attempt_id 回执 + 采集任务活动尝试）后才移出待派行，两侧可核对；
- 回报到达后两侧同步、容量释放可再次领取；
- 写入失败回滚占槽，不遗留无人可完成的活动名额；
- 两个执行端并发领取只允许一条待办进入执行，另一条仍可再次领取；
- 中断恢复区分「继续原尝试」与「重新领取」资格；
- 旧调用方 dispatch_next 对满载/空队列仍返回 None。

运行：cd backend && python3 -m pytest tests/test_scheduler_claim.py -v
"""
from __future__ import annotations

import threading

import pytest

from app.services import SERVICES
from app.store import store


@pytest.fixture()
def plan_with_two_pending():
    """上限 1 的计划：入队两条待派记录，返回 (计划, 任务1, 任务2)。"""
    sch = SERVICES["scheduler"]
    cm = SERVICES["crawl_mission"]
    plan, _ = sch.create_entry({
        "计划编号": "CLAIM-PLAN", "Cron表达式": "*/5 * * * *",
        "关联任务组": "claim-group", "并发上限": "1", "失败策略": "retry",
    })
    sch.run_action(plan["id"], "启动计划")
    m1, _ = cm.create_entry({"任务编号": "CLAIM-M1", "目标URL": "https://a.example.com", "优先级": "5"})
    m2, _ = cm.create_entry({"任务编号": "CLAIM-M2", "目标URL": "https://b.example.com", "优先级": "3"})
    sch.enqueue_mission(plan["id"], m1["id"], priority=5)
    sch.enqueue_mission(plan["id"], m2["id"], priority=3)
    return plan, m1, m2


class TestClaimConcurrencyLimit:
    def test_full_load_keeps_head_and_position(self, plan_with_two_pending):
        """上限 1 + 一个在途 + 两条待派：满载返回可重试原因，队头与选择位置不变。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]

        first = sch.claim_next(plan["id"])
        assert first["accepted"] is True
        assert first["mission_id"] == m1["id"]
        assert first["limit"] == 1 and first["active"] == 1 and first["available"] == 0

        # 第二次领取：满载，不得弹队列、不得计数
        deny = sch.claim_next(plan["id"])
        assert deny["accepted"] is False
        assert deny["retryable"] is True
        assert deny["reason_code"] == "concurrency_full"
        assert "并发上限" in deny["message"]
        assert deny["head"]["mission_id"] == m2["id"]
        assert deny["head"]["queue_position"] == 0

        snapshot = sch.queue_snapshot(plan["id"])
        assert [item["mission_id"] for item in snapshot["queue"]] == [m2["id"]]
        assert snapshot["queue"][0]["queue_position"] == 0
        assert snapshot["total_dispatched"] == 1  # 满载不计成功派发
        assert snapshot["active"] == 1
        assert snapshot["last_deny"]["reason_code"] == "concurrency_full"

    def test_legacy_dispatch_next_returns_none_when_full(self, plan_with_two_pending):
        """兼容旧调用方：成功返回任务字典，满载/空队列返回 None。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]

        ok = sch.dispatch_next(plan["id"])
        assert ok is not None and ok["mission_id"] == m1["id"]
        assert "attempt_id" in ok  # 新回执仍挂在兼容返回上，便于核对

        assert sch.dispatch_next(plan["id"]) is None  # 满载仍为 None
        # 空队列同样为 None
        empty_plan, _ = sch.create_entry({
            "计划编号": "CLAIM-EMPTY", "Cron表达式": "* * * * *", "关联任务组": "g",
        })
        sch.run_action(empty_plan["id"], "启动计划")
        assert sch.dispatch_next(empty_plan["id"]) is None

    def test_other_cooled_down_site_not_blocked(self, plan_with_two_pending):
        """无关站点的限速冷却不阻塞待派记录；容量释放后队头立即可领。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        rl = SERVICES["rate_limit"]
        policy, _ = rl.create_entry({
            "策略编号": "CLAIM-RATE", "目标域名": "blocked.example.net", "请求间隔": "1s",
        })
        rl.run_action(policy["id"], "标记生效")
        rl.trigger_cooldown(policy["id"], 60.0, mission_id=999)
        allowed, _ = rl.check_allow(policy["id"], "blocked.example.net")
        assert allowed is False  # 无关站点在冷却中

        sch.claim_next(plan["id"])  # 占满唯一槽位
        deny = sch.claim_next(plan["id"])
        assert deny["reason_code"] == "concurrency_full"
        # 回报释放容量后，冷却中的是别的站点，待派队头可立即领取
        report = sch.report_completion(plan["id"], m1["id"], True)
        assert report["reported"] is True and report["available"] == 1
        second = sch.claim_next(plan["id"])
        assert second["accepted"] is True and second["mission_id"] == m2["id"]


class TestClaimReceiptReconciliation:
    def test_attempt_visible_on_both_sides(self, plan_with_two_pending):
        """受理回执与采集任务活动尝试一一对应，成功后待派行才移出。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        cm = SERVICES["crawl_mission"]

        result = sch.claim_next(plan["id"], client_id="worker-A")
        attempt_id = result["attempt_id"]

        snapshot = sch.queue_snapshot(plan["id"])
        assert [item["mission_id"] for item in snapshot["queue"]] == [m2["id"]]
        assert snapshot["active_attempts"][0]["attempt_id"] == attempt_id

        attempts = cm.list_attempts(m1["id"], plan_id=plan["id"])
        assert len(attempts) == 1
        assert attempts[0]["attempt_id"] == attempt_id
        assert attempts[0]["status"] == "active"
        assert attempts[0]["client_id"] == "worker-A"
        # 采集任务详情可反查在途尝试
        active = cm.find_active_attempt_for_plan(m1["id"], plan["id"])
        assert active is not None and active["attempt_id"] == attempt_id

    def test_report_syncs_both_sides_and_frees_capacity(self, plan_with_two_pending):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        cm = SERVICES["crawl_mission"]

        claimed = sch.claim_next(plan["id"])
        attempt_id = claimed["attempt_id"]
        report = sch.report_completion(plan["id"], m1["id"], True)

        assert report["reported"] is True
        assert report["attempt_id"] == attempt_id
        assert report["available"] == 1  # 两侧回报后容量立即可再领

        assert cm.get_entry(m1["id"])["status"] == "已完成"
        assert cm.list_attempts(m1["id"])[0]["status"] == "succeeded"
        history = [h for h in sch.queue_snapshot(plan["id"])["history"]
                   if h["attempt_id"] == attempt_id][0]
        assert history["status"] == "succeeded"

        again = sch.claim_next(plan["id"])
        assert again["accepted"] is True and again["mission_id"] == m2["id"]

    def test_failed_report_requeues_without_losing_slot(self, plan_with_two_pending):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        cm = SERVICES["crawl_mission"]

        claimed = sch.claim_next(plan["id"])
        report = sch.report_completion(plan["id"], m1["id"], False)
        assert report["reported"] is True
        assert report["requeued"] is True
        assert report["available"] == 1  # 槽位已释放
        # 失败任务按低优先级回到队尾，另一条记录排到它前面
        snapshot = sch.queue_snapshot(plan["id"])
        assert [item["mission_id"] for item in snapshot["queue"]] == [m2["id"], m1["id"]]
        assert snapshot["queue"][-1]["priority"] == 0
        attempt = cm.list_attempts(m1["id"])[0]
        assert attempt["status"] == "failed"

    def test_report_without_active_attempt_does_not_orphan_slot(self, plan_with_two_pending):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        report = sch.report_completion(plan["id"], m1["id"], True)
        assert report["reported"] is False
        assert report["reason_code"] == "active_attempt_not_found"
        assert sch.available_capacity(plan["id"])["active"] == 0


class TestClaimWriteFailureRollback:
    def test_sync_failure_rolls_back_slot_and_keeps_head(self, plan_with_two_pending, monkeypatch):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        cm = SERVICES["crawl_mission"]
        original = cm.record_dispatch_attempt

        def boom(*args, **kwargs):
            raise RuntimeError("模拟活动尝试写入失败")

        monkeypatch.setattr(cm, "record_dispatch_attempt", boom)
        deny = sch.claim_next(plan["id"])
        monkeypatch.setattr(cm, "record_dispatch_attempt", original)

        assert deny["accepted"] is False
        assert deny["retryable"] is True
        assert deny["reason_code"] == "attempt_sync_failed"
        assert "回滚占槽" in deny["message"]

        snapshot = sch.queue_snapshot(plan["id"])
        assert snapshot["active"] == 0
        assert snapshot["total_dispatched"] == 0
        assert [item["mission_id"] for item in snapshot["queue"]] == [m1["id"], m2["id"]]
        assert cm.list_attempts(m1["id"]) == []  # 没有遗留无人可完成的活动名额

        # 写入恢复后同一条队头可以正常受理
        retry = sch.claim_next(plan["id"])
        assert retry["accepted"] is True and retry["mission_id"] == m1["id"]
        assert cm.list_attempts(m1["id"])[0]["status"] == "active"


class TestConcurrentClaim:
    def test_two_workers_only_one_enters_execution(self, plan_with_two_pending):
        """两个执行端并发领取同一计划：只允许一条待办进入执行，另一条满载可重试。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        results: list[dict] = []
        barrier = threading.Barrier(2)

        def worker(client_id: str) -> None:
            barrier.wait()
            results.append(sch.claim_next(plan["id"], client_id=client_id))

        t1 = threading.Thread(target=worker, args=("worker-1",))
        t2 = threading.Thread(target=worker, args=("worker-2",))
        t1.start(); t2.start(); t1.join(); t2.join()

        accepted = [r for r in results if r["accepted"]]
        denied = [r for r in results if not r["accepted"]]
        assert len(accepted) == 1
        assert len(denied) == 1
        assert denied[0]["reason_code"] == "concurrency_full"
        assert denied[0]["retryable"] is True

        snapshot = sch.queue_snapshot(plan["id"])
        assert snapshot["active"] == 1
        assert snapshot["total_dispatched"] == 1
        assert len(snapshot["queue"]) == 1
        # 被拒的一方在容量释放后仍可再次领取
        sch.report_completion(plan["id"], accepted[0]["mission_id"], True)
        retry = sch.claim_next(plan["id"], client_id="worker-2")
        assert retry["accepted"] is True
        assert retry["mission_id"] == m2["id"]


class TestResumeAfterInterruption:
    def test_resume_continues_same_attempt_before_report(self, plan_with_two_pending):
        """计划已选中任务、采集任务尚未回报：重试方续上原尝试而非重新领取。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        cm = SERVICES["crawl_mission"]

        claimed = sch.claim_next(plan["id"], client_id="worker-A")
        original_attempt = claimed["attempt_id"]

        resumed = sch.resume_claim(plan["id"], client_id="worker-B")
        assert resumed["accepted"] is True
        assert resumed["qualification"] == "continue"
        assert resumed["resumed"] is True
        assert resumed["attempt_id"] == original_attempt
        assert resumed["mission_id"] == m1["id"]

        snapshot = sch.queue_snapshot(plan["id"])
        assert snapshot["active"] == 1
        assert snapshot["total_dispatched"] == 1  # 不重复占槽、不重复计数
        assert len(snapshot["active_attempts"]) == 1
        # 采集任务侧仍是同一条活动尝试
        attempts = cm.list_attempts(m1["id"])
        assert len(attempts) == 1 and attempts[0]["attempt_id"] == original_attempt
        assert attempts[0]["acknowledged_at"] is not None

    def test_resume_after_report_becomes_new_claim(self, plan_with_two_pending):
        """回报完成后无在途尝试：重试方获得重新领取资格，受理的是下一条待办。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]

        claimed = sch.claim_next(plan["id"])
        sch.report_completion(plan["id"], m1["id"], True)

        resumed = sch.resume_claim(plan["id"], client_id="worker-A")
        assert resumed["accepted"] is True
        assert resumed["qualification"] == "new_claim"
        assert resumed["resumed"] is False
        assert resumed["attempt_id"] != claimed["attempt_id"]
        assert resumed["mission_id"] == m2["id"]

    def test_resume_falls_back_to_new_claim_when_no_active(self, plan_with_two_pending):
        """无在途尝试时 resume 走标准领取路径，成功时具备重新领取资格。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        sch.claim_next(plan["id"])
        sch.report_completion(plan["id"], m1["id"], True)
        result = sch.resume_claim(plan["id"])
        assert result["accepted"] is True  # 容量已释放 → 重新领取成功
        assert result["qualification"] == "new_claim"

    def test_resume_without_active_and_empty_queue_is_denied(self):
        """无在途尝试且队列已空：resume 给可重试的拒绝原因，不伪造受理。"""
        sch = SERVICES["scheduler"]
        plan, _ = sch.create_entry({
            "计划编号": "CLAIM-RESUME-EMPTY", "Cron表达式": "0 * * * *",
            "关联任务组": "g", "并发上限": "1",
        })
        sch.run_action(plan["id"], "启动计划")
        result = sch.resume_claim(plan["id"], client_id="worker-Z")
        assert result["accepted"] is False
        assert result["qualification"] == "not_qualified"
        assert result["reason_code"] == "queue_empty"
        assert result["retryable"] is True
        assert sch.available_capacity(plan["id"])["active"] == 0


class TestQueuePanelAndEnqueue:
    def test_queue_snapshot_shape_and_deny_cleared_after_success(self, plan_with_two_pending):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        sch.claim_next(plan["id"])
        deny = sch.claim_next(plan["id"])
        assert deny["accepted"] is False

        sch.report_completion(plan["id"], m1["id"], True)
        ok = sch.claim_next(plan["id"])
        assert ok["accepted"] is True
        snapshot = sch.queue_snapshot(plan["id"])
        assert snapshot["last_deny"] is None  # 成功受理后清空拒绝原因
        assert snapshot["plan_code"] == "CLAIM-PLAN"
        assert set(["plan_id", "limit", "active", "available", "queue",
                    "active_attempts", "history", "queue_depth"]).issubset(snapshot)

    def test_duplicate_enqueue_does_not_double_pending(self, plan_with_two_pending):
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        sch.enqueue_mission(plan["id"], m2["id"], priority=9)
        snapshot = sch.queue_snapshot(plan["id"])
        assert [item["mission_id"] for item in snapshot["queue"]] == [m1["id"], m2["id"]]

    def test_priority_order_preserved_after_deny(self, plan_with_two_pending):
        """满载拒绝不改变选择位置：仍是原高优先级队头在下一次被领取。"""
        plan, m1, m2 = plan_with_two_pending
        sch = SERVICES["scheduler"]
        sch.claim_next(plan["id"])
        sch.claim_next(plan["id"])  # 满载拒绝
        sch.report_completion(plan["id"], m1["id"], True)
        picked = sch.claim_next(plan["id"])
        assert picked["mission_id"] == m2["id"]
        assert picked["priority"] == 3


class TestRouteRegistration:
    def test_claim_routes_registered_before_resource_router(self):
        """面板与领取接口必须可路由，不被 /api/{module} 动态路径吞掉。"""
        from app.main import app
        paths = set(app.openapi()["paths"])
        assert "/api/scheduler/{plan_id}/claim" in paths
        assert "/api/scheduler/{plan_id}/enqueue" in paths
        assert "/api/scheduler/queues" in paths
        assert "/api/crawl_mission/{mission_id}/attempts" in paths
