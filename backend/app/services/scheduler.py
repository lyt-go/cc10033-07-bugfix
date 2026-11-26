"""调度计划领域服务：采集任务定时调度、并发控制与失败处理。

跨模块数据流：
- scheduler ← crawl_mission: 任务提交到调度计划
- scheduler ← seed_url: URL种子关联调度计划
- scheduler → scalable_pool: 负载预测通知弹性伸缩

领取（占槽）语义约定：
- 领取时先检查并发上限，再占槽；满载时不弹出队头，待派队列的顺序与选择位置保持不变。
- 只有真正拿到服务端领取回执（attempt_id）并同步到采集任务的活动尝试后，才把待派行移出队列。
- 满载返回带 ``retryable=True`` 的明确可重试原因；旧调用方使用的 ``dispatch_next``
  在满载/空队列时仍按原约定返回 ``None``。
- 成功受理才计入 ``_total_dispatched``；向采集任务写入活动尝试失败时回滚占槽，
  不遗留无人能完成的活动名额。
- 同一计划的在途尝试可通过 ``resume_claim`` 续上原尝试；无在途尝试时才具备重新领取资格。
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "调度计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划编号", "Cron表达式", "关联任务组", "并发上限", "优先级策略", "失败策略", "下次执行", "启用状态"]
    REQUIRED_FIELDS = ["计划编号", "Cron表达式", "关联任务组"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已暂停"]
    ACTION_RULES = {"提交验证": "待验证", "启动计划": "运行中", "暂停计划": "已暂停"}

    STRATEGIES = ["round_robin", "priority_first", "earliest_deadline", "shortest_job", "resource_aware"]

    def create_entry(self, values: dict[str, Any]) -> tuple:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        self._init_state(entry)
        store.record_metric("scheduler", "plan_created", 1,
                            {"cron": values.get("Cron表达式", "")})
        return entry, []

    def _init_state(self, entry: dict[str, Any]) -> dict[str, Any]:
        """初始化（或为历史种子行惰性补齐）领取相关状态。"""
        entry.setdefault("_task_queue", [])
        entry.setdefault("_execution_history", [])
        entry.setdefault("_total_dispatched", 0)
        entry.setdefault("_failed_dispatch", 0)
        entry.setdefault("_active_tasks", 0)
        entry.setdefault("_queue_seq", 0)
        entry.setdefault("_last_deny", None)
        if "_lock" not in entry:
            entry["_lock"] = threading.RLock()
        return entry

    # ── 待派队列 ──────────────────────────────

    def enqueue_mission(self, entry_id: int, mission_id: int, priority: int = 1) -> dict[str, Any] | None:
        """将采集任务加入调度队列。跨模块：crawl_mission → scheduler"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") not in ("运行中", "待验证"):
            return None
        self._init_state(entry)
        with entry["_lock"]:
            queue = entry["_task_queue"]
            # 同一任务已在待派队列时不重复登记，避免一条待办占两个位置
            if any(item["mission_id"] == mission_id for item in queue):
                return entry
            entry["_queue_seq"] += 1
            queue.append({
                "mission_id": mission_id,
                "priority": priority,
                "enqueued_at": time.time(),
                "seq": entry["_queue_seq"],
            })
            # 优先级降序，同优先级按入队序号先进先出，满载拒绝后顺序不会被打乱
            queue.sort(key=lambda item: (-item["priority"], item["seq"]))
        store.pipeline_trace(
            "crawl_mission", "scheduler", mission_id, entry_id,
            "enqueue", {"priority": priority}
        )
        store.record_metric("scheduler", "mission_enqueued", 1,
                            {"priority": str(priority)})
        return entry

    # ── 领取（占槽） ──────────────────────────

    def claim_next(self, entry_id: int, *, client_id: str | None = None) -> dict[str, Any]:
        """手工/执行端领取下一条待派任务。

        返回统一结果字典：``accepted=True`` 时携带服务端受理回执（attempt_id）；
        ``accepted=False`` 时携带 ``reason_code``、``retryable`` 与可读原因，
        且队列、计数、占槽均不发生变化。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._deny("plan_not_found", f"调度计划 {entry_id} 不存在或已归档", retryable=False)
        self._init_state(entry)
        with entry["_lock"]:
            queue = entry["_task_queue"]
            if not queue:
                deny = self._deny(
                    "queue_empty", "待派队列已空，没有可领取的任务", retryable=True,
                    active=entry.get("_active_tasks", 0),
                    limit=self._parse_int(entry.get("并发上限", "10")),
                )
                entry["_last_deny"] = deny
                return deny

            max_concurrent = self._parse_int(entry.get("并发上限", "10"))
            active = entry.get("_active_tasks", 0)
            head = queue[0]
            if active >= max_concurrent:
                # 先检查上限再占槽：队头原样保留，选择位置不变，不计成功派发
                deny = self._deny(
                    "concurrency_full",
                    f"并发上限 {max_concurrent} 已满（在途 {active}），"
                    f"队头任务 {head['mission_id']} 保留在待派位置，等待回报释放容量后可重试领取",
                    retryable=True,
                    active=active,
                    limit=max_concurrent,
                    head=self._queue_item_view(head, 0),
                )
                entry["_last_deny"] = deny
                store.record_metric("scheduler", "dispatch_blocked_concurrency", 1,
                                    {"active": str(active), "limit": str(max_concurrent)})
                return deny

            # 有容量：先向采集任务登记活动尝试（服务端受理），成功后才占槽、弹队列
            attempt_id = f"ATT-{uuid.uuid4().hex[:12]}"
            mission_service = self._mission_service()
            try:
                attempt = mission_service.record_dispatch_attempt(
                    head["mission_id"], entry_id, attempt_id,
                    priority=head["priority"], client_id=client_id,
                ) if mission_service is not None else None
            except Exception as exc:  # 写入失败：不占槽、不弹队列，名额可再次领取
                entry["_failed_dispatch"] += 1
                deny = self._deny(
                    "attempt_sync_failed",
                    f"采集任务活动尝试登记失败，已回滚占槽，队头任务 {head['mission_id']} 可重新领取：{exc}",
                    retryable=True,
                    active=active,
                    limit=max_concurrent,
                    head=self._queue_item_view(head, 0),
                )
                entry["_last_deny"] = deny
                store.record_metric("scheduler", "dispatch_attempt_sync_failed", 1,
                                    {"mission_id": str(head["mission_id"])})
                return deny
            if attempt is None:
                entry["_failed_dispatch"] += 1
                deny = self._deny(
                    "mission_not_found",
                    f"队头采集任务 {head['mission_id']} 不存在，无法登记活动尝试，待派行已保留待人工处理",
                    retryable=False,
                    active=active,
                    limit=max_concurrent,
                    head=self._queue_item_view(head, 0),
                )
                entry["_last_deny"] = deny
                return deny

            # 真正拿到受理回执后才移出待派行
            popped = queue.pop(0)
            receipt = {
                "attempt_id": attempt_id,
                "mission_id": popped["mission_id"],
                "plan_id": entry_id,
                "priority": popped["priority"],
                "enqueued_at": popped["enqueued_at"],
                "dispatched_at": time.time(),
                "status": "active",
                "completed_at": None,
                "client_id": client_id,
            }
            entry["_execution_history"].append(receipt)
            entry["_active_tasks"] = active + 1
            entry["_total_dispatched"] += 1
            entry["下次执行"] = time.strftime("%Y-%m-%d %H:%M:%S")
            entry["_last_deny"] = None
            store.record_metric("scheduler", "mission_dispatched", 1,
                                {"mission_id": str(popped["mission_id"]),
                                 "attempt_id": attempt_id})
            return {
                "accepted": True,
                "qualification": "new_claim",
                "resumed": False,
                **receipt,
                "active": entry["_active_tasks"],
                "limit": max_concurrent,
                "available": max(0, max_concurrent - entry["_active_tasks"]),
            }

    def resume_claim(self, entry_id: int, *, client_id: str | None = None) -> dict[str, Any]:
        """中断恢复：分清「继续原尝试」与「重新领取」的资格。

        - 计划已选中任务（存在 active 活动尝试）且采集任务尚未回报结果：续上原尝试，
          不新增占槽、不重复计数，返回 ``qualification='continue'``。
        - 没有在途尝试：具备重新领取资格，转入标准 ``claim_next`` 流程，
          返回 ``qualification='new_claim'`` 或满载拒绝原因。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return self._deny("plan_not_found", f"调度计划 {entry_id} 不存在或已归档", retryable=False)
        self._init_state(entry)
        with entry["_lock"]:
            active_receipts = [h for h in entry["_execution_history"] if h.get("status") == "active"]
            if active_receipts:
                receipt = self._match_active_receipt(active_receipts, client_id)
                mission_service = self._mission_service()
                if mission_service is not None:
                    mission_service.mark_attempt_acknowledged(
                        receipt["mission_id"], receipt["attempt_id"], client_id=client_id,
                    )
                return {
                    "accepted": True,
                    "qualification": "continue",
                    "resumed": True,
                    **self._receipt_view(receipt),
                    "active": entry.get("_active_tasks", 0),
                    "limit": self._parse_int(entry.get("并发上限", "10")),
                    "available": self.available_capacity(entry_id)["available"],
                    "message": f"存在在途尝试 {receipt['attempt_id']}，继续原尝试，不重复占槽",
                }
        # 无在途尝试：重试方获得重新领取资格
        result = self.claim_next(entry_id, client_id=client_id)
        if result.get("accepted"):
            result["qualification"] = "new_claim"
            result["message"] = "无在途尝试，已按重新领取受理"
        else:
            result["qualification"] = "not_qualified"
        return result

    def dispatch_next(self, entry_id: int) -> dict[str, Any] | None:
        """按优先调度下一个任务（旧调用方兼容入口）。

        兼容约定：满载、空队列或受理失败时仍返回 ``None``；成功时返回与历史结构
        兼容的任务字典（额外携带 attempt_id 便于与采集任务活动尝试核对）。
        """
        result = self.claim_next(entry_id)
        if not result.get("accepted"):
            return None
        return {
            "mission_id": result["mission_id"],
            "priority": result["priority"],
            "enqueued_at": result["enqueued_at"],
            "attempt_id": result["attempt_id"],
            "plan_id": result["plan_id"],
            "dispatched_at": result["dispatched_at"],
        }

    # ── 完成回报 ──────────────────────────────

    def report_completion(
        self, entry_id: int, mission_id: int, success: bool, *, client_id: str | None = None,
    ) -> dict[str, Any]:
        """任务完成回执：释放占槽并同步采集任务侧活动尝试。

        回报到达后调度侧与采集任务侧同时落账，容量立即可用于再次领取；
        失败且失败策略为 retry 时把任务按低优先级重新放回待派队列。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"reported": False, "reason_code": "plan_not_found",
                    "message": f"调度计划 {entry_id} 不存在或已归档", "retryable": False}
        self._init_state(entry)
        with entry["_lock"]:
            receipt = next(
                (h for h in entry["_execution_history"]
                 if h.get("mission_id") == mission_id and h.get("status") == "active"),
                None,
            )
            if receipt is None:
                # 兼容旧调用：保守回收一次占槽计数，但不伪造活动尝试
                entry["_active_tasks"] = max(0, entry.get("_active_tasks", 0) - 1)
                return {
                    "reported": False,
                    "reason_code": "active_attempt_not_found",
                    "message": f"任务 {mission_id} 没有在途活动尝试，无可回报的受理记录",
                    "retryable": False,
                }

            receipt["status"] = "succeeded" if success else "failed"
            receipt["completed_at"] = time.time()
            receipt["reported_by"] = client_id
            entry["_active_tasks"] = max(0, entry.get("_active_tasks", 0) - 1)

            mission_service = self._mission_service()
            attempt_view = None
            if mission_service is not None:
                attempt_view = mission_service.record_attempt_result(
                    mission_id, receipt["attempt_id"], success,
                )

            requeued = False
            if not success:
                entry["_failed_dispatch"] += 1
                if entry.get("失败策略", "retry") == "retry":
                    self.enqueue_mission(entry_id, mission_id, priority=0)
                    requeued = True
                    store.record_metric("scheduler", "mission_requeued_for_retry", 1,
                                        {"mission_id": str(mission_id)})

            store.pipeline_trace(
                "scheduler", "crawl_mission", entry_id, mission_id,
                "report_completion",
                {"attempt_id": receipt["attempt_id"], "success": str(success), "requeued": str(requeued)},
            )
            store.record_metric("scheduler", "mission_completion_reported", 1,
                                {"mission_id": str(mission_id), "success": str(success)})
            return {
                "reported": True,
                "attempt_id": receipt["attempt_id"],
                "mission_id": mission_id,
                "status": receipt["status"],
                "requeued": requeued,
                "attempt": attempt_view,
                **self.available_capacity(entry_id),
            }

    # ── 队列/容量视图（供手工领取面板使用） ──────

    def available_capacity(self, entry_id: int) -> dict[str, Any]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"plan_id": entry_id, "limit": 0, "active": 0, "available": 0, "queue_depth": 0}
        self._init_state(entry)
        with entry["_lock"]:
            limit = self._parse_int(entry.get("并发上限", "10"))
            active = entry.get("_active_tasks", 0)
            return {
                "plan_id": entry_id,
                "limit": limit,
                "active": active,
                "available": max(0, limit - active),
                "queue_depth": len(entry["_task_queue"]),
            }

    def queue_snapshot(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        self._init_state(entry)
        with entry["_lock"]:
            limit = self._parse_int(entry.get("并发上限", "10"))
            active = entry.get("_active_tasks", 0)
            return {
                "plan_id": entry_id,
                "plan_code": entry.get("计划编号", ""),
                "status": entry.get("status", ""),
                "limit": limit,
                "active": active,
                "available": max(0, limit - active),
                "queue_depth": len(entry["_task_queue"]),
                "total_dispatched": entry.get("_total_dispatched", 0),
                "failed_dispatch": entry.get("_failed_dispatch", 0),
                "queue": [
                    self._queue_item_view(item, index)
                    for index, item in enumerate(entry["_task_queue"])
                ],
                "active_attempts": [
                    self._receipt_view(receipt)
                    for receipt in entry["_execution_history"]
                    if receipt.get("status") == "active"
                ],
                "history": [self._receipt_view(receipt) for receipt in entry["_execution_history"]],
                "last_deny": entry.get("_last_deny"),
            }

    def queue_snapshots(self) -> list[dict[str, Any]]:
        return [
            snapshot for snapshot in
            (self.queue_snapshot(row["id"]) for row in store.rows(self.MODULE))
            if snapshot is not None
        ]

    def predict_load(self, entry_id: int) -> dict[str, Any]:
        """预测未来负载，供弹性伸缩决策。跨模块：scheduler → scalable_pool"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"predicted_load": 0}
        self._init_state(entry)
        with entry["_lock"]:
            queue_size = len(entry["_task_queue"])
            active = entry.get("_active_tasks", 0)
        hourly_rate = active * 2 + queue_size * 0.5
        prediction = {
            "current_load": active,
            "queue_depth": queue_size,
            "predicted_load_next_hour": hourly_rate,
            "recommend_scale": "up" if hourly_rate > 20 else "stable" if hourly_rate > 5 else "down",
        }
        store.record_metric("scheduler", "load_prediction", hourly_rate,
                            {"recommendation": prediction["recommend_scale"]})
        return prediction

    # ── 内部工具 ──────────────────────────────

    def _mission_service(self):
        from app.services import SERVICES
        return SERVICES.get("crawl_mission")

    def _match_active_receipt(self, receipts: list[dict[str, Any]], client_id: str | None) -> dict[str, Any]:
        if client_id:
            for receipt in receipts:
                if receipt.get("client_id") == client_id:
                    return receipt
        return receipts[0]

    def _queue_item_view(self, item: dict[str, Any], position: int) -> dict[str, Any]:
        return {
            "mission_id": item["mission_id"],
            "priority": item["priority"],
            "enqueued_at": item["enqueued_at"],
            "seq": item.get("seq"),
            "queue_position": position,
        }

    def _receipt_view(self, receipt: dict[str, Any]) -> dict[str, Any]:
        return {
            "attempt_id": receipt.get("attempt_id"),
            "mission_id": receipt.get("mission_id"),
            "plan_id": receipt.get("plan_id"),
            "priority": receipt.get("priority"),
            "status": receipt.get("status"),
            "client_id": receipt.get("client_id"),
            "dispatched_at": receipt.get("dispatched_at"),
            "completed_at": receipt.get("completed_at"),
        }

    def _deny(self, reason_code: str, message: str, *, retryable: bool,
              active: int = 0, limit: int = 0, head: dict[str, Any] | None = None) -> dict[str, Any]:
        return {
            "accepted": False,
            "reason_code": reason_code,
            "message": message,
            "retryable": retryable,
            "active": active,
            "limit": limit,
            "head": head,
        }

    def _parse_int(self, val: Any) -> int:
        try:
            return int(val)
        except (ValueError, TypeError):
            return 10

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        plans = store.rows(self.MODULE)
        base["active_plans"] = sum(1 for p in plans if p.get("status") == "运行中")
        base["total_dispatched"] = sum(p.get("_total_dispatched", 0) for p in plans)
        base["queue_depth"] = sum(len(p.get("_task_queue", [])) for p in plans)
        base["active_tasks"] = sum(p.get("_active_tasks", 0) for p in plans)
        base["available_capacity"] = sum(
            max(0, self._parse_int(p.get("并发上限", "10")) - p.get("_active_tasks", 0))
            for p in plans
        )
        return base
