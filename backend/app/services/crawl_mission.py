"""采集任务领域服务：任务定义、优先级调度与执行编排。

跨模块数据流：
- crawl_mission → scheduler: 任务提交到调度计划
- crawl_mission → page_render: 触发页面渲染
- crawl_mission → robot_verify: 任务执行中触发反封锁检测
- crawl_mission ← scheduler: 调度计划受理领取时登记/回报活动尝试（attempt）

活动尝试（``_attempts``）与调度计划的受理回执一一对应：
- 同一次调度受理只登记一条 active 尝试（按 attempt_id 幂等）；
- 调度回报到达后尝试落为 succeeded/failed，任务状态按尝试结果同步；
- 调度侧登记失败的写入会抛错，由调度服务回滚占槽，避免遗留无人能完成的活动名额。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CrawlMissionService(BaseModuleService):
    MODULE = "crawl_mission"
    ENTITY = "采集任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "目标URL", "优先级", "重试上限", "超时秒数", "渲染策略", "所属项目", "创建时间"]
    REQUIRED_FIELDS = ["任务编号", "目标URL", "优先级"]
    STATUS_ORDER = ["待调度", "执行中", "已完成", "已达上限"]
    ACTION_RULES = {"发起调度": "执行中", "标记完成": "已完成", "标记上限": "已达上限"}

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing
        priority = int(values.get("优先级", 1))
        entry["_priority"] = priority
        entry["_created_ts"] = time.time()
        entry["_attempts"] = []
        # 自动创建关联的调度事件
        store.enqueue_event("mission_created", {
            "mission_id": entry["id"],
            "target_url": values.get("目标URL", ""),
            "priority": priority,
        })
        store.record_metric("crawl_mission", "mission_created", 1,
                            {"priority": str(priority)})
        return entry, []

    def dispatch(self, entry_id: int, scheduler_plan_id: int | None = None) -> dict[str, Any] | None:
        """将采集任务提交到调度计划执行。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        if entry.get("status") != "待调度":
            return entry
        entry["status"] = "执行中"
        entry["pending"] = True
        entry["_dispatch_ts"] = time.time()
        if scheduler_plan_id:
            store.pipeline_trace(
                "crawl_mission", "scheduler", entry_id, scheduler_plan_id,
                "dispatch", {"url": entry.get("目标URL", "")}
            )
        store.record_metric("crawl_mission", "mission_dispatched", 1,
                            {"url": entry.get("目标URL", ""), "priority": str(entry.get("_priority", 1))})
        store.enqueue_event("mission_dispatched", {
            "mission_id": entry_id,
            "scheduler_plan_id": scheduler_plan_id,
            "ts": time.time(),
        })
        return entry

    def record_dispatch_attempt(
        self,
        entry_id: int,
        plan_id: int,
        attempt_id: str,
        *,
        priority: int = 1,
        client_id: str | None = None,
    ) -> dict[str, Any] | None:
        """登记一次调度计划受理的活动尝试。跨模块：scheduler → crawl_mission。

        按 attempt_id 幂等：同一受理回执重复登记不会产生第二条活动尝试。
        任务不存在返回 None；写入异常向上抛出，由调度侧回滚占槽。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        entry.setdefault("_attempts", [])
        for attempt in entry["_attempts"]:
            if attempt["attempt_id"] == attempt_id:
                if client_id and not attempt.get("client_id"):
                    attempt["client_id"] = client_id
                return dict(attempt)
        attempt = {
            "attempt_id": attempt_id,
            "plan_id": plan_id,
            "mission_id": entry_id,
            "priority": priority,
            "status": "active",
            "client_id": client_id,
            "dispatched_at": time.time(),
            "completed_at": None,
            "acknowledged_at": None,
        }
        entry["_attempts"].append(attempt)
        entry["status"] = "执行中"
        entry["pending"] = True
        entry.setdefault("_dispatch_ts", time.time())
        store.pipeline_trace(
            "scheduler", "crawl_mission", plan_id, entry_id,
            "attempt_registered", {"attempt_id": attempt_id, "client_id": client_id or ""},
        )
        store.record_metric("crawl_mission", "dispatch_attempt_registered", 1,
                            {"attempt_id": attempt_id, "plan_id": str(plan_id)})
        store.enqueue_event("dispatch_attempt_registered", {
            "mission_id": entry_id,
            "plan_id": plan_id,
            "attempt_id": attempt_id,
            "ts": attempt["dispatched_at"],
        })
        return dict(attempt)

    def mark_attempt_acknowledged(
        self, entry_id: int, attempt_id: str, *, client_id: str | None = None,
    ) -> dict[str, Any] | None:
        """执行端确认接管某条在途尝试（中断恢复继续原尝试时调用），不新增尝试。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        for attempt in entry.get("_attempts", []):
            if attempt["attempt_id"] == attempt_id:
                attempt["acknowledged_at"] = time.time()
                if client_id:
                    attempt["client_id"] = client_id
                return dict(attempt)
        return None

    def record_attempt_result(self, entry_id: int, attempt_id: str, success: bool) -> dict[str, Any] | None:
        """回报活动尝试结果，与调度计划受理回执核对落账。跨模块：scheduler → crawl_mission。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        target = next(
            (attempt for attempt in entry.get("_attempts", [])
             if attempt["attempt_id"] == attempt_id),
            None,
        )
        if target is None:
            return None
        target["status"] = "succeeded" if success else "failed"
        target["completed_at"] = time.time()
        # 没有其他在途尝试时，任务状态与最后一次回报同步
        still_active = any(
            attempt["status"] == "active" for attempt in entry["_attempts"]
        )
        if not still_active:
            entry["status"] = "已完成" if success else "执行中"
            entry["pending"] = not success
        if not success:
            entry["_retry_count"] = entry.get("_retry_count", 0) + 1
            entry["abnormal"] = True
        store.pipeline_trace(
            "scheduler", "crawl_mission", target.get("plan_id", 0), entry_id,
            "attempt_result", {"attempt_id": attempt_id, "success": str(success)},
        )
        store.record_metric("crawl_mission", "dispatch_attempt_result", 1,
                            {"attempt_id": attempt_id, "success": str(success)})
        store.enqueue_event("dispatch_attempt_result", {
            "mission_id": entry_id,
            "attempt_id": attempt_id,
            "success": success,
            "ts": target["completed_at"],
        })
        return dict(target)

    def list_attempts(self, entry_id: int, plan_id: int | None = None) -> list[dict[str, Any]] | None:
        """查看采集任务的活动尝试，可按调度计划过滤，用于与调度计划受理结果核对。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        attempts = list(entry.get("_attempts", []))
        if plan_id is not None:
            attempts = [attempt for attempt in attempts if attempt.get("plan_id") == plan_id]
        return attempts

    def find_active_attempt_for_plan(self, entry_id: int, plan_id: int) -> dict[str, Any] | None:
        """查找任务在指定计划下尚未回报的活动尝试，供中断恢复判断资格。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        return next(
            (dict(attempt) for attempt in entry.get("_attempts", [])
             if attempt.get("plan_id") == plan_id and attempt.get("status") == "active"),
            None,
        )

    def retry_mission(self, entry_id: int) -> dict[str, Any] | None:
        """重试已达到上限的任务，重置计数器。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        retry_limit = int(entry.get("重试上限", 3))
        retry_count = entry.get("_retry_count", 0)
        if retry_count >= retry_limit:
            entry["status"] = "已达上限"
            entry["abnormal"] = True
            store.record_metric("crawl_mission", "retry_limit_reached", 1,
                                {"mission_id": str(entry_id)})
            return entry
        entry["_retry_count"] = retry_count + 1
        entry["status"] = "执行中"
        entry["pending"] = True
        store.record_metric("crawl_mission", "mission_retried", 1,
                            {"attempt": str(entry["_retry_count"])})
        return entry

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        missions = store.rows(self.MODULE)
        base["avg_retry"] = (
            sum(m.get("_retry_count", 0) for m in missions) / max(len(missions), 1)
        )
        base["total_dispatched"] = sum(
            1 for m in missions if m.get("_dispatch_ts")
        )
        base["active_attempts"] = sum(
            1
            for mission in missions
            for attempt in mission.get("_attempts", [])
            if attempt.get("status") == "active"
        )
        base["total_attempts"] = sum(
            len(mission.get("_attempts", [])) for mission in missions
        )
        return base
