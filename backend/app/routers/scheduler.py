"""调度计划专用接口：手工领取队列面板的队列视图、领取、中断恢复与完成回报。

通用资源路由（列表/登记/动作）仍走 resource.py；这里只承载领取类领域动作，
路由必须在资源路由之前注册，避免被 ``/api/{module}`` 动态路径抢先匹配。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services import service_for
from app.services.scheduler import SchedulerService

router = APIRouter(prefix="/api/scheduler", tags=["调度领取"])


class ClaimPayload(BaseModel):
    client_id: str | None = Field(default=None, description="执行端标识，用于中断恢复区分尝试")


class EnqueuePayload(BaseModel):
    mission_id: int
    priority: int = 1


class ReportPayload(BaseModel):
    mission_id: int
    success: bool
    client_id: str | None = None


def _scheduler_service() -> SchedulerService:
    service = service_for("scheduler")
    assert isinstance(service, SchedulerService)
    return service


@router.get("/queues")
def queue_panel() -> dict[str, Any]:
    """全部调度计划的待派队列与在途尝试，供手工领取面板使用。"""
    service = _scheduler_service()
    return {"items": service.queue_snapshots()}


@router.get("/{plan_id}/queue")
def plan_queue(plan_id: int) -> dict[str, Any]:
    service = _scheduler_service()
    snapshot = service.queue_snapshot(plan_id)
    if snapshot is None:
        return {"ok": False, "message": f"调度计划 {plan_id} 不存在或已归档", "items": []}
    return {"ok": True, "message": "", **snapshot}


@router.post("/{plan_id}/enqueue")
def enqueue(plan_id: int, payload: EnqueuePayload) -> dict[str, Any]:
    """把采集任务挂到计划的待派队列，供手工领取面板排产。"""
    service = _scheduler_service()
    entry = service.enqueue_mission(plan_id, payload.mission_id, priority=payload.priority)
    if entry is None:
        return {
            "ok": False,
            "message": f"调度计划 {plan_id} 不存在或未处于可领取状态（待验证/运行中）",
        }
    snapshot = service.queue_snapshot(plan_id)
    return {"ok": True, "message": "已加入待派队列", **snapshot}


@router.get("/{plan_id}/capacity")
def plan_capacity(plan_id: int) -> dict[str, Any]:
    service = _scheduler_service()
    return service.available_capacity(plan_id)


@router.post("/{plan_id}/claim")
def claim(plan_id: int, payload: ClaimPayload) -> dict[str, Any]:
    """手工领取队头：满载时保留队头并返回可重试原因；受理成功才移出待派行。"""
    service = _scheduler_service()
    result = service.claim_next(plan_id, client_id=payload.client_id)
    if result.get("accepted"):
        result.setdefault("message", f"领取成功，受理回执 {result.get('attempt_id')}")
    return result


@router.post("/{plan_id}/resume")
def resume(plan_id: int, payload: ClaimPayload) -> dict[str, Any]:
    """中断恢复：有在途尝试时继续原尝试，否则按重新领取资格处理。"""
    service = _scheduler_service()
    return service.resume_claim(plan_id, client_id=payload.client_id)


@router.post("/{plan_id}/report")
def report(plan_id: int, payload: ReportPayload) -> dict[str, Any]:
    """完成回报：两侧同步落账并释放容量，失败任务按失败策略决定是否重回队列。"""
    service = _scheduler_service()
    return service.report_completion(
        plan_id, payload.mission_id, payload.success, client_id=payload.client_id,
    )
