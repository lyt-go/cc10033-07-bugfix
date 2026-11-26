"""采集任务专用接口：查看与调度计划受理回执一一对应的活动尝试。

活动尝试可跳回调度计划队列面板核对；路由在资源路由之前注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.services import service_for
from app.services.crawl_mission import CrawlMissionService

router = APIRouter(prefix="/api/crawl_mission", tags=["采集任务活动尝试"])


def _mission_service() -> CrawlMissionService:
    service = service_for("crawl_mission")
    assert isinstance(service, CrawlMissionService)
    return service


@router.get("/{mission_id}/attempts")
def list_attempts(mission_id: int, plan_id: int | None = None) -> dict[str, Any]:
    """采集任务详情中的活动尝试列表，可按调度计划过滤核对。"""
    service = _mission_service()
    attempts = service.list_attempts(mission_id, plan_id=plan_id)
    if attempts is None:
        raise HTTPException(status_code=404, detail=f"采集任务 {mission_id} 不存在或已归档")
    entry = service.get_entry(mission_id)
    return {
        "items": attempts,
        "total": len(attempts),
        "mission_id": mission_id,
        "mission_code": (entry or {}).get("任务编号", ""),
        "status": (entry or {}).get("status", ""),
    }


@router.get("/{mission_id}/attempts/active")
def active_attempt(mission_id: int, plan_id: int) -> dict[str, Any]:
    """查询任务在指定计划下的在途尝试，供中断恢复区分继续/重新领取。"""
    service = _mission_service()
    attempt = service.find_active_attempt_for_plan(mission_id, plan_id)
    return {
        "mission_id": mission_id,
        "plan_id": plan_id,
        "has_active": attempt is not None,
        "attempt": attempt,
    }
