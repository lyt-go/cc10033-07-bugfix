"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

专用领域路由（调度领取、活动尝试）需要在通用资源路由之前注册，
否则固定路径会被 ``/api/{module}`` 动态路径抢先匹配。
"""
from __future__ import annotations

from app.routers import crawl_mission, resource, scheduler

ROUTERS = [scheduler, crawl_mission, resource]
