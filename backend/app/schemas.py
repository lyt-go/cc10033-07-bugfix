"""通用接口契约；模块字段由领域服务持有，不再为每个模块复制一份模型类。"""
from __future__ import annotations

from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")

# 仅服务于进程内并发控制的运行时对象，不能出现在任何 JSON 响应里；
# 其余下划线前缀字段（队列、尝试等）是可 JSON 化的领域数据，仍然透出。
_NON_SERIALIZABLE_KEYS = frozenset({"_lock"})


def safe_entry(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    """剔除不可序列化的运行时字段，供资源路由统一出口使用。"""
    if entry is None:
        return None
    return {key: value for key, value in entry.items() if key not in _NON_SERIALIZABLE_KEYS}


class PageResult(BaseModel, Generic[T]):
    items: list[T]
    total: int
    page: int = 1
    size: int = 20


class ActionResult(BaseModel):
    ok: bool
    message: str
    entry: dict[str, Any] | None = None


class EntryPayload(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)
    remark: str | None = None


class ActionPayload(BaseModel):
    action: str
    remark: str | None = None
