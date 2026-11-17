"""运行期配置，全部来自环境变量，便于容器化与演示覆写。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = int(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须为正整数，实际为 {raw!r}")
    return value


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = float(raw)
    if value <= 0:
        raise ValueError(f"{name} 必须为正数，实际为 {raw!r}")
    return value


@dataclass(frozen=True)
class Settings:
    database_url: str
    max_sessions: int
    session_ttl_seconds: int
    page_size_cap: int
    sweeper_interval_seconds: float
    enable_debug_hooks: bool
    pool_min_size: int
    pool_max_size: int
    write_pool_max_size: int
    conn_timeout_seconds: float

    @staticmethod
    def from_env() -> "Settings":
        settings = Settings(
            database_url=os.environ.get(
                "DATABASE_URL",
                "postgresql://actuary:actuary@db:5432/claims",
            ),
            max_sessions=_int_env("MAX_SESSIONS", 2),
            session_ttl_seconds=_int_env("SESSION_TTL_SECONDS", 60),
            page_size_cap=_int_env("PAGE_SIZE_CAP", 100),
            sweeper_interval_seconds=_float_env("SWEEPER_INTERVAL_SECONDS", 0.25),
            enable_debug_hooks=os.environ.get(
                "ENABLE_DEBUG_HOOKS", "false"
            ).lower()
            in ("1", "true", "yes", "on"),
            # 快照会话专用池：连接被会话长期持有，池上限必须 >= 会话名额，
            # 否则第 N+1 个会话会卡在借连接上。
            pool_min_size=_int_env("POOL_MIN_SIZE", 1),
            pool_max_size=_int_env("POOL_MAX_SIZE", 2),
            # 源端写入专用池：与会话池完全隔离，会话占满时写入照常提交。
            write_pool_max_size=_int_env("WRITE_POOL_MAX_SIZE", 2),
            # 借连接等待上限：超时即失败返回 503，绝不无限等待。
            conn_timeout_seconds=_float_env("CONN_TIMEOUT_SECONDS", 10),
        )
        if settings.pool_max_size < settings.max_sessions:
            raise ValueError(
                f"POOL_MAX_SIZE（{settings.pool_max_size}）必须 >= "
                f"MAX_SESSIONS（{settings.max_sessions}）："
                "每条会话独占一条快照连接，池小了会话会借不到连接"
            )
        return settings


settings = Settings.from_env()
