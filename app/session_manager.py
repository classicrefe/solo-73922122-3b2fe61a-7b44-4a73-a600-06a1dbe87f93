"""会话注册表与快照生命周期管理。

设计要点
========
1. 连接分配（本次修复的核心）：**两类连接分池持有**。
   - 快照会话池（``_session_pool``，上限 ``POOL_MAX_SIZE``）：每条会话
     独占一条连接，并在其上开启
     ``START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY``。
     连接以 "idle in transaction" 状态保持，事务快照在整个会话期间不变；
     会话各页（含重复取页）读到的两张表始终是开启那一刻的同一快照（MVCC）。
   - 源端写入池（``_write_pool``，上限 ``WRITE_POOL_MAX_SIZE``）：专供
     admin 增改删，自动提交、单条语句执行即提交、用完立即归还。
   两个池完全隔离：即使两条会话把会话池占满（MAX_SESSIONS =
   POOL_MAX_SIZE = 2），写入请求也能立刻借到写入池连接并提交，
   不再等待会话关闭；写入失败也只影响写入池自己的连接，
   触碰不到任何会话快照。
2. 会话状态机：``open -> closing -> closed/expired``。关闭请求受理的
   第一时间即把状态翻成 ``closing``，此后尚未开始响应的取页一律失败；
   已经在执行中的取页允许跑完，随后回滚事务、归还连接、释放名额。
3. 名额（默认 2）在内存注册表中控制，会话状态全部保存在本进程内；
   进程重启或与数据库的连接丢失后旧会话不可恢复，符合
   "不得换快照续取" 的要求。名额检查只持锁做计数，借连接、
   开事务等 I/O 均在锁外进行，等待中的开启/写入不会阻塞
   状态查询与主动关闭。
"""
from __future__ import annotations

import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Iterator, Optional

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool, PoolTimeout

from .config import Settings


class SessionError(Exception):
    """会话层面的可预期错误，路由层据此映射 HTTP 状态码。"""

    def __init__(self, code: str, message: str, http_status: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status


class CapacityReached(SessionError):
    def __init__(self, max_sessions: int):
        super().__init__(
            "capacity_reached",
            f"会话名额已满（最多 {max_sessions} 个），请关闭其它会话后重试",
            http_status=429,
        )


class SessionNotFound(SessionError):
    def __init__(self, session_id: str):
        super().__init__(
            "session_not_found",
            f"会话 {session_id} 不存在或已失效（服务重启/连接丢失后旧会话不可续取）",
            http_status=404,
        )


class SessionUnavailable(SessionError):
    """会话处于 closing/closed/expired，取页请求被拒绝。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Session:
    session_id: str
    region: str
    conn: psycopg.Connection
    opened_at: datetime
    expires_at: datetime
    # state_lock 保护 status / 计数器 / 时间戳；
    # io_lock 串行化同一会话上的查询，保证取页与关闭不会并发使用连接。
    state_lock: threading.RLock = field(default_factory=threading.RLock)
    io_lock: threading.Lock = field(default_factory=threading.Lock)
    status: str = "open"
    pages_served: int = 0
    last_page_at: Optional[datetime] = None
    in_flight: int = 0
    snapshot_time: Optional[datetime] = None
    snapshot_id: Optional[str] = None

    def is_open(self) -> bool:
        with self.state_lock:
            return self.status == "open" and _utcnow() < self.expires_at


class SessionManager:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._registry: dict[str, Session] = {}
        self._registry_lock = threading.RLock()
        # 正在开启流程中（已预占名额、尚未注册）的会话数，
        # 与 open/closing 会话一起计入名额，防止并发开启超额。
        self._opening = 0
        self._session_pool: Optional[ConnectionPool] = None
        self._write_pool: Optional[ConnectionPool] = None
        self._stop_event = threading.Event()
        self._sweeper: Optional[threading.Thread] = None

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------
    @staticmethod
    def _reset_connection(conn: psycopg.Connection) -> None:
        """连接归还池时：回滚残留事务并恢复自动提交，供下一个借用者干净使用。

        清理失败（如连接已被数据库侧断开）时向外抛出，由连接池丢弃该
        连接而不是回收复用，避免坏连接流到下一个借用者手里。
        """
        if conn.info.transaction_status != psycopg.pq.TransactionStatus.IDLE:
            conn.rollback()
        conn.autocommit = True

    def start(self) -> None:
        # 会话池：自动提交模式下借出后显式 START TRANSACTION，
        # 长期持有快照事务；上限须 >= 会话名额（启动时已校验）。
        if self._session_pool is None or self._session_pool.closed:
            self._session_pool = ConnectionPool(
                self._settings.database_url,
                min_size=self._settings.pool_min_size,
                max_size=self._settings.pool_max_size,
                kwargs={"row_factory": dict_row, "autocommit": True},
                # 连接归还池时恢复自动提交并回滚任何残留事务
                reset=self._reset_connection,
                open=True,
            )
        # 写入池：专供源端增改删，自动提交、用完即还；
        # 与会话池隔离，会话占满时写入照常立即提交。
        if self._write_pool is None or self._write_pool.closed:
            self._write_pool = ConnectionPool(
                self._settings.database_url,
                min_size=0,
                max_size=self._settings.write_pool_max_size,
                kwargs={"row_factory": dict_row, "autocommit": True},
                reset=self._reset_connection,
                open=True,
            )
        # 每次 start 都确保清理线程在运行（stop 后重新 start 时需要重建）
        if self._sweeper is None or not self._sweeper.is_alive():
            self._stop_event = threading.Event()
            self._sweeper = threading.Thread(
                target=self._sweep_loop, name="session-sweeper", daemon=True
            )
            self._sweeper.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._sweeper is not None:
            self._sweeper.join(timeout=5)
        self._sweeper = None
        with self._registry_lock:
            session_ids = list(self._registry.keys())
        for sid in session_ids:
            try:
                self.close(sid, reason="shutdown")
            except SessionError:
                pass
        for pool in (self._session_pool, self._write_pool):
            if pool is not None:
                pool.close()
        self._session_pool = None
        self._write_pool = None
        # 进程重启语义：内存注册表全部丢弃，旧会话 id 不可再查、不可续取
        with self._registry_lock:
            self._registry.clear()

    # ------------------------------------------------------------------
    # 开启会话
    # ------------------------------------------------------------------
    def open_session(self, region: str) -> Session:
        # 1) 名额预占：检查与计数在同一把锁内完成，并发开启不会超额。
        #    锁内只做计数，借连接/开事务等 I/O 全部在锁外进行，
        #    因此开启流程（即使因故等待）不会阻塞状态查询与主动关闭。
        with self._registry_lock:
            active = sum(
                1 for s in self._registry.values()
                if s.status in ("open", "closing")
            )
            if active + self._opening >= self._settings.max_sessions:
                raise CapacityReached(self._settings.max_sessions)
            self._opening += 1

        registered = False
        try:
            pool = self._session_pool
            if pool is None:
                raise SessionError(
                    "not_started", "服务尚未启动", http_status=503
                )
            try:
                conn = pool.getconn(
                    timeout=self._settings.conn_timeout_seconds
                )
            except PoolTimeout as exc:
                raise SessionError(
                    "session_pool_busy",
                    "快照连接获取超时，请稍后重试",
                    http_status=503,
                ) from exc
            try:
                # 保持 autocommit=True：直接发 START TRANSACTION 由服务端
                # 显式开启 RR 只读事务（若先关 autocommit，psycopg 会隐式
                # 发 BEGIN，再发 START TRANSACTION 会产生重复事务警告）。
                # 事务在首条语句冻结快照，连接随会话停留在
                # "idle in transaction" 状态，快照保持不变。
                conn.execute(
                    "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                # pg_export_snapshot 返回当前事务快照 ID，
                # 同时确认快照已在事务内建立（RR 下首条语句即冻结快照）。
                row = conn.execute(
                    "SELECT now() AS snapshot_time, "
                    "pg_export_snapshot() AS snapshot_id"
                ).fetchone()
            except Exception:
                try:
                    conn.rollback()
                finally:
                    pool.putconn(conn)
                raise
            now = _utcnow()
            session = Session(
                session_id=uuid.uuid4().hex,
                region=region,
                conn=conn,
                opened_at=now,
                expires_at=now
                + timedelta(seconds=self._settings.session_ttl_seconds),
                snapshot_time=row["snapshot_time"],
                snapshot_id=row["snapshot_id"],
            )
            # 注册与归还预占名额在同一把锁内原子完成：
            # 名额统计任一时刻只看到"开启中"或"已注册"其一，不会重复计数。
            with self._registry_lock:
                self._registry[session.session_id] = session
                self._opening -= 1
                registered = True
            return session
        finally:
            if not registered:
                with self._registry_lock:
                    self._opening -= 1

    # ------------------------------------------------------------------
    # 源端写入连接（与会话快照池隔离）
    # ------------------------------------------------------------------
    @contextmanager
    def write_conn(self) -> Iterator[psycopg.Connection]:
        """借一条源端写入专用连接（自动提交），用完即还。

        写入连接取自独立的写入池：两条会话占满会话池时，写入仍能
        立即借到连接并提交；无论成功、失败还是异常，连接都在
        ``finally`` 中归还，绝不遗留。
        """
        pool = self._write_pool
        if pool is None:
            raise SessionError("not_started", "服务尚未启动", http_status=503)
        try:
            conn = pool.getconn(timeout=self._settings.conn_timeout_seconds)
        except PoolTimeout as exc:
            raise SessionError(
                "write_pool_busy",
                "写入连接繁忙（等待超时），请稍后重试",
                http_status=503,
            ) from exc
        try:
            yield conn
        finally:
            try:
                pool.putconn(conn)
            except Exception:
                # 池已关闭等收尾阶段：连接由池自行处置，不掩盖业务异常
                pass

    # ------------------------------------------------------------------
    # 取页
    # ------------------------------------------------------------------
    def fetch_page(self, session_id: str, page: int, page_size: int):
        """在会话快照上取第 ``page`` 页（从 1 开始），claim_id 升序。"""
        session = self._get(session_id)

        with session.state_lock:
            if session.status != "open":
                raise SessionUnavailable(
                    "session_not_open",
                    f"会话 {session_id} 状态为 {session.status}，"
                    "关闭已受理或会话已结束，取页失败且不返回明细",
                    http_status=409,
                )
            if _utcnow() >= session.expires_at:
                # 到期瞬间、清理线程尚未接手：直接拒绝，清理稍后完成。
                raise SessionUnavailable(
                    "session_expired",
                    f"会话 {session_id} 已到期，取页失败且不返回明细",
                    http_status=410,
                )
            session.in_flight += 1

        try:
            # io_lock 保证关闭流程与查询不会同时操作同一条连接。
            with session.io_lock:
                # 拿到连接使用权后再复核一次状态：关闭可能在等锁期间受理。
                with session.state_lock:
                    if session.status != "open":
                        raise SessionUnavailable(
                            "session_not_open",
                            f"会话 {session_id} 关闭已受理，"
                            "该取页尚未开始响应，已失败且不返回明细",
                            http_status=409,
                        )
                rows, has_more = self._query_page(session, page, page_size)
                with session.state_lock:
                    session.pages_served += 1
                    session.last_page_at = _utcnow()
                return rows, has_more
        except SessionUnavailable:
            raise
        except psycopg.Error as exc:
            # 连接丢失等数据库错误：会话作废，绝不换连接/换快照续取。
            self._teardown(session, reason="connection_lost")
            raise SessionError(
                "session_broken",
                f"数据库连接或快照失效，会话 {session_id} 已作废，"
                f"不会换快照续取：{exc.sqlstate_name if hasattr(exc, 'sqlstate_name') else exc}",
                http_status=503,
            )
        finally:
            with session.state_lock:
                session.in_flight -= 1

    def _query_page(self, session: Session, page: int, page_size: int):
        # 多取一行判断 has_more；OFFSET 分页基于快照内固定结果集，
        # 重复取同一页得到完全相同的数据。
        offset = (page - 1) * page_size
        sql = """
            SELECT c.id          AS claim_id,
                   c.policy_id   AS policy_id,
                   p.region      AS region,
                   c.amount      AS amount
              FROM claims c
              JOIN policies p ON p.id = c.policy_id
             WHERE p.region = %s
             ORDER BY c.id ASC
             LIMIT %s OFFSET %s
        """
        rows = session.conn.execute(
            sql, (session.region, page_size + 1, offset)
        ).fetchall()
        has_more = len(rows) > page_size
        if has_more:
            rows = rows[:page_size]
        return rows, has_more

    # ------------------------------------------------------------------
    # 关闭 / 到期
    # ------------------------------------------------------------------
    def close(self, session_id: str, reason: str = "closed") -> Session:
        """受理关闭：先翻状态拒绝新取页，再停事务、还连接、释放名额。"""
        session = self._get(session_id)
        with session.state_lock:
            if session.status in ("closed", "expired"):
                return session
            # 第一时间受理：此后任何取页请求都失败。
            session.status = "closing"
        # 等待在途取页结束（它们持有 io_lock），然后独占连接做清理。
        self._teardown(session, reason=reason)
        return session

    def _teardown(self, session: Session, reason: str) -> None:
        with session.io_lock:
            with session.state_lock:
                if session.status in ("closed", "expired"):
                    return
            try:
                # 停止该会话自身的 SQL：回滚即结束快照事务。
                session.conn.rollback()
            except psycopg.Error:
                pass
            if self._session_pool is not None:
                try:
                    # 释放快照连接，归还会话池供后续会话复用。
                    self._session_pool.putconn(session.conn)
                except Exception:
                    pass
            with session.state_lock:
                session.status = "expired" if reason == "expired" else "closed"
        # 终态记录保留在注册表中以便查询会话状态；名额只统计
        # open/closing 会话（见 open_session），故终态记录不占名额。

    def _sweep_loop(self) -> None:
        while not self._stop_event.wait(self._settings.sweeper_interval_seconds):
            try:
                self._sweep_once()
            except Exception as exc:
                # 清理线程永不因单次异常（含数据库连接丢失）退出。
                print(f"[sweeper] 本轮清理失败，将继续重试: {exc!r}", flush=True)

    def _sweep_once(self) -> None:
        now = _utcnow()
        with self._registry_lock:
            due = [
                s for s in list(self._registry.values())
                if s.status == "open" and now >= s.expires_at
            ]
            # 终态记录保留 10 分钟供状态查询，之后回收注册表项。
            stale = [
                s for s in list(self._registry.values())
                if s.status in ("closed", "expired")
                and now - s.expires_at > timedelta(minutes=10)
            ]
        for session in due:
            try:
                self.close(session.session_id, reason="expired")
            except SessionError:
                pass
            except Exception as exc:
                # 连接丢失等情况下，先在注册表内标记终态释放名额，
                # 失效连接留给连接池自身的健康检查回收。
                print(f"[sweeper] 会话 {session.session_id} 到期清理失败: {exc!r}",
                      flush=True)
                with session.state_lock:
                    if session.status == "open":
                        session.status = "expired"
        with self._registry_lock:
            for session in stale:
                self._registry.pop(session.session_id, None)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _get(self, session_id: str) -> Session:
        with self._registry_lock:
            session = self._registry.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return session

    def get_status(self, session_id: str) -> Session:
        return self._get(session_id)

    def list_sessions(self) -> list[Session]:
        with self._registry_lock:
            return list(self._registry.values())

    def session_pool(self) -> ConnectionPool:
        if self._session_pool is None:
            raise SessionError(
                "not_started", "服务尚未启动", http_status=503
            )
        return self._session_pool

    def write_pool(self) -> ConnectionPool:
        if self._write_pool is None:
            raise SessionError(
                "not_started", "服务尚未启动", http_status=503
            )
        return self._write_pool

    def ttl_seconds(self) -> int:
        return self._settings.session_ttl_seconds
