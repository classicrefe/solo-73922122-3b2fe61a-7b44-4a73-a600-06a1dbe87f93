"""端到端自测：覆盖分页、快照隔离、名额、关闭、到期、连接丢失与重启语义。

运行方式（本地已起好 PostgreSQL 15，见 README）：
    DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:55432/claims \
    python -m unittest tests.test_api -v

测试固定以 MAX_SESSIONS=2、POOL_MAX_SIZE=2 运行：两条会话即可占满
快照会话池，用于验证源端写入走独立写入池、不再等待会话关闭。
"""
from __future__ import annotations

import os
import threading
import time
import unittest

import httpx
import psycopg
import uvicorn

# 本地测试用默认值
os.environ.setdefault(
    "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
)
os.environ.setdefault("SESSION_TTL_SECONDS", "5")
os.environ.setdefault("ENABLE_DEBUG_HOOKS", "true")
os.environ.setdefault("SWEEPER_INTERVAL_SECONDS", "0.1")
# 复现"会话占满快照池"的约束配置：会话池上限 = 会话名额 = 2；
# 写入池单独限为 1，便于精确制造"写入排队等连接"的交错。
os.environ.setdefault("POOL_MAX_SIZE", "2")
os.environ.setdefault("WRITE_POOL_MAX_SIZE", "1")

from app.config import settings  # noqa: E402
from app.main import app, manager  # noqa: E402
from app.session_manager import SessionError  # noqa: E402

SEED_SQL = os.path.join(os.path.dirname(__file__), "..", "db", "init", "02_seed.sql")


def reset_seed_data() -> None:
    """每个用例前重置演示数据，保证改/删类测试可重复运行。"""
    with open(SEED_SQL, encoding="utf-8") as fh:
        sql = fh.read()
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(sql)

TTL = settings.session_ttl_seconds


class ApiTestBase(unittest.TestCase):
    # 整个套件共享一个 uvicorn 实例与一个 manager 单例，
    # 避免多实例争抢同一端口导致请求串到旧进程。
    _server: "uvicorn.Server | None" = None
    _server_thread: threading.Thread | None = None
    client: httpx.Client

    @classmethod
    def setUpClass(cls):
        if ApiTestBase._server is None:
            config = uvicorn.Config(
                "app.main:app", host="127.0.0.1", port=18080,
                log_level="warning",
            )
            ApiTestBase._server = uvicorn.Server(config)
            ApiTestBase._server_thread = threading.Thread(
                target=ApiTestBase._server.run,
                name="test-uvicorn", daemon=True,
            )
            ApiTestBase._server_thread.start()
            for _ in range(100):
                try:
                    httpx.get("http://127.0.0.1:18080/health", timeout=1)
                    break
                except httpx.TransportError:
                    time.sleep(0.1)
            # 全套件以干净的种子数据开始
            reset_seed_data()
        cls.client = httpx.Client(
            base_url="http://127.0.0.1:18080", timeout=15
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        # 每个用例前清掉所有未终结会话，保证相互独立
        for s in manager.list_sessions():
            if s.status in ("open", "closing"):
                try:
                    manager.close(s.session_id)
                except SessionError:
                    pass
        # 写入类用例会改数据，统一在用例内自行安排快照时机，
        # 不在全局 setUp 中重置（否则会清掉同用例刚提交的写入）。

    @classmethod
    def _reset_seed(cls):
        reset_seed_data()

    def open(self, region="华北"):
        r = self.client.post("/sessions", json={"region": region})
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def close(self, sid):
        return self.client.post(f"/sessions/{sid}/close")

    def page(self, sid, page=1, page_size=100, headers=None):
        return self.client.get(
            f"/sessions/{sid}/claims",
            params={"page": page, "page_size": page_size},
            headers=headers or {},
        )

    def all_claim_ids(self, sid) -> set[int]:
        """翻完全部页，收集该会话快照内的全部 claim_id。"""
        ids: set[int] = set()
        page_no = 1
        while True:
            r = self.page(sid, page=page_no)
            self.assertEqual(r.status_code, 200, r.text)
            body = r.json()
            ids.update(row["claim_id"] for row in body["rows"])
            if not body["has_more"]:
                return ids
            page_no += 1


class TestPagination(ApiTestBase):
    def test_pages_ordered_and_capped(self):
        s = self.open()
        r1 = self.page(s["session_id"])
        self.assertEqual(r1.status_code, 200)
        body = r1.json()
        self.assertEqual(len(body["rows"]), 100)
        self.assertTrue(body["has_more"])
        ids = [r["claim_id"] for r in body["rows"]]
        self.assertEqual(ids, sorted(ids))
        self.assertTrue(all(r["region"] == "华北" for r in body["rows"]))
        self.assertTrue(all(isinstance(r["amount"], int) for r in body["rows"]))

        r2 = self.page(s["session_id"], page=2)
        self.assertEqual(len(r2.json()["rows"]), 20)
        self.assertFalse(r2.json()["has_more"])
        self.assertGreater(
            r2.json()["rows"][0]["claim_id"], body["rows"][-1]["claim_id"]
        )

        r3 = self.page(s["session_id"], page=3)
        self.assertEqual(r3.json()["rows"], [])
        self.assertFalse(r3.json()["has_more"])

    def test_page_size_forced_to_cap(self):
        s = self.open()
        r = self.page(s["session_id"], page_size=999)
        self.assertEqual(r.json()["page_size"], 100)
        self.assertEqual(len(r.json()["rows"]), 100)

    def test_invalid_pagination_params(self):
        s = self.open()
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page": 0}
        ).status_code, 422)
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page_size": 0}
        ).status_code, 422)

    def test_repeat_page_same_snapshot(self):
        s = self.open()
        b1 = self.page(s["session_id"]).json()
        b2 = self.page(s["session_id"]).json()
        self.assertEqual(
            [r["claim_id"] for r in b1["rows"]],
            [r["claim_id"] for r in b2["rows"]],
        )
        self.assertEqual(b1["snapshot_time"], b2["snapshot_time"])


class TestSnapshotIsolation(ApiTestBase):
    def test_source_writes_invisible_inside_session(self):
        self._reset_seed()  # 本用例需要干净的种子数据
        s = self.open("华北")
        before = self.page(s["session_id"]).json()
        before_ids = {r["claim_id"] for r in before["rows"]}

        # 源端：新增保单+理赔、改金额、删理赔，均独立提交
        pol = self.client.post("/admin/policies", json={"region": "华北"})
        self.assertEqual(pol.status_code, 201)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 777},
        )
        self.assertEqual(nc.status_code, 201)
        first_id = before["rows"][0]["claim_id"]
        second_id = before["rows"][1]["claim_id"]
        self.assertEqual(self.client.patch(
            f"/admin/claims/{first_id}", params={"amount": 1}
        ).status_code, 200)
        self.assertEqual(self.client.delete(
            f"/admin/claims/{second_id}"
        ).status_code, 204)

        after = self.page(s["session_id"]).json()
        after_ids = {r["claim_id"] for r in after["rows"]}
        self.assertEqual(before_ids, after_ids)
        self.assertNotIn(nc.json()["id"], after_ids)
        self.assertNotEqual(after["rows"][0]["amount"], 1)
        self.assertIn(second_id, after_ids)

        # 新会话能看到源端最新提交（翻完全部页核对）
        s2 = self.open("华北")
        fresh_ids = set()
        page_no = 1
        while True:
            body = self.page(s2["session_id"], page=page_no).json()
            fresh_ids.update(r["claim_id"] for r in body["rows"])
            if not body["has_more"]:
                break
            page_no += 1
        self.assertIn(nc.json()["id"], fresh_ids)
        self.assertNotIn(second_id, fresh_ids)
        self.close(s2["session_id"])

    def test_snapshot_covers_both_tables(self):
        # 地区来自 policies 表、金额来自 claims 表：JOIN 结果同样固定
        s = self.open("华东")
        b = self.page(s["session_id"]).json()
        self.assertEqual(len(b["rows"]), 100)
        self.assertTrue(all(r["region"] == "华东" for r in b["rows"]))


class TestWritePathIsolation(ApiTestBase):
    """连接分配修复的核心验证：MAX_SESSIONS=POOL_MAX_SIZE=2 时，
    两条会话占满快照会话池，源端写入仍走独立写入池立即提交。"""

    def test_writes_commit_promptly_with_full_quota(self):
        self._reset_seed()
        s1 = self.open("华北")
        s2 = self.open("华东")
        # 名额与快照会话池均已占满：第三个会话被 429 拒绝
        self.assertEqual(
            self.client.post("/sessions", json={"region": "华南"}).status_code,
            429,
        )
        # 源端写入不再等待会话关闭（TTL=5s，断言远小于 TTL）
        t0 = time.monotonic()
        pol = self.client.post("/admin/policies", json={"region": "华北"})
        self.assertEqual(pol.status_code, 201, pol.text)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 4242},
        )
        self.assertEqual(nc.status_code, 201, nc.text)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 2.0, f"写入耗时 {elapsed:.2f}s，疑似在等会话连接")

        # 两条旧会话仍读到开启时的快照：新补录理赔不可见
        self.assertNotIn(nc.json()["id"], self.all_claim_ids(s1["session_id"]))
        # 两条快照持续可分页
        self.assertEqual(
            self.page(s2["session_id"], page_size=5).status_code, 200
        )

        # 关闭一条会话后，新会话读到已提交改动
        self.close(s1["session_id"])
        s3 = self.open("华北")
        self.assertIn(nc.json()["id"], self.all_claim_ids(s3["session_id"]))
        self.close(s2["session_id"])
        self.close(s3["session_id"])

    def test_two_sessions_keep_paging_while_writes_commit(self):
        self._reset_seed()
        s1 = self.open("华北")
        s2 = self.open("华北")
        p1_s1 = self.page(s1["session_id"], page_size=50).json()
        p1_s2 = self.page(s2["session_id"], page_size=50).json()
        self.assertEqual(
            [r["claim_id"] for r in p1_s1["rows"]],
            [r["claim_id"] for r in p1_s2["rows"]],
        )

        # 源端增/改/删照常提交
        pol = self.client.post("/admin/policies", json={"region": "华北"}).json()
        nc = self.client.post(
            "/admin/claims", json={"policy_id": pol["id"], "amount": 999}
        ).json()
        first_id = p1_s1["rows"][0]["claim_id"]
        second_id = p1_s1["rows"][1]["claim_id"]
        original_amount = p1_s1["rows"][0]["amount"]
        self.assertEqual(
            self.client.patch(
                f"/admin/claims/{first_id}", params={"amount": 7}
            ).status_code,
            200,
        )
        self.assertEqual(
            self.client.delete(f"/admin/claims/{second_id}").status_code, 204
        )

        # 会话 1 继续取后续页：仍是开启时的两表快照
        ids_s1 = self.all_claim_ids(s1["session_id"])
        self.assertEqual(len(ids_s1), 120)
        self.assertNotIn(nc["id"], ids_s1)      # 新增不可见
        self.assertIn(second_id, ids_s1)        # 删除不可见
        again = self.page(s1["session_id"], page_size=50).json()
        self.assertEqual(again["rows"][0]["amount"], original_amount)  # 修改不可见

        # 会话 2 重复取第 1 页，与会话 1 首次完全一致
        p1_s2b = self.page(s2["session_id"], page_size=50).json()
        self.assertEqual(
            [r["claim_id"] for r in p1_s2b["rows"]],
            [r["claim_id"] for r in p1_s1["rows"]],
        )
        self.close(s1["session_id"])
        self.close(s2["session_id"])

    def test_failed_write_keeps_snapshots_and_connections(self):
        self._reset_seed()
        s1 = self.open("华北")
        s2 = self.open("华东")
        before = self.all_claim_ids(s1["session_id"])

        # 外键指向不存在的保单 -> 400，语句未提交
        for _ in range(4):
            r = self.client.post(
                "/admin/claims", json={"policy_id": 999999, "amount": 1}
            )
            self.assertEqual(r.status_code, 400, r.text)
            self.assertNotIn("rows", r.json())

        # 失败不遗留连接（WRITE_POOL_MAX_SIZE=1）：正常写入仍立即成功
        t0 = time.monotonic()
        ok = self.client.post("/admin/claims", json={"policy_id": 1, "amount": 100})
        self.assertEqual(ok.status_code, 201, ok.text)
        self.assertLess(time.monotonic() - t0, 2.0)

        # 快照未被失败写入损坏：两会话取页如常，看不到任何新写入
        self.assertEqual(self.all_claim_ids(s1["session_id"]), before)
        self.assertEqual(
            self.page(s2["session_id"], page_size=5).status_code, 200
        )

        # 关闭一条后新会话看到已提交写入；失败写入无痕迹
        self.close(s2["session_id"])
        s3 = self.open("华北")
        ids3 = self.all_claim_ids(s3["session_id"])
        self.assertIn(ok.json()["id"], ids3)
        self.assertEqual(len(ids3), 121)  # 只多 1 条成功写入
        self.close(s1["session_id"])
        self.close(s3["session_id"])

    def test_status_and_close_available_while_write_waits(self):
        s = self.open("华北")
        sid = s["session_id"]
        # 占满写入池（WRITE_POOL_MAX_SIZE=1），让随后的写请求排队等连接
        held = manager.write_pool().getconn(timeout=5)
        box: dict = {}

        def writer():
            box["r"] = self.client.post(
                "/admin/policies", json={"region": "西南"}, timeout=20
            )

        t = threading.Thread(target=writer, name="blocked-writer", daemon=True)
        try:
            t.start()
            time.sleep(0.5)  # 写请求已在等待写入连接
            # 等待写入期间：状态查询可用
            t0 = time.monotonic()
            info = self.client.get(f"/sessions/{sid}")
            self.assertEqual(info.status_code, 200, info.text)
            self.assertEqual(info.json()["status"], "open")
            self.assertLess(time.monotonic() - t0, 3)
            # 等待写入期间：主动关闭可用
            t0 = time.monotonic()
            r = self.close(sid)
            self.assertEqual(r.status_code, 200, r.text)
            self.assertEqual(r.json()["status"], "closed")
            self.assertLess(time.monotonic() - t0, 3)
        finally:
            # 释放写入连接，排队的写请求得以完成；连接绝不遗留
            manager.write_pool().putconn(held)
            t.join(timeout=20)
        self.assertEqual(box["r"].status_code, 201, box["r"].text)


class TestCapacity(ApiTestBase):
    def test_max_two_sessions_and_release(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        r = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(r.json()["code"], "capacity_reached")

        # 满额不影响既有会话
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)

        self.assertEqual(self.close(s2["session_id"]).json()["status"], "closed")
        s3 = self.open("华北")  # 名额归还后可再开
        self.assertEqual(s3["status"], "open")
        # 另一个会话不受影响
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)
        self.close(s1["session_id"])
        self.close(s3["session_id"])

    def test_unknown_session_404(self):
        r = self.client.get("/sessions/nonexistent/claims")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.client.get("/sessions/nonexistent").status_code, 404)

    def test_concurrent_opens_never_exceed_quota(self):
        # 8 路并发开启：名额预占在锁内完成，恰好 2 个成功、6 个 429
        results: list[int] = []

        def opener():
            results.append(
                self.client.post("/sessions", json={"region": "华北"}).status_code
            )

        threads = [threading.Thread(target=opener) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(
            sorted(results), [201, 201, 429, 429, 429, 429, 429, 429]
        )


class TestCloseSemantics(ApiTestBase):
    def test_close_rejects_later_pages_but_keeps_status_queryable(self):
        s = self.open()
        self.close(s["session_id"])
        r = self.page(s["session_id"])
        self.assertEqual(r.status_code, 409)
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{s['session_id']}").json()
        self.assertEqual(info["status"], "closed")

    def test_slow_page_then_close(self):
        s = self.open("华东")
        sid = s["session_id"]
        box = {}

        def slow():
            box["r"] = self.client.get(
                f"/sessions/{sid}/claims",
                params={"page": 1, "page_size": 10},
                headers={"X-Debug-Sleep-Seconds": "3"},
                timeout=15,
            )

        t = threading.Thread(target=slow)
        t.start()
        time.sleep(0.8)
        r = self.close(sid)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "closed")
        t.join()
        self.assertIn(box["r"].status_code, (409, 410))
        self.assertNotIn("rows", box["r"].json())

        # 关闭后名额已归还，可开新会话
        s2 = self.open("华东")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])

    def test_close_idempotent_and_new_session_unaffected(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        self.close(s1["session_id"])
        again = self.close(s1["session_id"])
        self.assertEqual(again.json()["status"], "closed")
        # 另一个会话照常工作
        self.assertEqual(self.page(s2["session_id"], page_size=5).status_code, 200)
        self.close(s2["session_id"])


class TestExpiry(ApiTestBase):
    def test_expiry_rejects_pages_and_releases_slot(self):
        s = self.open("华南")
        sid = s["session_id"]
        time.sleep(TTL + 0.6)
        r = self.page(sid)
        self.assertIn(r.status_code, (409, 410))
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{sid}").json()
        self.assertEqual(info["status"], "expired")
        # 名额已释放：能连开两个新会话
        a = self.open("华东")
        b = self.open("华北")
        self.assertEqual(a["status"], "open")
        self.assertEqual(b["status"], "open")
        self.close(a["session_id"])
        self.close(b["session_id"])

    def test_pages_before_expiry_still_work(self):
        s = self.open("华南")
        r = self.page(s["session_id"], page_size=10)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["rows"]), 10)


class TestConnectionLossAndRestart(ApiTestBase):
    """连接丢失（终止会话后端）与服务重启语义。

    本环境禁止 kill 系统调用，故用 pg_terminate_backend 从 SQL 层
    断开会话持有的后端连接，对客户端而言与网络/进程断开等价。
    """

    def _terminate_session_backend(self, sid: str) -> None:
        sess = manager.get_status(sid)
        with psycopg.connect(settings.database_url, autocommit=True) as admin:
            # 按客户端后端 PID 精准终止该会话独占的连接
            admin.execute(
                "SELECT pg_terminate_backend(%s)",
                (sess.conn.info.backend_pid,),
            )
        # 给客户端一点时间感知 TCP 断开
        deadline = time.time() + 5
        while time.time() < deadline:
            if sess.conn.closed:
                return
            time.sleep(0.1)

    def test_connection_loss_invalidates_session_no_snapshot_swap(self):
        s = self.open("华东")
        sid = s["session_id"]
        # 从 SQL 层断开会话持有的后端连接 -> 快照事务随之消失
        self._terminate_session_backend(sid)
        r = self.page(sid)
        # 旧会话直接失败，绝不换连接/换快照续取；
        # 409（清理线程先标记终态）或 503（取页时发现连接断裂）均合规
        self.assertIn(r.status_code, (409, 503))
        self.assertNotIn("rows", r.json())
        # 旧 id 无法续取（失效会话已移出注册表或已为终态）
        info = self.client.get(f"/sessions/{sid}")
        if info.status_code == 200:
            self.assertIn(info.json()["status"], ("closed", "expired"))
            self.assertEqual(self.page(sid).status_code, 409)
        else:
            self.assertEqual(info.status_code, 404)
        # 新会话在池恢复后的连接上正常开启
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=5)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 5)
        self.close(s2["session_id"])

    @staticmethod
    def _restart_uvicorn():
        """完整重启共享 uvicorn：lifespan 负责 manager.stop()/start()。"""
        ApiTestBase._server.should_exit = True
        ApiTestBase._server_thread.join(timeout=10)
        config = uvicorn.Config(
            "app.main:app", host="127.0.0.1", port=18080, log_level="warning",
        )
        ApiTestBase._server = uvicorn.Server(config)
        ApiTestBase._server_thread = threading.Thread(
            target=ApiTestBase._server.run,
            name="test-uvicorn-restart", daemon=True,
        )
        ApiTestBase._server_thread.start()
        for _ in range(100):
            try:
                if httpx.get(
                    "http://127.0.0.1:18080/health", timeout=1
                ).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError("uvicorn 重启后健康检查失败")

    def test_service_restart_drops_in_memory_sessions(self):
        s = self.open("华南")
        sid = s["session_id"]

        # 模拟 API 服务重启（新进程语义：内存注册表为空、池与 sweeper 重建）
        self._restart_uvicorn()

        # 旧会话 id 在新进程中不存在，不得换快照续取
        r = self.client.get(f"/sessions/{sid}")
        self.assertEqual(r.status_code, 404)
        r2 = self.page(sid)
        self.assertEqual(r2.status_code, 404)
        s2 = self.open("华南")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
