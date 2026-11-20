"""封口合一红测与同态回归（本地夹具，SQLite）。

红测一 test_gap_dirty_write_red：复现「关窗与物化两次点击之间被入账污染」
    ——入账连接在读到『开启』之后、写入之前被关窗提交插身而过，旧两段写路径
    下该事件被当成窗内，随后物化吃到脏事件，对账无法察觉。
红测二 test_seal_contention_red：两人同时对同一窗点关窗/物化，旧实现无合一锁、
    可产出两张物化台账且无争用登记。

绿相回归覆盖：五处同态、失败整体回滚、入账冻结、封口合一册（当天禁改口/
偏册拒写记『合一偏了』）。
"""
from __future__ import annotations

import threading
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from app import app, init_db
from db import connect
import ingest_svc
import locks
import materialize_svc
import windows_svc


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "seal.db")
    init_db()
    # WAL：读不再挡写，消除夹具里『读事务持锁阻塞写事务』的伪冲突，
    # 让争用只反映业务合一锁/写事务的真实序列化。
    with connect() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
    yield


def _auth(c: TestClient, username: str = "admin", password: str = "123456") -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


def _open_window(c: TestClient, h: dict, name: str = "W-GAP") -> int:
    r = c.post(
        "/api/windows",
        headers=h,
        json={
            "name": name,
            "open_at": "2026-11-10T00:00:00",
            "close_at": "2026-11-10T23:59:59",
        },
    )
    assert r.status_code == 200, r.text
    return r.json()["id"]


def _ingest(c: TestClient, h: dict, name: str, biz: str, amount: float = 10.0):
    return c.post(
        "/api/events",
        headers=h,
        json={
            "window_name": name,
            "biz_key": biz,
            "event_ts": "2026-11-10T12:00:00",
            "amount": amount,
        },
    )


class _ConnProxy:
    """只代理 execute 的连接包装（C 级 sqlite3.Connection 属性只读，不能直接换）。"""

    def __init__(self, conn, gate):
        self._conn = conn
        self._gate = gate

    def execute(self, sql, *params):
        if not self._gate.entered.is_set() and str(sql).strip().startswith(
            self._gate.prefix
        ):
            # 一次性闸门：arming 之后第一条匹配 SQL 必停，与执行线程无关
            # （TestClient 端点跑在 anyio 线程池，线程号不可用于过滤）。
            self._gate.entered.set()
            self._gate.release.wait(timeout=10)
        return self._conn.execute(sql, *params)

    def __getattr__(self, name):
        return getattr(self._conn, name)

    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc):
        return self._conn.__exit__(*exc)


class _OnceGate:
    """SQL 间隙闸门：命中指定前缀的第一次 execute 先等放行，再执行。

    请求顺序由用例保证（先起卡在间隙的请求，再发关窗），因此无需线程过滤。
    """

    def __init__(self, monkeypatch, module, sql_prefix: str):
        self.prefix = sql_prefix.strip()
        self.entered = threading.Event()
        self.release = threading.Event()
        monkeypatch.setattr(module, "connect", self._patched_connect)

    def _patched_connect(self):
        return _ConnProxy(connect(), self)


class _FuncGate:
    """函数级闸门：按 holder 过滤，在目标函数真正执行前停车等放行。

    同时挂多个模块的同名属性，兼容 `from locks import release`（旧 svc 在
    导入时绑定原函数）与 `locks.release`（新合一路径经模块属性调用）两种写法，
    保证同一份夹具在新旧写路径上都接得住。
    """

    def __init__(self, monkeypatch, modules, func_name: str, park_holder: str):
        self.park_holder = park_holder
        self.entered = threading.Event()
        self.release = threading.Event()
        self._originals = []
        for module in modules:
            if not hasattr(module, func_name):
                continue
            orig = getattr(module, func_name)
            self._originals.append((module, func_name, orig))

            def make_wrapper(original):
                def wrapped(*args, **kwargs):
                    holder = args[2] if len(args) >= 3 else kwargs.get("holder")
                    if holder == park_holder:
                        self.entered.set()
                        self.release.wait(timeout=10)
                    return original(*args, **kwargs)

                return wrapped

            monkeypatch.setattr(module, func_name, make_wrapper(orig))


# ---------------------------------------------------------------------------
# 红测一：关窗/物化间隙被并发入账污染（本地 SQLite 夹具复现脏写）
# ---------------------------------------------------------------------------
def test_gap_dirty_write_red(monkeypatch):
    c = TestClient(app)
    h = _auth(c)
    wid = _open_window(c, h)
    # 封口前已合法入账一笔，物化口径应永远等于这笔（金额 10，行数 1）。
    assert _ingest(c, h, "W-GAP", "BASE-1", 10.0).json()["disposition"] == "窗内"

    worker = threading.Thread(target=c.post, args=("/api/events",), kwargs={
        "headers": h,
        "json": {
            "window_name": "W-GAP",
            "biz_key": "DIRTY-1",
            "event_ts": "2026-11-10T12:05:00",
            "amount": 7.0,
        },
    })
    # 旧实现：入账读到『开启』后、INSERT 前被关窗插身；
    # 新实现：同一点位于合一事务的写锁（BEGIN IMMEDIATE）之后。
    gate = _OnceGate(monkeypatch, ingest_svc,
                     "SELECT * FROM windows WHERE name=?")

    worker.start()
    assert gate.entered.wait(timeout=5), "夹具未卡在入账间隙"

    # 关窗（新实现中即合一封口：关窗+物化同事务）。
    close_r = c.post(f"/api/windows/{wid}/close", headers=h)
    assert close_r.status_code == 200, close_r.text

    gate.release.set()
    worker.join(timeout=10)
    assert not worker.is_alive()

    # 旧两段路径下物化是第二次点击；合一后 close 已带物化，此调幂等返回。
    mat_r = c.post(f"/api/windows/{wid}/materialize", headers=h)
    assert mat_r.status_code == 200, mat_r.text
    mat = c.get("/api/materializations", headers=h).json()["items"]
    win_mats = [m for m in mat if m["window_id"] == wid]

    ev_rows = c.get("/api/events", headers=h).json()["items"]
    dirty = [e for e in ev_rows if e["biz_key"] == "DIRTY-1"]
    assert len(dirty) == 1

    # 不变量一：封口决定之后到达的事件，绝不允许以『窗内』进入被物化口径。
    assert dirty[0]["disposition"] == "迟到桶", (
        "关窗与物化间隙被入账污染：DIRTY-1 被当成窗内"
    )
    # 不变量二：物化台账只有一张，且口径恒等于封口前快照（1 行 / 10 元）。
    assert len(win_mats) == 1, "合一写路径只能产出一张物化台账"
    assert win_mats[0]["row_count"] == 1
    assert win_mats[0]["total_amount"] == 10.0
    # 不变量三：五处同态——对账必须能发现并拒绝脏口径（此处天然对齐）。
    assert c.get(f"/api/reconcile/{wid}", headers=h).json()["aligned"] is True


# ---------------------------------------------------------------------------
# 红测二：两人同时点关窗/物化同一窗，须占合一锁，后到只记争用
# ---------------------------------------------------------------------------
def test_seal_contention_red(monkeypatch):
    c = TestClient(app)
    h1 = _auth(c, "admin", "123456")
    h2 = _auth(c, "closer", "close123")
    wid = _open_window(c, h1, name="W-RACE")
    assert _ingest(c, h1, "W-RACE", "RACE-BASE", 5.0).status_code == 200

    errors: list = []

    def worker_req(headers: dict, path: str, out: list):
        try:
            r = c.post(path, headers=headers)
            out.append((r.status_code, r.json()))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    # 先到者（admin 的关窗）业务已提交、但在释放其锁之前停车：
    # 此时关窗已落地（窗=已关闭），后到者的 /materialize 不会被『未关窗』前置
    # 挡回，能真正进入临界区撞上同一把合一锁。release 在新旧两版都被调用，
    # 且按 holder=admin 过滤，只卡先到者。
    gate = _FuncGate(
        monkeypatch, (windows_svc, materialize_svc, locks), "release", "admin"
    )
    out1: list = []
    t1 = threading.Thread(
        target=worker_req, args=(h1, f"/api/windows/{wid}/close", out1)
    )
    out2: list = []
    t2 = threading.Thread(
        target=worker_req, args=(h2, f"/api/windows/{wid}/materialize", out2)
    )

    t1.start()
    assert gate.entered.wait(timeout=5), "夹具未卡在关窗锁释放前"
    t2.start()
    # 后到者撞锁：旧版锁键不同→它也成功（两笔写入，红）；
    # 新版共用 seal_unify 锁→只记争用、409、不写业务（绿）。
    t2.join(timeout=10)
    assert not errors
    gate.release.set()
    t1.join(timeout=10)
    assert not errors, errors

    statuses = sorted([out1[0][0], out2[0][0]])
    # 先到/后到只有一方 200，另一方 409 争用——绝不允许两个 200 各写一笔。
    assert statuses == [200, 409], f"两人同点未串行化: {out1=} {out2=}"

    contention = c.get("/api/contention", headers=h1).json()["items"]
    assert any(x["lock_key"] == f"seal_unify:{wid}" for x in contention), (
        "后到者必须以合一锁键登记争用"
    )

    mats = [
        m
        for m in c.get("/api/materializations", headers=h1).json()["items"]
        if m["window_id"] == wid
    ]
    assert len(mats) == 1, "争用失败方不得再写物化台账"
    w = c.get(f"/api/windows/{wid}", headers=h1).json()["window"]
    assert w["status"] == "已关闭"
    assert c.get(f"/api/reconcile/{wid}", headers=h1).json()["aligned"] is True


# ---------------------------------------------------------------------------
# 绿相回归：失败整体回滚（窗态与台账同生共死）
# ---------------------------------------------------------------------------
def test_seal_fail_rolls_back_window_and_ledger():
    c = TestClient(app)
    h = _auth(c)
    wid = _open_window(c, h, name="W-ROLL")
    assert _ingest(c, h, "W-ROLL", "ROLL-1", 3.0).status_code == 200

    r = c.post(f"/api/windows/{wid}/seal", headers=h, json={"fail_simulate": True})
    assert r.status_code == 500

    with connect() as conn:
        status = conn.execute(
            "SELECT status FROM windows WHERE id=?", (wid,)
        ).fetchone()[0]
        n_mat = conn.execute(
            "SELECT COUNT(*) c FROM materializations WHERE window_id=?", (wid,)
        ).fetchone()["c"]
        n_audit = conn.execute(
            "SELECT COUNT(*) c FROM audit_trail WHERE action='seal_unify_ok'"
        ).fetchone()["c"]
    assert status == "开启", "物化失败窗态必须回滚"
    assert n_mat == 0, "物化失败台账不得留痕"
    assert n_audit == 0, "失败事务内审计随回滚，仅留失败审计"
    # 回滚后允许重试且成功。
    ok = c.post(f"/api/windows/{wid}/seal", headers=h)
    assert ok.status_code == 200, ok.text
    assert c.get(f"/api/reconcile/{wid}", headers=h).json()["aligned"] is True


# ---------------------------------------------------------------------------
# 绿相回归：封口合一册——择定、当天禁改口、偏册拒写记『合一偏了』
# ---------------------------------------------------------------------------
def test_seal_register_locks_path_same_day_and_rejects_deviation():
    c = TestClient(app)
    h = _auth(c)

    # 册内尚未择定时，首次封口即把甲路径写册。
    wid = _open_window(c, h, name="W-REG")
    r = c.post(f"/api/windows/{wid}/seal", headers=h)
    assert r.status_code == 200
    with connect() as conn:
        decisions = conn.execute(
            "SELECT path FROM seal_unify_ledger WHERE kind='择定' ORDER BY id"
        ).fetchall()
    assert [d["path"] for d in decisions] == ["甲"]

    # 同一路径重复登记幂等。
    again = c.post("/api/seal-register", headers=h, json={"path": "甲"})
    assert again.status_code == 200 and again.json()["path"] == "甲"

    # 当天改口乙路径：拒写并记『合一偏了』。
    bad = c.post("/api/seal-register", headers=h, json={"path": "乙"})
    assert bad.status_code == 409
    with connect() as conn:
        dev = conn.execute(
            "SELECT * FROM seal_unify_ledger WHERE kind='合一偏了'"
        ).fetchall()
        paths = {
            r["path"]
            for r in conn.execute(
                "SELECT DISTINCT path FROM seal_unify_ledger WHERE kind='择定'"
            ).fetchall()
        }
    assert len(dev) == 1 and dev[0]["detail"].find("乙") >= 0
    assert paths == {"甲"}, "偏离不得入册，口径仍是甲"

    # 未知路径同样拒写。
    weird = c.post("/api/seal-register", headers=h, json={"path": "丙"})
    assert weird.status_code == 400


# ---------------------------------------------------------------------------
# 绿相回归：五处同态（窗态/台账/入账/对账/审计）
# ---------------------------------------------------------------------------
def test_five_places_homomorphism():
    c = TestClient(app)
    h = _auth(c)
    wid = _open_window(c, h, name="W-HOMO")
    _ingest(c, h, "W-HOMO", "H-1", 2.0)
    _ingest(c, h, "W-HOMO", "H-2", 3.0)

    r = c.post(f"/api/windows/{wid}/seal", headers=h)
    assert r.status_code == 200
    payload = r.json()
    assert payload["row_count"] == 2 and payload["total_amount"] == 5.0

    # 封口后同窗再入账：进迟到桶，不碰窗内口径。
    late = _ingest(c, h, "W-HOMO", "H-LATE", 9.0)
    assert late.json()["disposition"] == "迟到桶"

    # 1) 窗口状态
    w = c.get(f"/api/windows/{wid}", headers=h).json()
    assert w["window"]["status"] == "已关闭" and w["window"]["seal_token"]
    # 2) 物化台账
    mats = [
        m
        for m in c.get("/api/materializations", headers=h).json()["items"]
        if m["window_id"] == wid
    ]
    assert len(mats) == 1 and mats[0]["status"] == "成功"
    # 3) 事件入账：窗内仍是 2 笔，迟到 1 笔。
    assert w["in_window_count"] == 2 and w["in_window_amount"] == 5.0
    assert c.get("/api/late-bucket", headers=h).json()["count"] >= 1
    # 4) 对账
    recon = c.get(f"/api/reconcile/{wid}", headers=h).json()
    assert recon["aligned"] is True and recon["late_bucket"] >= 1
    # 5) 审计：合一动作可追溯，且不存在拆开的 window_close/materialize_ok 双写。
    actions = {
        x["action"]: x["c"]
        for x in c.get("/api/audit/actions", headers=h).json()["items"]
    }
    assert actions.get("seal_unify_ok", 0) >= 1
    assert "materialize_ok" not in actions


def test_idempotent_retry_after_sealed():
    c = TestClient(app)
    h = _auth(c)
    wid = _open_window(c, h, name="W-IDEM")
    _ingest(c, h, "W-IDEM", "I-1", 1.0)
    assert c.post(f"/api/windows/{wid}/seal", headers=h).status_code == 200

    # 已封口窗再次走任一旧入口：幂等返回既有物化，不新增台账、不记争用。
    for path in (
        f"/api/windows/{wid}/seal",
        f"/api/windows/{wid}/close",
        f"/api/windows/{wid}/materialize",
    ):
        r = c.post(path, headers=h)
        assert r.status_code == 200 and r.json().get("already") is True, path
    mats = [
        m
        for m in c.get("/api/materializations", headers=h).json()["items"]
        if m["window_id"] == wid
    ]
    assert len(mats) == 1
    with connect() as conn:
        n_cont = conn.execute(
            "SELECT COUNT(*) c FROM contention_log WHERE lock_key=?",
            (f"seal_unify:{wid}",),
        ).fetchone()["c"]
    assert n_cont == 0


def test_ingest_contention_is_persisted():
    """入账争用 409 时，contention_log 必须已提交，不随连接关闭丢失。"""
    c = TestClient(app)
    h1 = _auth(c, "admin", "123456")
    h2 = _auth(c, "closer", "close123")
    _open_window(c, h1, name="W-CONT")
    # admin 先占住入账锁且不释放。
    r1 = c.post("/api/locks/acquire", headers=h1,
                json={"lock_key": "ingest:W-CONT"})
    assert r1.status_code == 200
    # closer 同窗入账撞锁：409，但争用登记须落盘。
    r2 = _ingest(c, h2, "W-CONT", "CONT-1", 1.0)
    assert r2.status_code == 409
    items = c.get("/api/contention", headers=h1).json()["items"]
    assert any(
        x["lock_key"] == "ingest:W-CONT" and x["actor"] == "closer" for x in items
    )


def test_seal_register_visible_via_api():
    c = TestClient(app)
    h = _auth(c)
    wid = _open_window(c, h, name="W-VIS")
    c.post(f"/api/windows/{wid}/seal", headers=h)
    r = c.get("/api/seal-register", headers=h)
    assert r.status_code == 200
    item = r.json()
    assert item["path"] == "甲" and item["kind"] == "择定"
    assert item["decided_at"][:10] == datetime.now().isoformat(timespec="seconds")[:10]
