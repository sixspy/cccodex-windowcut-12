"""事件入账：窗内 / 拒收 / 迟到桶分流。

写路径封口合一加固：窗内分支在真正 INSERT 前以 BEGIN IMMEDIATE 取得库级写锁
并复检窗口状态。封口合一事务（seal_svc）持写锁期间，本事务在 BEGIN IMMEDIATE
处等待；待其提交后复检到『已关闭』，事件改判迟到桶，绝不以『窗内』进入被
物化口径——关窗与物化之间因此不存在可脏间隙。
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from locks import release, require_lock
from models import EventIn

router = APIRouter(tags=["ingest"])


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


@router.post("/api/events")
def ingest_event(body: EventIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    lock_key = f"ingest:{body.window_name}"
    conn = connect()
    try:
        require_lock(conn, lock_key, user)
        # 锁行/争用登记先落盘并结束该隐式事务，下面才能显式 BEGIN IMMEDIATE；
        # 即便 require_lock 争用抛 409，争用登记也已提交，不随连接关闭丢失。
        conn.commit()
        # 乐观读：仅决定分流方向，不持写锁，避免与封口事务互相阻塞。
        w = conn.execute(
            "SELECT * FROM windows WHERE name=?", (body.window_name,)
        ).fetchone()
        if not w:
            conn.rollback()
            raise HTTPException(404, "窗口不存在")
        ts = body.event_ts
        created = _now()

        if w["status"] == "已关闭":
            return _route_late(conn, user, body, ts, created)

        if not (w["open_at"] <= ts <= w["close_at"]):
            return _route_reject(conn, user, body, ts, created)

        # 窗内分支：先取库级写锁，再复检窗态——封口事务可能已在乐观读之后提交。
        conn.execute("BEGIN IMMEDIATE")
        fresh = conn.execute(
            "SELECT id,status FROM windows WHERE name=?", (body.window_name,)
        ).fetchone()
        if fresh is None:
            conn.rollback()
            raise HTTPException(404, "窗口不存在")
        if fresh["status"] == "已关闭":
            # 封口已落地：本事件属于封口决定之后到达，改判迟到桶。
            conn.rollback()
            return _route_late(conn, user, body, ts, created)

        cur = conn.execute(
            """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
               VALUES (?,?,?,?,?,?)""",
            (fresh["id"], body.biz_key, ts, body.amount, "窗内", created),
        )
        write_audit(
            conn,
            user,
            "ingest_in_window",
            body.biz_key,
            {"window_id": fresh["id"]},
        )
        conn.commit()
        return {"disposition": "窗内", "window_id": fresh["id"], "event_id": cur.lastrowid}
    finally:
        release(conn, lock_key, user)
        conn.commit()
        conn.close()


def _route_late(conn, user: str, body: EventIn, ts: str, created: str) -> dict:
    cur = conn.execute(
        """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
           VALUES (?,?,?,?,?,?)""",
        (None, body.biz_key, ts, body.amount, "迟到桶", created),
    )
    eid = cur.lastrowid
    conn.execute(
        """INSERT INTO late_bucket(event_id,window_name,biz_key,event_ts,amount,reason,created_at)
           VALUES (?,?,?,?,?,?,?)""",
        (eid, body.window_name, body.biz_key, ts, body.amount, "关窗后到达", created),
    )
    write_audit(
        conn, user, "ingest_late", body.biz_key, {"window": body.window_name}
    )
    conn.commit()
    return {"disposition": "迟到桶", "event_id": eid}


def _route_reject(conn, user: str, body: EventIn, ts: str, created: str) -> dict:
    conn.execute(
        """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
           VALUES (?,?,?,?,?,?)""",
        (None, body.biz_key, ts, body.amount, "拒收", created),
    )
    conn.execute(
        """INSERT INTO reject_slips(window_name,biz_key,event_ts,amount,reason,created_at)
           VALUES (?,?,?,?,?,?)""",
        (body.window_name, body.biz_key, ts, body.amount, "窗外且窗仍开启", created),
    )
    write_audit(
        conn, user, "ingest_reject", body.biz_key, {"window": body.window_name}
    )
    conn.commit()
    raise HTTPException(409, "事件时间落在窗外且窗仍开启：拒收")


@router.get("/api/events")
def list_events(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 200"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/rejects")
def list_rejects(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM reject_slips ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
