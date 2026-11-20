"""物化台账。物化写路径已并入 seal_svc 的封口合一（甲路径同事务）。

本模块保留台账只读视图与分区空桩；/materialize 不再独立写台账，而是委托
合一写路径，杜绝关窗与物化两段之间的可脏间隙。
"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
from models import MaterializeIn, SealIn
from seal_svc import seal_window

router = APIRouter(tags=["materialize"])


@router.post("/api/windows/{wid}/materialize")
def materialize(
    wid: int,
    authorization: AuthHeader = None,
    body: MaterializeIn = MaterializeIn(),
):
    user = require_user(authorization)
    # 维持原前置语义：未关窗不能物化（合一入口内部也会再判一次）。
    with connect() as conn:
        w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        if not w:
            raise HTTPException(404, "窗口不存在")
        sealed = w["status"] == "已关闭"
    if not sealed:
        raise HTTPException(409, "未关窗不能物化")
    # 已关窗则进入合一写路径：有台账则幂等返回，无台账（遗留窗）则同事务补齐。
    return seal_window(
        wid,
        user,
        SealIn(partition_key=body.partition_key, fail_simulate=body.fail_simulate),
    )


@router.get("/api/materializations")
def list_mats(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT m.*, w.name AS window_name
               FROM materializations m
               JOIN windows w ON w.id=m.window_id
               ORDER BY m.id DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.post("/api/materializations/partition-stub")
def partition_stub(authorization: AuthHeader = None):
    """空桩模块：部分物化失败回滚策略待题面落地。"""
    user = require_user(authorization)
    with connect() as conn:
        write_audit(
            conn,
            user,
            "partition_mat_stub",
            None,
            {
                "paths": [
                    "整窗回滚",
                    "保留成功分区",
                ],
                "note": "正确性口径须择一，母仓不实现",
            },
        )
        conn.commit()
    return {"ok": True, "stub": True, "module": "partition_mat"}
