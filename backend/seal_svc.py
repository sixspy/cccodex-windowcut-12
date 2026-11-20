"""封口合一写路径（本仓唯一正确性口径：甲路径）。

甲路径：关闭窗口（status/seal_token）与写入物化行（materializations）在同一
SQLite 事务内提交；事务以 BEGIN IMMEDIATE 立即取得库级写锁，期间任何入账写
事务只能在锁外等待，封口决定与物化口径之间不存在可被并发入账插入的间隙。
物化失败则整笔回滚：窗态、台账、审计同生共死。

乙路径（close_draft 冻结后原子切换）在本仓不落地，两路径禁止混用。
口径择定写入 seal_unify_ledger（封口合一册），当天禁止改口；偏册请求拒写
并记『合一偏了』。同一窗的关窗/物化共用合一锁 seal_unify:{wid}，后到只记
争用，不写业务。
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect, write_audit
import locks
from models import SealIn, SealRegisterIn

router = APIRouter(tags=["seal_unify"])

# 封口合一册：本仓落地的唯一口径。
PATH_A = "甲"
PATH_B = "乙"
LEDGER_KIND_DECIDE = "择定"
LEDGER_KIND_DEVIATION = "合一偏了"


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _today() -> str:
    return datetime.now().date().isoformat()


def get_decision(conn) -> dict | None:
    row = conn.execute(
        """SELECT * FROM seal_unify_ledger
           WHERE kind=? ORDER BY id DESC LIMIT 1""",
        (LEDGER_KIND_DECIDE,),
    ).fetchone()
    return dict(row) if row else None


def _bootstrap_decision(conn, actor: str) -> dict:
    """册内无择定时，以本仓唯一口径甲路径写册（与封口同事务）。"""
    existing = get_decision(conn)
    if existing:
        return existing
    now = _now()
    conn.execute(
        """INSERT INTO seal_unify_ledger(kind,path,actor,detail,decided_at)
           VALUES (?,?,?,?,?)""",
        (
            LEDGER_KIND_DECIDE,
            PATH_A,
            actor,
            "首次封口自动择定：关窗与物化同库事务（甲路径）",
            now,
        ),
    )
    return {
        "kind": LEDGER_KIND_DECIDE,
        "path": PATH_A,
        "actor": actor,
        "detail": "首次封口自动择定：关窗与物化同库事务（甲路径）",
        "decided_at": now,
    }


def _record_deviation(conn, actor: str, requested: str, detail: str) -> None:
    conn.execute(
        """INSERT INTO seal_unify_ledger(kind,path,actor,detail,decided_at)
           VALUES (?,?,?,?,?)""",
        (LEDGER_KIND_DEVIATION, requested, actor, detail, _now()),
    )


def run_seal(conn, wid: int, user: str, body: SealIn) -> dict:
    """甲路径合一写。调用方负责 BEGIN IMMEDIATE 与最终 commit/rollback。

    窗口状态、物化台账、审计在同一事务内写入，任一失败整体回滚。
    """
    decision = _bootstrap_decision(conn, user)
    if decision["path"] != PATH_A:
        # 册被改成非甲口径即偏离本仓写路径，拒写。
        raise HTTPException(409, "封口合一册偏离甲路径，拒写")

    w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
    if not w:
        raise HTTPException(404, "窗口不存在")

    # 已封口：幂等返回既有物化，不新增台账、不记争用。
    if w["status"] == "已关闭":
        mat = conn.execute(
            "SELECT * FROM materializations WHERE window_id=? ORDER BY id DESC LIMIT 1",
            (wid,),
        ).fetchone()
        return {
            "ok": True,
            "already": True,
            "seal_token": w["seal_token"],
            "row_count": mat["row_count"] if mat else 0,
            "total_amount": mat["total_amount"] if mat else 0,
            "partition_key": mat["partition_key"] if mat else None,
        }

    rows = conn.execute(
        """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
           FROM events WHERE window_id=? AND disposition='窗内'""",
        (wid,),
    ).fetchone()
    now = _now()
    seal_token = f"seal-{wid}-{w['generation']}-{now}"
    partition = body.partition_key or f"{w['name']}-g{w['generation']}"

    # 夹具/演练注入：模拟物化阶段失败，检验整体回滚。
    if body.fail_simulate:
        raise HTTPException(500, "模拟物化失败（夹具用）：窗态与台账整体回滚")

    # —— 同一事务内的两笔关键写：窗态封口 + 物化台账 ——
    conn.execute(
        "UPDATE windows SET status='已关闭', seal_token=? WHERE id=?",
        (seal_token, wid),
    )
    conn.execute(
        """INSERT INTO materializations(window_id,status,row_count,total_amount,partition_key,created_at)
           VALUES (?,?,?,?,?,?)""",
        (wid, "成功", rows["c"], rows["s"], partition, now),
    )
    write_audit(
        conn,
        user,
        "seal_unify_ok",
        str(wid),
        {
            "path": PATH_A,
            "seal_token": seal_token,
            "row_count": rows["c"],
            "total_amount": rows["s"],
            "partition": partition,
        },
        created_at=now,
    )
    return {
        "ok": True,
        "already": False,
        "seal_token": seal_token,
        "row_count": rows["c"],
        "total_amount": rows["s"],
        "partition_key": partition,
    }


def seal_window(wid: int, user: str, body: SealIn | None = None) -> dict:
    """合一锁 + 同事务封口的完整服务入口（被 /seal、/close、/materialize 复用）。"""
    body = body or SealIn()
    lock_key = f"seal_unify:{wid}"
    conn = connect()
    try:
        # 合一锁在独立短事务内申请并持久化，确保争用登记不被业务回滚带走。
        if not locks.try_acquire(conn, lock_key, user):
            conn.commit()
            raise HTTPException(409, "争用：关窗物化合一锁被占用，后到只记争用")
        conn.commit()

        # BEGIN IMMEDIATE：立即拿库级写锁，封口期间冻结一切入账写。
        conn.execute("BEGIN IMMEDIATE")
        try:
            result = run_seal(conn, wid, user, body)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        return result
    except HTTPException:
        # 404/409/500 等业务失败：锁若仍在本会话名下则清理，避免悬挂。
        try:
            locks.release(conn, lock_key, user)
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()
        raise
    except Exception:
        conn.rollback()
        locks.release(conn, lock_key, user)
        conn.commit()
        raise
    finally:
        # 成功路径：业务已封口（或幂等），释放合一锁。
        try:
            locks.release(conn, lock_key, user)
            conn.commit()
        except Exception:  # noqa: BLE001
            conn.rollback()
        conn.close()


def seal_window_with_failure_audit(wid: int, user: str, body: SealIn) -> dict:
    """封口失败时，在业务回滚后另开事务补一条失败审计（不污染同事务口径）。"""
    try:
        return seal_window(wid, user, body)
    except HTTPException as exc:
        if exc.status_code == 500 and body.fail_simulate:
            with connect() as audit_conn:
                write_audit(
                    audit_conn,
                    user,
                    "seal_unify_fail",
                    str(wid),
                    {"fail_simulate": True, "note": "窗口与物化已整体回滚"},
                )
                audit_conn.commit()
        raise


@router.post("/api/windows/{wid}/seal")
def seal(wid: int, body: SealIn = SealIn(), authorization: AuthHeader = None):
    user = require_user(authorization)
    return seal_window_with_failure_audit(wid, user, body)


@router.get("/api/seal-register")
def get_register(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        decision = get_decision(conn)
        if not decision:
            return {"path": None, "kind": None, "decided_at": None}
        return decision


@router.post("/api/seal-register")
def register_path(body: SealRegisterIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    if body.path not in (PATH_A, PATH_B):
        raise HTTPException(400, "path 须为甲（关窗物化同事务）或乙（封口影子再原子物化）")
    with connect() as conn:
        decision = get_decision(conn)
        if decision is None:
            if body.path != PATH_A:
                # 本仓只实现甲路径：乙从未落地，无册可改。
                _record_deviation(
                    conn,
                    user,
                    body.path,
                    "本仓唯一落地口径为甲路径，乙（close_draft 影子两段）未实现，拒写",
                )
                conn.commit()
                raise HTTPException(409, "乙路径未在本仓落地，禁止混用，已记合一偏了")
            conn.execute(
                """INSERT INTO seal_unify_ledger(kind,path,actor,detail,decided_at)
                   VALUES (?,?,?,?,?)""",
                (LEDGER_KIND_DECIDE, PATH_A, user, "显式择定甲路径", _now()),
            )
            conn.commit()
            return get_decision(conn)

        # 已有择定：当天禁止改口。
        if body.path == decision["path"]:
            return decision
        if decision["decided_at"][:10] == _today():
            _record_deviation(
                conn,
                user,
                body.path,
                f"当天已择定{decision['path']}路径，申请改口{body.path}被拒，两路径不得混用",
            )
            conn.commit()
            raise HTTPException(409, "当天禁止改口：偏离封口合一册，已记合一偏了")
        # 跨日仍只允许本仓落地的甲路径，乙永不开放。
        _record_deviation(
            conn,
            user,
            body.path,
            f"跨日申请{body.path}，本仓唯一口径为甲，拒写",
        )
        conn.commit()
        raise HTTPException(409, "乙路径未在本仓落地，禁止混用，已记合一偏了")
