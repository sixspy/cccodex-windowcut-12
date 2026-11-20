# 业务窗关账

> 遵守 `ops/底座生成前必读_效率与技术栈.md`（**≥3× 厚度**）。  
> FastAPI + Vue 3 + SQLite；交付须 `docker compose up -d --build` 一键起。  
> **禁止同构换皮**：本仓主链路、表结构、接口与页面均独立。

## 骨架一句话

事件按业务窗裁切，迟到进旁路或拒收，关窗后物化结果并与窗口对账。

## 基本信息

| 项 | 值 |
|----|-----|
| 产品名（首页） | 业务窗关账 |
| 目录 | `08-windowcut` |
| 接口 | http://127.0.0.1:11022 |
| 库 | `data/app.db` |
| 账号 | admin / 123456 |

## 厚度（相对薄壳 ≥3×）

| 项 | 目标 |
|----|------|
| 业务 LOC | ≥900（目标 1000+） |
| 后端具名模块 | `db` / `auth` / `locks` / `windows_svc` / `ingest_svc` / `late_svc` / `materialize_svc` / `reconcile_svc` / `audit_svc` / `hold_svc` / `reopen_svc` |
| 前端视图 | 业务窗 / 入账 / 迟到桶 / 物化 / 对账 / 审计争用 |
| 空桩 | hold 晋升、迟到补入、分区物化、窗重开 |

## 为何能出多题

开窗关窗、迟到策略、窗重开、部分物化失败回滚、旁路暂存 vs 拒收、争用锁与审计。

## 互斥切口候选

- 迟到拒收 vs 进迟到桶
- 物化失败整窗回滚 vs 保留成功分区
- 窗重开作废重算 vs 增量补丁合并
- 切版旁路暂存 vs 开始瞬间拒收

## 封口合一写路径（甲路径，唯一口径）

关窗与物化不再是两次点击、两段独立写。本仓择定并落地**甲路径**，择定写入
`seal_unify_ledger`（封口合一册），当天禁止改口；申请乙路径或改口一律拒写
（409）并在册内记『合一偏了』，两路径不得混用，乙（`close_draft` 影子两段）
在本仓不落地。

- `POST /api/windows/{wid}/seal`：合一写入口。`BEGIN IMMEDIATE` 取得库级写锁后，
  在**同一 SQLite 事务**内更新窗口状态（含 `seal_token`）并插入物化行与
  `seal_unify_ok` 审计；任一步失败整体回滚，窗态、台账、审计同生共死。
- `/close` 与 `/materialize` 旧入口保留并委托同一合一写路径：未关窗调物化仍
  409；已封口窗幂等返回既有物化，不新增台账。
- 合一锁 `seal_unify:{wid}`：两人同时对同窗关窗/物化，后到者只在 `contention_log`
  记争用并 409，不写业务。
- 入账加固（`ingest_svc`）：窗内分支 `INSERT` 前先 `BEGIN IMMEDIATE` 复检窗态；
  封口事务持锁期间入账在锁外等待，封口提交后复检到『已关闭』即改判迟到桶，
  封口决定与物化口径之间无可脏间隙。
- 五处同态：窗口状态、物化台账、事件入账、对账、审计随同一事务一致推进。

## 启动（一键）

```powershell
cd projects/08-windowcut
docker compose up -d --build
```

浏览器：http://127.0.0.1:11022/  
健康检查：GET http://127.0.0.1:11022/api/health

本机开发：

```powershell
python -m pip install -r requirements.txt
python -m uvicorn app:app --app-dir backend --host 127.0.0.1 --port 10605
python -m pytest -q
```
