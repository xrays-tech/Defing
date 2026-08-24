# 设计文档：项目管理员（Project Admin）功能

状态: v3（已吸收 Oracle 两轮审核：R1 B1-B6/N1-N10，R2 B7-B9/N11-N17，待终审）
日期: 2026-08-15
范围: dsh-core / dsh-api / dsh-observability / dsh-publish / dsh-testkit / dsh-jobs / dsh-raft

## 1. 背景与目标

当前 dsh 只有一个全局管理员（`--admin-password` / `set-password`，单会话）。多项目场景下需要「项目管理员」：仅管理**所属项目**配置、可发布，但**不能修改共享配置**（共享项/共享草稿/共享发布/共享引用绑定），也不能触碰全局面（账号、集群、其他项目）。

### 已确认的需求决策（与用户逐条确认）

| 决策点 | 结论 |
| --- | --- |
| 账号创建 | 全局管理员通过 API 创建/删除/改密 |
| 账号-项目映射 | 一个账号只管一个项目；一个项目可有多个账号 |
| 登录端点 | 复用 `POST /api/v1/login`，新增可选 `username` 字段；缺省=全局管理员（向后兼容） |
| 会话策略 | 每账号单会话（互不踢线）；全局管理员保留现状单会话 |
| 结构修改 | 项目管理员可修改**所属项目的非共享结构**（结构项定义本身不涉及共享实体） |

## 2. 账号模型

新实体 `ProjectAdminAccount`（dsh-core/model.rs，参照 `AdminSession` 模式）：

```
ProjectAdminAccount {
  username: String,            // 集群内唯一（校验：[A-Za-z0-9_-]{2,64}，禁用 "admin"）
  project: ProjectId,          // 所属项目（必须已存在）
  salt: String,                // 每账号随机 16B hex
  password_hash: String,       // SHA-256(salt || password) hex
  created_at: u64,
}
```

- KV：`adm/pa/{username}`（keys.rs 新增前缀）。唯一性由 key 保证；创建时校验项目存在。
- `list_project_admins(project)` 需全量扫 `adm/pa/` 前缀过滤——**O(账号数)**，账号量小可接受（设计取舍，明示）。
- 密码哈希加盐 SHA-256（现状全局管理员为无盐比对，不构成劣化）；不引入新依赖。

## 3. 会话模型与 token 路由

**多会话按主体分 key**：全局管理员 `sess/admin`（不变）；项目管理员 `sess/pa/{username}`。

**token 自带主体路由信息**（解决中间件 O(1) 定位，无缓存一致性问题）：

```
全局管理员 token: "adm." + base64url(32B random)
项目管理员 token: "pa." + urlsafe(username) + "." + base64url(32B random)
```

中间件解析 token 前缀 → 直接定位唯一会话 key → 恰好一次 KV 读（与现状同量级）。username 字符集 `[A-Za-z0-9_-]` 保证 token 无歧义分隔。明文 token 仍不落库。

**升级兼容（N12）**：无前缀的旧格式 token → fallback 到 `sess/admin`（滚动升级期间旧会话不失效；剥掉 `pa.` 前缀伪造的 token 会路由到 sess/admin 但 hash 比对必败）；集群启用 PA 功能的操作顺序 = **先全集群升级到新版本，再创建 PA 账号**（混合版本集群中旧节点无法反序列化新命令变体，写入 docs 升级说明）。`/metrics`、`/admin` 静态 UI 不在 `/api/v1` 管辖内，维持现状（N20）。

`AdminSession` 扩展（serde 向后兼容，需 `impl Default for Principal = Admin`）：

```
AdminSession {
  token_hash, issued_at, expires_at, device_id,     // 不变
  #[serde(default)] principal: Principal,           // 旧数据无此字段 → Admin
}
Principal 外部标签形如 {"kind":"admin"} / {"kind":"project_admin","username","project"}
```

**会话生命周期约束（状态机确定性）**：
- apply 层**只判断 `is_some()`**（与现状 `state.rs:1336` 同语义），**不读墙钟**（D16 约定，apply 在各节点重放结果必须一致）。
- 过期会话的重登在 **API 层**处理：login 时先 `get_session` 查该主体，若存在且已过期 → 先提交 `SessionLogout` 再提交 `SessionLogin`；未过期 → 409 `ERR_SESSION_IN_USE`。

### 3.1 命令设计（Raft wire 兼容，B1/N10）

**保持现有变体不动**（旧日志重放安全），**纯新增**：

```
Command::SessionLogin        // 现有，admin 专用，不动（保留 issued_at 语义）
Command::SessionLogout       // 现有 unit variant，不动（admin 专用）
新增：
ProjectAdminCreate { project, username, salt, password_hash }
ProjectAdminDelete { username }                 // apply 级联删 sess/pa/{username}
ProjectAdminSetPassword { username, salt, password_hash }  // apply 级联删会话
PaSessionLogin { username, token_hash, issued_at, expires_at, device_id }
PaSessionLogout { username }
PaSessionHeartbeat { username, expires_at: Option<i64> }   // 语义照抄现有 SessionHeartbeat（command.rs:99）：None=永不自动过期，Some=续期到该时刻
```

**operator 贯穿**（审计与版本记录身份）：以下既有命令**新增字段全部 `#[serde(default)] operator: String`**（外部标签加字段向后兼容，旧日志 default 空串 → apply 映射 "admin"）：
`DraftUpdate / Publish / StructureDraftSet / StructurePublish / Rollback / BranchCreate / BranchDelete / ProjectCreate / ProjectDelete / SharedDraftUpdate / SharedPublish / RefBind / RefUnbind`（注：Promote 非 Raft 命令，是 API 层组合 DraftUpdate 实现，不在清单（N19））
构造点更新（**以 `rg 'Command::' server/` 全量输出为准**，已核对的清单）：dsh-api 各 handler、`dsh-publish/src/lib.rs` 四个公开方法（:117,139,175,204 签名加 `operator: &str`）、`dsh-testkit/src/lib.rs`（:58,64,72,80,100 共 5 处）、`dsh-jobs/src/lib.rs`（:160-337 共 12 处，`#[cfg(test)]`）、`dsh-raft/tests/cluster.rs`、`snapshot_persist.rs`、`forward_hint.rs`、`dsh-api/tests/grpc_data_plane.rs`。
`VersionRecord.operator`（state.rs:675,881,943 硬编码 "admin"）改取命令 operator；级联仍记 "shared"（state.rs:1166）。

## 4. 授权矩阵（auth_middleware 改造）

中间件解析 Bearer → 按 token 前缀定位会话 key → 校验未过期 → `Principal` 注入 request extensions。豁免路径不变（login/healthz/readyz/cluster-join）。

**路径提取安全规则（N2）**：中间件从 `uri().path()` 提取 `/api/v1/projects/{p}/...` 中的 `{p}`（未解码），强制 `valid_name`（`[a-z0-9-]`，state.rs:47）校验；不匹配（含 URL 编码 `%70`/`%2F`、大写、尾斜杠）→ 按「非项目路径」处理 → 对 PA **默认拒绝**。无绕过面。

**全局管理员：全部端点（现状不变）。**

**项目管理员（所属项目 P）：**

| 类别 | 端点 | 结论 |
| --- | --- | --- |
| 自身会话 | `POST /logout`、`POST /heartbeat` | ✅ 允许（principal 从 extensions 取，不可从 body 注入）——**显式放行，防 PA 过期后锁死** |
| 项目详情 | `GET /projects/{p}` | ✅ 仅限 P |
| 项目配置-结构 | `GET/PUT /projects/P/structure-draft`、`POST .../structure-draft/publish` | ✅ 允许 |
| 项目配置-值 | `GET/PUT /projects/P/branches/{b}/draft`、`POST .../publish`、`POST .../rollback`、**`GET .../config`（含 ?reveal=true，本项目）** | ✅ 允许 |
| 版本历史 | `GET /projects/P/branches/{b}/versions` | ✅ 仅限 P |
| 分支对比 | `GET /projects/P/diff` | ✅ 仅限 P |
| 分支管理 | `POST /projects/P/branches`、`DELETE /projects/P/branches/{b}` | ✅ 允许 |
| promote | `POST /projects/P/promote` | ✅ 允许 |
| 读项目列表 | `GET /projects` | ✅ 过滤为仅 P |
| 审计 | `GET /audit` | ✅ handler 层强制过滤 `project=P`（principal 来自 extensions，无 query 绕过面；`get_audit` 需扩展 project 参数） |
| 共享配置-读 | `GET /shared`、`GET /shared-draft` | ✅ 允许（只读；secret 值已掩码；`GET /shared` 的 refs 过滤为仅自己项目，N11） |
| 共享配置-写 | `POST /shared`、`PUT /shared-draft`、`POST /shared/publish`、`DELETE /shared/{key}`、`DELETE /shared-draft/{key}` | ❌ 拒绝 403 |
| 共享引用-写 | `POST/DELETE /shared/refs` | ❌ 拒绝（绑定共享 secret → 物化进项目可读，构成权限升级） |
| 共享引用-读 | `GET /shared/refs` | ✅ 允许（只读）；handler 强制覆写 `project=P`（RefsQuery.project 来自 query，N11，防跨项目元数据读取） |
| 管理语义 reveal | `/v1/projects/P/branches/{b}/config?reveal=true`（不在 /api/v1 下！） | ✅ 允许且仅限 P——**该端点现有手动会话校验（lib.rs:1346-1377）改为按 Principal 校验归属，否则 PA 可解密任意项目 secret（B2 权限提升漏洞）**；审计已有。与中间件共用 `resolve_principal` helper，禁止手抄第三份 token 解析（N15） |
| 项目面 | `POST /projects`、`DELETE /projects/{p}` | ❌ 拒绝 |
| 跨项目 | 任何 `/projects/{其他}` 路径 | ❌ 拒绝 |
| 账号/全局 | `POST /admin/set-password`、`force-logout`、PA 账号管理端点 | ❌ 拒绝 |
| 集群 | join/promote/remove、rotate-master-key、snapshot、`GET /cluster/members`、`GET /admin/retention-status` | ❌ 拒绝（N17：后两个默认拒绝端点显式列出供逐行测试断言） |
| 数据面 gRPC/SSE watch | 不变（全局 data-plane token，只读；watch 无鉴权为既有姿态，另行立项） |

实现：**默认拒绝、显式放行**（全局面端点白名单）。

## 5. API 设计（新增端点，均全局管理员专用）

```
POST   /api/v1/projects/{p}/admins            {"username","password"}     创建
GET    /api/v1/projects/{p}/admins            → [{"username","created_at"}]（不返回哈希/盐）
DELETE /api/v1/projects/{p}/admins/{username} 删除（连带删会话）
PUT    /api/v1/projects/{p}/admins/{username} {"password"}               改密（连带删会话）
```

错误码新增：`ERR_BAD_CREDENTIALS`(401，login 失败统一用此码，替换现有 `"ERR_FORBIDDEN"`+401 的码位冲突 N4；文案统一不区分账号是否存在，防枚举)、`ERR_FORBIDDEN`(403)、`ERR_ACCOUNT_EXISTS`(409)、`ERR_ACCOUNT_NOT_FOUND`(404)。

**登录细节**：
- login 失败补审计 `login_failed`（记 username，不记密码，N6）。
- **非 leader 节点 login 转发体必须透传 `username`**（改 lib.rs:1483-1489 硬编码的 `LoginReq { password }`，N1）。
- login 成功响应 `{"token","role":"admin"|"project_admin","project"?}`；PA token 即 `pa.{username}.{secret}` 格式。
- **过期重登的并发加固（N13）**：API 层 logout+login 序列中，若 login 收到 `SessionInUse`，复查该会话确已过期则重试一轮（有界，仅一次）；并发双 login 最终恰一会话成立。
- **force-logout 恢复 username 参数（N16）**：`POST /api/v1/admin/force-logout {"username"?}`——缺省踢全局管理员会话；指定 username 踢对应 PA 会话（不动账号本体，比改密/删号的运维破坏性小）。

**级联**：`DELETE /projects/{p}` 的 apply 级联删除该 project 全部 PA 账号及其会话（读 `adm/pa/` 前缀过滤 → 逐个删 `sess/pa/{u}` → 删账号）。apply 单线程持锁执行，顺序无死锁面。

## 6. 审计

- `dsh-observability::AuditLog::append` 加 `operator` 参数（替换硬编码 "admin"，lib.rs:37）；dsh-api 全部调用点传 `principal.operator()`（`"admin"` / `"pa:{username}"`）。
- PA 登录/登出/心跳、login_failed 均审计。
- `GET /audit` 对 PA 强制 `project=P` 过滤；`project=None` 条目（login/logout/shared_publish）对 PA 不可见——shared_publish 级联改 PA 项目值但来源审计不可见，**取舍**：级联产生的审计条目落 project 字段（在 cascade 路径补充），否则文档记录（开发时按实现成本取前者优先）。
- `dsh_session_active` 指标（observability lib.rs:126）改为 admin||PA 聚合或 label 区分，避免语义退化（N7）。

## 7. 兼容性

1. `login` 不带 username → 与现状完全一致（CLI/旧脚本/example 不受影响）。
2. 旧会话 JSON 无 `principal` → default `Admin`；旧命令无 `operator` → default 空串 → "admin"。
3. **既有命令变体形状不变**（SessionLogin/SessionLogout 保持原样，PA 用新变体）→ Raft 日志重放安全（滚动升级）。
4. 数据面（gRPC/SSE watch）零变化。
5. dev-single 重启丢会话不变；集群模式账号与会话随 Raft 持久化。

## 8. 测试计划（TDD：先写测试）

**dsh-core/tests/state_machine.rs（状态机层，M1）**
- PA 创建/重复创建(ACCOUNT_EXISTS)/删除/改密；项目不存在创建失败；禁用名 "admin"
- 删除项目级联删 PA+会话；删除账号/改密后旧会话 key 消失
- `PaSessionLogin` per-username 单会话（is_some 判定，不涉时钟）；PA 与全局管理员会话并存互不影响
- `VersionRecord.operator` 记录 `pa:{username}`；旧命令 default → "admin"
- operator 字段 serde 兼容：无 operator 字段的 JSON 反序列化成功

**dsh-api/tests/http_project_admin.rs（新建 HTTP 集成测试，M2，参照 grpc_data_plane.rs start_server 模式）**
- 全局管理员创建 PA → PA 登录 → **授权矩阵逐行断言**（含：logout/heartbeat 可用且续期；共享库只读放行（`GET /shared`、`GET /shared-draft` → 200，secret 值掩码、refs 仅自己项目）而写全组 403；`GET /cluster/members`、`GET /admin/retention-status` 403；跨项目 403；账号管理 403；集群端点 403）
- **路径绕过组**：`%70`(编码p)、大写、尾斜杠、`%2F` → 全部 403
- **token 路由负形测试（N14）**：伪造前缀（PA secret 拼 `adm.`、空 username、`pa.admin.x`）、截断 token → 全部 401
- **`/v1/.../config?reveal=true`**：PA 只能 reveal 自己项目（B2 回归用例）；PA reveal 其他项目 403
- `GET /projects` 过滤；`GET /audit` 强制过滤且无 query 绕过；`GET /shared/refs` 强制覆写 project
- 错误密码/不存在账号 → 401 同文案（防枚举）；login_failed 有审计
- PA 改密/删除后旧 token 401；force-logout 带 username 踢 PA 会话（N16）
- 过期会话重登（API 层 logout+login 序列）；**并发重登恰一会话**（N13）
- heartbeat 续期后 TTL 顺延（B7 回归）
- 非 leader login 转发带 username（dsh-raft cluster 测试环境）
- 审计 operator 断言（登录/发布/reveal 的 operator 值）

**testkit**：加 `seed_project_admin(project, username, password)` 辅助（供集成测试与未来 CLI 复用）。

**回归**：`cargo test --workspace` 全绿（含既有 grpc_data_plane / state_machine / cluster）；docker 重建后 example 全局管理员流程不回归；PA 矩阵手测脚本一遍。

## 9. 开发计划（阶段与验收，B6 修订）

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| M1 dsh-core（纯新增，不动现有变体） | model/keys/command（PA 命令 + operator 字段 serde default）/state apply + 访问器 + 级联 + **core 测试先行**。operator 字段加入既有命令会破坏全 workspace 编译——**M1 阶段同步补齐全部构造点**（以 `rg 'Command::' server/` 全量为准：dsh-api handlers、dsh-publish、dsh-testkit、dsh-jobs、dsh-raft tests、grpc_data_plane 测试） | `cargo test -p dsh-core` 绿 + **`cargo check --workspace --all-targets`** 绿（B9：--all-targets 才覆盖 tests/ 与 cfg(test)，普通 build 有洞） |
| M2 dsh-api | 登录改造（token 路由前缀 + username 透传 + 非 leader 转发）+ PA 账号管理端点 + auth_middleware Principal + 授权矩阵（含 /v1 reveal 修复）+ operator 贯穿（observability + 调用点）+ 审计过滤 + **HTTP 集成测试先行** | `cargo test --workspace` 绿 |
| M3 端到端 | docker 重建 dsh；实测矩阵（PA 登录/授权边界/会话/审计/reveal）；example 回归 | 实测全过；Oracle review 零阻塞 |

规模预估（吸收 B3/B6 后上调）：核心 ~700 行 + 测试 ~700 行。

## 10. 风险与权衡

1. **auth_middleware 是唯一强制点**——默认拒绝表 + 集成测试逐端点断言（含路径绕过组）。
2. **共享 secret 升级路径封死**：PA 无 shared 写权（只读放行、secret 值恒掩码）+ 无 refs 写权；reveal 修复后仅限本项目。
3. **SHA-256 快哈希**与现状同水位（PA 加盐优于现状 admin 无盐）；argon2 另立需求。
4. **会话定位 O(1)**：token 前缀路由，无缓存、无扫描。
5. **Raft wire 兼容**：既有变体不动 + 新字段全 serde default；旧日志/旧命令重放安全。
6. **apply 不读墙钟**：过期重登在 API 层组合 logout+login，状态机确定性保持。

## 11. 明确不做（本期）

- CLI 的 PA 管理子命令、账号级限流、密码复杂度策略、argon2、账号多项目、gRPC 数据面按项目凭据、Web 控制台、watch SSE 鉴权（既有姿态，另行立项）。
