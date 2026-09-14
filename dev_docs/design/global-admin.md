# 设计文档：多全局管理员（Global Admin）与超级管理员

状态: v1（待终审）
日期: 2026-09-14
范围: dsh-core / dsh-api / Admin UI / api/openapi / schema / docs / README / dev_docs。**零改动**：dsh-observability、dsh-testkit、dsh-jobs、dsh-raft、dsh-publish、proto/、sdk/、dsh-cli（D4：本期不新增 GA CLI 子命令，`defing admin` 现有语义不动）
上游: dev_docs/design/project-admin.md（v3，本设计与其同构，复用其全部已验证模式）

## 1. 背景与现状（代码调查结论）

现状是「单管理员 + 项目管理员」两级模型：

| 维度 | 现状实现 | 位置 |
| --- | --- | --- |
| 唯一全局管理员 | 节点配置 `--admin-password`；或 `POST /api/v1/admin/set-password` 落状态机键 `sess/admin-pw`（argon2 PHC 优先，未设置回退节点配置） | cli/main.rs:138,681；api/lib.rs:2775-2782；core/state.rs:2831-2839 |
| 身份表示 | `Principal::Admin` 是无字段 unit 变体——**没有任何账号身份**，审计操作人恒为字符串 `"admin"` | core/model.rs:438-450；api/lib.rs:3145-3150 |
| 会话 | `sess/admin`（旧单会话）+ `sess/admin/{sid}`（多会话）；token `adm.{sid}.{secret}` | core/keys.rs:15,100-104；api/lib.rs:2736 |
| 项目管理员 | `adm/pa/{username}` → `ProjectAdminAccount{salt,password_hash,created_at}`；token `pa.{u}.{sid}.{secret}`；会话 `sess/pa/{u}/{sid}` | core/model.rs:454-466；core/state.rs:2543-2618 |
| 鉴权唯一强制点 | `auth_middleware` → `resolve_principal` → `pa_allowed`（「非 PA 即全放行」的二元矩阵） | api/lib.rs:442, 608, 663 |
| 忘记密码 | `defing admin reset-password`（Bearer = 节点 `--join-token`，写 `sess/admin-pw` + 踢全部管理员会话） | api/lib.rs:3932；cli/main.rs:252 |
| 密码哈希 | argon2id PHC（`$argon2` 前缀），旧数据 `sha256(salt\|\|pw)` 兼容分支 | api/lib.rs:2102-2134 |

**核心结论**：现有「管理员」是一个**匿名、单例的根凭据**，不是账号实体。新增「多个全局管理员」必须解决三件事：

1. **账号实体与凭据**（新 KV 前缀 + 新命令变体，参照 `ProjectAdminAccount` 模式）；
2. **`Principal` 的身份字段**——否则审计无法区分不同管理员（现状 operator 恒为 `admin`）；
3. **授权矩阵由二元升级为三级**（super / global / project），且必须逐端点显式化。

会话与 token 路由**不需要新机制**：project-admin 已验证的 `{prefix}.{username}.{sid}.{secret}` + O(1) 前缀路由模式可直接复用。

## 2. 需求与已确认决策

用户需求：

1. 可以有多个全局管理员，由 super-admin 增加 / 删除 / 重置密码；
2. 全局管理员登录后可管理**所有项目**的配置，以及**共享配置**；
3. 任何管理员登录后可以修改**自己的**密码。

已确认决策（本次与用户逐条确认）：

| # | 决策点 | 结论 |
| --- | --- | --- |
| D1 | 全局管理员权限范围 | **最小集**：仅「全部项目配置 + 全部共享配置」。项目创建/删除、项目访问令牌、集群写、快照、主密钥轮换、他人账号与改密，一律仍仅 super-admin |
| D2 | super-admin 身份 | **沿用现有根账号**（`--admin-password` / `sess/admin-pw`）：登录时不填用户名即为它，零迁移、向后兼容；`defing admin reset-password`（ops token）仍是其唯一找回通道 |
| D3 | 用户名命名空间 | **GA 与 PA 全局唯一、创建时互斥**。登录体只有一个 username，命名唯一即无歧义；冲突返回 409 |
| D4 | CLI 范围 | 本期**只做 API + Admin UI**；不新增 `defing admin` 的 GA 子命令 |

## 3. 角色模型

三级角色。`Principal` 新增 1 个变体，既有 2 个变体语义不变：

| 角色 | Principal | 凭据来源 | 权限边界 | 审计 operator |
| --- | --- | --- | --- | --- |
| 超级管理员 super-admin | `Admin`（不变） | 节点配置 / `sess/admin-pw` | 全部（现状不变） | `admin` |
| 全局管理员 GA（新） | `GlobalAdmin{username}` | `adm/ga/{u}` | 全部项目配置 + 全部共享配置（D1） | `ga:{username}` |
| 项目管理员 PA | `ProjectAdmin{username,project}` | `adm/pa/{u}` | 所属项目配置 + 共享只读（不变） | `pa:{username}` |

`Principal` 变体名与 serde wire（**注意**：现状实际是 PascalCase，`project-admin.md:61` 写的 `{"kind":"admin"}` 是文档漂移，见 §12）：

```json
{"kind":"Admin"}
{"kind":"ProjectAdmin","username":"alice","project":"alpha"}
{"kind":"GlobalAdmin","username":"alice"}
```

审计 operator 字符串 `"admin"` **不改为** `"super"`：历史审计条目不可改写，文档注明其含义为「超级管理员」。

## 4. 账号模型

```rust
/// 全局管理员账号（跨项目；由超级管理员创建/删除/改密）。
/// 与 ProjectAdminAccount 同构，无 project 字段；salt 为保留字段
/// （新格式盐内嵌 PHC，置空；旧 sha256(salt||pw) 兼容分支照旧）。
pub struct GlobalAdminAccount {
    pub username: String,       // 集群内唯一（与 PA 互斥），[A-Za-z0-9_-]{2,64}，禁用 admin（不区分大小写）
    pub salt: String,
    pub password_hash: String,  // argon2id PHC
    pub created_at: i64,
}
```

- KV：`adm/ga/{username}`（keys.rs 新增 `K_GA_ACCOUNT` / `global_admin_key()`；`list_global_admins` 前缀扫，O(账号数)，量小可接受）。
- 唯一性：`adm/ga/{u}` 保证 GA 内部唯一；**GA↔PA 互斥在 apply 层强制**（`apply_global_admin_create` 查 `adm/pa/{u}`；`apply_project_admin_create` 查 `adm/ga/{u}`；冲突 → `ErrorKind::Conflict` → 409）。必须落在状态机而非仅 API 层，才能保证多节点重放一致。
- **唯一性口径（D3 的精确化）**：GA↔PA 互斥**不区分大小写**——除精确键比对（快路径）外，再对另一命名空间做 `eq_ignore_ascii_case` 扫描，命中即 Conflict；否则 `ga:Alice` 与 `pa:alice` 可并存，而 D3 与本节「审计里视觉难辨」的理由同样适用于这一对。只影响**创建**校验，既有账号不受影响。

  **GA 内部仍是精确键唯一**（`ga:alice` 与 `ga:Alice` 可并存，`adm/ga/` 是单一精确键空间）——这是本期的显式取舍：只对 GA↔PA 做大小写折叠，因为跨命名空间的混淆（`ga:`/`pa:` 前缀相同、仅用户名大小写不同）比同一命名空间内更易在审计里误读。
- 用户名校验：复用现有 `valid_pa_username`（state.rs:2533），重命名为 `valid_account_username` 供两个实体共用（2-64 位 `[A-Za-z0-9_-]`），**保留名改为大小写不敏感**：`!name.eq_ignore_ascii_case("admin")`——否则 `Admin`/`ADMIN` 可注册成 GA，与超级管理员在审计里视觉难辨（`ga:Admin` vs `admin`）。这是本次唯一一处对既有 `valid_pa_username` 行为的改动（只影响创建校验，不影响既有账号）。
- 不设「最后一个 GA 不可删」约束：super-admin 不是 GA 记录，不存在自锁面。

## 5. 会话与 token 路由

| 主体 | 会话键 | token |
| --- | --- | --- |
| super-admin | `sess/admin`（旧）+ `sess/admin/{sid}` | `adm.{sid}.{secret}`（旧：`adm.{secret}`） |
| GA | `sess/ga/{username}/{sid}` | `ga.{username}.{sid}.{secret}` |
| PA | `sess/pa/{username}/{sid}` | `pa.{username}.{sid}.{secret}` |

- `resolve_principal`（api/lib.rs:442）新增 `ga.` 分支：`rest.split('.')` 且 `len >= 3` → `get_ga_session_with(username, sid)`；格式不符 → `Err`（401）。username 字符集不含 `.`，分隔无歧义。
- `token_session_id`（api/lib.rs:499）同步新增 `ga.` 分支（logout/heartbeat 用）。
- **主体以状态机中存储的 `principal` 为准**（token 只做 O(1) 路由，不可伪造身份归属）——沿用现有纪律（api/lib.rs:488-489）。
- GA 不提供旧格式单会话键：全新实体，无历史数据需要兼容。
- 前缀空间互不重叠：`sess/admin*` / `sess/ga/*` / `sess/pa/*`，因此现有 `MultiSessionLogoutAll`（只清 `sess/admin*`）语义不变——这是 super 改密不误踢 GA 会话的结构性保证（需回归测试断言）。

## 6. 命令设计（Raft wire，纯新增）

**既有变体一律不动**（旧日志重放安全）。新增 8 个：

```rust
GlobalAdminCreate { username: String, salt: String, password_hash: String, #[serde(default)] ts: i64 }
GlobalAdminDelete { username: String }                          // 级联删其全部会话
GlobalAdminSetPassword { username: String, salt: String, password_hash: String }  // 级联删其全部会话
MultiGaSessionLogin { username, token_hash, issued_at, expires_at: Option<i64>, device_id, session_id }  // 与 MultiPaSessionLogin 同形
MultiGaSessionLogout { username, session_id }
MultiGaSessionHeartbeat { username, session_id, expires_at: Option<i64> }
MultiGaSessionLogoutAll { username }
GlobalAdminSessionsRevokeAll                                     // unit：踢全部 GA 账号的全部会话
```

apply 语义（**全部不读墙钟**，D16 确定性；apply 层只判 `is_some()`）：

| 命令 | 语义 |
| --- | --- |
| `GlobalAdminCreate` | 校验用户名 → `adm/ga/{u}` 已存在 → Conflict；`adm/pa/{u}` 已存在 → Conflict；→ save |
| `GlobalAdminDelete` | 不存在 → NotFound；→ 删账号 + `delete_all_ga_sessions(u)` |
| `GlobalAdminSetPassword` | 不存在 → NotFound；→ 覆盖 hash + `delete_all_ga_sessions(u)` |
| 会话三件套 | 照抄 `apply_multi_pa_session_*`（state.rs:2469-2515），键换 `sess/ga/{u}/{sid}` |
| `GlobalAdminSessionsRevokeAll` | 扫 `adm/ga/` 前缀取全部用户名 → 逐个 `delete_all_ga_sessions` |

对既有 `apply_project_admin_create`（state.rs:2543）的唯一改动：增加 GA 同名冲突检查。确定性读、各节点重放结果一致，滚动升级安全。

## 7. 授权矩阵

在 `pa_allowed`（api/lib.rs:608-660，含 :609 的 let-else 兜底）**之外新增**唯一入口 `authorize(principal, method, path)` 做三分支分派：super 恒 `true`（现状不变）；`GlobalAdmin` → `ga_allowed(method, path)`；`ProjectAdmin` → `pa_allowed(principal, method, path)`。

**`pa_allowed` 保留原名、原三元签名与原实现，一字不改**（**禁止**改名或改成 `(method, path, pid)`——否则 `mod pa_matrix_tests`（api/lib.rs:4706-4786）的 22 处调用全部 E0308）；`auth_middleware`（api/lib.rs:702）改调 `authorize`。**默认拒绝、显式放行**纪律不变。

| 端点 | super | GA | PA |
| --- | --- | --- | --- |
| `POST /api/v1/login`、`POST /api/v1/cluster/join`、`POST /api/v1/admin/reset-password`、`GET /healthz`、`GET /readyz` | 中间件豁免（不经矩阵，各 handler 自校验，见后注 2） | — | — |
| `POST /logout`、`/heartbeat`、`/me/password` | ✅ | ✅ | ✅ |
| `GET /projects` | ✅ 全量 | ✅ 全量 | ✅ 过滤为自己项目 |
| `POST /projects`（建项目） | ✅ | ❌ 403 | ❌ 403 |
| `DELETE /projects/{p}` | ✅ | ❌ 403 | ❌ 403 |
| `GET /projects/{p}`、`/branches*`、`/structure*`、`PUT .../draft`、`.../publish`、`.../rollback`、`/promote`、`/diff`、`.../versions`、`/gray-*`、`.../config` | ✅ 任意项目 | ✅ **任意项目** | ✅ 仅自己项目 |
| `GET/POST /projects/{p}/admins`、`PUT/DELETE /projects/{p}/admins/{u}` | ✅ | ❌ 403 | ❌ 403 |
| `GET/POST /projects/{p}/tokens`、`DELETE /projects/{p}/tokens/{id}` | ✅ | ❌ 403（D1） | ❌ 403 |
| `GET /shared`、`GET /shared-draft` | ✅ | ✅ | ✅ 只读（`refs` 过滤为仅自己项目） |
| `POST /shared`、`PUT /shared-draft`、`POST /shared/publish`、`DELETE /shared/{key}`、`DELETE /shared-draft/{key}` | ✅ | ✅ **写** | ❌ 403 |
| `GET /audit` | ✅ 全量 | ✅ 全量 | ✅ 强制 project 过滤 |
| `GET /cluster/members` | ✅ | ✅ 只读 | ✅ 只读 |
| `POST /cluster/{promote,remove}` | ✅ | ❌ 403 | ❌ 403 |
| `POST /admin/{set-password,force-logout,rotate-master-key}`、`GET /admin/{snapshot,retention-status}` | ✅ | ❌ 403 | ❌ 403 |
| **新增** `/api/v1/admins*` | ✅ | ❌ 403 | ❌ 403 |
| `/v1/projects/{p}/branches/{b}/config`（渲染；中间件之外手动校验） | ✅ 任意项目 | ✅ 任意项目 | ✅ 仅自己项目（含 `reveal=true`） |

> **漂移说明**：`/shared/refs` 端点已在 shared-ref-branch-scope 改造中移除——共享引用改为分支 `BranchState.shared_bindings`、随 `PUT /projects/{p}/branches/{b}/draft` 保存（shared-ref-branch-scope.md:29,85,166）。project-admin.md §4 中 `/shared/refs` **两行**（:114-115，:116 的 reveal 行保留）是历史漂移，见 §12。因此 GA/PA 的「引用共享」能力**天然由项目草稿端点承载**，矩阵无需额外行。

**实现约束与边界（红队审计补充，实现者必读）**：

1. **`/api/v1/projects/{p}` 必须 method 敏感**：同一路径承载 `GET` 项目详情（GA 放行，与 PA 现状一致，见 api/lib.rs:651-656）与 `DELETE` 删除项目（GA 403）；**禁止**写成「`rest.is_empty()` → 拒绝」的粗粒度判定。
2. **`/cluster/join` 不在本矩阵内**：它在中间件豁免名单（api/lib.rs:674-681），由 handler 内 `join_token_ok` 校验节点 `--join-token`（api/lib.rs:3298-3339）——非 join token（含 GA/PA/super 会话 token）→ **401 `ERR_UNAUTHORIZED`，与角色无关，永不 403**；测试不得断言 403。
3. **矩阵未覆盖的路由（现状行为，本次不改）**：`GET /metrics`、`GET /admin`、`GET /admin/{*path}` 落在中间件两条前缀分支之外（api/lib.rs:682/:713），对匿名亦开放，三种角色行为一致；`/v1/projects/{p}/branches/{b}/snapshot` 与 `.../watch` 走数据面 token 鉴权，管理会话（含 GA）一律 401（§11.4 零变化）；`GET /api/v1/cluster/members` 仅在集群模式注册（api/lib.rs:4549），dev-single 下三种角色均 404（断言必须在集群 harness 内）。
4. **已知歧义（既有行为）**：`POST /api/v1/shared/publish` 与 `DELETE /api/v1/shared/{key}` 相邻注册（api/lib.rs:4519-4521），静态段优先 → `DELETE /api/v1/shared/publish` 返回 **405**，即 key 名为 `publish` 的共享项（super/GA 均）删不掉；`ga_allowed` 返回 true 不改变结论，测试不得断言 204。
5. **403 文案**：`auth_middleware` 的 403 message 由「项目管理员无权访问该资源」改为「无权访问该资源」（三角色共用，api/lib.rs:707），并写进 docs/09-admin.md 的权限说明。
6. **URL 归一化会改变路径构造测试的结论**（`reqwest`/`url` 按 WHATWG 规范）：`/api/v1/projects/p1/../admins` 与 `/api/v1/projects/p1/%2e%2e/admins` 在**发出前**就被归一化为 `/api/v1/projects/admins`（合法项目名 → GA 放行 → 命中 `GET /api/v1/projects/{p}` → 404），而 `//admins` 与 `%61dmins` 保持原样。因此：`//admins` 应由中间件空段守卫判 403；`..` 只能用不归一化的客户端（`hyper` 手工 `Uri`/裸 TCP）验证，否则不得断言 403；`%61dmins` 由路由 404 兜底（**非 2xx** 即可，勿断言 403）。

**中间件之外的两处历史权限判定**必须同步改造，这是 GA 能渲染/解密任意项目配置的落点：

- api/lib.rs:734（`/v1/.../config` 的 `session_allowed`）
- api/lib.rs:2636（`render_config` 的 `reveal=true` 会话校验）

两处由 `Ok(Principal::Admin) => true` 改为「super 或 GA → `true`；PA → `project.0 == pid`」。

handler 内 `matches!(principal, Principal::Admin)` 的防御性校验（api/lib.rs:2251 建令牌、2344 删令牌）语义保持 super-only，但改为可读的 `is_super(&principal)` 辅助函数，避免后续漏改。

## 8. API 设计（新增 4 个端点，全部 super-admin 专用）

```
POST   /api/v1/admins              {"username","password"}  → 201 {username, created_at}
GET    /api/v1/admins              → [{username, created_at}]        // 不返回 salt/hash
PUT    /api/v1/admins/{username}   {"password"}             → 200 {changed:true}
DELETE /api/v1/admins/{username}                            → 204
```

- 命名：`/api/v1/admins` = 全局管理员；现有 `/api/v1/projects/{p}/admins` = 项目管理员（不变）。
- 错误码复用：`ERR_ACCOUNT_EXISTS`(409)、`ERR_ACCOUNT_NOT_FOUND`(404)、`ERR_FORBIDDEN`(403)、`ERR_BAD_CREDENTIALS`(401)、`ERR_VALIDATION`(422)。
- **密码长度下限**：`POST /api/v1/admins` 与 `PUT /api/v1/admins/{u}` 均强制 ≥6 位（与 `POST /api/v1/admin/set-password` 一致，api/lib.rs:3900；不足 → 422）。这是**长度下限**，不属于 §16「不做密码复杂度策略」；PA 创建端点（api/lib.rs:2136）本期不动，行为差异记入 docs/09-admin.md。
- 审计 action：`global_admin_create` / `global_admin_delete` / `global_admin_set_password`，operator = `admin`。

**登录扩展**（`POST /api/v1/login`，api/lib.rs:2745）：

1. `username` 缺省/空 → super-admin 路径，**逐字节保持现状**（含 `sess/admin-pw` 优先、节点配置回退）；
2. `username` 非空 → **先查 `adm/ga/{u}`**：命中走 GA 登录（token `ga.{u}.{sid}.{secret}`、role `global_admin`、审计 operator `ga:{u}`）；未命中回退现有 PA 路径（完全不变）；
3. 失败统一 401 `ERR_BAD_CREDENTIALS`（不区分账号是否存在，防枚举）+ `login_failed` 审计（detail 记 username）；
4. 非 leader 转发体已透传 username，leader 侧执行同一分派（GA→PA 顺序），无需额外分支；
5. 响应新增可选字段 `username`（GA/PA 回填，供 UI 显示）；`role` 取值新增 `global_admin`。

**自助改密**（`POST /api/v1/me/password`，api/lib.rs:3812）新增 GA 分支：

- 校验当前密码（GA 账号 argon2 / legacy）→ 失败统一 401 `ERR_BAD_CREDENTIALS`；
- 成功 → `GlobalAdminSetPassword{username}` → apply 级联收回**该账号全部会话**（改密即重登）；
- 审计 `set_password_self`，operator `ga:{u}`；
- super-admin 分支不变（仍只踢 `sess/admin*`）→ **GA 会话不受 super 改密影响**（两条凭据独立），需测试锁定。

**force-logout**（`POST /api/v1/admin/force-logout`，api/lib.rs:3716，super 专用）分派扩展：

| 请求体 | 现状 | 新语义 |
| --- | --- | --- |
| `{}` | 踢全部 super 会话 | 踢全部 super 会话 + **全部 GA 会话**（`MultiSessionLogoutAll` + `GlobalAdminSessionsRevokeAll`）；**后一条仅在存在 GA 账号时写入**（`has_global_admins()` 守卫，与任务 4.6b 同口径）——否则无 GA 的混合版本集群里旧节点读日志会**硬失败**（dsh-raft `store.rs:176`），违反 §11.1「不创建 GA 时行为与现状完全一致」；**PA 会话不在此列**（见 §16：无「全部 PA 会话」命令，须按账号逐个踢）。本行是「管理员侧全量下线」，**不得**表述为「全系统恐慌按钮」——按下后 PA 仍可持旧 token 写配置 |
| `{session_id}` | 踢单个 super 会话 | 不变 |
| `{username}` | 踢该 PA 账号全部会话 | 先查 `adm/ga/{u}`：命中踢该 GA 全部会话；未命中按 PA 处理（现状）。**`username` 为 `admin` 时是静默 no-op**（`admin` 为保留名、不是 GA 账号，PA 亦无此账号）→ 返回 200 但 super 会话无变化；super 会话只能由 `{}` 或 `{session_id}` 踢，测试需锁定该语义 |
| `{username, session_id}` | 踢该 PA 单会话 | 同上，GA 命中则踢该 GA 单会话 |

## 9. 审计

- operator 取值集合：`admin`（super）、`ga:{username}`（新）、`pa:{username}`、`data-plane:{name}`、`ops-token`。
- 新 action：`global_admin_create` / `global_admin_delete` / `global_admin_set_password`。
- GA 的 login / logout / set_password_self / config_reveal 均以 `ga:{u}` 记账（可回答「哪个管理员看过哪份明文」）。
- **共享草稿写必须按主体记账（本次顺带修复的既有审计逃逸）**：改签名后全仓 8 处引用都要改（定义 :1721、生产 :1840/:1849、cfg(test) 导入 :4952 与 4 处测试调用 :5016/:5046/:5092/:5117，见计划任务 4.7）。`write_shared_draft`（api/lib.rs:1721）当前把 `Command::SharedDraftUpdate.operator` 与审计 operator **硬编码为 `admin`**（api/lib.rs:1813、:1827），且 `create_shared`（:1836）/ `update_shared_draft`（:1845）不接收 principal。GA 对这两条路径矩阵明示放行（§7），若不改，GA 把任意值（含 secret）灌进所有项目时审计记为超级管理员——无法归因。修正见计划任务 4.7。
- `GET /audit`：GA 全量（不强制 project 过滤，与其「可管理所有项目」定位一致）；PA 过滤不变。
- 指标：`dsh_session_active` 已按 S7 移除（dsh-observability/src/lib.rs:109），本次**无指标改动**。

## 10. Admin UI

| 位置 | 改动 |
| --- | --- |
| index.html:51-68 登录卡片 | 用户名占位/说明改为「超级管理员留空；全局管理员 / 项目管理员填用户名」 |
| index.html:79-85 侧边导航 | 新增 `#nav-globaladmins`「全局管理员」；`#nav-admins` 标签改为「项目管理员」 |
| index.html:437-474 新视图 | 新增 `#view-globaladmins`（列表 + 创建表单 + 改密/删除） |
| app.js:48 常量 / :365-372 `renderSession` | **先追加 `LS_USER = 'dsh_admin_username'` 常量**（不存在，直接用会 `ReferenceError` → 登录卡死）；`renderSession` 新增 `global_admin` → 「全局管理员 / 全局 · <username> / G」；`doLogin` 落盘、`doLogout`(:337)/`sessionExpired`(:107)/改密清理(:2312) 三处移除 |
| app.js:379-383 导航门控 | 二元 `isPa` → 三分支：`nav-globaladmins`/`nav-admins`/`tab-tokens` 仅 super；`nav-shared` super+GA |
| app.js:2156 集群视图 | 判据由 `role==='project_admin'` 改为「非 super 一律只读」（GA 不得出现移除/提升按钮） |
| app.js:2208-2286 管理员视图 | 保留为 PA 账号管理（仅 super 可见）；文案中「全局管理员」→「超级管理员」 |
| 新 action | `loadGlobalAdmins` / `createGlobalAdmin` / `setGlobalAdminPw` / `deleteGlobalAdmin`（走 `/api/v1/admins`） |

标题/称谓统一：super-admin = 「超级管理员」，新角色 = 「全局管理员」，PA = 「项目管理员」。

## 11. 兼容性与迁移

1. **零迁移**：不创建 GA 账号时，行为与现状完全一致（super-admin 仍以空用户名登录）。
2. **既有命令变体形状不变**，新变体只由新代码写入；**滚动升级纪律**：旧节点无法反序列化 `GlobalAdmin*` 变体 → 「全集群升级完成后再创建第一个 GA」，写入 deployment-guide（与 project-admin §3 同一纪律）。
3. `AdminSession.principal` 新增变体不影响旧数据（`#[serde(default)]` 缺省 `Admin`）；旧格式 token（无 sid、无前缀）路由不变。
4. 数据面（`/v1` 项目令牌、gRPC、SSE watch）零变化。
5. 会话键空间互不重叠 → `MultiSessionLogoutAll` 不碰 `sess/ga/*`（回归断言）。
6. UI：旧 localStorage 的 `admin`/`project_admin` 语义不变，新增 `global_admin`。
7. openapi：新增 2 个 path（`/api/v1/admins`、`/api/v1/admins/{u}`，共 4 个 operation），并**补齐既有缺失字段**——`LoginRequest` 缺 `username`、`LoginResponse` 缺 `role`/`project`/`username`（api/openapi.v1.yaml:965-977），见 §12。schema：`$defs` 新增 `GlobalAdminAccount`，并修正 `AdminSession` 缺 `principal` 的既有漂移（§12）。`scripts/check-contracts.sh` 保持绿（其实际校验仅为「每个 operation 有 responses」与「`$defs` 必填清单存在」，见 scripts/check-contracts.sh:17-21、30-34）。

## 12. 调查发现的既有文档漂移（本次顺带修正）

| 位置 | 漂移 | 修正 |
| --- | --- | --- |
| dev_docs/design/project-admin.md:61 | 写作 `{"kind":"admin"}` / `{"kind":"project_admin"}` | 实际为 `{"kind":"Admin"}` / `{"kind":"ProjectAdmin"}`（枚举无 `rename_all`，core/model.rs:440）；以新增 serde 单测锁定，并改文档 |
| dev_docs/design/project-admin.md:114-115（两行，含 POST/DELETE/GET 三个 `/shared/refs` 端点） | 矩阵含 `/shared/refs` 条目 | 该端点已随 shared-ref-branch-scope 移除（引用改为分支 `shared_bindings`，随 `PUT /projects/{p}/branches/{b}/draft` 保存）；删这**两行**（:116 的 reveal 行保留） |
| dev_docs/design/project-admin.md:153 | 要求改造 `dsh_session_active` 指标 | 该指标已按 S7 移除（dsh-observability/src/lib.rs:109）；删行 |
| dev_docs/design/project-admin.md:9,15,17,18,37,41,46,95,125,132,135,144,168,173,187 | 通篇称现有根账号为「全局管理员」 | 本设计起统一为「超级管理员」；「全局管理员」称谓让给新角色，逐处改写以避免混淆 |
| docs/09-admin.md:40 | 「导航仅显示配置管理/审计日志/集群节点」（与 `nav-admins`/集群只读一致） | 补充新角色与全局管理员视图说明 |
| README.md:19,87 | 访问令牌说明写作「仅全局管理员」（该角色将改名「超级管理员」，且新增同名新角色） | 两处改为「仅超级管理员」；并在 README.md:97 的「核心能力 → `**安全**`」条目末尾补「多全局管理员（GA）：由超级管理员创建/删除/改密，可管理全部项目配置与共享配置，不能管理账号、集群写与访问令牌」（README **没有**「安全」小节，注意落点） |
| api/openapi.v1.yaml:965-977、:18 | `LoginRequest` 无 `username`；`LoginResponse` 无 `role`/`project`/`username`；summary 仍写「单一管理员会话 I7；第二个登录返回 409」且保留 `409 SessionInUse` | PA/多会话改造时的既有漂移：本次补齐字段 + 改 summary + 删 409 响应行（多会话登录不 409，见 api/lib.rs:2973） |
| schema/storage.v1.schema.json:456-485 | `AdminSession` 无 `principal` 且 `additionalProperties: false` | PA 会话早已带 `principal`（core/model.rs:498-500），真实记录会被该 schema 判非法；本次补 `principal` 三形态 |

## 13. 测试计划

**dsh-core（状态机层，新增 `tests/global_admin.rs`，参照 `tests/project_admin.rs`）**

- 创建 / 重复创建（Conflict）/ 删除 / 改密 / 改密目标不存在（NotFound）；
- 用户名校验：过短、`admin`、**`Admin`/`ADMIN`**（保留名大小写不敏感）、非法字符；
- **GA↔PA 互斥双向（含大小写）**：先 PA 后建同名 GA → Conflict；先 GA 后建同名 PA → Conflict；**大小写变体同样 Conflict**（先 PA `alice` → 建 GA `Alice`；先 GA `bob` → 建 PA `Bob`）；**GA 内部为精确键唯一**：`carol` 与 `Carol` 可并存（§4 的显式取舍）；
- 删除 GA 级联删其全部会话；改密级联删其全部会话；
- 会话：`MultiGaSessionLogin` 多会话并存；GA 与 super 会话互不影响；`MultiSessionLogoutAll` **不删** GA 会话（回归）；`GlobalAdminSessionsRevokeAll` 删全部 GA 会话但**不删** super 会话；
- `Principal` serde：`{"kind":"GlobalAdmin","username":"alice"}` 往返；旧 `{"kind":"ProjectAdmin",...}` 与缺省 `{}`→`Admin` 语义不变（锁定 §12 漂移修正）。

**dsh-api（新增 `tests/http_global_admin.rs`，参照 `tests/http_project_admin.rs` 的 TestServer 模式）**

- super 创建 GA → GA 登录（role `global_admin`）；
- **授权矩阵逐行断言**：允许项（任意项目的结构/草稿/发布/回滚/promote/diff/版本/灰度/config、共享读写与发布删除、`/projects` 全量、**`GET /projects/{p}` 项目详情（与 `DELETE /projects/{p}` 同路径，必须分别断言 200/403）**、**`GET /audit` 的「不过滤」语义（造 p1/p2 两条审计后 GA 可见 p2 条目，PA 仅见自己项目）**、`/cluster/members`（集群 harness）、`/v1/.../config` 掩码与 `reveal=true`、heartbeat（200）；**logout（204）自成一组放末尾**——它会删掉当前 GA 会话，同夹具内其后的请求在中间件层即 401（api/lib.rs:688-700），与其并列会写红；`me-password` 不在本组（改密同样级联收回会话、会与 logout 争「最后」的位置，改密由「自助改密」条目单独覆盖））；拒绝项 403（`/api/v1/admins*`、`/api/v1/admin/{set-password,force-logout,rotate-master-key,snapshot,retention-status}`（**不含 `/api/v1/admin/reset-password`**：中间件豁免 + ops token 校验，非 ops token → 401，api/lib.rs:678/3938-3947）、`/cluster/{promote,remove}`、`POST /projects`、`DELETE /projects/{p}`、`GET/POST /projects/{p}/admins`、**`PUT /projects/{p}/admins/{u}`**、`GET/POST /projects/{p}/tokens`、**`DELETE /projects/{p}/tokens/{id}`**）；
- 路径绕过组（`%70`、大写、尾斜杠、`%2F`）→ 403（复用 PA 用例形态）；
- token 路由负形：`ga.` 段数不足、`ga.admin.x`、PA secret 拼 `ga.` 前缀、截断 token → 401；
- 自助改密：GA 改自己密码 → 旧 token 401、新密码可登录、该 GA 其他会话失效、**super 会话存活**；
- super 重置 GA 密码 → GA 全部会话失效；GA 旧密码 401；
- super 删除 GA → 该 GA 登录 401；
- super 改自己密码 → **GA 会话不失效**（键空间隔离回归）；
- force-logout：`{username:<GA>}` 踢 GA 全部会话；`{}` 同时踢 super 与 GA 会话；
- 非 leader 登录转发带 GA username（集群测试环境）；
- 审计 operator 断言：GA 的 login / publish / reveal / set_password_self 均为 `ga:{u}`；`global_admin_create` 的 operator 为 `admin`；
- **logout 有效性（非仅 200）**：GA `POST /logout` 后原 GA token 再请求 `GET /projects` → 401；
- **join 负形**：`POST /api/v1/cluster/join`（带 GA token）→ **401 `ERR_UNAUTHORIZED`**（不得断言 403）。**该断言必须落在「真 raft + 已配置 `--join-token`」的夹具**：① join 路由仅在 `app.raft.is_some()` 时注册（api/lib.rs:4549-4555），无 raft 夹具恒 **404**；② `join_token_ok` 在未配置 join-token 时**放行**（api/lib.rs:3298-3307），故未注入 join-token 的集群夹具也不是 401。做法见计划 §5 第 6 条；
- **路径构造组**：`/api/v1/projects/p//admins` → **403**（中间件空段守卫）；`/api/v1/projects/p/%61dmins` → **非 2xx**（中间件放行、由路由 404 兜底，**不得断言 403**）；`/api/v1/projects/p/../admins` 经 `reqwest`/`url` 按 WHATWG 归一化后实际发出 `/api/v1/projects/admins` → **404**（**不得断言 403**；要锁定中间件的 `..` 守卫只能用 `hyper` 手工 `Uri` 或裸 TCP 这类不归一化的客户端，且**不得为凑 403 反改守卫**）——与 §7 注 6 同口径；
- **共享写审计**：GA 的 `POST /shared` 与 `PUT /shared-draft` 审计条目 operator == `ga:{u}`（当前会写成 `admin`，属必须修复的审计逃逸）；
- **恢复通道会话范围**：`POST /api/v1/admin/reset-password`（ops token）**body 必带 `{"password":"pw123456"}`**（`SetPasswordReq { password }` 无 serde default，api/lib.rs:3781-3784/3935；缺 body → 415、`{}` → 422，重置不会执行 ⇒ 后续断言全假）→ **先断言该请求 200**，再看 super 与 GA 旧 token 均 401、PA token 仍 200（任务 4.6b）；
- **矩阵行端点补全**：`GET /api/v1/projects/p2/branches/dev/config`（admin_config）、灰度全组（**必须分两轮**：轮 1 gray-publish/gray-status/gray-promote、轮 2 gray-publish/gray-abort——`apply_gray_promote`/`apply_gray_abort` 各自清零灰度状态（state.rs:2095-2096/2139-2140），无活动灰度时两者均 `Validation` → 422（state.rs:2063-2067/2132-2136），同轮要求两者都 2xx 自相矛盾；配方见计划 §5 第 3 条）、分支列表/创建、**自建分支删除**（已发布分支恒 409，state.rs:1402-1406）、`GET .../structure` 对 GA 均为 2xx（原先只覆盖了渲染端点与 gray-status）；
- **密码下限 + 保留名**：`POST /api/v1/admins {username:"ga2",password:"12345"}` → **422 `ERR_VALIDATION`**（**body 必须带 `username`**，否则先被 `Json` 提取器以纯文本 422 拒掉，拿不到 `ERR_VALIDATION`、也没测到 ≥6 规则）、`PUT /api/v1/admins/{u}` 传 5 位密码 → 422；`POST /api/v1/admins {username:"Admin",password:"pw12345"}` → **422**（不得 201，锁定 §4 的保留名大小写不敏感口径）；
- **非 leader 登录转发**：向 follower 发 GA 登录 → 200 且 `role==global_admin`、`username==ga1`（锁定转发分支的 username 回填）；
- 回归：现有 `http_project_admin.rs` / `http_project_token.rs` / `grpc_data_plane.rs` 全绿；
- **既有单测同步**：`lib.rs` 内 `mod pa_matrix_tests`（:4705-4786，22 条断言）改调 `authorize` 后全绿；新增 `mod ga_matrix_tests` 按 §7 矩阵逐行断言 GA（详见计划任务 4.2）。

**全量验证**

```bash
cd server && source ../scripts/build-env.sh
cargo test --workspace
cargo check --workspace --all-targets        # 覆盖 tests/ 与 cfg(test)
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
bash scripts/check-contracts.sh
bash scripts/api-surface-test.sh
```

**UI**：`node --check server/crates/dsh-api/admin/app.js` + `cargo build -p dsh-api`（rust_embed 重嵌）+ 无头 Chrome e2e（可选，参照 `scripts/ui-e2e-project-clone.js`）。

## 14. 开发计划（slice）

| 阶段 | 内容 | 验收 |
| --- | --- | --- |
| S1 dsh-core | model/keys/command 新实体与 8 个新变体；apply + 访问器 + 级联；PA 创建加 GA 互斥；`valid_account_username` 重命名（保留名大小写不敏感）；core 单测 | `cargo test -p dsh-core` 绿 + `cargo check -p dsh-core --all-targets` 绿。**S1 结束时 workspace 整体编译不过**（新增 `Principal` 变体会让 dsh-api 7 处穷尽 match 报 E0004：api/lib.rs 的 734/2636/3099/3147/3167/3274/3823 臂所在 match —— 起始行 733/2635/3098/3146/3166/3273/3822），workspace 级 `cargo check --workspace --all-targets` 是 **S2 的首个动作**；S1+S2 必须合并提交（回滚单位） |
| S2 dsh-api 鉴权与登录 | `resolve_principal`/`token_session_id` 的 `ga.` 分支；`authorize` 三级矩阵（`pa_allowed` 保持三元签名）；两处渲染判定；`is_super` 辅助；login GA 分派；**logout/heartbeat 的 GA 臂**；`/me/password` GA 分支；force-logout 分派；`/api/v1/admins*` 四个端点；`principal_op`；`mod pa_matrix_tests` 同步 + 新增 `mod ga_matrix_tests`；**任务 4.6b**（`admin_reset_password` 加 `has_ga` 守卫并回收全部 GA 会话）；**任务 4.7**（`write_shared_draft` 增 `operator` 形参，全仓 8 处引用同步） | `cargo test --workspace` 绿 + `cargo check --workspace --all-targets` 绿 |
| S3 API 集成测试 | `tests/http_global_admin.rs` 全矩阵 | `cargo test -p dsh-api` 绿 |
| S4 Admin UI | index.html + app.js（登录文案、三分支导航、新视图、集群只读判据） | `node --check` + 重编 dsh-api + 手测一遍 |
| S5 契约与文档 | openapi 新增 2 个 path（4 个 operation）+ 补齐 login 的 `LoginRequest.username` 与 `LoginResponse.role/project/username`；schema `$defs` 与 `AdminSession.principal`；docs/09-admin.md、README、deployment-guide；称谓全量替换（含 docs/07-tokens.md、docs/quickstart.md、docs/index.md）；修正 §12 的 **8 行**漂移（docs/README/openapi/schema 各行见 §12 表） | `check-contracts.sh` 绿 + **docs 审读**：逐条对照 §12 的 **8 行**漂移与 §16「明确不做」，并 `rg -n 全局管理员 server/crates/dsh-api/admin docs README.md dev_docs/deployment-guide.md dev_docs/design/project-admin.md api/openapi.v1.yaml scripts server/crates/dsh-api/src` 确认无残留旧称谓（范围与计划 §6.3 一致；**含 `dev_docs/design/`**，否则 project-admin.md 的 15 行称谓改动无人验证） |

规模预估：核心 ~600 行 + 测试 ~800 行 + UI ~250 行。

## 15. 风险与权衡

1. **授权矩阵回归风险（最高）**：唯一强制点由「非 PA 即放行」改为三分支，且 api/lib.rs 内有 11 处 `Principal::Admin` 匹配点（476,479,734,2251,2344,2636,3099,3147,3167,3274,3823），另有 core/state.rs:2405,2440 两处构造点。缓解：`authorize` 单函数集中 + 逐端点集成测试断言 + 编译期穷尽匹配。**注意编译器只兜住一部分**：api/lib.rs 的 11 处 `Principal::Admin` 匹配点（合理分解 = 2 处构造 + 7 处穷尽 `match` + 2 处 `matches!`）中，**734/2636/3099/3147（`principal_op`）/3167/3274/3823 共 7 处**是穷尽 `match`（新增变体即编译失败）；另有 4 类**非穷尽**点会静默走「非 PA」分支——:813（`list_projects` 的 if-let，GA 得全量列表 ✅）、:1864（`list_shared` 的 `_` 通配，GA 得全量 refs ✅）、:1721+1836/1845（共享草稿写 handler **根本没有 principal 形参**，operator 硬编码 `admin` → 审计逃逸，由计划任务 4.7 引入 operator 形参修复）、:2680（reveal 审计 operator 走 `Ok(p) => principal_op(p)` 绑定臂，随 `principal_op` 补 GA 分支自动正确 ✅；:609 的 `pa_allowed` let-else 将被 `authorize` 分派取代）。这些点语义上符合 §7 或已被任务 4.7 覆盖，但**必须逐条进入评审清单**，不得依赖编译器。矩阵覆盖率的最终保障是 `tests/http_global_admin.rs` 对 `build_router` 注册的每条路由至少一条 GA 断言，而不是编译器。另外 `mod pa_matrix_tests`（:4705-4786）会因函数改名而编译失败，属预期（见计划任务 4.2）。
2. **GA 的共享写权是实质性提权**：共享项可被项目分支引用并物化进项目值（含 secret），因此「能写共享」≈「能影响任意项目」。这与 D1 授予的「管理所有项目配置」范围一致，但必须在文档中显式声明，不隐藏。
3. **锁死风险**：GA 不能管理账号 → GA 丢密只能由 super 重置；super 丢密只能走 ops-token 通道（既有）。**前提条件**：`POST /api/v1/admin/reset-password` 属中间件豁免路径（api/lib.rs:674-681），其唯一凭据是节点 `--join-token`（`ops_token_ok`，api/lib.rs:3314-3324），可覆写 `sess/admin-pw`，并在 `MultiSessionLogoutAll`（只清 `sess/admin*`）之外由**任务 4.6b** 追加 `GlobalAdminSessionsRevokeAll`，即同时回收 **super 会话 + 全部 GA 会话**（api/lib.rs:3952-3960；`sess/pa/*` 不在此通道内）——因此「GA 不能修改 super 凭据」成立的前提是 **GA 不持有节点 join-token**；该令牌须与主密钥同级保管（docs/09-admin.md:35）。除该通道外无其他互锁面。
4. **混合版本集群**：新变体旧节点不可解析 → 升级纪律（§11.2）。这是既有 PA/多会话已确立的纪律，不是新增风险。
5. **命名歧义**：GA/PA 全局唯一由 apply 强制；若未来要放开同名，需改为登录体显式带 scope，属破坏性变更，本次明确不做。
6. **审计身份不可回填**：历史条目的 operator 恒为 `admin`，无法追溯是哪个 super 操作（当时只有一个）。不做历史改写。

## 16. 明确不做（本期）

- `defing admin` 的 GA CLI 子命令（D4）；
- 项目级角色细化（GA 限定到项目子集）、只读管理员、临时/有效期账号；
- GA 的密码复杂度策略、MFA、登录 IP 白名单、账号级限流（沿用现有全局登录节流）；
- 把 super-admin 也改造成具名账号（D2 已否决）；
- 共享配置的细粒度授权（按 key / 按项目）；
- 历史审计条目 operator 回填；
- **`MultiPaSessionsRevokeAll`（一次踢全部 PA 账号会话）**：本期不做——PA 无全量回收命令（command.rs:250 的 `MultiPaSessionLogoutAll` 必须逐账号指名），运维需按账号 `{username}` 逐个收口，或轮换 `--join-token` 后走 `reset-password`（任务 4.6b 后该路径清 **super 会话 + 全部 GA 会话**，仍不清 `sess/pa/*`）。**因此 §8 的 force-logout `{}` 不是「全系统下线」**。
- **GA/PA 的密码复杂度策略**（长度下限 ≥6 位除外，见 §8）；
- handler 内对 `create_project`/`delete_project` 追加 `is_super` 纵深防御校验（super-only 由中间件矩阵保证，本期不加 handler 双重判定，避免出现设计外的改动与测试空白）。

## 17. 开放问题（实现期可决，不阻塞）

1. `GET /api/v1/admins` 是否返回各账号「活跃会话数」以便运维判断在线情况？建议**先不做**，需要时再加只读字段。
2. GA 是否需要「最后登录时间」展示？建议由审计查询承担，账号记录不冗余。
