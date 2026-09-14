# 开发计划：多全局管理员（Global Admin）与超级管理员

日期: 2026-09-14
范围确认: 设计文档 dev_docs/design/global-admin.md v1（本次调查 + 4 项决策确认后定稿）
上游: dev_docs/design/global-admin.md
执行路线: 单工作区逐 slice 实现，每 slice 后跑对应验证；全部完成后整体验证 + reviewer 交叉对比
基线: main HEAD 306e2a8（工作区干净）

## TDD Route

```text
TDD Route:
- Mode: off
- Decision: skipped
- Strict authority: not applicable（用户未要求 strict TDD；本期为功能开发 + 全量回归）
- Test posture: post-change regression —— 先实现 S1/S2，再以 state 单测 + API 集成测试逐条锁定行为；随后 UI 与文档
- Reason: 变更集中在「新实体 + 新命令变体 + 授权矩阵三分支」，风险点是覆盖度而非设计未定；用逐端点断言矩阵替代 RED 先行
- Verification: cargo test --workspace + cargo check --workspace --all-targets + cargo fmt --check + cargo clippy --all-targets --all-features -- -D warnings + bash scripts/check-contracts.sh + bash scripts/api-surface-test.sh + node --check app.js + UI 手测
```

## 0. 目标与基线

- **目标**：新增「全局管理员（GA）」角色——由超级管理员创建/删除/重置密码；GA 登录后可管理**所有项目**的配置与**全部共享配置**；任何角色登录后可自助修改自己的密码。
- **决策边界（已确认）**：D1 GA = 最小集（仅全部项目配置 + 全部共享配置）；D2 super-admin 沿用现有根账号；D3 GA/PA 用户名全局唯一互斥；D4 本期不做 CLI 子命令。
- **明确不做（本期）**：见设计 §16（CLI 子命令、项目子集授权、只读/临时账号、MFA、共享细粒度授权、历史审计回填）。
- **兼容边界**：不创建 GA 时行为与现状完全一致；既有命令变体形状不动；新增 8 个变体；`AdminSession.principal` 新增变体不影响旧数据；`sess/admin*` / `sess/ga/*` / `sess/pa/*` 前缀互不重叠；数据面零变化。
- **基线命令**：
  - `cd server && source ../scripts/build-env.sh && cargo test --workspace`（全绿基线）
  - `cargo check --workspace --all-targets`（覆盖 tests/ 与 cfg(test)，普通 build 有洞）
  - UI 改动需重编 dsh-api（rust_embed 嵌入 admin/）：`cargo build -p dsh-api`
  - 契约：`bash scripts/check-contracts.sh`；e2e：`bash scripts/api-surface-test.sh`

## 1. 文件地图

| 文件 | 动作 | 内容 |
| --- | --- | --- |
| server/crates/dsh-core/src/model.rs | 改 | `Principal` 新增 `GlobalAdmin { username }`；新增 `GlobalAdminAccount`（**必须带 `#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]`**，对齐 model.rs:454 的 `ProjectAdminAccount`，否则 `load`/`save_pending` 报 E0277） |
| server/crates/dsh-core/src/keys.rs | 改 | `K_GA_ACCOUNT`/`K_GA_SESSION` + `global_admin_key`/`ga_session_prefix`/`ga_session_key_with`；key_shapes 单测补断言 |
| server/crates/dsh-core/src/command.rs | 改 | 新增 8 个变体（纯新增，既有变体不动） |
| server/crates/dsh-core/src/state.rs | 改 | apply 分派 + 8 个 apply 函数 + 访问器 + GA↔PA 互斥；`valid_pa_username` → `valid_account_username` |
| server/crates/dsh-core/src/lib.rs | 改 | 导出 `GlobalAdminAccount` |
| server/crates/dsh-core/tests/global_admin.rs | 新增 | 状态机单测（账号/会话/互斥/serde） |
| server/crates/dsh-api/src/lib.rs | 改 | `ga.` token 路由；`authorize` 三级矩阵（`pa_allowed` 三元签名不变）；login GA 分派；logout/heartbeat 的 GA 臂；`/me/password` GA 分支；force-logout 分派；`/api/v1/admins*` 四端点；`is_super` 辅助；`principal_op`；两处渲染判定；`mod pa_matrix_tests` 同步 + 新增 `mod ga_matrix_tests`；**任务 4.6b**（`admin_reset_password` 加 `has_ga` 守卫并回收 GA 会话）；**任务 4.7**（`write_shared_draft` 增 `operator` 形参，全仓 8 处引用同步） |
| server/crates/dsh-api/tests/http_global_admin.rs | 新增 | API 集成测试（授权矩阵逐行 + 会话 + 登录；两套夹具，见 §5 T13.0） |
| server/crates/dsh-api/tests/leader_write_forward.rs | 改 | 新增 `ga_login_forwarded_with_username`（非 leader GA 登录转发 + username 回落；集群下 members 200 / promote 403） |
| server/crates/dsh-api/admin/index.html | 改 | 登录文案；导航新增 `#nav-globaladmins`；新视图 `#view-globaladmins`；`#nav-admins` 标签改「项目管理员」 |
| server/crates/dsh-api/admin/app.js | 改 | `renderSession` GA 分支；导航三分支门控；集群只读判据；全局管理员视图四个 action |
| api/openapi.v1.yaml | 改 | 新增 2 个 path（4 个 operation）；`LoginRequest` 补 `username`、`LoginResponse` 补 `role`/`project`/`username`；新增 `GlobalAdminAccount` schema；login summary 与 409 响应行修正；`/me/password` 描述三角色化 |
| schema/storage.v1.schema.json | 改 | `$defs` 新增 `GlobalAdminAccount`；修 `AdminSession` 缺 `principal` 的既有漂移 |
| docs/09-admin.md | 改 | 角色表、全局管理员视图、登录方式、审计 operator 表、403 文案、密码下限 |
| README.md | 改 | :19/:87 称谓改「超级管理员」；:97「核心能力 → 安全」条目补多全局管理员（README 无独立「安全」小节） |
| dev_docs/deployment-guide.md | 改 | 升级纪律（先全集群升级再建首个 GA）+ 找回通道边界 |
| dev_docs/design/project-admin.md | 改 | 修正 §12 列出的漂移（其中 project-admin.md 占 4 处） |
| docs/07-tokens.md、docs/quickstart.md、docs/index.md | 改 | 称谓替换（「仅全局管理员」→「仅超级管理员」）+ quickstart 登录说明（超级管理员留空用户名） |
| dev_docs/design/global-admin.md | 新增 | 本设计（已写） |

零改动：proto/（仅数据面 4 个 RPC，grep 无 principal/admin）、dsh-raft（快照载荷是 KV 对，无变体分派；`apply_inner` 是唯一穷尽 match）、dsh-publish、dsh-jobs、dsh-testkit、dsh-observability（`dsh_session_active` 已按 S7 移除）、dsh-cli（D4：不做 GA 子命令，`defing admin` 现有语义不动）、sdk/*（纯数据面 gRPC 客户端，无 login 调用）、scripts/api-surface-test.sh（除非断言需补 GA 用例）。

## 2. Slice 划分

- **S1 dsh-core**：model/keys/command/state + 状态机单测 → `cargo test -p dsh-core` + `cargo check -p dsh-core --all-targets`（**包级**）
- **S2 dsh-api**：token 路由 + 授权矩阵 + 登录/改密/force-logout + `/api/v1/admins*` 端点 + **任务 4.6b**（恢复通道回收 GA 会话）+ **任务 4.7**（共享草稿写 operator）→ `cargo test --workspace`
- **S3 API 集成测试**：`tests/http_global_admin.rs` + `tests/leader_write_forward.rs` 的 GA 集群用例 → `cargo test -p dsh-api`（含既有 http_project_admin / http_project_token / grpc_data_plane 回归）
- **S4 Admin UI**：index.html + app.js → `node --check` + `cargo build -p dsh-api` + 手测
- **S5 契约与文档**：openapi + schema + docs + 称谓全量替换 + 漂移修正 → `bash scripts/check-contracts.sh` 绿 + **docs 审读**（逐条对照设计 §12 的 **8 行**漂移与 §16「明确不做」，并 `rg -n 全局管理员 server/crates/dsh-api/admin docs README.md dev_docs/deployment-guide.md dev_docs/design/project-admin.md` 确认无残留旧称谓）
- **全量**：`cargo test --workspace` + `cargo check --workspace --all-targets` + fmt + clippy + check-contracts + api-surface-test + UI 手测
- **完成后**：reviewer（subagent）对照设计/计划复核 → 修复 → 提交 → push → CI 全绿

## 3. S1：dsh-core

### 任务 3.1 model.rs（T1）

`Principal`（model.rs:438-450）新增变体（既有两变体一字不动）：

```rust
/// 会话主体（三级角色：超级管理员 / 全局管理员 / 项目管理员）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "kind")]
pub enum Principal {
    /// 超级管理员（根账号：节点 --admin-password / sess/admin-pw 状态机哈希）。
    #[default]
    Admin,
    /// 项目管理员：仅管理 `project` 的配置，不能触碰共享面/全局面。
    ProjectAdmin {
        username: String,
        project: ProjectId,
    },
    /// 全局管理员：可管理全部项目配置与全部共享配置；
    /// 不能管理账号（GA/PA）、集群写、项目创建删除、访问令牌。
    GlobalAdmin { username: String },
}
```

新增实体（紧随 `ProjectAdminAccount` 之后）：

```rust
/// 全局管理员账号（设计 dev_docs/design/global-admin.md §4）。
/// 与 ProjectAdminAccount 同构（model.rs:454 逐字对齐 derive），无 project 字段；
/// salt 为保留字段（新格式盐内嵌 PHC 置空，旧 sha256(salt||pw) 兼容分支照旧）。
/// **derive 必须齐全**：`load`（state.rs:61 DeserializeOwned）/`save_pending`（:197 Serialize）
/// /`list_global_admins` 的 `serde_json::from_slice` 都依赖它，缺一个即 E0277。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalAdminAccount {
    /// 集群内唯一（与 PA 用户名互斥），[A-Za-z0-9_-]{2,64}，禁用 "admin"。
    pub username: String,
    pub salt: String,
    /// argon2id PHC（或旧 sha256 hex）。
    pub password_hash: String,
    pub created_at: i64,
}
```

### 任务 3.2 keys.rs（T2）

```rust
/// 全局管理员账号前缀：adm/ga/{username} → GlobalAdminAccount。
pub const K_GA_ACCOUNT: &str = "adm/ga/";
/// 全局管理员会话前缀：sess/ga/{username}/{session_id} → AdminSession。
pub const K_GA_SESSION: &str = "sess/ga/";

pub fn global_admin_key(username: &str) -> String {
    format!("{K_GA_ACCOUNT}{username}")
}
pub fn ga_session_prefix(username: &str) -> String {
    format!("{K_GA_SESSION}{username}/")
}
pub fn ga_session_key_with(username: &str, sid: &str) -> String {
    format!("{K_GA_SESSION}{username}/{sid}")
}
```

`key_shapes` 单测补三条断言（`adm/ga/alice`、`sess/ga/alice/`、`sess/ga/alice/abc`）。

### 任务 3.3 command.rs（T3）

在 multisession 变体区（command.rs:211+）末尾纯新增：

```rust
    // ---------------- 全局管理员（Global Admin，纯新增） ----------------
    /// 创建全局管理员账号（用户名 [A-Za-z0-9_-]{2,64} 且 ≠ "admin"；与 PA 用户名互斥）。
    GlobalAdminCreate {
        username: String,
        salt: String,
        password_hash: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
    },
    /// 删除全局管理员账号（级联删除其全部会话）。
    GlobalAdminDelete { username: String },
    /// 修改全局管理员密码（级联删除其全部会话，需重新登录）。
    GlobalAdminSetPassword {
        username: String,
        salt: String,
        password_hash: String,
    },
    /// 多会话 GA 登录：写 sess/ga/{username}/{session_id}（多会话并存，不 409）。
    MultiGaSessionLogin {
        username: String,
        token_hash: String,
        issued_at: i64,
        expires_at: Option<i64>,
        device_id: String,
        session_id: String,
    },
    /// 多会话 GA 登出（幂等）。
    MultiGaSessionLogout {
        username: String,
        session_id: String,
    },
    /// 多会话 GA 心跳续期（None = 永不过期，语义同 SessionHeartbeat）。
    MultiGaSessionHeartbeat {
        username: String,
        session_id: String,
        expires_at: Option<i64>,
    },
    /// 踢某 GA 账号全部会话（前缀扫全部删）。
    MultiGaSessionLogoutAll { username: String },
    /// 踢全部 GA 账号的全部会话（force-logout 全量用）。
    GlobalAdminSessionsRevokeAll,
```

### 任务 3.4 state.rs（T4）

**(a) 工具函数重命名 + 保留名收紧**：`valid_pa_username`（state.rs:2533，全仓 1 定义 + 1 调用 :2551）→ `valid_account_username`（两处调用点：`apply_project_admin_create` 与新增 GA create）。除保留名判定外行为不变（仍是 2-64 位 `[A-Za-z0-9_-]`）：`name != "admin"` → **`!name.eq_ignore_ascii_case("admin")`**（否则 `Admin`/`ADMIN` 可注册为 GA，与超级管理员在审计中视觉难辨：`ga:Admin` vs `admin`）。只影响创建校验，不影响既有账号。

**(b) apply 分派**（`apply_inner`，紧随 PA 分派 state.rs:1079-1097 之后）：

```rust
            Command::GlobalAdminCreate { username, salt, password_hash, ts } => self
                .apply_global_admin_create(username, salt, password_hash, Self::eff_ts(ts, now_ms)),
            Command::GlobalAdminDelete { username } => self.apply_global_admin_delete(username),
            Command::GlobalAdminSetPassword { username, salt, password_hash } => {
                self.apply_global_admin_set_password(username, salt, password_hash)
            }
            Command::MultiGaSessionLogin { username, token_hash, issued_at, expires_at, device_id, session_id } => self
                .apply_multi_ga_session_login(username, token_hash, *issued_at, *expires_at, device_id, session_id),
            Command::MultiGaSessionLogout { username, session_id } => {
                self.apply_multi_ga_session_logout(username, session_id)
            }
            Command::MultiGaSessionHeartbeat { username, session_id, expires_at } => {
                self.apply_multi_ga_session_heartbeat(username, session_id, *expires_at)
            }
            Command::MultiGaSessionLogoutAll { username } => self.apply_multi_ga_session_logout_all(username),
            Command::GlobalAdminSessionsRevokeAll => self.apply_global_admin_sessions_revoke_all(),
```

> 匹配处按既有写法对齐（现有代码用 `&` 借用解构），以编译期为准；关键是**穷尽 match（无 `_` 通配）**——这样未来新增 Principal/Command 变体会编译失败，是本次防漏改的结构性保障。

**(c) 账号 apply**：

```rust
    // ---------------- 全局管理员（Global Admin，设计 §4/§6） ----------------

    fn apply_global_admin_create(
        &mut self,
        username: &str,
        salt: &str,
        password_hash: &str,
        now_ms: i64,
    ) -> ApplyOutcome {
        if !Self::valid_account_username(username) {
            return Err(Error::new(
                ErrorKind::Validation,
                "用户名须为 2-64 位 [A-Za-z0-9_-] 且不可为 admin",
            ));
        }
        let key = global_admin_key(username);
        if load::<GlobalAdminAccount>(&*self.store, &key)?.is_some() {
            return Err(Error::new(ErrorKind::Conflict, "账号已存在"));
        }
        // D3：GA 与 PA 用户名全局唯一（互斥在状态机强制，保证多节点重放一致）
        if load::<ProjectAdminAccount>(&*self.store, &project_admin_key(username))?.is_some() {
            return Err(Error::new(ErrorKind::Conflict, "用户名已被项目管理员占用"));
        }
        // 设计 §4（D3 精确化）：互斥**不区分大小写**。上面的精确键比对只是快路径，
        // 必须再扫 adm/pa/ 的 **key 后缀**（不解析值——list_project_admins 的
        // filter_map(ok()) 会静默丢弃损坏记录）。纯状态读 + eq_ignore_ascii_case，
        // 无时钟/随机数 → 各节点重放一致。
        for (k, _) in self.get_prefix_merged(K_PA_ACCOUNT.as_bytes())? {
            if let Some(u) = std::str::from_utf8(&k)
                .ok()
                .and_then(|s| s.strip_prefix(K_PA_ACCOUNT))
            {
                if u.eq_ignore_ascii_case(username) {
                    return Err(Error::new(ErrorKind::Conflict, "用户名已被项目管理员占用"));
                }
            }
        }
        let acct = GlobalAdminAccount {
            username: username.to_string(),
            salt: salt.to_string(),
            password_hash: password_hash.to_string(),
            created_at: now_ms,
        };
        self.save_pending(&key, &acct)?;
        Ok(vec![])
    }

    fn apply_global_admin_delete(&mut self, username: &str) -> ApplyOutcome {
        let key = global_admin_key(username);
        if load::<GlobalAdminAccount>(&*self.store, &key)?.is_none() {
            return Err(Error::new(ErrorKind::NotFound, "账号不存在"));
        }
        self.delete_pending(key.as_bytes())?;
        self.delete_all_ga_sessions(username)?;
        Ok(vec![])
    }

    fn apply_global_admin_set_password(
        &mut self,
        username: &str,
        salt: &str,
        password_hash: &str,
    ) -> ApplyOutcome {
        let key = global_admin_key(username);
        let Some(mut acct) = load::<GlobalAdminAccount>(&*self.store, &key)? else {
            return Err(Error::new(ErrorKind::NotFound, "账号不存在"));
        };
        acct.salt = salt.to_string();
        acct.password_hash = password_hash.to_string();
        self.save_pending(&key, &acct)?;
        self.delete_all_ga_sessions(username)?; // 改密即收回全部会话
        Ok(vec![])
    }

    /// 删除某 GA 账号的全部会话（前缀扫 sess/ga/{username}/）。
    fn delete_all_ga_sessions(&mut self, username: &str) -> Result<(), Error> {
        let prefix = ga_session_prefix(username);
        let rows = self.get_prefix_merged(prefix.as_bytes())?;
        for (k, _) in rows {
            self.delete_pending(&k)?;
        }
        Ok(())
    }
```

**(d) 会话 apply**（模板 `apply_multi_pa_session_*`，state.rs:2469-2527——含 :2523-2527 的 `apply_multi_pa_session_logout_all` 与 :2579-2589 `delete_all_pa_sessions` 的前缀扫删范式；GA 无旧格式单 key，`delete_all_ga_sessions` 只扫前缀即可）。**差异有且仅有 3 处**：
① 键一律换 `ga_session_key_with` / `ga_session_prefix`；
② **`apply_multi_ga_session_login` 不得复用 `self.get_project_admin(...)`**（照抄该行会让 GA 登录恒 `NotFound`）：账号存在性校验改用 `self.get_global_admin(username)?`，`principal` 直接构造 ——

```rust
    let session = AdminSession {
        token_hash: token_hash.to_string(),
        issued_at,
        expires_at,
        device_id: device_id.to_string(),
        principal: Principal::GlobalAdmin { username: username.to_string() },
    };
```

③ `apply_multi_ga_session_logout_all` 调 `delete_all_ga_sessions`。其余（心跳无会话 → `ErrorKind::SessionExpired`；登出幂等删；login 不检查已存在、不 409）逐字照抄。：`apply_multi_ga_session_login`（写 `sess/ga/{u}/{sid}`，`principal: Principal::GlobalAdmin { username }`，**不检查已存在、不 409**）、`apply_multi_ga_session_logout`（幂等删）、`apply_multi_ga_session_heartbeat`（无会话 → `ErrorKind::SessionExpired`）、`apply_multi_ga_session_logout_all`（`delete_all_ga_sessions`）、`apply_global_admin_sessions_revoke_all`：

```rust
    /// 踢全部 GA 账号会话：直接扫 adm/ga/ 前缀取 **key 后缀**当用户名（不解析值——
    /// `list_global_admins` 用 `filter_map(...ok())` 会静默丢弃损坏记录，那样会让
    /// 「全量收口」漏掉一个仍有活会话的账号）。字典序遍历，终态与顺序无关。
    fn apply_global_admin_sessions_revoke_all(&mut self) -> ApplyOutcome {
        for (k, _) in self.get_prefix_merged(K_GA_ACCOUNT.as_bytes())? {
            if let Some(u) = std::str::from_utf8(&k)
                .ok()
                .and_then(|s| s.strip_prefix(K_GA_ACCOUNT))
            {
                self.delete_all_ga_sessions(u)?;
            }
        }
        Ok(vec![])
    }
```

**(e) PA 创建加互斥**（`apply_project_admin_create`，state.rs:2543）：在既有 `adm/pa/{u}` 已存在检查之后、save 之前插入：

```rust
        // D3：GA 与 PA 用户名全局唯一（反向互斥）
        if load::<GlobalAdminAccount>(&*self.store, &global_admin_key(username))?.is_some() {
            return Err(Error::new(ErrorKind::Conflict, "用户名已被全局管理员占用"));
        }
        // 设计 §4（D3 精确化）：反向同样不区分大小写（扫 adm/ga/ 的 key 后缀）。
        for (k, _) in self.get_prefix_merged(K_GA_ACCOUNT.as_bytes())? {
            if let Some(u) = std::str::from_utf8(&k)
                .ok()
                .and_then(|s| s.strip_prefix(K_GA_ACCOUNT))
            {
                if u.eq_ignore_ascii_case(username) {
                    return Err(Error::new(ErrorKind::Conflict, "用户名已被全局管理员占用"));
                }
            }
        }
```

确定性读、各节点重放一致，滚动升级安全（旧日志仍按顺序重放）。

**(f) 访问器**（紧随 `get_project_admin`/`list_project_admins`/`get_pa_session_with`，state.rs:2788-2817）：

```rust
    pub fn get_global_admin(&self, username: &str) -> Result<Option<GlobalAdminAccount>, Error> {
        self.load_merged(&global_admin_key(username))
    }

    /// 是否存在任何 GA 账号（**只判键空间，不解析值**）。
    /// 供「无 GA 时不写新变体」的兼容守卫使用；**不得**用 list_global_admins() 代替——
    /// 后者 filter_map(...ok()) 会静默丢弃损坏记录，一条损坏的 adm/ga/{u} 若仍有活会话，
    /// 守卫会误判「无 GA」而漏收（与 3.4(d) 键后缀扫描同一理由）。
    pub fn has_global_admins(&self) -> Result<bool, Error> {
        Ok(!self.get_prefix_merged(K_GA_ACCOUNT.as_bytes())?.is_empty())
    }

    /// 列出全部全局管理员（前缀扫 adm/ga/；O(账号数)，量小可接受）。
    pub fn list_global_admins(&self) -> Result<Vec<GlobalAdminAccount>, Error> {
        let raw = self.get_prefix_merged(K_GA_ACCOUNT.as_bytes())?;
        Ok(raw
            .into_iter()
            .filter_map(|(_, v)| serde_json::from_slice::<GlobalAdminAccount>(&v).ok())
            .collect())
    }

    /// 多会话 GA 会话：读 sess/ga/{username}/{session_id}。
    pub fn get_ga_session_with(
        &self,
        username: &str,
        session_id: &str,
    ) -> Result<Option<AdminSession>, Error> {
        self.load_merged(&ga_session_key_with(username, session_id))
    }
```

**(g) 常量导入**：state.rs 顶部 `use crate::keys::*` 已覆盖新键（若为显式清单则补 `K_GA_ACCOUNT`）。

### 任务 3.5 lib.rs 导出（T5）

`dsh-core/src/lib.rs:23` 的 `project_admin` 相关 re-export 行补 `GlobalAdminAccount`。

### 任务 3.6 状态机单测（T6，`tests/global_admin.rs`）

参照 `tests/project_admin.rs` 的**真实**辅助函数模式：`fn sm() -> StateMachine`（:9-11）、`fn current_structure_version()`（:14）、`fn setup_project()`（:24-37）、`fn create_pa()`（:40-52），统一走 `s.apply(&Command::..., now_ms)`：

1. 创建/列出/删除/改密主流程；重复创建 → Conflict；删改不存在 → NotFound。
2. 用户名非法（1 字符、`admin`、**`Admin`/`ADMIN`（保留名大小写不敏感，§3.4(a)）**、含 `.`）→ Validation。**这条必须有**：否则 §3.4(a) 的 `!name.eq_ignore_ascii_case("admin")` 被改回 `!=` 时 `cargo test` 仍全绿，无人拦。
3. **互斥双向（含大小写）**：先建 PA `alice` → 建 GA `alice` → Conflict；先建 GA `bob` → 建 PA `bob` → Conflict；**先建 PA `alice` → 建 GA `Alice` → Conflict；先建 GA `bob` → 建 PA `Bob` → Conflict**（设计 §4 的不区分大小写口径）；另断言 **GA 内部** `carol` 与 `Carol` 可并存（`adm/ga/` 是精确键唯一，见设计 §4 的显式声明）。
4. 删除 GA → `sess/ga/{u}/*` 全清；改密 → 同。
5. 会话：`MultiGaSessionLogin` 两次并存（两个 sid）；`MultiGaSessionLogout` 只删一个；`MultiGaSessionHeartbeat` 续期；`MultiSessionLogoutAll` **不删** `sess/ga/*`（回归，键空间隔离）；`GlobalAdminSessionsRevokeAll` 删全部 GA 会话且**不删** `sess/admin/*`。
6. 旧日志 serde 兼容：`serde_json::from_str(r#"{"GlobalAdminDelete":{"username":"a"}}"#)` 成功；既有 `{"ProjectAdminDelete":{"username":"a"}}` 仍成功（形状未变）。
7. `Principal` serde：`{"kind":"GlobalAdmin","username":"alice"}` 往返相等；`{"kind":"ProjectAdmin","username":"a","project":"p"}` 仍可解析；缺省形态（无 `principal` 字段的旧 `AdminSession`）→ `Admin` 语义不变。

## 4. S2：dsh-api

### 任务 4.1 token 路由（T7）

`new_ga_token`（紧随 `new_pa_token`，api/lib.rs:2741）：

```rust
/// 生成全局管理员 token：ga.{username}.{session_id}.{secret}（multisession）。
fn new_ga_token(username: &str, session_id: &str) -> String {
    format!("ga.{username}.{session_id}.{}", new_token())
}
```

`resolve_principal`（api/lib.rs:442-493）在 `pa.` 分支之后、`adm.` 分支之前插入：

```rust
    } else if let Some(rest) = token.strip_prefix("ga.") {
        let parts: Vec<&str> = rest.split('.').collect();
        // ga.{username}.{sid}.{secret}：段数不足 → 无会话 → 401（负形测试锁定）
        let session = if parts.len() >= 3 {
            sm.get_ga_session_with(parts[0], parts[1]).ok().flatten()
        } else {
            None
        };
        (dsh_core::Principal::GlobalAdmin { username: String::new() }, session)
    }
```

> 与 PA 分支同一纪律：**解析出的 principal 只作占位，最终以 `session.principal` 为准**（api/lib.rs:488-489），因此 token 无法伪造身份归属。

`token_session_id`（api/lib.rs:499-513）新增 `ga.` 分支（`parts.len() >= 3` → `Some(parts[1])`）。

### 任务 4.2 授权矩阵三级化（T8）

`authorize(principal, method, path)`（api/lib.rs:608 处**新增**，作为唯一入口）做三分支分派；**原 `pa_allowed` 保留原名、原三元签名与原实现**（`&Principal` 首参，api/lib.rs:608-660 逻辑一字不改），仅由 `authorize` 的 `ProjectAdmin` 分支调用——**禁止**把它改成 `(method, path, pid)` 或把 `pid` 直接传进去（会让 `mod pa_matrix_tests` 的 22 处调用全部 E0308）：

```rust
/// 授权矩阵（设计 global-admin §7：默认拒绝、显式放行）。
/// super 恒放行；GA = 全部项目配置 + 全部共享配置；PA = 所属项目。
fn authorize(principal: &dsh_core::Principal, method: &str, path: &str) -> bool {
    match principal {
        // 超级管理员：全部端点（现状不变）
        dsh_core::Principal::Admin => true,
        dsh_core::Principal::GlobalAdmin { .. } => ga_allowed(method, path),
        dsh_core::Principal::ProjectAdmin { .. } => pa_allowed(principal, method, path),
    }
}

/// 全局管理员矩阵（D1 最小集）。
fn ga_allowed(method: &str, path: &str) -> bool {
    // 自身会话 + 自助改密
    if method == "POST"
        && (path == "/api/v1/logout"
            || path == "/api/v1/heartbeat"
            || path == "/api/v1/me/password")
    {
        return true;
    }
    // 全量项目列表 / 全量审计 / 集群只读
    if (method == "GET" && path == "/api/v1/projects")
        || (method == "GET" && path == "/api/v1/audit")
        || (method == "GET" && path == "/api/v1/cluster/members")
    {
        return true;
    }
    // 共享配置：读 + 写 + 发布 + 删除（全部放行）
    if path == "/api/v1/shared"
        || path == "/api/v1/shared-draft"
        || path == "/api/v1/shared/publish"
        || (path.starts_with("/api/v1/shared/") && method == "DELETE")
        || (path.starts_with("/api/v1/shared-draft/") && method == "DELETE")
    {
        return true;
    }
    // 项目路径：全部项目放行，但剔除全局面子路径
    if let Some(p) = project_segment(path) {
        let rest = path
            .strip_prefix(&format!("/api/v1/projects/{p}"))
            .unwrap_or("");
        // 项目本体：GET 详情放行（矩阵 §7 行 6），DELETE 删除项目仅 super
        if rest.is_empty() {
            return method == "GET";
        }
        // 防御（红队 M5）：残留路径禁止空段 / "." / ".." —— 黑名单前缀判定在归一化差异下不可靠
        // （"//admins"、"/../admins" 都不以 "/admins" 开头；"%61dmins" 见下注，只能靠路由 404 兜底）
        if !rest.starts_with('/')
            || rest[1..].split('/').any(|s| s.is_empty() || s == "." || s == "..")
        {
            return false;
        }
        // 账号管理 / 访问令牌：仅 super
        if rest.starts_with("/admins") || rest.starts_with("/tokens") {
            return false;
        }
        return true;
    }
    // 非项目路径：默认拒绝（/api/v1/admins、/admin/*、集群写、POST /projects 等）
    false
}
```

`pa_allowed(principal, method, path)` **保持三元签名与现有实现不变**（api/lib.rs:608-660，含 :609 的 let-else 兜底；逻辑一字不改），仅由 `authorize` 在 `ProjectAdmin` 分支调用。**禁止**把首参改成 `pid`（见下方单测约束）。`authorize(principal, method, path)` 三个分支分别委派 super=true / `ga_allowed(method, path)` / `pa_allowed(principal, method, path)`，签名与既有单测保持一致。

调用点：`auth_middleware`（api/lib.rs:702）改调 `authorize`；403 文案由「项目管理员无权访问该资源」改为「无权访问该资源」（三角色共用）。

> **既有单测同步改造（必做，否则 `cargo check --workspace --all-targets` 直接红）**：`mod pa_matrix_tests`（api/lib.rs:4705-4786）的 `use super::pa_allowed;`（:4707）与 **22 处调用**（:4720,4722,4723,4724,4731,4732,4734,4735,4736,4737,4738,4740,4741,4743,4753,4758,4763,4768,4775,4777,4778,4780）机械替换 `pa_allowed(` → `authorize(`（同为三元、首参 `&Principal`，PA 语义不变，22 条断言应全绿）。**注意 `use super::pa_allowed;`（api/lib.rs:4707）本身不含括号，机械替换覆盖不到**：`mod pa_matrix_tests` 的导入改为 `use super::authorize;`（它只调 `authorize`）；新增的 `mod ga_matrix_tests` 用 `use super::{authorize, ga_allowed};`（既要走 `authorize` 的整链，也要直接断言 `ga_allowed` 的布尔结果）。漏改会让 `pa_allowed` 导入变成未使用 → `unused_imports` 在 `clippy -D warnings` 下直接红。另紧随其后新增 `mod ga_matrix_tests`，按设计 §7 矩阵逐行断言 GA：允许项（`GET /api/v1/projects/p`、任意项目草稿/发布、共享读写、`/audit`、`/projects`、`/cluster/members`、logout/heartbeat/me-password）为 `true`；拒绝项（`/api/v1/admins*`、`/api/v1/admin/*`、`POST /api/v1/projects`、`DELETE /api/v1/projects/p`、`/projects/p/admins*`、`/projects/p/tokens*`、`/cluster/{join,promote,remove}`）为 `false`。

> **两处不需改动的 Principal 匹配（已核实，避免实现者误改）**：api/lib.rs:1864（`list_shared`，臂为 `ProjectAdmin{..} => 过滤` + `_ => 不过滤`，GA 落 `_` 即「可见全部 refs」，符合矩阵）与 api/lib.rs:2680（`render_config` 的 reveal 审计 operator，`Ok(p) => principal_op(p)` 是绑定臂，新增变体后自动经 `principal_op` 得到 `ga:{u}`）。两者都无需修改，且不会编译失败。
>
> **必查清单（分级；「编译器会兜住」只对第 ① 类成立）**
>
> ① **编译期必报错（穷尽 match，7 处；下列坐标同为 **臂行**，与 :488 口径一致，match 起始行写在括号里）**：734（渲染 `session_allowed`，match 起于 :728、臂块 :727-741）、2636（`reveal` 会话校验，match :2635）、3099（logout，match :3098）、3147（`principal_op`，match :3146）、3167（heartbeat，match :3166）、3274（audit 过滤，match :3273）、3823（`change_my_password`，match :3822，任务 4.4 新增 GA 臂）。
> ② **不报错但必须确认语义（4 处）**：476/479（`resolve_principal` 的路由占位构造，**不改**）、2251/2344（`matches!` 防御，改 `is_super`）。
> ③ **通配站点（不报错；GA 落默认分支恰好正确，必须在代码里加注释锁定，否则后续改动没有回归网）**：813（`list_projects` 的 if-let → GA 不过滤，看全量）、1864-1872（`list_shared` 的 `_` → GA 不过滤 refs）、1721+1836/1845（共享草稿写 handler 无 principal 形参 → 由任务 4.7 引入 operator）。
> ④ **构造点（不需改）**：dsh-core/src/state.rs:2405、2440（管理员会话的 `principal: Principal::Admin`）。
>
> 行号口径：**全篇统一用 `Principal::Admin` 的「臂行」**（与 `rg -n Principal::Admin` 的输出一致）：734、2636、3099、3147、3167、3274、3823；对应的 `match` 起始行分别为 733/2635/3098/3146/3166/3273/3822（差 1）。设计 §14/§15.1 同此口径。

新增辅助：

```rust
/// 是否超级管理员（根账号）。仅用于 handler 内防御性校验与非授权面判定。
fn is_super(p: &dsh_core::Principal) -> bool {
    matches!(p, dsh_core::Principal::Admin)
}
```

**两处渲染判定**改为「super 或 GA 放行，PA 比对项目」：

- api/lib.rs:734：`Ok(dsh_core::Principal::Admin) => true,` → `Ok(dsh_core::Principal::Admin | dsh_core::Principal::GlobalAdmin { .. }) => true,`
- api/lib.rs:2636：同上（`reveal=true` 分支）。

**handler 防御校验**：2251（`create_project_token`）、2344（`delete_project_token`）的 `!matches!(&*principal, Principal::Admin)` → `!is_super(&principal)`；403 文案「仅超级管理员可管理访问令牌」。**不**给 `create_project`/`delete_project` 追加 handler 级 `is_super` 校验——设计 §16 明确把该改动列为「本期不做」（super-only 由中间件矩阵保证），避免设计外改动与测试空白。

**audit 过滤**（api/lib.rs:3273-3276）：`match` 改三分支——`Admin | GlobalAdmin{..} => None`，`ProjectAdmin{project,..} => Some(project.0)`。

**`principal_op`**（api/lib.rs:3145-3150）新增分支：`GlobalAdmin { username } => format!("ga:{username}")`。

### 任务 4.3 login GA 分派（T9）

`LoginResp`（api/lib.rs:2720-2728）新增可选字段：

```rust
    /// 账号用户名（GA/PA 回填；super-admin 不返回）。
    #[serde(skip_serializing_if = "Option::is_none")]
    username: Option<String>,
```

`LoginReq.username` 文档注释更新为「账号用户名（缺省/空 = 超级管理员，向后兼容）」。

`login`（api/lib.rs:2765-2774）分派改造：

```rust
    let account_username = req
        .username
        .as_deref()
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(str::to_string);
    if let Some(username) = account_username {
        // D3：用户名全局唯一 → 先查 GA，未命中回退 PA（两条路径的既有语义都不变）
        let is_ga = {
            let sm = app.sm.read().map_err(lock_err)?;
            sm.get_global_admin(&username).ok().flatten().is_some()
        };
        return if is_ga {
            ga_login(app, req, username, &ip, &headers).await
        } else {
            pa_login(app, req, username, &ip, &headers).await
        };
    }
```

**`LoginResp` 构造点补齐（全文 4 处，缺字段即 E0063）**——`api/lib.rs:2841`（super 分支）补 `username: None`；`api/lib.rs:2882`（login 的非 leader 转发分支）与 `api/lib.rs:3032`（pa_login 的转发分支）必须从 leader 响应体回落新字段：

```rust
    username: body.get("username").and_then(|u| u.as_str()).map(str::to_string),
```

`api/lib.rs:3006`（pa_login 本地分支）补 `username: Some(username.clone())`。**转发分派逻辑**确实无需新增分支（leader 侧执行同一 GA→PA 分派），但**转发响应构造**必须补该字段，否则 follower 登录（Admin UI 命中 follower 是常态）响应缺 `username`。

`ga_login`：**逐行照抄 `pa_login`（api/lib.rs:2931-3085）**，差异仅 6 处：

| 项 | pa_login | ga_login |
| --- | --- | --- |
| 账号读取 | `sm.get_project_admin(&username)` | `sm.get_global_admin(&username)` |
| 会话命令 | `Command::MultiPaSessionLogin { username, .. }` | `Command::MultiGaSessionLogin { username, .. }` |
| token | `new_pa_token(&username, &session_id)` | `new_ga_token(&username, &session_id)` |
| operator | `pa:{username}` | `ga:{username}` |
| 响应 | `role: "project_admin", project: Some(project), username: Some(username)` | `role: "global_admin", project: None, username: Some(username)` |
| 非 leader 转发响应 | `LoginResp { token, role, project }`（api/lib.rs:3032） | 同处补 `username`（从 body 回落，见上） |

（login 审计的 `project` 参数：GA 传 `None`；PA 传 `Some(project)` 不变。）
非 leader 转发沿用 `LoginReq { password, username }` 透传，leader 侧执行同一 GA→PA 分派——**无需新增转发分支**。
`pa_login` 的响应补 `username: Some(username)`（其余不变）。

### 任务 4.3b logout / heartbeat 的 GA 臂（T9b）

`logout`（api/lib.rs:3098-3129）与 `heartbeat`（api/lib.rs:3166-3211）的 `match principal.0` 都是**穷尽匹配**（无 `_` 通配），新增 `GlobalAdmin` 变体后必然编译失败，必须补臂：

```rust
        dsh_core::Principal::GlobalAdmin { ref username, .. } => match sid {
            Some(sid) => {
                app.write(
                    &Command::MultiGaSessionLogout {
                        username: username.clone(),
                        session_id: sid,
                    },
                    now_ms(),
                )
                .await?;
            }
            // GA token 恒带 sid（ga.{u}.{sid}.{secret}）；缺 sid 只可能来自畸形 token
            None => {
                return Err((
                    StatusCode::UNAUTHORIZED,
                    Json(ApiErrorBody {
                        code: "ERR_SESSION_EXPIRED".into(),
                        message: "需要管理员会话".into(),
                        detail: None,
                    }),
                ))
            }
        },
```

`heartbeat` 同型，写 `Command::MultiGaSessionHeartbeat { username, session_id, expires_at: expires }`（`None` 臂同样 401）。审计沿用 `principal_op(&principal)`（任务 4.2 已补 `ga:{u}` 分支），action 仍为 `logout`，与现状一致。

### 任务 4.4 自助改密 GA 分支（T10）

`change_my_password`（api/lib.rs:3812-3893）新增 match 臂（放在 `ProjectAdmin` 臂之后）：

```rust
        dsh_core::Principal::GlobalAdmin { username } => {
            let account = {
                let sm = app.sm.read().map_err(lock_err)?;
                sm.get_global_admin(username).ok().flatten()
            };
            let ok = account
                .as_ref()
                .map(|acct| verify_password(&req.current_password, &acct.password_hash, &acct.salt))
                .unwrap_or(false);
            if !ok {
                return Err(bad_current_password());
            }
            let (salt, hash) = salted_password_hash(&req.new_password).map_err(ApiError::from)?;
            app.write(
                &Command::GlobalAdminSetPassword {
                    username: username.clone(),
                    salt,
                    password_hash: hash,
                },
                now,
            )
            .await?;
            // apply 已级联收回该账号全部会话（改密即重登）
            app.audit
                .append(
                    "set_password_self",
                    None,
                    None,
                    None,
                    None,
                    serde_json::json!({ "username": username }),
                    &operator,
                )
                .await;
        }
```

super-admin 臂不变（`AdminSetPassword` + `MultiSessionLogoutAll` 只清 `sess/admin*`）→ GA 会话存活，测试锁定。

### 任务 4.5 force-logout 分派（T11）

`admin_force_logout`（api/lib.rs:3716-3779）改造：

- 新增辅助（handler 内）：`let is_ga = username.map(|u| sm.get_global_admin(u)...is_some())`（读锁内取，锁不跨 `.await`）。
- `(Some(username), Some(sid))`：GA 命中 → `MultiGaSessionLogout`；否则现状 `MultiPaSessionLogout`。
- `(Some(username), None)`：GA 命中 → `MultiGaSessionLogoutAll`；否则现状 `MultiPaSessionLogoutAll`。
- `(None, None)`：追加一次 `Command::GlobalAdminSessionsRevokeAll`（先 `MultiSessionLogoutAll`，再 revoke；两次写，顺序无关），但**必须与任务 4.6b 同样加 `has_ga` 守卫**（无 GA 时不写新变体，保证滚动升级窗口内全量下线仍可用）。**PA 会话不在其中**（无「全部 PA 会话」命令，见设计 §16）；文档与 UI 文案**不得**称其为「全系统恐慌按钮」。
- `(Some("admin"), _)`：走 PA 分支 → `delete_all_pa_sessions("admin")` 是**静默 no-op**（`admin` 是保留名，既非 GA 也非 PA）→ 返回 200 但 super 会话不变。**保留该现状**（不特殊处理），但测试需锁定语义（§5 第 11 条）。
- 审计 detail 补 `scope: "ga"|"pa"|"admin"`（便于事后区分），action `force_logout` 不变。
- `ForceLogoutReq` 注释更新（username 现在同时覆盖 GA/PA）。

### 任务 4.6 `/api/v1/admins` 四端点（T12）

请求/响应体（紧随 `ProjectAdminResp`，api/lib.rs:2095-2100）：

```rust
#[derive(Deserialize)]
struct GlobalAdminCreateReq {
    username: String,
    password: String,
}

#[derive(Serialize)]
struct GlobalAdminResp {
    username: String,
    created_at: i64,
}
```

handler（`create_project_admin` 同构）：

```rust
async fn create_global_admin(
    principal: axum::Extension<dsh_core::Principal>,
    State(app): State<ApiState>,
    Json(req): Json<GlobalAdminCreateReq>,
) -> Result<(StatusCode, Json<GlobalAdminResp>), (StatusCode, Json<ApiErrorBody>)> {
    if req.password.len() < 6 {
        return Err(ApiError(dsh_core::Error::validation("密码至少 6 位")).into());
    }
    let now = now_ms();
    // GA 与 PA 同用 argon2 PHC（salted_password_hash 返回 salt="" 占位）
    let (salt, hash) = salted_password_hash(&req.password).map_err(ApiError::from)?;
    match app
        .write(
            &Command::GlobalAdminCreate {
                username: req.username.clone(),
                salt,
                password_hash: hash,
                ts: now,
            },
            now,
        )
        .await
    {
        Ok(_) => {
            app.audit
                .append(
                    "global_admin_create",
                    None,
                    None,
                    None,
                    None,
                    serde_json::json!({ "username": req.username }),
                    &principal_op(&principal),
                )
                .await;
        }
        // Conflict 必须显式 return，否则冲突请求会落到下面的 201（账号并未创建）
        Err(e) if e.0.kind == ErrorKind::Conflict => {
            return Err((
                StatusCode::CONFLICT,
                Json(ApiErrorBody {
                    code: "ERR_ACCOUNT_EXISTS".into(),
                    message: e.0.message.clone(),
                    detail: e.0.detail.clone(),
                }),
            ))
        }
        Err(e) => return Err(e.into()),
    }
    Ok((StatusCode::CREATED, Json(GlobalAdminResp { username: req.username, created_at: now })))
}
```

> 实现细节：四个 handler 都取 `principal: axum::Extension<dsh_core::Principal>` 并用 `principal_op(&principal)` 记审计（实际恒为 `admin`，因为中间件已保证 super-only；防御性一致）。

**四个 handler 的形参**：`create_global_admin` / `list_global_admins` / `set_global_admin_password` / `delete_global_admin` **都**把 `principal: axum::Extension<dsh_core::Principal>` 放在**第一位**（`principal_op(&principal)` 需要它；漏了即 `E0425: cannot find value principal`），`Json<T>` 提取器必须放最后（axum 提取器顺序约束）。

- `list_global_admins`：`sm.list_global_admins()` → `[{username, created_at}]`（**不含 salt/hash**）。
- `set_global_admin_password`：body 复用 `SetPasswordReq`；`< 6` → 422 `ERR_VALIDATION`；`GlobalAdminSetPassword`；NotFound → **`return Err((StatusCode::NOT_FOUND, Json(ApiErrorBody { code: "ERR_ACCOUNT_NOT_FOUND", ... })))`**（参照 api/lib.rs:2427-2434，勿写成空块）；审计 `global_admin_set_password`；成功 **200 `{"changed": true}`**（对齐 api/lib.rs:3896-3924 的 `admin_set_password`，**不是** PA 的 204）。
- `delete_global_admin`：`GlobalAdminDelete`；NotFound → 显式 404 `ERR_ACCOUNT_NOT_FOUND`（同型）；审计 `global_admin_delete`；成功 204。
- **密码长度下限**：`create_global_admin` 与 `set_global_admin_password` 均校验 ≥6 位（与 `admin_set_password` 一致，api/lib.rs:3900），不足 → 422；注意 PA 创建端点（api/lib.rs:2136）**没有**该校验，差异记入 docs/09-admin.md（设计 §8 已登记）。

路由（api/lib.rs:4467 附近，`/api/v1/projects` 之前）：

```rust
        .route("/api/v1/admins", get(list_global_admins).post(create_global_admin))
        .route(
            "/api/v1/admins/{u}",
            delete(delete_global_admin).put(set_global_admin_password),
        )
```

> 路由顺序提示：axum 0.8（server/Cargo.toml:31）的 `/api/v1/admins` 与 `/api/v1/projects/{p}/admins` 是不同静态段，matchit 无冲突；`/api/v1/admins/{u}` 与既有 `/admin/{*path}`（api/lib.rs:4452）亦互不遮蔽。

### 任务 4.6b 恢复通道同步回收 GA 会话（T12c）

**问题**：`admin_reset_password`（api/lib.rs:3932-3975）只写 `Command::MultiSessionLogoutAll`（:3960），而该命令**只清 `sess/admin*`**（keys.rs:100 + state.rs:2592-2599）。设计 §15.3 要求「GA 不持有 join-token 时无法接管 super 凭据」，但如果只回收 super 会话，运维按指引用「轮换 join-token + reset-password」收口时，**GA 旧 token 仍可管理全部项目配置与共享配置（含 secret）**——文档承诺与行为不符。

```rust
    // admin_reset_password 内，MultiSessionLogoutAll 之后追加：
    // 仅在确实存在 GA 账号时才写新变体——无 GA 时保持旧日志形状，
    // 混合版本集群（滚动升级窗口）里恢复通道仍可用（兼容边界 §0 / 设计 §11.1）。
    let has_ga = {
        let sm = app.sm.read().map_err(lock_err)?;
        sm.has_global_admins().map_err(ApiError::from)?   // 只判键空间，见访问器注释
    };
    if has_ga {
        app.write(&Command::GlobalAdminSessionsRevokeAll, now_ms()).await?;
    }
```

**为什么必须加这个守卫**：`GlobalAdminSessionsRevokeAll` 是**新变体**，旧节点反序列化整条日志 Entry 会硬失败（dsh-raft `store.rs:176 out.push(de(v.value())?)`，不是"跳过一条"）。若无条件写，运维在**滚动升级窗口**里用唯一的找回通道（`defing admin reset-password`）、或在没有任何 GA 账号时执行 `force-logout {}`，都会写下旧节点读不了的条目——恰恰在最需要恢复通道的时刻把它打坏，而风险表里"不建 GA 即无影响"的缓解覆盖不到这条路径。判定只读状态机、确定性、不跨 `.await` 持锁；有 GA 时语义与本节完全一致，§5 第 8b 条的「GA token → 401」断言不受影响。

- 与 force-logout `{}` 同口径（同时收口 super 与 GA 会话），**仍不触碰 `sess/pa/*`**（PA 逐账号踢，见设计 §16）。
- 测试（并入 §5 第 **8b** 条）：ops token 调 `POST /api/v1/admin/reset-password` 后 —— super 旧 token → 401、**GA token → 401**、PA token → 200（三者同时断言）。

### 任务 4.7 共享草稿写按主体记账（T12b，修复既有审计逃逸）

**问题**：`write_shared_draft`（api/lib.rs:1721）把 `Command::SharedDraftUpdate.operator`（:1813）与审计 operator（:1827）**硬编码为 `"admin"`**，且 `create_shared`（:1836）与 `update_shared_draft`（:1845）不接收 principal。GA 对这两条路径矩阵明示放行（设计 §7 行 12），若不改，GA 把任意值（含 secret）灌进所有项目时，状态机算子与审计都记成超级管理员——无法归因，是本次唯一的审计逃逸面。对照同族端点：`delete_shared`（:1964/1977）与 `publish_shared`（:1944）已用 `principal_op(&principal)`。

```rust
async fn write_shared_draft(
    app: &ApiState,
    req: SharedItemReq,
    action: &str,
    operator: &str,
) -> Result<serde_json::Value, (StatusCode, Json<ApiErrorBody>)> {
    // ... 原逻辑不变，仅两处替换：
    //   Command::SharedDraftUpdate { item, operator: operator.to_string() }   // :1813
    //   app.audit.append(action, ..., operator).await;                        // :1827
}
```

- `create_shared`（:1836）与 `update_shared_draft`（:1845）增加形参 `principal: axum::Extension<dsh_core::Principal>`，调用处传 `&principal_op(&principal)`；两个 handler 都在 `auth_middleware` 之后，扩展必定存在。
- **全仓引用共 8 处，改签名后必须全部改到**（漏一处即 `cargo check --workspace --all-targets` 失败）：定义 `:1721`、生产调用 `:1840`/`:1849`、**cfg(test) 模块导入 `:4952`（`use super::{write_shared_draft, ApiState, SharedItemReq};`）与 4 处三参测试调用 `:5016`/`:5046`/`:5092`/`:5117`**。测试调用统一补第 4 参 `"admin"`（这些用例只覆盖密文保留/描述更新等分支，与主体无关），例如 `write_shared_draft(&app, req, "shared_draft_update", "admin")`。**新签名第 4 参是必填参数（不加默认值），因此这 4 处会报 E0061。**
- 对 super 而言 operator 仍是 `"admin"`（`principal_op(Admin)`），**行为无变化**；PA 到不了这两个 handler（矩阵 403）。
- 测试：GA 的 `POST /api/v1/shared` 与 `PUT /shared-draft` 产生的**审计条目** operator == `ga:{u}`（§5 第 13 条）。**状态机侧无法断言**：`apply_shared_draft_update` 的 `_operator` 形参被丢弃且不落 KV（state.rs:2159/2179），命令侧字段由代码评审覆盖。

## 5. S3：API 集成测试（T13）

**测试环境（T13.0，先做）——`tests/*.rs` 每个文件是**独立 crate**，不能跨文件 `use`，因此按下述方式分工**：

- **(a) 单节点内存夹具**（仿 `http_project_admin.rs:19-66` 的 `start()` / `start_with_cipher()`：`ApiState::new(sm, WatchHub::new(), None, None, ...)`，**raft = None**）：覆盖创建链路、登录、允许项、拒绝项、路径绕过、token 负形、会话、force-logout、审计等**全部非集群条目**。注意 `app.raft.is_none()` 时集群路由根本不注册（api/lib.rs:4549-4555），任何 `/api/v1/cluster/*` 断言都会得到 **404 而非 200/403**（`http_project_admin.rs:333-338` 已用 `assert_ne!(c, 403)` 处理该事实）。
- **(b) 本文件内的「真 raft + 已配置 `--join-token`」夹具**（仿 `http_project_admin.rs:69-87` 的 `start_with_join_token()`，它已用 `ApiState::with_retention(…, join_token, …)` 注入成功；真 raft 骨架参照 `cluster_join_idempotent.rs:32-68`）：只服务第 6 条的 `POST /api/v1/cluster/join` → 401 断言。
- **(c) 集群用例（不在本文件）**：`GET /api/v1/cluster/members` → 200、`POST /api/v1/cluster/{promote,remove}` → 403、**非 leader 登录转发**（第 15 条）就地扩展 `tests/leader_write_forward.rs`（复用其 `start3()` 与 `login(any_base)`，:31-144）——该文件是另一个 crate，本文件无法引用它。

**每条编号断言各自独立 `#[tokio::test]` + 各自 `start()`**（照 `http_project_admin.rs:181-219` 一测一服务器）：登录节流是**进程内按对端 IP 计数**、窗口 600s、失败 ≥5 即 blocked → **429 `ERR_TOO_MANY_ATTEMPTS`**（api/lib.rs:4252-4303、2755-2763），同一夹具内累计的失败登录会把后续本该 401 的断言变成 429；同一用例内确需多次失败登录时，两次之间插一次成功登录以 `reset` 计数。同理，**第 8b 与第 10 条必须各用独立夹具**：两者都会覆写**状态机里的超级管理员密码哈希**（`Command::AdminSetPassword`，api/lib.rs:3904-3908 / 3952-3958）并清空全部 `sess/admin*` 会话（api/lib.rs:3912 / 3960）；**不存在 `sess/admin-pw` 这个键**。同夹具内后续用 `admin-pw` 的 super 登录必然 401——因为 login 优先读状态机哈希且命中后不再回落 `--admin-password`（api/lib.rs:2776-2782）。

测试内用 `Command::GlobalAdminCreate` 直接 seed 或走 HTTP 创建：

1. **创建链路**：super 登录（空 username）→ `POST /api/v1/admins {username:"ga1",password:"pw12345"}` → 201；`GET /api/v1/admins` 含 `ga1` 且**不含** `salt`/`password_hash` 字段；重复创建 → 409。
2. **登录**：`POST /api/v1/login {username:"ga1",password:"pw12345"}` → 200 且 `role=="global_admin"`、`token` 以 `ga.ga1.` 开头；错密码 → 401 `ERR_BAD_CREDENTIALS`；`login_failed` 审计存在。
3. **允许项矩阵**（对**任意**两个项目 p1/p2 断言 **2xx**：GET、`POST /shared`/`publish`/`promote`/`gray-*` 为 200；`DELETE /shared/{k}`、`DELETE /shared-draft/{k}`、**自建且未发布的** `DELETE /projects/{p}/branches/{b}` 为 **204**——已发布分支恒 **409**，见下方分支删除断言；**`POST /api/v1/logout` 为 204，但它是破坏性的，必须自成一组放在末尾**（见本条末）。

   **前置 seed（必做，否则一批断言必红）**：新建项目 `active_version == 0`（state.rs:1280-1289 只落空结构与三条分支状态），而 `get_config` 在无已发布版本时返回 `Error::not_found("no published version yet")`（state.rs:685-692）→ **`/projects/{p}/branches/dev/config`（admin_config 与渲染端点）恒 404**；`gray-publish` 无草稿时 **409**。因此先对 **p1 与 p2 各**跑一遍（**seed 全部请求都用 GA token 发出**——本条整体是 GA 明文矩阵，且 `结构 draft/publish` 已不再单列断言，seed 成功本身即隐含覆盖了 `/structure-draft` 的 GA 可写面）：`PUT /projects/{p}/structure-draft`（`{"base_version":1,"groups":[{"name":"g","items":[{"key":"k","type":"string","required":true}]}]}`）→ `POST .../structure-draft/publish` → `PUT /projects/{p}/branches/dev/draft`（写 `g/k`）→ `POST .../branches/dev/publish`（写法参照 `http_project_admin.rs:230-251`：**先发布再断言 config 200**）→ **再写一份草稿**（`PUT .../branches/dev/draft`，把 `g/k` 改成灰度值）。**这一步不能省**：`apply_publish` 末尾会执行 `st.value_draft.clear()`（state.rs:1891/2026），发布后值草稿为空，紧接着的 `gray-publish` 会命中 `no pending draft` → **409**（state.rs:2003）。

   **灰度组必须拆两轮执行**（`gray-promote` 与 `gray-abort` 是互斥前置条件）：`apply_gray_promote` 成功后清零灰度状态（`st.gray_seq = 0; st.gray_rule = None;`，state.rs:2095-2096），`apply_gray_abort` 同样清零（state.rs:2139-2140）；两者在无活动灰度时都返回 `Validation` → **422**（state.rs:2065/2134）。因此同一条列表里要求「gray-promote 与 gray-abort 都 2xx」是自相矛盾的，必须分两轮（照 `scripts/api-surface-test.sh:166-192` 的既有写法）：

   - 轮 1：`PUT .../branches/dev/draft`（灰度值）→ `POST .../branches/dev/gray-publish` body `{"rule":{"match_labels":[{"key":"zone","value":"cn-north-1"}]},"comment":"g1","request_id":"ga-g1"}`（200）→ `GET .../branches/dev/gray-status`（200）→ `POST .../branches/dev/gray-promote` body `{"comment":"p","request_id":"ga-p1"}`（200）；
   - 轮 2：`PUT .../branches/dev/draft`（再改值）→ `POST .../branches/dev/gray-publish` body `{"rule":{"percentage":100},"comment":"g2","request_id":"ga-g2"}`（200）→ `POST .../branches/dev/gray-abort` body `{"comment":"a","request_id":"ga-a1"}`（200）。

   （`GrayPublishReq` 必带 `rule`，见 api/lib.rs:290-296。）

   **分支删除断言（用自建分支）**：`POST /projects/p2/branches {"name":"tmp"}` → `DELETE /projects/p2/branches/tmp` → **204**（照 `scripts/api-surface-test.sh:107-111`）。**不要**断言 `DELETE /projects/p2/branches/dev`：该分支已被 seed 发布（`active_version` 在 seed 后为 **2**、轮 1 的 `gray-promote` 后为 **3**），加上 seed 末尾重写的草稿使 `value_draft` 非空，两个条件同时命中守卫 → apply 返回 `Conflict` → **409**（state.rs:1402-1406；`lib.rs:219-220` 映射），既不是 204、也不会让后续 `/branches/dev/...` 变 404——原先「必须放最后」的排序理由不成立，改为「自建分支的删除断言排在它自身创建之后」即可。**`GET /projects/p1` 与 `GET /projects/p2` 项目详情 → 200**、`GET .../structure`、分支列表/创建（`GET/POST /projects/p2/branches`）、**注意「结构 draft/publish」不要重复断言**（它就是 seed 本身：`base_version` 恒为 1，而 seed 发布后 `structure.version == 2`，再 PUT `{"base_version":1}` 必 **409**，state.rs:1424-1429）、**自建分支删除（见上，勿用 `dev`）**、分支 draft/publish、**rollback（`to_version` 必须满足 `0 < v < active_version`，见下）**、promote、diff、versions、灰度全组（按上面两轮配方：`gray-publish`/`gray-status`/`gray-promote` 轮 1，`gray-publish`/`gray-abort` 轮 2——**不要**放在同一轮里同时断言 promote 与 abort）。**这三个端点的前置/参数必须写对，否则必红**：

   - **rollback**：`apply_rollback` 守卫 `to_version == 0 || to_version >= st.active_version` → `Validation` → **422**（state.rs:1929-1934）。**版本号要算准**：seed 的 `POST .../structure-draft/publish` 会给**每条分支**写入 v1（state.rs:1482-1512 `st.active_version = vno`，vno=1），随后的 `POST .../branches/dev/publish` 使 dev 变 **2**（state.rs:1874/1889），轮 1 的 `gray-promote` 再推到 **3**（state.rs:911-913/2094；旁证 `scripts/api-surface-test.sh:184` 在同型 seed 下断言 `active_version==3`——注意 :169 断言的是 `gray_seq`/事件，别一起引）。因此：`to_version=1` 在 **seed 之后**就已满足 `0 < v < active`（守卫 state.rs:1929-1934）→ 200。**注意上述推导只对 `dev` 成立**：`test`/`prod` 在 seed 后仍是 `active_version == 1`（只有 dev 被后续 publish 推到 2），对它们 rollback(`to_version=1`) 会命中 `to_version >= active` → **422**。**补救不是 `promote`**：`POST /promote` 只把源分支的**活动值写进目标分支的草稿**（api/lib.rs:1630-1643 调 `publish.update_draft(...)`，`apply_draft_update` 不碰 `active_version`），所以 promote 之后 test 仍是 `active_version == 1`，rollback(1) 依旧 422 —— 必须先对 test 做一次**真正的值发布**（`POST /projects/p2/branches/test/publish`）把 active 推到 ≥2 再 rollback。本条枚举未指名分支，实现时统一按 **dev** 写。把 rollback 放在轮 1 `gray-promote` 之后是**充分而非必要**（保证快照 v1 一定存在），实现时按你的执行次序取其一即可，不要写成「seed 后必须 422」。
   - **promote**：body 必须 `{"from":"dev","to":"test"}`（handler 用 `get_config(from, 0)` 读源分支，api/lib.rs:1569-1571 → 源未发布则 **404**；`to` 不要用 `dev`，它会往目标分支写草稿（api/lib.rs:1630-1643），污染后续灰度断言）。
   - **diff**：必须带查询串 `?branch_a=dev&branch_b=test`——`DiffQuery { branch_a, branch_b }` **没有** `serde(default)`（api/lib.rs:1484-1488），缺参 → **400**（不是 200）。**`GET /api/v1/projects/p2/branches/dev/config`（admin_config 掩码）**、`GET /projects`（含 p1+p2）、`GET /audit`、`GET /cluster/members`（**本文件为无 raft 夹具 → 按 `http_project_admin.rs:333-338` 写法断言 `assert_ne!(c, 403)`**：dev-single 为 404、集群为 200；不得断言 200。集群环境下的 200 断言放在第 15 条）、共享读（`GET /shared`、`GET /shared-draft`）、共享写（`POST /shared` body `{"key":"sk","type":"string","value":{"type":"string","str_value":"sv"},"secret":false,"required":false}`（`SharedItemReq` 的 `key`/`type`/`value` **均无默认值**，api/lib.rs:1670-1681）、`PUT /shared-draft` 同 body、`POST /shared/publish` body `{"comment":"sp"}`（`PublishReq` 的 `comment` 必填，api/lib.rs:273-278）、`DELETE /shared/{k}`、`DELETE /shared-draft/{k}`）——**不要照抄 `http_project_admin.rs:302-310` 的 `json!({})` 写法**：PA 用例能在中间件就 403、body 无关，GA 侧会拿到 422；共享写 body 照 `http_project_admin.rs:349` 起的建共享项写法。、`/v1/projects/p2/branches/dev/config`（掩码）与 `reveal=true`（会话授权）、**`heartbeat`（200）**。**`me-password` 不进本条**：它同样会级联收回该 GA 的全部会话（`GlobalAdminSetPassword`），与下面的 logout 组争抢「最后」的位置；改密已由第 7 条完整覆盖（200 + t1/t2 401 + super 存活），此处不再重复。

   **`logout` 自成一组、放整条最后**（本条唯一的破坏性断言）：`POST /api/v1/logout` → **204**，随后用**同一个** token 请求 `GET /api/v1/projects` → **401**（只断言 204 测不出「登出无效」这类缺陷）。**必须排在 heartbeat 等所有非破坏性断言之后、且本条内不得再有 me-password 之类的破坏性断言**：logout 会删掉该 sid 的 GA 会话（任务 4.3b），而本文件是「一测一服务器」，同夹具内 logout 之后的任何请求都会在中间件层 `resolve_principal` 失败 → 401（api/lib.rs:688-700）。第 7 条另有一次独立的 logout 覆盖（用新密码重登的 token），两者互不干扰。
4. **拒绝项矩阵**（403）：`GET/POST /api/v1/admins`、`PUT/DELETE /api/v1/admins/ga2`、`POST /api/v1/admin/set-password`、`POST /api/v1/admin/force-logout`、`POST /api/v1/admin/rotate-master-key`、`GET /api/v1/admin/snapshot`、`GET /api/v1/admin/retention-status`、`POST /api/v1/cluster/{promote,remove}`、`POST /api/v1/projects`、`DELETE /api/v1/projects/p1`、`GET/POST /api/v1/projects/p1/admins`、**`PUT /api/v1/projects/p1/admins/{u}`**、`DELETE /api/v1/projects/p1/admins/x`、`GET/POST /api/v1/projects/p1/tokens`、**`DELETE /api/v1/projects/p1/tokens/{id}`**。**不要**把 `POST /api/v1/cluster/join` 列进 403 组——它在中间件豁免名单内，非 join token 恒 401（用例见第 6 条）。
5. **路径绕过组**（GA 拒绝面不得被路径写法绕过）：对 `/api/v1/admins` 与 `/api/v1/admin/{set-password,force-logout,rotate-master-key,snapshot,retention-status}` 逐条断言 **403**——**含**这些变体：尾斜杠（`/api/v1/admins/`）、大写（`/api/v1/ADMINS`）、重复斜杠（`/api/v1//admins`）、`%2F`/`%61dmins` 等编码段。**不含 `/api/v1/admin/reset-password`**：它豁免于中间件，非 ops token 恒 **401**。

   **项目作用域路径**（`.admins`/`.tokens` 拒绝面）另测：`GET /api/v1/projects/{p}/admins` 的编码写法（`%70` 编码项目名、大写项目名、尾斜杠、`%2F`）→ 全部 **403**（`project_segment` 只接受 `[a-z0-9-]`，见 api/lib.rs:517-529），与 PA 用例同形。**路径构造组（新增守卫的锁定用例）**：`GET /api/v1/projects/p1//admins` → **403**（中间件空段守卫；`//` 不被 URL 归一化改写，已实测）。

**`..` 一条在本夹具形态下不得断言 403**：`reqwest` / `url` 会先按 WHATWG 规范归一化 —— `GET /api/v1/projects/p1/../admins` 实际发出的是 `/api/v1/projects/admins`（`%2e%2e` 同样被归一化，已实测），于是 `project_segment` 得到合法项目名 `admins`、残留段为空 → GA 放行 → 命中 `GET /api/v1/projects/{p}`（api/lib.rs:4484-4487）→ 项目不存在 → **404**。若要锁定中间件的 `..` 守卫，必须用不归一化的客户端（`hyper::Request` 手工 `Uri` 或裸 `TcpStream`）；否则删掉该条，不要为了凑 403 去改守卫。

`GET /api/v1/projects/p1/%61dmins` → **非 2xx**（中间件层放行，路由 404 兜底，**不得断言 403**）。
6. **token 负形 / join 负形**：`Authorization: Bearer ga.`、`ga.ga1`（段数不足）、`ga.ga1.<pa-secret>`、超长/截断、`adm.` 拼 GA secret → 全部 401 `ERR_SESSION_EXPIRED`。**同组加**：`POST /api/v1/cluster/join`（带 GA token）→ **401 `ERR_UNAUTHORIZED`**（**不得断言 403**）。**但该断言不能落在本文件的单节点夹具上，必须单开一个「真 raft + 已配置 `--join-token`」的夹具**：

1. `/api/v1/cluster/join` 与 `/api/v1/cluster/*` 仅在 `app.raft.is_some()` 时注册（api/lib.rs:4549-4555）——无 raft 夹具下路由不存在 → **404**（既非 401 也非 403；设计 §7 注 3 已载明同一事实）；
2. 更隐蔽的是：`join_token_ok` 在节点**未配置** join-token 时直接 `None => true` 放行（api/lib.rs:3298-3307），所以 `leader_write_forward.rs` 的 `start3()`（:105-114，`join_token=None`）**也不能**用来断言 401——带 GA token 打过去会真的进入 handler。

做法：仿 `tests/cluster_join_idempotent.rs:32-68` 起一个**单节点真 raft** 夹具，但通过 `ApiState::with_retention(..., Some(Arc::from("ops-secret")), ...)` 注入 join-token（参数序见 api/lib.rs:105-119）；先建 GA 并登录拿 token，再带该 token `POST /api/v1/cluster/join` body `{"node_id":9,"http_addr":"127.0.0.1:9009","raft_addr":"127.0.0.1:7009"}` → 断言 **401 `ERR_UNAUTHORIZED`**。**body 必带可解析的 `node_id`**：`JoinReq { node_id: u64, #[serde(default)] http_addr, #[serde(default)] raft_addr }`（api/lib.rs:328-336），缺 body → **415**、`{}` → **422**，都在 handler 之前被 `Json` 拒掉；写法照 `cluster_join_idempotent.rs:84-97` 的 `join()`。第 15 条的 `GET /api/v1/cluster/members` → 200 与 `POST /api/v1/cluster/promote` → 403 仍放在 `leader_write_forward.rs` 的 `start3()`。
7. **自助改密 + 登出有效性**：GA 登录两次拿 t1/t2 → `POST /api/v1/me/password` body `{"current_password":"pw12345","new_password":"pw123456"}` → **200**。**字段名必须是 `current_password`/`new_password`**（`ChangeMyPasswordReq`，api/lib.rs:3786-3792，无 serde alias）；写成 `{current,new}` 会被 axum 的 `Json` 提取器直接拒绝 → **422 纯文本**，正确写法见 http_project_admin.rs:861-863。改密后 t1/t2 均 **401**（`GlobalAdminSetPassword` 在 apply 层级联收回该账号全部会话）；新密码可登录；**super 会话仍有效**（键空间隔离回归）。另：**用新密码重新登录拿到的 token** 调 `POST /api/v1/logout` → **204**（`Ok(StatusCode::NO_CONTENT)`，api/lib.rs:3141），随后用**同一个** token 请求 `GET /api/v1/projects` → **401**（只断言 204 测不出「登出无效」这类缺陷）。**注意别用 t1/t2**——本条上文已改密，t1/t2 早已级联失效，用它们调 logout 连 204 都拿不到。
8. **super 重置 GA 密码** → GA 会话全失效、旧密码 401、新密码可登录。
8b. **恢复通道（ops token）**：`POST /api/v1/admin/reset-password`（Bearer = 节点 `--join-token`）body **必带** `{"password":"pw123456"}`（≥6 位）→ **先断言该请求本身 200**，再看 —— super 旧 token → **401**、**GA token → 401**（任务 4.6b）、**PA token → 200**（`sess/pa/*` 不在此通道内）。

   **body 不能省**：handler 是 `Json(req): Json<SetPasswordReq>`（api/lib.rs:3935），`SetPasswordReq { password: String }` **没有** `serde(default)`（api/lib.rs:3781-3784）⇒ 缺 body/Content-Type → **415**、`{}` → **422**、<6 位 → 422（:3948-3950），全都在 handler 之前被拒：`Command::AdminSetPassword`（:3952-3958）与 `MultiSessionLogoutAll`（:3960）**根本不会执行**，于是「super 旧 token → 401」「GA token → 401」双双假红，而「PA token → 200」是无鉴别力的假绿。

   **夹具还必须自己 seed**：`http_project_admin.rs:69-90` 的 `start_with_join_token()` 体内**没有任何 seed**（只 `new` 一个空 `InMemoryStore` 的 StateMachine，:70-72）——有 seed 的是它的兄弟 `start_with_cipher()`（:27-53，建 p1/p2 + alice）。照抄前者会拿不到 PA token（登录恒 401）；需自行补 `Command::ProjectCreate` + `Command::ProjectAdminCreate`（PA 口令 `"alicepw"`，见第 11 条）。

   **⚠ 该条同样不能落在夹具 (a) 上**：`ops_token_ok` 在节点**未配置** `--join-token` 时**恒拒绝**（`let Some(expected) = &app.join_token else { return false }`，api/lib.rs:3314-3317——注意它比 `join_token_ok` 更严格，后者未配置是放行）。夹具 (a) 的 `join_token=None` ⇒ 该请求恒 401、`AdminSetPassword` 与 `GlobalAdminSessionsRevokeAll` **都不会执行**，于是「GA token → 401」必然红（GA 会话从未被回收）。做法：用**已注入 join-token 的夹具**（`http_project_admin.rs:69-87` 的 `start_with_join_token()` 即是 raft=None + join-token 的现成形态，`with_retention` 的 join_token 为第 11 参），并在该夹具里先登录 super/GA/PA 拿三个 token 再调 reset-password。
9. **super 删 GA** → GA 登录 401；`GET /api/v1/admins` 不再含该账号。
10. **super 改自己密码**（`POST /api/v1/admin/set-password` 或 `/me/password`）→ **GA 会话不失效**（回归）。
11. **force-logout（注意调用者会话会被 `{}` 一起清掉）**：先备好 PA 会话——沿用夹具 seed 的 `alice`（**口令 `"alicepw"`**，见 http_project_admin.rs:42-52：seed 里写的是 `salt:"s1"` + `password_hash: token_hash("s1alicepw")`，而 legacy 校验算的是 `token_hash(salt‖password)`（api/lib.rs:2123-2125），所以 `"s1alicepw"` 是**盐与口令的拼接串、不是口令**；同文件 21 处用例用的都是 `"alicepw"`），或先用 super token 经 `POST /api/v1/projects/p1/admins` 现建；拿 `pa_t` 时**必须让 PA 登录排在 `{}` 之前**（否则「PA 不受影响」恒真、无鉴别力）。步骤：

   ① `{username:"ga1"}` → **200**，GA token → **401**，账号仍在（`GET /api/v1/admins` 仍含 `ga1`）；
   ② `{}` → **200** —— 它走 `(None,None)` → `MultiSessionLogoutAll`（api/lib.rs:3763-3765 → state.rs:2518-2521 `delete_all_admin_sessions()`），**连调用者自己的 super 会话一起清**，故此后原 super token 一律 401（可顺带断言）；`pa_t` 仍 **200**；
   ③ **必须先重新 `POST /api/v1/login`（空 username）拿全新 super 会话 t2**，再用 t2 发 `{username:"admin"}` → **200 且 t2 仍 200**（`admin` 是保留名：先查 `adm/ga/admin` 必不命中，走 PA 分支 `delete_all_pa_sessions("admin")` 为静默 no-op，api/lib.rs:3753-3761 + 任务 4.5），以此锁定 no-op 语义。
12. **互斥（以 super 会话发起）**：super token `POST /api/v1/projects/p1/admins {username:"ga1",password:"pw123456"}` → 409 `ERR_ACCOUNT_EXISTS`（apply 层 GA↔PA 互斥）；**大小写变体**：`{username:"GA1",password:"pw123456"}`（对应已存在的 GA `ga1`）→ 409（不区分大小写口径）。**两处 body 都必须带 `password`**：`ProjectAdminCreateReq { username, password }` 两个字段都**没有** `serde(default)`（api/lib.rs:2089-2093），只发 `{username}` 会在 handler 之前被 axum 的 `Json` 提取器拒掉 → **422**（无 Content-Type 时 415），永远到不了 apply 层、拿不到 409；**负形**：同一请求换成 GA token → 403 `ERR_FORBIDDEN`（中间件先拦，到不了 handler）。
13. **审计 operator**：GA 的 login / publish / reveal / set_password_self / **shared_draft_update（`POST /shared`、`PUT /shared-draft`，任务 4.7 的锁定用例）** 条目 operator == `ga:ga1`。**不要断言「状态机 `SharedDraftUpdate.operator` 同值」**——`apply_shared_draft_update(&mut self, item: &SharedItem, _operator: &str)`（state.rs:2159）**丢弃**该参数且不落 KV（函数体只 `save_pending(shared_draft_key, item)`，:2179），HTTP 夹具与 dsh-testkit 都无回读通道，该断言无法实现；命令侧字段的正确性由 `api/lib.rs:1813` 传 `operator` 的代码评审覆盖。`global_admin_create` 条目 operator == `admin`。
14. **矩阵单测回归**：`mod pa_matrix_tests` 22 条断言改调 `authorize` 后全绿；`mod ga_matrix_tests` 断言 GA 矩阵（S2 内完成，属 `cargo check --workspace --all-targets` 的必过项）。
15. **集群用例（落在 `server/crates/dsh-api/tests/leader_write_forward.rs`，不要放进 `cluster_join_idempotent.rs`）**：新增 `ga_login_forwarded_with_username` —— 复用该文件既有 `start3()` 与 `login(any_base)`（:31-144），先用 admin token `POST /api/v1/admins` 建 `ga1`，再**经 follower 节点** `POST /api/v1/login {"username":"ga1","password":...}`，断言 200 且 `role == "global_admin"`、`username == "ga1"`、token 以 `ga.ga1.` 开头（锁定 LoginResp 的转发回落），并在同一夹具断言 follower 侧 `GET /api/v1/cluster/members` 对 GA 返回 200、`POST /api/v1/cluster/promote` 返回 403。**注意 follower 登录有偶发 401 风险**：login 的 GA 分派是**收包节点本地**读 `sm.get_global_admin`（计划任务 4.3），而 `read_mode` 默认 `Stale`、login 不做线性读门控——follower 尚未 apply 到 GA 创建条目时会回落 `pa_login` → 401。先在 leader 建好 `ga1` 再对 follower 断言，或注册一个短暂重试/轮询（仓库既有用例 `leader_write_forward.rs:215-226` 也是"写后直接读任意节点"的风格，故属既有风格问题而非新引入）。
16. **`GET /audit` 的「不过滤」语义（设计 §13 明确要求，仅断言可达不够）**：对照 PA 用夹具 seed 的 `alice`（`username:"alice"`、**`password:"alicepw"`**——不是 `"s1alicepw"`，那是盐 `s1` 与口令的拼接串，见 http_project_admin.rs:42-52 与 api/lib.rs:2123-2125），或用 super token 经 `POST /api/v1/projects/p1/admins` 现建（注意该端点**无**密码长度下限，与 GA 端点 ≥6 不同，见任务 4.6 末注）；在 p1、p2 各造一条审计 → GA `GET /api/v1/audit` **可见 p2 条目**（未被强制 project 过滤）；对照 PA 登录后只可见自己项目条目。
17. **密码下限 + 保留名**：`POST /api/v1/admins {username:"ga2",password:"12345"}` → **422 `ERR_VALIDATION`**（**body 必须带 `username`**：`GlobalAdminCreateReq { username, password }` 两字段都无 `serde(default)`，只发 `{password}` 会先被 `Json` 提取器以**纯文本 422**（无 `code` 字段）拒掉——那样既拿不到 `ERR_VALIDATION`，也根本没测到 ≥6 规则）；`PUT /api/v1/admins/ga1 {password:<5 位>}` → 422；`POST /api/v1/admins {username:"Admin",password:"pw12345"}` → **422**（不得 201，锁定 §3.4(a)/设计 §4 的保留名大小写不敏感口径）。

## 6. S4：Admin UI（T14-T15）

### 任务 6.1 index.html（T14）

- 登录卡片（:58-68）：label/placeholder 改「用户名」「超级管理员留空；全局管理员 / 项目管理员填写」，脚注同步。
- 导航（:79-85）：新增
  ```html
  <button type="button" class="nav-item" id="nav-globaladmins" data-act="switchView" data-nav="globaladmins"><svg class="ic"><use href="#i-admin"/></svg><span>全局管理员</span></button>
  ```
  `#nav-admins` 文案「管理员」→「项目管理员」。
- 新视图（紧随 `#view-admins`）：`#view-globaladmins`，含创建表单（`#ga-username` / `#ga-password` / `#ga-err` / 创建按钮）、账号表（`#ga-body`：用户名 / 创建时间 / 改密 / 删除）、说明卡（GA 权限边界：全部项目配置 + 全部共享配置；不能管理账号与集群）。

### 任务 6.2 app.js（T15）

- `renderSession`（:365-372）：新增
  ```js
  else if (S.role === 'global_admin') { name = '全局管理员'; sub = '全局'; ch = 'G'; }
  ```
  `admin` 分支文案「管理员/全局」→「超级管理员/全局」。
- 门控（:379-383）：
  ```js
  const isSuper = S.role === 'admin';
  const isPa = S.role === 'project_admin';
  // 仅超级管理员：项目管理员账号管理、全局管理员管理、访问令牌
  for (const id of ['nav-admins', 'nav-globaladmins', 'tab-tokens']) {
    const el = $(id); if (el) el.classList.toggle('hidden', !isSuper);
  }
  // 共享配置：PA 隐藏（GA 与 super 可见）
  { const el = $('nav-shared'); if (el) el.classList.toggle('hidden', isPa); }
  ```
- 集群只读判据（:2156）：`S.role === 'project_admin'` → `S.role !== 'admin'`。
- 新 action（紧随 :2286 的 PA 管理块）：`loadGlobalAdmins` / `actions.createGlobalAdmin` / `actions.setGlobalAdminPw` / `actions.deleteGlobalAdmin`，分别 GET/POST `/api/v1/admins` 与 PUT/DELETE `/api/v1/admins/{u}`；改密/删除走 `openModal`（复用现有交互）。
- 视图切换登记：`switchView`（app.js:215-224）补 `if (v === "globaladmins") loadGlobalAdmins()`。
- **username 落盘与显示**（否则服务端新增的 `username` 是无消费者的死字段）。**注意 `LS_USER` 目前不存在，必须先在 app.js:48 的常量声明里追加**（否则 `localStorage.setItem(LS_USER, ...)` 抛 `ReferenceError`，被 `doLogin` 的 `catch`（:359-361）吞掉 → token 已写入但 `enterApp()` 永不执行 → **三种角色全部登不进去**，且 `node --check` 是纯语法检查抓不到）：

  ```js
  const LS_TOKEN = 'dsh_admin_token', LS_ROLE = 'dsh_admin_role', LS_PROJ = 'dsh_admin_project', LS_THEME = 'dsh_theme', LS_USER = 'dsh_admin_username';
  ```

  再：`doLogin`（app.js:342-357）增 `localStorage.setItem(LS_USER, r.username || "")`；**三处清理点**同步 `localStorage.removeItem(LS_USER)`——`doLogout`（:337）、`sessionExpired`（:107）、改密成功后的本地清理（:2312）；`enterApp`（:374-387）在调用 `renderSession()` **之前**赋 `S.username = localStorage.getItem(LS_USER) || ""`；`renderSession` 的 GA 分支改为 `name = "全局管理员"; sub = S.username ? "全局 · " + S.username : "全局"; ch = "G";`，PA 分支 sub 追加 `S.username`。

### 任务 6.3 称谓替换（T15b，收尾必做）

落地后「全局管理员」同时指 super 与新角色会直接误导：GA 会在访问令牌页看到「仅全局管理员可查看访问令牌」（app.js:243）却被 403。**原指根账号的一律改「超级管理员」，新角色一律写「全局管理员（GA）」**：

| 文件 | 行 |
| --- | --- |
| server/crates/dsh-api/admin/index.html | 59、68（脚注）、310、427、436（注释）、440、471、524（注释） |
| server/crates/dsh-api/admin/app.js | 243、348、2208、2288 |
| docs/09-admin.md | 10、14、16、21、24、26、43、74 |
| docs/07-tokens.md | 9 |
| docs/quickstart.md | 27（+ 登录说明「超级管理员留空用户名」） |
| docs/index.md | 47 |
| README.md | 19、87（+ :97 安全条目补 GA） |
| dev_docs/deployment-guide.md | 113、460、477 |
| api/openapi.v1.yaml | 848（用户可见契约：「仅全局管理员可管理」）；**54**（`/api/v1/me/password` 的 summary「自助修改自己的密码（全局管理员 / 项目管理员通用）」——与 docs/09-admin.md:16 同一句语义，必须一并三角色化） |
| scripts/sdk-contract-test.sh、scripts/sdk-grpc-contract-test.sh | 28、30（注释「仅全局管理员可建」） |

**以下 6 处不是改名，必须逐处改写（机械替换会写出与必测项相反的话）**：

- `docs/09-admin.md:16` → 「### 修改自己的密码（超级管理员 / 全局管理员 / 项目管理员）」
- `docs/09-admin.md:18` → 「（🔒 图标按钮，三种角色均可见）」（原文「两种角色」漏掉 GA，且不含「全局管理员」字样，`rg` 兜不住）
- `docs/09-admin.md:21` → 「修改成功后**该主体的全部旧会话立即失效**，需重新登录（超级管理员改密只失效其 `sess/admin*` 会话，**不影响 GA/PA**；全局管理员与项目管理员改密仅失效该账号自身的全部会话）」——原文「全局管理员改密使全部管理员会话失效」与必测项「super 改自己密码 → **GA 会话不失效**」（§5 第 10 条）**直接相反**
- `index.html:471` → 「…；**超级管理员**改密同样走该入口，改后其全部会话失效（不影响全局管理员）；**全局管理员**改密走该入口，改后该账号全部会话失效。」
- `index.html:524` 注释 → 「（自助：超级管理员 / 全局管理员 / 项目管理员，需验证当前密码）」
- `app.js:2288` 注释 → 同上

验收：`rg -n 全局管理员 server/crates/dsh-api/admin docs README.md dev_docs/deployment-guide.md **dev_docs/design/project-admin.md** api/openapi.v1.yaml scripts server/crates/dsh-api/src` 逐条人工判定（**`dev_docs/design/` 必须在列表内**——本任务要改的 `project-admin.md` 15 行称谓就在那里，仅列 `dev_docs/deployment-guide.md` 会扫不到，漏改后判据照样绿）——只允许指向**新角色**的用法残留（`api/openapi.v1.yaml:848` 与两个 sdk 契约脚本的注释也在范围内；`server/crates/dsh-api/src` 内为内部注释，可选但建议一并改），且上述 6 处的语义必须与 §5 第 7/10 条的会话失效范围断言一致。

验证：`node --check server/crates/dsh-api/admin/app.js`（**仅语法**，抓不到未声明标识符）+ `cargo build -p dsh-api` + **未声明常量自检**：

```bash
# 判据：无输出（有输出即失败，退出码非 0 才能接 CI）
bad=0
for v in $(rg -o 'LS_[A-Z_]+' server/crates/dsh-api/admin/app.js | sort -u); do
  rg -q "(^|[ ,])$v *=" server/crates/dsh-api/admin/app.js || { echo "UNDECLARED $v"; bad=1; }
done
exit $bad
```

> **正则必须写成 `(^|[ ,])$v *=`，不能写 `const .*[ ,]$v *=`**：常量列表的**第一个**声明符紧跟在 `const ` 之后（如 `const LS_TOKEN = ...`），前面既无空格也无逗号，后者会**恒报 `UNDECLARED LS_TOKEN`**——即实现全对时脚本照样输出一行，通过判据「期望无输出」不可达；更糟的是假阳性与真阳性同形，会把执行者训练成忽略该行，而这条自检正是唯一能拦住「漏声明常量 → `ReferenceError` → 三种角色全部登不进去」的机器门禁（`node --check` 只查语法）。已用当前 `app.js` 实测：旧正则输出 `UNDECLARED LS_TOKEN` 且退出码恒 0，新正则零输出、可作门禁。

+ 手测（super 可见三入口、GA 可见共享配置无账号入口、PA 仅配置/审计/集群只读）+ **实测登录一次**（三种角色各一次，确认不是停在登录页）。

## 7. S5：契约与文档（T16-T18）

### 任务 7.1 openapi.v1.yaml（T16）

**(1) 新增 2 个 path（4 个 operation）**（`/api/v1/admins` 的 GET/POST、`/api/v1/admins/{u}` 的 PUT/DELETE，每个 operation 都必须带 `responses`，否则 `scripts/check-contracts.sh:17-21` 失败）。

**(2) login 契约补齐（是「新增缺失字段」，不是「更新描述」——现状根本没有这两个字段）**：
- `LoginRequest`（api/openapi.v1.yaml:965-970，现仅 `password`/`device_id`）增 `username: { type: string, description: "账号用户名（缺省/空 = 超级管理员）；非空时先匹配全局管理员，未命中再匹配项目管理员" }`；
- `LoginResponse`（:971-977，现仅 `token`/`expires_at`/`must_change_password`）增 `role: { type: string, enum: [admin, global_admin, project_admin] }`、`project: { type: string, nullable: true }`、`username: { type: string }`，`required` 由现状 `[token, expires_at]` **收敛为 `[token]`**（服务端从不返回 `expires_at`/`must_change_password`，连同这两个 property 一并删除）；
- 该 path 的 `summary`（:18「登录（单一管理员会话 I7；第二个登录返回 409）」）改为「登录（多会话并存；GA/PA 由 username 分派）」，并删除 `'409': SessionInUse` 响应行（多会话登录不再 409，见 api/lib.rs:2973、2981-2988）。

### 任务 7.2 schema/storage.v1.schema.json（T16）

**(1)** `$defs` 新增 `GlobalAdminAccount`（描述「全局管理员账号；key = adm/ga/{username}」）：**`required: [username, salt, password_hash, created_at]`**，properties：`username`（string，描述「[A-Za-z0-9_-]{2,64}，≠ admin（不区分大小写），与 PA 用户名互斥（不区分大小写）」）、`salt`（string，描述「保留字段：新格式盐内嵌 PHC 故为空串；旧数据为 sha256(salt||pw) 的盐」）、`password_hash`（string，描述「argon2id PHC（$argon2 前缀）或旧 sha256 hex」）、`created_at`（integer，墙钟 ms）。

**关键**：该文件是**存储工件** Schema（storage.v1.schema.json:4-5「Raft 状态机内持久化工件的 JSON Schema」），必须按落库结构定义——`ProjectTokenRecord`（:486-497，`additionalProperties: false` 且 required 含 `hash`/`created_by`/`revoked` 这些**API 响应里没有**的字段）就是范例。**不得**按 `GET /api/v1/admins` 的响应裁剪字段（那会重犯本任务 (2) 正在修的同类漂移：把所有真实 `adm/ga/{u}` 记录判为非法）；响应字段裁剪在 handler 层（任务 4.6）。另注：该 schema **并没有** `ProjectAdminAccount`（PA 实体也属漏登记，本期不追溯补齐）。

**(2) 顺带修既有漂移（与本次同域，必须一起修）**：`AdminSession`（schema/storage.v1.schema.json:456-485）当前 `required: [token_hash, issued_at, expires_at]` + `additionalProperties: false` + 无 `principal`——而 core 的 `AdminSession` 自 PA 改造起就带 `#[serde(default)] principal: Principal`（core/model.rs:498-500），**真实会话记录会被该 schema 判为非法**。补 `principal`（`oneOf`：`{kind:"Admin"}` / `{kind:"ProjectAdmin",username,project}` / `{kind:"GlobalAdmin",username}`，默认 `{kind:"Admin"}`）；**同时修类型漂移**（只改 `principal` 救不了这类记录）：`issued_at` 现为 `{"type":"string","format":"date-time"}`（schema:469-472）→ 改 `{"type":"integer","description":"墙钟 ms（API 层注入）"}`；`expires_at`（schema:473-476）**必须同时允许 null** —— core 是 `Option<i64>` 且**没有** `skip_serializing_if`（core/model.rs:489-501），`--session-ttl 0`（「0 = 不自动过期」，cli/main.rs:139-141）时每条会话都会序列化成 `"expires_at": null`（api/lib.rs:2821/2985 的 `.then(...)`）：

```json
"expires_at": { "type": ["integer", "null"], "description": "过期时间（墙钟 ms，API 层注入）；null = 不自动过期（--session-ttl 0）" }
```

`required` 收敛为 `[token_hash, issued_at]`；`device_id` 与 `principal` 一并登记为属性。**同时删除 `must_change_password`**（schema:480-483）——core 的 `AdminSession`（core/model.rs:489-501）**没有**该字段，真实记录永不出现它（全仓 grep 仅命中 openapi:977、schema:480 与历史文档）；留着它就与本任务 (1) 写下的「必须按落库结构定义」原则自相矛盾（openapi 面已按同一理由删除它，见任务 7.1）。

**否则 (2) 的目标不成立**：只补 `principal` 而留 string/date-time，任何真实 `AdminSession` 仍会因 `issued_at` 类型不符被判非法——那就与本任务 (1) 刚写下的「必须按落库结构定义」自相矛盾。改完请用**三条**真实记录过一遍 JSON Schema 校验器留证：① `--session-ttl 86400`（`expires_at` 为整数）；② `--session-ttl 0`（`expires_at` 为 **null**）；③ 旧格式（无 `principal` 字段 → 默认 `Admin`）。

`scripts/check-contracts.sh:30-34` 只校验 `$defs` 中一组名称存在、不校验结构，故上述改动不影响其绿灯。

### 任务 7.3 文档（T17-T18）

- `docs/09-admin.md`：新增「角色总览」表（超级管理员/全局管理员/项目管理员 + 权限边界 + 审计 operator）；「全局管理员」视图截图位与操作说明；登录方式（超级管理员留空用户名）；审 operator 表补 `ga:{username}`；「忘记全局管理员密码」章节改为「忘记超级管理员密码」并说明 GA 丢密由 super 重置；**并在权限说明处写明 403 文案为「无权访问该资源」（三角色共用，设计 §7 注 5），列出 GA 对被拒面（`/api/v1/admins*`、`/projects/{p}/tokens*`、集群写、`/api/v1/admin/*`）一律 403**。
- `README.md`：**改 :19 与 :87**（访问令牌说明「仅全局管理员」→「仅超级管理员」）+ **在 :97-98 的「核心能力 → `**安全**`」条目末尾补**「多全局管理员（GA）：由超级管理员创建/删除/改密，可管理全部项目配置与共享配置，不能管理账号、集群写与访问令牌」（README **没有**独立「安全」小节，勿新建）。
- `dev_docs/deployment-guide.md`：滚动升级纪律（先全集群升级再创建首个 GA）+ 找回通道边界（ops token 通道重置超级管理员密码，**且任务 4.6b 后同时回收全部 GA 会话**——必须写清，否则运维会误以为 GA 旧 token 仍然有效）。
- `dev_docs/design/project-admin.md`：按 `global-admin.md` §12 表修正 **8 行**漂移（其中 project-admin.md 占 4 行，另 4 行含 `docs/09-admin.md:40`、`README.md:19,87`、openapi、schema）——serde wire 大小写（:61）、`/shared/refs` **两行**（:114-115，:116 reveal 行保留）、`dsh_session_active` 行（:153）、「全局管理员」称谓（:9,15,17,18,37,41,46,95,125,132,135,144,168,173,187）；另两处属 openapi/schema 面（见任务 7.1/7.2）。`docs/09-admin.md:40` 同步更新。

## 8. 全量验证与收尾

```bash
cd server && source ../scripts/build-env.sh
cargo test --workspace
cargo check --workspace --all-targets
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cd .. && bash scripts/check-contracts.sh
bash scripts/api-surface-test.sh
```

- 收尾：reviewer（subagent）对照 `dev_docs/design/global-admin.md` 逐条复核实现与矩阵覆盖；修复发现的问题；补 `dev_docs/aegis/work/2026-09-14-global-admin/`（10-intent.md / 20-checkpoint.md）；提交 → push → CI 全绿。

## 9. 风险与回滚点

| 风险 | 缓解 | 回滚点 |
| --- | --- | --- |
| 授权矩阵改错导致越权/锁死 | `authorize` 单函数集中 + 矩阵逐行集成测试 + 编译期穷尽 match（仅 7 处受保护，见任务 4.2 分级清单） | **回滚单位是 S1+S2 合并提交**：S1 结束时工作区整体编译不过（新增 `Principal` 变体会让 dsh-api 7 处穷尽 match 报 E0004），S1 不可单独存在 |
| `sess/admin*` 与 `sess/ga/*` 前缀误伤 | 键空间隔离回归测试（§5 第 7、10 项：改密/重置只失效对应会话，另一端仍有效） | 同上 |
| 渲染/reveal 判定漏改一处（权限提升面） | api/lib.rs:734 与 2636 两处同步改 + GA reveal 任意项目 / PA 仅自己项目双向断言 | 同上 |
| UI 门控漏改（PA 看到账号入口） | 手测三角色 +（可选）UI e2e | S4 单独提交 |
| 混合版本集群遇到新变体 | deployment-guide 升级纪律 + 文档 + **无 GA 账号时恢复通道/全量下线不写新变体（`has_ga` 守卫，任务 4.6b/4.5）** | 无（无 GA 时日志形状与现状一致；有 GA 时按升级纪律） |
