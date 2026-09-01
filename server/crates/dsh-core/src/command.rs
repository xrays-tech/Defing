//! 状态机写命令（Raft 日志载荷；确定性 apply，模块 01 §3）。
//! operator 字段（审计身份）：空串 = 旧客户端/全局管理员 → 状态机落 "admin"；
//! 项目管理员为 "pa:{username}"。全部 `#[serde(default)]` 保证旧日志重放兼容。

use serde::{Deserialize, Serialize};

use crate::model::{BranchName, GroupDef, ProjectId, SharedItem, Value};

/// 值草稿更新条目。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DraftUpdateItem {
    pub group: String,
    pub key: String,
    pub value: Value,
}

/// 分支级共享引用绑定条目（DraftUpdate 载荷；shared_key 空串 = 解除绑定）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedBinding {
    pub group: String,
    pub key: String,
    pub shared_key: String,
}

/// 状态机写命令（M1 子集；M2 追加 Rollback/SharedPublish/Promote/会话命令；共享引用选择在分支
/// BranchState.shared_bindings，结构仅声明 ItemDef.shared 标记——设计 shared-ref-branch-scope）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Command {
    ProjectCreate {
        name: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 可选：从该项目的已发布结构克隆初始化结构（groups 直接进入新项目已发布结构 v1）。
        #[serde(default)]
        clone_from: Option<String>,
    },
    ProjectDelete {
        id: ProjectId,
        #[serde(default)]
        operator: String,
    },
    /// source：可选，从该分支的活动版本值物化出初始值草稿（缺省为空草稿）。
    BranchCreate {
        project: ProjectId,
        name: BranchName,
        source: Option<BranchName>,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
    },
    BranchDelete {
        project: ProjectId,
        name: BranchName,
        #[serde(default)]
        operator: String,
    },
    /// 整体替换结构草稿；base_version 必须等于当前已发布结构版本。
    StructureDraftSet {
        project: ProjectId,
        base_version: u64,
        groups: Vec<GroupDef>,
        #[serde(default)]
        operator: String,
    },
    /// 发布结构草稿：对全部分支同时生效（I3/I5）。
    PublishStructure {
        project: ProjectId,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 校验策略（G1/D35，同 Publish）
        #[serde(default)]
        policy: crate::model::PublishPolicy,
    },
    /// 更新分支值草稿（不生效，I4）。
    /// `expected_draft_rev`（乐观锁）：`Some(rev)` 时校验 == 当前 draft_rev，不匹配 →
    /// Conflict 409（并发编辑冲突检测，客户端刷新最新草稿后重试）；`None` = 不校验
    /// （旧客户端/旧日志，last-write-wins）。
    DraftUpdate {
        project: ProjectId,
        branch: BranchName,
        updates: Vec<DraftUpdateItem>,
        /// 待删除 item："group/key"
        deletes: Vec<(String, String)>,
        /// 分支级共享引用绑定 upsert/解除（空 shared_key = 解除）；旧日志无此字段 → 空（兼容）。
        #[serde(default)]
        shared_bindings: Vec<SharedBinding>,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 期望的草稿修订号（乐观锁；None = 不校验，兼容旧客户端/旧日志）
        #[serde(default)]
        expected_draft_rev: Option<u64>,
    },
    /// 发布分支版本（原子：固化草稿→版本→指针→diff→事件；幂等 I10）。
    Publish {
        project: ProjectId,
        branch: BranchName,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 校验策略（G1/D35：Warn = 校验失败仅记录继续发布；缺省 Block。serde default 兼容旧日志）
        #[serde(default)]
        policy: crate::model::PublishPolicy,
    },
    /// 回滚：基于历史版本内容创建新版本（历史不可变，I6/I9）。
    Rollback {
        project: ProjectId,
        branch: BranchName,
        to_version: u64,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
    },
    /// 更新共享项草稿（写共享草稿，发布后生效）。
    SharedDraftUpdate {
        item: SharedItem,
        #[serde(default)]
        operator: String,
    },
    /// 发布共享项（级联引用它的所有项目分支；原子）。
    SharedPublish {
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 级联模式（G1/D36：Manual = 只更共享版本，引用分支下次发布物化；缺省 Auto）
        #[serde(default)]
        cascade: crate::model::SharedCascadeMode,
        /// 校验策略（G1/D35）
        #[serde(default)]
        policy: crate::model::PublishPolicy,
    },
    /// 删除共享项（草稿 + 已发布；被项目结构引用 → 拒绝）。
    SharedDelete {
        key: String,
        #[serde(default)]
        operator: String,
    },
    /// 管理员登录（I7）：token 哈希入库；已有活动会话 → ERR_SESSION_IN_USE。
    /// 密码校验在 API 层（admin_password 是节点配置，不进状态机）。
    /// 注意：全局管理员会话命令保持原状（Raft wire 兼容），项目管理员用 Pa* 变体。
    SessionLogin {
        token_hash: String,
        issued_at: i64,
        expires_at: Option<i64>,
    },
    /// 登出：清除会话（幂等）。unit 变体保持原状（旧日志 wire 兼容）。
    SessionLogout,
    /// 心跳续期：更新 expires_at；无会话 → ERR_SESSION_EXPIRED。
    SessionHeartbeat { expires_at: Option<i64> },
    /// 创建项目管理员账号（项目须存在；用户名 [A-Za-z0-9_-]{2,64} 且 ≠ "admin"）。
    ProjectAdminCreate {
        project: ProjectId,
        username: String,
        salt: String,
        password_hash: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
    },
    /// 删除项目管理员账号（级联删除其全部会话）。
    ProjectAdminDelete { username: String },
    /// 修改项目管理员密码（级联删除其全部会话，需重新登录）。
    ProjectAdminSetPassword {
        username: String,
        salt: String,
        password_hash: String,
    },
    /// 项目管理员登录：写 sess/pa/{username}；该账号已有会话 → ERR_SESSION_IN_USE
    /// （只判 is_some，不读墙钟，保证 Raft 重放确定性）。
    PaSessionLogin {
        username: String,
        token_hash: String,
        issued_at: i64,
        expires_at: Option<i64>,
        device_id: String,
    },
    /// 项目管理员登出（幂等）。
    PaSessionLogout { username: String },
    /// 项目管理员心跳续期（None = 永不过期，语义同 SessionHeartbeat）。
    PaSessionHeartbeat {
        username: String,
        expires_at: Option<i64>,
    },
    /// 修改管理员密码（哈希落状态机，集群一致；登录优先用它校验，回退节点配置）。
    AdminSetPassword { password_hash: String },

    // ---------------- 多会话变体（multisession 改造，纯新增，B1/N10：既有变体不动） ----------------
    /// 多会话管理员登录：写 sess/admin/{session_id}（多会话并存，不检查已存在、不 409）。
    /// 仅新代码使用；旧节点反序列化本变体失败 → 升级纪律：全集群升级后启用多会话。
    MultiSessionLogin {
        token_hash: String,
        issued_at: i64,
        expires_at: Option<i64>,
        session_id: String,
    },
    /// 多会话管理员登出：删 sess/admin/{session_id}（幂等）。
    MultiSessionLogout { session_id: String },
    /// 多会话管理员心跳：续期 sess/admin/{session_id}（无该会话 → ERR_SESSION_EXPIRED）。
    MultiSessionHeartbeat {
        session_id: String,
        expires_at: Option<i64>,
    },
    /// 多会话 PA 登录：写 sess/pa/{username}/{session_id}（多会话并存，不 409）。
    MultiPaSessionLogin {
        username: String,
        token_hash: String,
        issued_at: i64,
        expires_at: Option<i64>,
        device_id: String,
        session_id: String,
    },
    /// 多会话 PA 登出：删 sess/pa/{username}/{session_id}（幂等）。
    MultiPaSessionLogout {
        username: String,
        session_id: String,
    },
    /// 多会话 PA 心跳：续期 sess/pa/{username}/{session_id}。
    MultiPaSessionHeartbeat {
        username: String,
        session_id: String,
        expires_at: Option<i64>,
    },
    /// 踢全部管理员会话（multisession：删旧 key + 前缀扫全部；force-logout 批量）。
    MultiSessionLogoutAll,
    /// 踢某 PA 账号全部会话（multisession：删旧 key + 前缀扫全部）。
    MultiPaSessionLogoutAll { username: String },
    /// 审计落库（seq 由状态机单调分配并覆写；经 Raft 复制，集群一致）。
    AuditAppend { entry: crate::model::AuditEntry },
    /// 主密钥轮换（集群一致）：新 KEK 经 Raft 复制到全部节点；各节点 apply 时更新本地 keyring 并持久化 ring 文件。
    /// F7b：新命令 `kek` 置空、`kek_enc` 携带「当前 KEK 自加密的新 KEK」（Raft 日志无明文）；
    /// 旧日志仅含 `kek` 明文（`#[serde(default)]` 兼容重放），钩子实现方按字段选择解密路径。
    RotateMasterKey {
        /// 明文新 KEK（32B；旧日志路径，新命令置空）
        #[serde(default)]
        kek: Vec<u8>,
        /// 自加密的新 KEK（AES-256-GCM，用提交时刻的当前 KEK 加密；F7b）
        #[serde(default)]
        kek_enc: Vec<u8>,
    },

    // ---------------- 灰度发布（G2，纯新增变体，B1/N10：既有变体不动） ----------------
    /// 灰度发布：固化草稿 → 灰度快照（gray-snap/{seq}，独立灰度序号 gray_seq，Q1）+ 设置灰度规则。
    /// 复用既有 EventType（ValuePublish）+ gray:bool 标记（Q3）；I10 幂等（last_request_id）。
    GrayPublish {
        project: ProjectId,
        branch: BranchName,
        rule: crate::model::GrayRule,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
        /// 校验策略（G1/D35）
        #[serde(default)]
        policy: crate::model::PublishPolicy,
    },
    /// 灰度转正：读灰度快照内容 → 写新 active_version（next = max(active, gray)+1，Q1）→ 清灰度。
    /// 事件 gray=true 携带新 active 版本号（灰度客户端据此重拉；Q4）。
    GrayPromote {
        project: ProjectId,
        branch: BranchName,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        #[serde(default)]
        ts: i64,
    },
    /// 灰度下量/回滚：清灰度（gray_seq=0, gray_rule=None）。
    /// 事件 gray=true 携带回落版本号（active_version；灰度客户端据此重拉稳定版，Q4）。
    GrayAbort {
        project: ProjectId,
        branch: BranchName,
        comment: String,
        request_id: String,
        #[serde(default)]
        operator: String,
        #[serde(default)]
        ts: i64,
    },

    // ---------------- 项目访问令牌（project-token，纯新增变体，既有变体不动） ----------------
    /// 创建项目访问令牌：校验项目存在、name 项目内唯一；只落 SHA-256 hash（明文不落库/不落日志）。
    ProjectTokenCreate {
        project: ProjectId,
        name: String,
        token_hash: String,
        #[serde(default)]
        operator: String,
        /// 墙钟 ms（API 层注入；0 = 回退 apply 的 now_ms 参数，旧日志重放兼容）
        #[serde(default)]
        ts: i64,
    },
    /// 吊销项目访问令牌（软删除：revoked=true；重复吊销幂等）。
    ProjectTokenRevoke {
        project: ProjectId,
        token_id: String,
    },
}
