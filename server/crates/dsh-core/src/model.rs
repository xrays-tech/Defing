//! 数据模型实体（对齐 schema/storage.v1.schema.json 与 design-v2 §3）。
//! 所有实体派生 serde，作为 Raft 状态机持久化的序列化格式。

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// 项目 ID（slug 形式，全局唯一）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProjectId(pub String);

impl ProjectId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for ProjectId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for ProjectId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 分支名（默认 dev/test/prod + 自定义，命名规则见模块 05 校验）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchName(pub String);

impl BranchName {
    pub const DEV: &'static str = "dev";
    pub const TEST: &'static str = "test";
    pub const PROD: &'static str = "prod";

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for BranchName {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl std::fmt::Display for BranchName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 值类型（与 proto config.v1.ValueType 对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValueType {
    String,
    Int,
    Float,
    Bool,
    Json,
    Array,
    Secret,
}

/// secret 值密文载体（wire 格式对齐 schema Ciphertext，design-v2 §7.3）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ciphertext {
    pub enc: String,        // aes-256-gcm | chacha20-poly1305
    pub v: u32,             // 恒 1
    pub dek_v: u64,         // DEK 版本（轮换用）
    pub nonce: String,      // base64 12B
    pub ct: String,         // base64 密文
    pub edek: String,       // base64，KEK 加密的 DEK
    pub edek_nonce: String, // base64 12B
}

/// 配置值（与 proto Value 对应；JSON 存规范化文本）。
/// 序列化形状：{"type": "<lowercase>", "<字段>": 值}，与 api/openapi.v1.yaml 及
/// schema/storage.v1.schema.json 的 Value 定义对齐；secret 携带 ciphertext。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Json(String),
    Array(Vec<String>),
    Secret(Ciphertext),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::String(_) => ValueType::String,
            Value::Int(_) => ValueType::Int,
            Value::Float(_) => ValueType::Float,
            Value::Bool(_) => ValueType::Bool,
            Value::Json(_) => ValueType::Json,
            Value::Array(_) => ValueType::Array,
            Value::Secret(_) => ValueType::Secret,
        }
    }
}

impl Serialize for Value {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(2))?;
        match self {
            Value::String(v) => {
                map.serialize_entry("type", "string")?;
                map.serialize_entry("str_value", v)?;
            }
            Value::Int(v) => {
                map.serialize_entry("type", "int")?;
                map.serialize_entry("int_value", v)?;
            }
            Value::Float(v) => {
                map.serialize_entry("type", "float")?;
                map.serialize_entry("float_value", v)?;
            }
            Value::Bool(v) => {
                map.serialize_entry("type", "bool")?;
                map.serialize_entry("bool_value", v)?;
            }
            Value::Json(v) => {
                map.serialize_entry("type", "json")?;
                map.serialize_entry("json_value", v)?;
            }
            Value::Array(v) => {
                map.serialize_entry("type", "array")?;
                map.serialize_entry("list_value", v)?;
            }
            Value::Secret(c) => {
                map.serialize_entry("type", "secret")?;
                map.serialize_entry("ciphertext", c)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        let m = BTreeMap::<String, serde_json::Value>::deserialize(deserializer)?;
        let ty = m
            .get("type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| D::Error::custom("Value: missing type"))?;
        let field = |name: &str| m.get(name).cloned().unwrap_or(serde_json::Value::Null);
        let value = match ty {
            "string" => Value::String(field("str_value").as_str().unwrap_or_default().to_string()),
            "int" => Value::Int(field("int_value").as_i64().unwrap_or_default()),
            "float" => Value::Float(field("float_value").as_f64().unwrap_or_default()),
            "bool" => Value::Bool(field("bool_value").as_bool().unwrap_or_default()),
            "json" => Value::Json(field("json_value").as_str().unwrap_or_default().to_string()),
            "array" => Value::Array(
                field("list_value")
                    .as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default(),
            ),
            "secret" => {
                let c = serde_json::from_value(field("ciphertext")).map_err(D::Error::custom)?;
                Value::Secret(c)
            }
            other => return Err(D::Error::custom(format!("Value: unknown type {other:?}"))),
        };
        Ok(value)
    }
}

/// item 定义（结构中的最小单元）。
/// 字段 ty 序列化为 "type"，与 openapi/storage schema 的 ItemDef 对齐。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemDef {
    pub key: String,
    #[serde(rename = "type")]
    pub ty: ValueType,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub secret: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validate: Option<String>,
    /// 助记描述（自由文本 ≤200 字节；不进入渲染输出）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// 引用共享标记：true = 本项值为共享来源，由各分支在 shared_bindings 中选择引用哪个共享项。
    /// 引用项只读：值完全来自共享库，type/required/secret 语义随绑定共享项（设计 shared-ref-branch-scope）。
    #[serde(default, skip_serializing_if = "is_false")]
    pub shared: bool,
}

/// serde 辅助：布尔字段序列化时省略 false（与 `Vec::is_empty` 模式一致）。
fn is_false(b: &bool) -> bool {
    !*b
}

/// 分组定义。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GroupDef {
    pub name: String,
    pub items: Vec<ItemDef>,
}

/// 已发布结构（不可变，version 单调递增）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Structure {
    pub version: u64,
    pub groups: Vec<GroupDef>,
}

/// 结构草稿（base_version 发布时必须等于当前已发布结构版本）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructureDraft {
    pub base_version: u64,
    pub groups: Vec<GroupDef>,
}

/// 项目实体。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Project {
    pub id: ProjectId,
    pub name: String,
    pub created_at: i64,
}

/// 值草稿中单个 item 的草稿值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DraftValue {
    pub value: Value,
    pub updated_at: i64,
}

/// 标签选择器（key=value；灰度规则内任一命中即命中，OR 语义）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelSelector {
    pub key: String,
    pub value: String,
}

/// 灰度规则（状态机数据，Raft 复制；selector 求值在读取路径——apply 不读请求/墙钟，D20）。
/// 求值次序固定：labels → IP → percent（任一命中即命中；无身份永不进灰度，Q2）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GrayRule {
    /// 标签匹配（OR：任一 key=value 命中即命中）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub match_labels: Vec<LabelSelector>,
    /// IP 段（CIDR，如 "10.0.0.0/8"；任一命中即命中）。
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ip_cidrs: Vec<String>,
    /// 百分比放量（0-100；fnv1a(instance_id) % 100 < pct 命中）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub percentage: Option<u32>,
}

/// 分支状态（含值草稿、活动版本、幂等键、灰度）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BranchState {
    pub active_version: u64,
    pub structure_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_request_id: Option<String>,
    /// group → (key → DraftValue)
    #[serde(default)]
    pub value_draft: BTreeMap<String, BTreeMap<String, DraftValue>>,
    /// 草稿修订号（乐观锁）：每次 DraftUpdate 提交 +1；客户端保存时带 expected_draft_rev
    /// 校验，不匹配 → 409 Conflict（并发编辑冲突检测）。旧数据无此字段 → 0（兼容）。
    #[serde(default)]
    pub draft_rev: u64,
    /// 灰度序号（G2/Q1：分支级独立单调递增，不与 active_version 版本号空间冲突；
    /// 0 = 无灰度。旧数据无此字段 → 0，兼容）。
    #[serde(default)]
    pub gray_seq: u64,
    /// 灰度规则（Some = 灰度活跃；None = 无灰度。旧数据无此字段 → None，兼容）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gray_rule: Option<GrayRule>,
    /// 分支级共享引用绑定：group → key → 共享项 key（仅对结构标记 shared=true 的 item 有意义）。
    #[serde(default)]
    pub shared_bindings: BTreeMap<String, BTreeMap<String, String>>,
    /// 绑定是否有未发布的变更（发布守卫/级联判定；设计 shared-ref-branch-scope §4.6）。
    /// 值草稿用「非空」判定，绑定必须跨发布持久化，故用脏标记区分「有无待发布变更」。
    #[serde(default)]
    pub bindings_dirty: bool,
}

impl BranchState {
    pub fn new(structure_version: u64) -> Self {
        Self {
            active_version: 0,
            structure_version,
            last_request_id: None,
            value_draft: BTreeMap::new(),
            draft_rev: 0,
            gray_seq: 0,
            gray_rule: None,
            shared_bindings: BTreeMap::new(),
            bindings_dirty: false,
        }
    }
}

/// 版本存储形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionKind {
    Full,
    Diff,
}

/// 版本记录（不可变）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionRecord {
    pub no: u64,
    pub structure_version: u64,
    pub created_at: i64,
    pub operator: String,
    pub comment: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rollback_of: Option<u64>,
    pub kind: VersionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_ref: Option<String>,
    /// 产生本版本的事件类型（D-TYPE：watch 重放保真；旧日志无此字段 → 按 rollback_of 推断）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_ty: Option<EventType>,
    /// 灰度转正标记（G2/Q3：由 GrayPromote 创建的版本；复用既有 EventType，不新增枚举值。
    /// serde default 兼容旧快照/旧日志；watch 重放据此还原 gray 事件标记）。
    #[serde(default)]
    pub gray: bool,
}

/// 变更种类（diff 与事件共用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeKind {
    Upsert,
    Delete,
}

/// diff 条目。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiffEntry {
    pub group: String,
    pub key: String,
    pub kind: ChangeKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_value: Option<Value>,
}

/// 事件类型（与 proto EventType 对应）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    ValuePublish,
    StructurePublish,
    SharedCascade,
    Rollback,
}

/// 发布事件（Raft apply 的确定性副作用，供 watch 扇出）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublishEvent {
    pub project: ProjectId,
    pub branch: BranchName,
    pub version: u64,
    pub ty: EventType,
    pub structure_version: u64,
    pub comment: String,
    pub request_id: String,
    pub changes: Vec<DiffEntry>,
    /// 灰度事件标记（G2/Q3：GrayPublish/Promote/Abort 事件 gray=true；
    /// 复用既有 EventType（ValuePublish），serde default 兼容旧节点/旧日志——不新增枚举值，
    /// 防新节点写的灰度记录进快照后旧节点反序列化失败。SDK 契约：gray=true 事件永不按版本过滤）。
    #[serde(default)]
    pub gray: bool,
}

/// 发布校验策略（G1/D35：编码进发布命令——apply 确定性由日志序保证）。
/// 默认 Block = 校验失败拒绝发布（现状）；Warn = 校验失败仅记录继续发布。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PublishPolicy {
    #[default]
    Block,
    Warn,
}

/// 共享发布级联模式（G1/D36：编码进 SharedPublish 命令）。
/// 默认 Auto = 发布共享时自动级联引用分支（现状，原子 D15）；Manual = 只更共享版本，
/// 引用分支下次发布时物化新值（防风暴开关 D7）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SharedCascadeMode {
    #[default]
    Auto,
    Manual,
}

/// 读取模式（G1/D37 修订：节点配置，读不产生日志无确定性问题）。
/// 默认 Stale = 本地直接读（现状，零破坏）；Linear = 读前 ReadIndex 门控（读已提交）——
/// 集群下 follower 的 ensure_linearizable 返回 ForwardToLeader（openraft 0.9 无 follower
/// 侧 ReadIndex）→ 复用写路径重定向：ERR_LEADER_REDIRECT + leader http（客户端跟随）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadMode {
    #[default]
    Stale,
    Linear,
}

/// 跨项目共享项（集群级；扁平库：无分组，key 全局唯一）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SharedItem {
    pub key: String,
    pub ty: ValueType,
    #[serde(default)]
    pub secret: bool,
    #[serde(default)]
    pub required: bool,
    pub value: Value,
    pub version: u64,
    /// 助记描述（自由文本 ≤200 字节；不进入渲染输出）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// 会话主体（三级角色：超级管理员 / 全局管理员 / 项目管理员；旧数据无此字段 → Admin）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "kind")]
pub enum Principal {
    /// 超级管理员（根账号：节点 --admin-password / 状态机 sess 内密码哈希）。
    #[default]
    Admin,
    /// 项目管理员：仅管理 `project` 的配置，不能触碰共享面/全局面。
    ProjectAdmin {
        username: String,
        project: ProjectId,
    },
    /// 全局管理员（设计 dev_docs/design/global-admin.md §3）：
    /// 可管理全部项目配置与全部共享配置；不能管理账号（GA/PA）、集群写、
    /// 项目创建删除、项目访问令牌。
    GlobalAdmin { username: String },
}

/// 项目管理员账号（设计文档 dev_docs/design/project-admin.md §2）。
/// 密码 = SHA-256(salt || password) 加盐哈希；明文与哈希均不出现在日志。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectAdminAccount {
    /// 全局唯一，[A-Za-z0-9_-]{2,64}，禁用 "admin"。
    pub username: String,
    /// 所属项目（创建时必须存在）。
    pub project: ProjectId,
    /// 每账号随机盐（hex）。
    pub salt: String,
    /// SHA-256(salt || password) hex。
    pub password_hash: String,
    /// 创建时间（墙钟 ms，API 层注入）。
    pub created_at: i64,
}

/// 全局管理员账号（设计 dev_docs/design/global-admin.md §4）。
/// 与 ProjectAdminAccount 同构但无 project 字段；salt 为保留字段
/// （新格式盐内嵌 PHC 故为空串，旧数据为 sha256(salt||pw) 的盐）。
/// derive 必须齐全：`load`(DeserializeOwned)/`save_pending`(Serialize)/serde_json 往返都依赖它。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlobalAdminAccount {
    /// 集群内唯一（与 PA 用户名互斥，不区分大小写），[A-Za-z0-9_-]{2,64}，禁用 admin。
    pub username: String,
    /// 保留字段：新格式盐内嵌 PHC 故为空串。
    pub salt: String,
    /// argon2id PHC（`$argon2` 前缀）或旧 sha256 hex。
    pub password_hash: String,
    /// 创建时间（墙钟 ms，API 层注入）。
    pub created_at: i64,
}

/// 项目访问令牌（机器凭据）：数据面鉴权用；明文仅在创建响应出现一次，
/// 落盘只存 SHA-256（token_hash）。key = tok/{hash}（扁平，鉴权单次 KV 读）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectTokenRecord {
    /// token id（= hash 前 16 位 hex），集群内唯一。
    pub id: String,
    /// 展示名（如 "订单服务 2025-08"），项目内唯一（校验 [A-Za-z0-9._-]{1,64}）。
    pub name: String,
    /// 所属项目（鉴权时校验请求项目 == 记录项目）。
    pub project: ProjectId,
    /// SHA-256(明文 token) hex —— 落盘/备份/审计永无明文。
    pub hash: String,
    pub created_at: u64,
    /// 创建人（principal_op 输出："admin" / "pa:{username}"）。
    pub created_by: String,
    /// 软删除标记（数据面鉴权过滤；保留记录供审计追溯）。
    pub revoked: bool,
}

/// 管理员会话（每主体单会话；状态机内只存 token 哈希，明文令牌不落库/不落日志）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AdminSession {
    /// SHA-256(token) hex。
    pub token_hash: String,
    /// 签发时间（墙钟 ms；由 API 层注入，仅作数据存储，不参与确定性判定）。
    pub issued_at: i64,
    /// 过期时间（墙钟 ms；None = 不自动过期）。
    pub expires_at: Option<i64>,
    /// 设备标识（MVP 固定 "cli"）。
    pub device_id: String,
    /// 会话主体（旧数据缺省 = 全局管理员）。
    #[serde(default)]
    pub principal: Principal,
}

/// 审计条目（落库 audit/{seq:020}；对齐 schema/storage.v1.schema.json 的 AuditEntry）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditEntry {
    pub seq: u64,
    /// 墙钟 ms（API 层注入，仅作数据）。
    pub ts: i64,
    pub operator: String,
    pub action: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub branch: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default)]
    pub detail: serde_json::Value,
}

/// 物化后的配置快照（渲染/发布/diff 的中间表示）。
pub type SnapshotMap = BTreeMap<String, BTreeMap<String, Value>>;
