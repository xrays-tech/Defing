//! Defing 配置服务 —— 数据模型与状态机核心（模块 01）。
//!
//! 职责：数据模型（实体 + KV 键构造）、校验器（item 校验/限额）、diff 计算、结构一致性不变量。
//! 约定：本 crate 不依赖 Raft/存储/网络/加密算法；secret 值以 `Value::Secret(Ciphertext)` 透传。
//! 确定性：apply 命令的实现不读墙钟/不 IO/不日志（design-v2 D16），时间由调用方注入。

pub mod command;
pub mod diff;
pub mod error;
pub mod keys;
pub mod limits;
pub mod model;
pub mod state;
pub mod store;
pub mod validator;
pub mod wire;

pub use command::{Command, DraftUpdateItem};
pub use error::{Error, ErrorKind};
pub use model::{
    AdminSession, AuditEntry, BranchName, BranchState, ChangeKind, Ciphertext, DiffEntry,
    DraftValue, EventType, GrayRule, GroupDef, ItemDef, LabelSelector, Principal, Project,
    ProjectAdminAccount, ProjectId, ProjectTokenRecord, PublishEvent, SharedItem, SnapshotMap,
    Structure, StructureDraft, Value, ValueType, VersionKind, VersionRecord,
};
pub use state::{
    token_hash, ApplyOutcome, ClientCtx, ConfigSnapshot, ResolvedVersion, StateMachine,
};
pub use store::{InMemoryStore, Store};
