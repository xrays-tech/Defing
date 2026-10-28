//! Raft 类型配置（模块 03 §2）。

use dsh_core::command::Command;
use openraft::declare_raft_types;
use serde::{Deserialize, Serialize};

pub type NodeId = u64;

/// 节点信息。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeInfo {
    pub grpc_addr: String,
    pub http_addr: String,
    pub raft_addr: String,
}

impl std::fmt::Display for NodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.raft_addr)
    }
}

// NodeInfo 满足 NodeEssential（Default/Debug/Clone/PartialEq/Eq + serde），openraft::Node 由 blanket impl 提供。

/// 客户端写响应（F6 修复）：版本号 + 本命令 apply 产出的事件（含 changes/affected）。
/// - 非发布命令（如 DraftUpdate）事件为空、version=0；
/// - 幂等重复发布（同 request_id）事件为空、version=0（调用方按活动版本兜底展示）；
/// - 成功发布：version=新版本号，events 携带 diff（dev-single 与集群行为一致）。
///
/// 不进 Raft 日志（仅 client_write 响应体），旧日志重放不受影响。
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WriteAck {
    pub version: u64,
    pub events: Vec<dsh_core::model::PublishEvent>,
}

// 客户端写响应：WriteAck（版本 + 事件）。
// Err 携带状态机 apply 错误（I7 会话冲突/发布校验等），随 Raft 响应返回给客户端 ——
// 之前 R=u64 会把 apply 错误吞掉（日志警告后返回 0），跨节点错误码无法传播。
// openraft 0.10: SnapshotData 移出 RaftTypeConfig（declare_raft_types 默认不含
// SnapshotData/Snapshot）；快照数据类型在 RaftStateMachine::SnapshotData 与
// RaftNetworkV2::SnapshotData（P2a decision: PathBuf——file-backed payload files under
// {data_dir}/snapshots/, see dev_docs/research/openraft-010-compat.md §12）。
declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Result<WriteAck, dsh_core::Error>,
        NodeId = NodeId,
        Node = NodeInfo,
);
