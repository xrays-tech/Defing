//! Defing 配置服务 —— openraft 集成（模块 03）。
//! 本版：进程内直连网络（测试/联调）；存储基于 dsh-storage redb。

pub mod http_network;
pub mod network;
pub mod raft;
pub mod raft_http_server;
pub mod snapshot_stream;
pub mod store;
pub mod types;

pub use http_network::{HttpNetwork, HttpNetworkFactory};
pub use network::{Network, NetworkFactory, RaftHandle};
/// openraft 重新导出（成员变更等高级 API 供 dsh-api 使用）。
pub use openraft;
pub use raft::{
    client_write, dev_config, initialize_cluster, initialize_single, leader_http_addr,
    new_raft_node, try_client_write, wait_for_leader, wait_until, write_command, WriteError,
    WriteOutcome,
};
pub use raft_http_server::{raft_router, RaftServerState};
pub use snapshot_stream::{
    check_total, decode_frame, encode_frame, ChunkWriter, Crc32, Frame, FrameDecoder, FrameEmitter,
    SnapshotProtoError, MAX_CHUNK_BYTES, MAX_FRAME_BYTES, MAX_SNAPSHOT_BYTES,
};
pub use store::{LogStore, StateMachineStore};
pub use types::{NodeId, NodeInfo, TypeConfig};
