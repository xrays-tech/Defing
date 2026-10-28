//! 进程内直连网络（模块 03：测试/联调用）。
//! openraft 0.10: RaftNetworkV2（append_entries/vote/full_snapshot；pre_vote 默认；SnapshotData
//! 在 network trait，not RaftTypeConfig）。AFIT 原生 async fn in trait（no async-trait 宏）。
#![allow(clippy::result_large_err)] // RPCError Err 变体较大（上游类型）

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, RwLock};

use openraft::error::ReplicationClosed;
use openraft::error::StreamingError;
use openraft::error::{NetworkError, RPCError, Unreachable};
use openraft::network::RPCOption;
use openraft::network::RaftNetworkFactory;
use openraft::network::RaftNetworkV2;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::SnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::type_config::alias::SnapshotOf;
use openraft::type_config::alias::VoteOf;

use crate::store::StateMachineStore;
use crate::types::{NodeId, NodeInfo, TypeConfig};

// 0.10：Raft 的 SM 泛型默认 () no longer satisfies RaftStateMachine —— 显式 StateMachineStore
pub type RaftHandle = openraft::Raft<TypeConfig, StateMachineStore>;

/// 直连网络（发送给指定目标节点）。
pub struct Network {
    target: NodeId,
    #[allow(dead_code)]
    // 0.10：RemoteError{target_node} 已 gone（E=Infallible）——仅 factory/metrics 用途
    node: NodeInfo,
    peers: Arc<RwLock<HashMap<NodeId, RaftHandle>>>,
}

impl Network {
    // 0.10: RPCError<C, E>（E = 远端错误类型；默认 Infallible —— network 边界固定 Infallible）
    fn peer(&self) -> Result<RaftHandle, RPCError<TypeConfig>> {
        self.peers
            .read()
            .expect("peers lock")
            .get(&self.target)
            .cloned()
            .ok_or_else(|| {
                let e = io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("peer {} not found", self.target),
                );
                RPCError::Network(NetworkError::new(&e))
            })
    }

    // 0.10：RaftNetworkV2 方法 Err is RPCError<C>（E=Infallible 固定）——0.9 的
    // RPCError<C, RaftError<C,E>> 泛型 source 已 gone（远端 typed error 无法跨 network 边界）。
    // 上游 errors/decompose.rs 惯例：远端 RaftError::Fatal → RPCError::Unreachable（backoff 100，
    // 与 0.9 RemoteError 等价）；APIError（如 ForwardToLeader）经本地 client 路径（raft.rs）
    // 而非 network，因此进程内测试网络兜底 Unreachable。
    fn wrap<E: std::error::Error + Send + Sync + 'static>(&self, e: &E) -> RPCError<TypeConfig> {
        RPCError::Unreachable(Unreachable::new(e))
    }
}

impl RaftNetworkV2<TypeConfig> for Network {
    // P2a: file-backed SnapshotData（进程内网络：PathBuf 即本机文件系统，直接 pass-through）
    type SnapshotData = std::path::PathBuf;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        let peer = self.peer()?;
        peer.append_entries(rpc).await.map_err(|e| self.wrap(&e))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        let peer = self.peer()?;
        peer.vote(rpc).await.map_err(|e| self.wrap(&e))
    }

    // 0.10: v1 install_snapshot 分块 RPC 已移出核心（openraft-legacy 提供 V1 适配）;
    // 进程内测试网络直接调用 target.install_full_snapshot。
    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        let peer = self.peer()?;
        peer.install_full_snapshot(vote, snapshot)
            .await
            .map_err(|e| StreamingError::Network(NetworkError::new(&e)))
    }
}

/// 网络工厂：持有节点表，为每个目标创建直连网络。
#[derive(Clone, Default)]
pub struct NetworkFactory {
    peers: Arc<RwLock<HashMap<NodeId, RaftHandle>>>,
}

impl NetworkFactory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, id: NodeId, raft: RaftHandle) {
        self.peers.write().expect("peers lock").insert(id, raft);
    }

    pub fn remove(&self, id: NodeId) {
        self.peers.write().expect("peers lock").remove(&id);
    }

    pub fn get(&self, id: &NodeId) -> Option<RaftHandle> {
        self.peers.read().expect("peers lock").get(id).cloned()
    }
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Network;

    async fn new_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        Network {
            target,
            node: node.clone(),
            peers: self.peers.clone(),
        }
    }
}
