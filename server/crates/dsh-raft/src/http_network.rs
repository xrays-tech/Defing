//! HTTP 网络传输（多进程/生产，模块 03）：Raft RPC 经 HTTP+JSON。
//! 服务端：raft_router（axum, /raft/* 端点）；客户端：HttpNetwork（reqwest）。
//! openraft 0.10 spike: V2 协议——snapshot 经单 `full_snapshot` RPC（V1 chunked
//! install_snapshot 已移出核心；`openraft-legacy` 提供 V1 桥）。生产分块/流传输见
//! dev_docs/research/openraft-010-compat.md（option.soft_ttl/snapshot_chunk_size 消费网络实现方）。
//! P2b1: append_entries/vote/pre_vote 使用 MessagePack（Content-Type: application/msgpack,
//! rmp_serde 边界 encode/decode, per-request timeout = RPCOption::soft_ttl()）;
//! factory 三段独立连接池 new_client/new_heartbeat_client/new_snapshot_client
//!（HTTP/1.1 无 multiplex，pool 隔离避免 replication/snapshot head-of-line blocking）。
//! P2b2: full_snapshot 经流式 msgpack 帧（snapshot_stream.rs：Header/Chunk/End）.
//! 说明：错误以 Network 变体返回（retry/backoff 由 openraft）。

use std::future::Future;
use std::io;
use std::time::Duration;

use openraft::error::{NetworkError, RPCError, ReplicationClosed, StreamingError};
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
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::snapshot_stream::FrameEmitter;
use crate::snapshot_stream::MAX_CHUNK_BYTES;
use crate::store::StateMachineStore;
use crate::types::{NodeId, NodeInfo, TypeConfig};

// 0.10：Raft 的 SM 泛型默认 () no longer satisfies RaftStateMachine —— 显式 StateMachineStore
pub type RaftHandle = openraft::Raft<TypeConfig, StateMachineStore>;

/// HTTP 网络客户端（发送给指定目标）。
pub struct HttpNetwork {
    base: String,
    /// 角色 pool：new_client -> 数据 pool（append/vote/pre_vote）、
    /// new_heartbeat_client -> heartbeat pool（lease 探针不受 replication backpressure 阻塞）
    client: reqwest::Client,
    /// snapshot 传输 pool（P2b2: streaming binary frames; 300s total cap as
    /// outer backstop — openraft cancel future is the authoritative abort）.
    snapshot_client: reqwest::Client,
    /// 可选 token：Some 时每个 RPC 请求携带 `Authorization: Bearer <token>` 头。
    token: Option<String>,
}

impl HttpNetwork {
    // P2b1 binary RPC (append/vote/pre_vote): MessagePack request/response
    // (`application/msgpack` + rmp_serde); per-request timeout = RPCOption::soft_ttl()
    // (openraft budget: soft_ttl < hard_ttl — the network layer fails before openraft
    // shuts down the in-flight RPC; send/decode failures map to Network,
    // retry/backoff is openraft's job)
    #[allow(clippy::result_large_err)]
    async fn post_msgpack<T: Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
        option: RPCOption,
    ) -> Result<R, RPCError<TypeConfig>> {
        let url = format!("{}{path}", self.base);
        let payload = rmp_serde::to_vec(body).map_err(|e| RPCError::Network(NetworkError::new(&e)));
        let payload = payload?;
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/msgpack")
            .timeout(option.soft_ttl())
            .body(payload);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        if !resp.status().is_success() {
            let e = io::Error::other(format!("raft rpc {} -> {}", path, resp.status()));
            return Err(RPCError::Network(NetworkError::new(&e)));
        }
        let raw = resp
            .bytes()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)));
        let raw = raw?;
        let out: R = rmp_serde::from_slice(raw.as_ref())
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        Ok(out)
    }
}

// result-large-err：RPCError 体积由 openraft trait 契约决定，豁免（同上）。
#[allow(clippy::result_large_err)]
impl RaftNetworkV2<TypeConfig> for HttpNetwork {
    // P2a: file-backed SnapshotData
    type SnapshotData = std::path::PathBuf;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
        // P2b1: MessagePack + per-request timeout = option.soft_ttl() (data pool)
        self.post_msgpack("/raft/append-entries", &rpc, option)
            .await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        // P2b1: MessagePack (election hot path—small body, soft_ttl budget honored)
        self.post_msgpack("/raft/vote", &rpc, option).await
    }

    async fn pre_vote(
        &mut self,
        rpc: VoteRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
        // P2b1: pre_vote RPC (config-gated enable_pre_vote; server side Raft::pre_vote)
        self.post_msgpack("/raft/pre-vote", &rpc, option).await
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<TypeConfig>,
        snapshot: SnapshotOf<TypeConfig, Self::SnapshotData>,
        cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
        option: RPCOption,
    ) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
        // P2b2 streaming frames (snapshot_stream.rs): the local payload file (PathBuf)
        // is pumped as Header -> Chunk(<=option.snapshot_chunk_size(), clamped <=1MiB)
        // -> End frames via reqwest::Body::wrap_stream (chunked transfer, no
        // whole-payload buffer client-side, no axum body-limit cliff). openraft
        // `cancel` aborts the in-flight send via select! -> StreamingError::Closed;
        // ack is the msgpack SnapshotResponse written by the server handler.
        let chunk = match option.snapshot_chunk_size() {
            Some(c) => c,
            None => MAX_CHUNK_BYTES,
        };
        let opened = FrameEmitter::open(snapshot.snapshot, vote, snapshot.meta.clone(), chunk);
        let emitter = match opened.await {
            Ok(e) => Ok(e),
            Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
        };
        let emitter = emitter?;
        let url = format!("{}{}", self.base, "/raft/full-snapshot");
        let mut req = self
            .snapshot_client
            .post(&url)
            .header("Content-Type", "application/msgpack")
            .body(reqwest::Body::wrap_stream(emitter));
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let send = req.send();
        let raced = tokio::select! {
            closed = cancel => Err(closed),
            r = send => Ok(r),
        };
        let resp = match raced {
            Ok(r) => match r {
                Ok(r) => Ok(r),
                Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
            },
            Err(closed) => {
                return Err(StreamingError::Closed(closed));
            }
        };
        let resp = resp?;
        if !resp.status().is_success() {
            let e = io::Error::other(format!("raft rpc full-snapshot -> {}", resp.status()));
            return Err(StreamingError::Network(NetworkError::new(&e)));
        }
        let raw = match resp.bytes().await {
            Ok(b) => Ok(b),
            Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
        };
        let raw = raw?;
        let ack = match rmp_serde::from_slice(raw.as_ref()) {
            Ok(r) => Ok(r),
            Err(e) => Err(StreamingError::Network(NetworkError::new(&e))),
        };
        let ack: SnapshotResponse<TypeConfig> = ack?;
        Ok(ack)
    }
}

/// HTTP 网络工厂（P2b1:三个独立 reqwest 连接池—HTTP/1.1 无 multiplex,so replication
/// batch / snapshot transfer never head-of-line block heartbeat probes）。
#[derive(Clone, Default)]
pub struct HttpNetworkFactory {
    data_client: reqwest::Client,
    heartbeat_client: reqwest::Client,
    snapshot_client: reqwest::Client,
    /// 可选 token:Some 时 new_* 产出的 HttpNetwork 携带该 token。
    token: Option<String>,
}

impl HttpNetworkFactory {
    pub fn new() -> Self {
        // 超时选择依据（same connect_timeout 3s for all pools:对端黑洞/不可达快速失败,
        // avoid OS-level TCP hang）:
        // - data:no client-level total timeout—append/vote/pre_vote enforce
        //   RPCOption::soft_ttl() per request (post_msgpack);
        // - heartbeat:client-level cap 3s (lease probes must stay short;
        //   per-request soft_ttl still applied by openraft budgets);
        // - snapshot:300s total—P2b2 streaming frames; openraft cancel future
        //   aborts mid-stream, this cap only backstops a wedged connection.
        // clients are fixed-config builds, no TLS-backend-switch failure paths,
        // expect never fires.
        let data_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("reqwest data client 构建失败");
        let heartbeat_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(3))
            .build()
            .expect("reqwest heartbeat client 构建失败");
        let snapshot_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(300))
            .build()
            .expect("reqwest snapshot client 构建失败");
        Self {
            data_client,
            heartbeat_client,
            snapshot_client,
            token: None,
        }
    }

    pub fn with_token(token: Option<String>) -> Self {
        let mut f = Self::new();
        f.token = token;
        f
    }

    // test/diagnostic accessors:each pool is an independently-built Client (different
    // builder config visible in reqwest Client Debug output).
    pub fn data_client(self: &HttpNetworkFactory) -> reqwest::Client {
        self.data_client.clone()
    }
    pub fn heartbeat_client(self: &HttpNetworkFactory) -> reqwest::Client {
        self.heartbeat_client.clone()
    }
    pub fn snapshot_client(self: &HttpNetworkFactory) -> reqwest::Client {
        self.snapshot_client.clone()
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    // data pool for replication/election RPCs
    async fn new_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        let _ = target;
        HttpNetwork {
            base: format!("http://{}", node.raft_addr),
            client: self.data_client.clone(),
            snapshot_client: self.snapshot_client.clone(),
            token: self.token.clone(),
        }
    }

    // P2b1 override:heartbeat probes on an isolated pool (openraft default would
    // share the replication client—HTTP/1.1 head-of-line blocking risk).
    async fn new_heartbeat_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        let _ = target;
        HttpNetwork {
            base: format!("http://{}", node.raft_addr),
            client: self.heartbeat_client.clone(),
            snapshot_client: self.snapshot_client.clone(),
            token: self.token.clone(),
        }
    }

    // P2b1 override:snapshot transfer on its own pool (large/long single RPC until
    // P2b2 streaming;note:default stream_append on this client also uses the snapshot
    // pool—openraft only routes snapshot transmission through it).
    async fn new_snapshot_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        let _ = target;
        HttpNetwork {
            base: format!("http://{}", node.raft_addr),
            client: self.snapshot_client.clone(),
            snapshot_client: self.snapshot_client.clone(),
            token: self.token.clone(),
        }
    }
}
