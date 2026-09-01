//! HTTP 网络传输（多进程/生产，模块 03）：Raft RPC 经 HTTP+JSON。
//! 服务端：RaftHttpServer（axum，/raft/* 端点）；客户端：HttpNetwork（reqwest）。
//! 说明：M1 简化——错误以 500+JSON 返回，客户端映射为 Network 错误（重试）；
//! 快照分块由 openraft 默认 full_snapshot 按 chunk 调用 install_snapshot。

use std::io;
use std::time::Duration;

use openraft::error::{NetworkError, RPCError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::de::DeserializeOwned;

use crate::types::{NodeId, NodeInfo, TypeConfig};

pub type RaftHandle = openraft::Raft<TypeConfig>;

/// HTTP 网络客户端（发送给指定目标）。
pub struct HttpNetwork {
    base: String,
    client: reqwest::Client,
    /// 可选 token：Some 时每个 RPC 请求携带 `Authorization: Bearer <token>` 头。
    token: Option<String>,
}

impl HttpNetwork {
    // 返回类型 RPCError<NodeId, NodeInfo, …> 由 openraft RaftNetwork trait 契约固定（体积大）；
    // 装箱会改签名/波及调用点，显式豁免 result-large-err（CI stable clippy 1.98 新增 lint）。
    #[allow(clippy::result_large_err)]
    async fn post<T: serde::Serialize, R: DeserializeOwned>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, RPCError<NodeId, NodeInfo, RaftErrorPlaceholder>> {
        let url = format!("{}{path}", self.base);
        let mut req = self.client.post(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .json(body)
            .send()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;
        if !resp.status().is_success() {
            let e = io::Error::other(format!("raft rpc {} -> {}", path, resp.status()));
            return Err(RPCError::Network(NetworkError::new(&e)));
        }
        resp.json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))
    }
}

/// 占位错误类型（RPCError 的 E 参数；实际错误以 Network 变体返回）。
pub type RaftErrorPlaceholder = openraft::error::RaftError<NodeId>;

// result-large-err：RPCError 体积由 openraft trait 契约决定，豁免（同上）。
#[allow(clippy::result_large_err)]
impl RaftNetwork<TypeConfig> for HttpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftErrorPlaceholder>>
    {
        self.post("/raft/append-entries", &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, NodeInfo, RaftErrorPlaceholder>> {
        self.post("/raft/vote", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<
            NodeId,
            NodeInfo,
            openraft::error::RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    > {
        let resp: InstallSnapshotResponse<NodeId> = self
            .post("/raft/install-snapshot", &rpc)
            .await
            .map_err(|e| match e {
                RPCError::Network(ne) => RPCError::Network(ne),
                _ => RPCError::Network(NetworkError::new(&io::Error::other("rpc error"))),
            })?;
        Ok(resp)
    }
}

/// HTTP 网络工厂。
#[derive(Clone, Default)]
pub struct HttpNetworkFactory {
    client: reqwest::Client,
    /// 可选 token：Some 时 new_client 产出的 HttpNetwork 携带该 token。
    token: Option<String>,
}

impl HttpNetworkFactory {
    pub fn new() -> Self {
        // 超时选择依据：connect_timeout 3s——对端黑洞/不可达时快速失败，
        // 避免 Raft 复制挂起至 OS 级 TCP 超时（可达数分钟）；timeout 60s——
        // 总请求超时，覆盖大快照（install_snapshot 分块传输）等长请求。
        // client 为固定配置构建，无 TLS 后端切换等可失败路径，expect 不会触发。
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(60))
            .build()
            .expect("reqwest client 构建失败");
        Self {
            client,
            token: None,
        }
    }

    pub fn with_token(token: Option<String>) -> Self {
        let mut f = Self::new();
        f.token = token;
        f
    }
}

impl RaftNetworkFactory<TypeConfig> for HttpNetworkFactory {
    type Network = HttpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &NodeInfo) -> Self::Network {
        let _ = target;
        HttpNetwork {
            base: format!("http://{}", node.raft_addr),
            client: self.client.clone(),
            token: self.token.clone(),
        }
    }
}
