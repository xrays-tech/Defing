//! Raft RPC HTTP 服务端（axum，/raft/* 端点）。
//! openraft 0.10 spike: V2 端点——`/raft/full-snapshot` 接受 {vote, snapshot} 单请求
//!（`Raft::install_full_snapshot`）；旧 V1 `/raft/install-snapshot` 分块端点移出核心协议。

use bytes::Bytes;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;

use axum::extract::{DefaultBodyLimit, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::routing::post;
use axum::RequestExt;
use axum::Router;

use openraft::storage::Snapshot;
use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::VoteOf;

use crate::http_network::RaftHandle;
use crate::snapshot_stream::check_total;
use crate::snapshot_stream::ChunkWriter;
use crate::snapshot_stream::Crc32;
use crate::snapshot_stream::Frame;
use crate::snapshot_stream::FrameDecoder;
use crate::snapshot_stream::MAX_CHUNK_BYTES;
use crate::store::abort_payload_part;
use crate::store::finalize_payload_part;
use crate::store::payload_part_path;
use crate::store::snapshot_file_name;
use futures_util::stream::StreamExt;
use serde::Serialize;

use crate::types::TypeConfig;

#[derive(Clone)]
pub struct RaftServerState {
    pub raft: RaftHandle,
    /// P2a: 本节点 snapshot 文件目录（`{data_dir}/snapshots`）——full-snapshot handler 的
    /// received bytes 先 materialize 落此处（install_snapshot 再落盘 own canonical copy；
    /// handler 删除 this materialized file after install returns，失败即 delete too）.
    pub snap_dir: std::path::PathBuf,
    /// 可选 token：Some 时 /raft/* 端点要求 `Authorization: Bearer <token>` 完全相等；
    /// None 时保持无鉴权（默认行为，兼容现有部署）。
    pub token: Option<std::sync::Arc<str>>,
}

impl RaftServerState {
    pub fn new(raft: RaftHandle, snap_dir: std::path::PathBuf) -> Self {
        Self {
            raft,
            snap_dir,
            token: None,
        }
    }

    pub fn with_token(
        raft: RaftHandle,
        token: Option<std::sync::Arc<str>>,
        snap_dir: std::path::PathBuf,
    ) -> Self {
        Self {
            raft,
            token,
            snap_dir,
        }
    }
}

/// 统一鉴权：state.token 为 Some 时校验请求头 `Authorization: Bearer <token>` 完全相等，
/// 不匹配返回 401；token 为 None 时放行。
fn authed(
    state: &RaftServerState,
    req: &axum::http::HeaderMap,
) -> Result<(), (StatusCode, String)> {
    if let Some(token) = &state.token {
        let expected = format!("Bearer {}", token);
        let ok = req
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(|v| v == expected)
            .unwrap_or(false);
        if !ok {
            return Err((StatusCode::UNAUTHORIZED, "unauthorized".to_string()));
        }
    }
    Ok(())
}

/// 构建 Raft RPC 路由（挂到 raft_addr 或 http_addr 均可）。
///
/// P2b1 历史（superseded by P2b2 for full-snapshot）:单 JSON full-snapshot RPC 需
/// 256MiB axum body limit（JSON 数字数组 inflation ~4x,实测 150MB raw -> ~600MB JSON,
/// 覆盖 <=64MiB raw dev state 预算,详见 dev_docs/research/openraft-010-compat.md）。
/// P2b2 streaming:the three msgpack RPC routes keep a fixed 32MiB cap(payloads tiny);
/// /raft/full-snapshot runs with DefaultBodyLimit::disable() — its streaming frame
/// body legitimately reaches MAX_SNAPSHOT_BYTES(640MiB),server memory is bounded by
/// the protocol guards instead:MAX_FRAME_BYTES per frame(FrameDecoder)+ check_total
/// per chunk(payload accumulation). /raft/* 端点已 token 鉴权(RaftServerState.token),
/// 因此放宽上限无匿名 DoS 面。
pub fn raft_router(state: RaftServerState) -> Router {
    let rpc = Router::new()
        .route("/raft/append-entries", post(append_entries))
        .route("/raft/vote", post(vote))
        .route("/raft/pre-vote", post(pre_vote))
        .layer(DefaultBodyLimit::max(32 * 1024 * 1024));
    let snap = Router::new()
        .route("/raft/full-snapshot", post(full_snapshot))
        .layer(DefaultBodyLimit::disable());
    Router::new().merge(rpc).merge(snap).with_state(state)
}

// P2b1: MessagePack 请求/响应 helpers (append/vote/pre-vote endpoints).
// Content-Type must be application/msgpack; wrong type -> 415 (no silent JSON
// fallback: a legacy/misconfigured peer must fail loudly, not half-speak).
fn is_msgpack(headers: &HeaderMap) -> bool {
    let ok = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.starts_with("application/msgpack"))
        .unwrap_or(false);
    ok
}

// msgpack body -> (bytes body, content-type header); (Vec<u8>, HeaderMap) is
// an axum IntoResponse (Vec<u8> body + HeaderMap parts).
fn msgpack_resp<T: Serialize>(v: &T) -> Result<(HeaderMap, Vec<u8>), (StatusCode, String)> {
    let packed = rmp_serde::to_vec_named(v).map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("msgpack encode: {e}"),
        )
    });
    let packed = packed?;
    let ct = HeaderValue::from_str("application/msgpack")
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("header: {e}")));
    let ct = ct?;
    let mut headers = HeaderMap::new();
    headers.insert("content-type", ct);
    Ok((headers, packed))
}

async fn append_entries(
    State(s): State<RaftServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(HeaderMap, Vec<u8>), (StatusCode, String)> {
    authed(&s, &headers)?;
    if !is_msgpack(&headers) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected Content-Type: application/msgpack".to_string(),
        ));
    }
    let decoded = rmp_serde::from_slice(body.as_ref()).map_err(|e| {
        tracing::warn!(
            "raft rpc msgpack decode failed (path=/raft/append-entries, body={} bytes): {}",
            body.len(),
            e
        );
        (StatusCode::BAD_REQUEST, format!("msgpack decode: {e}"))
    });
    let rpc: AppendEntriesRequest<TypeConfig> = decoded?;
    let resp = s
        .raft
        .append_entries(rpc)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    let rpc_resp: AppendEntriesResponse<TypeConfig> = resp?;
    msgpack_resp(&rpc_resp)
}

async fn vote(
    State(s): State<RaftServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(HeaderMap, Vec<u8>), (StatusCode, String)> {
    authed(&s, &headers)?;
    if !is_msgpack(&headers) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected Content-Type: application/msgpack".to_string(),
        ));
    }
    let decoded = rmp_serde::from_slice(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("msgpack decode: {e}")));
    let rpc: VoteRequest<TypeConfig> = decoded?;
    let resp = s
        .raft
        .vote(rpc)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    let rpc_resp: VoteResponse<TypeConfig> = resp?;
    msgpack_resp(&rpc_resp)
}

async fn pre_vote(
    State(s): State<RaftServerState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(HeaderMap, Vec<u8>), (StatusCode, String)> {
    authed(&s, &headers)?;
    if !is_msgpack(&headers) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected Content-Type: application/msgpack".to_string(),
        ));
    }
    let decoded = rmp_serde::from_slice(body.as_ref())
        .map_err(|e| (StatusCode::BAD_REQUEST, format!("msgpack decode: {e}")));
    let rpc: VoteRequest<TypeConfig> = decoded?;
    let resp = s
        .raft
        .pre_vote(rpc)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()));
    let rpc_resp: VoteResponse<TypeConfig> = resp?;
    msgpack_resp(&rpc_resp)
}

// P2b3 true streaming snapshot endpoint (protocol in snapshot_stream.rs):
// body data-stream parts -> FrameDecoder -> per-frame validation + immediate
// ChunkWriter write of each Chunk payload into the `.part` file. The canonical
// name is minted ONCE from the Header meta (store::snapshot_file_name); part
// path = `{name}.part` (store payload helpers). The server never buffers the
// whole payload - at most one decoded frame is live at a time. Frames must
// arrive Header-first, Chunk seq strictly 1,2,3... (<=MAX_CHUNK_BYTES, running
// total <=MAX_SNAPSHOT_BYTES), End last with total_len/crc agreeing with what
// the server actually received (Crc32 over the payloads as streamed). On End:
// flush -> rename `.part` -> install_full_snapshot (persists its own canonical
// copy) -> best-effort remove of the received file. EVERY failure path after
// the part was opened removes `.part` (abort_payload_part); protocol violations
// answer 4xx, IO faults 5xx, before any Raft state change.
async fn full_snapshot(
    State(s): State<RaftServerState>,
    headers: axum::http::HeaderMap,
    req: Request,
) -> Result<(HeaderMap, Vec<u8>), (StatusCode, String)> {
    authed(&s, &headers)?;
    if !is_msgpack(&headers) {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "expected Content-Type: application/msgpack".to_string(),
        ));
    }
    let mut stream = req.into_limited_body().into_data_stream();
    let mut dec = FrameDecoder::init();
    let mut crc = Crc32::init();
    let mut total: u64 = 0;
    let mut last_seq: u64 = 0;
    let mut header_vote: Option<VoteOf<TypeConfig>> = None;
    let mut header_meta: Option<SnapshotMetaOf<TypeConfig>> = None;
    let mut end_seen: bool = false;
    let mut end_total: u64 = 0;
    let mut end_crc: u32 = 0;
    // P2b3: `.part` file minted ONCE from the Header meta; every Chunk payload
    // streams straight through ChunkWriter (no whole-payload `received` buffer).
    let mut part_name: String = "".to_string();
    let mut part_opened: bool = false;
    let mut writer: Option<ChunkWriter> = None;
    // central failure channel: Some = abort loops, clean `.part`, map to HTTP
    let mut fail: Option<(StatusCode, String)> = None;
    let mut stream_exhausted: bool = false;
    while fail.is_none() && !stream_exhausted {
        let item = stream.next().await;
        match item {
            None => {
                stream_exhausted = true;
            }
            Some(item) => match item {
                Ok(chunk) => {
                    dec.feed(&chunk[..]);
                }
                Err(e) => {
                    // truncated/garbled transport stream -> protocol failure
                    fail = Some((StatusCode::BAD_REQUEST, format!("body stream: {e}")));
                }
            },
        };
        // drain every complete frame now in the decoder buffer
        let mut more_frames: bool = true;
        while fail.is_none() && more_frames {
            let got = dec.next_frame();
            match got {
                Err(e) => {
                    fail = Some((StatusCode::BAD_REQUEST, e.msg));
                }
                Ok(None) => {
                    more_frames = false;
                }
                Ok(Some(frame)) => match frame {
                    Frame::Header { vote, meta } => {
                        if header_vote.is_some() {
                            fail =
                                Some((StatusCode::BAD_REQUEST, "Header frame twice".to_string()));
                        } else {
                            header_vote = Some(vote);
                            header_meta = Some(meta.clone());
                            // mint the canonical payload name ONCE, then open `.part`
                            part_name = snapshot_file_name(&meta);
                            part_opened = true;
                            let opened = ChunkWriter::create(payload_part_path(
                                s.snap_dir.clone(),
                                part_name.clone(),
                            ));
                            match opened.await {
                                Ok(w) => {
                                    writer = Some(w);
                                }
                                Err(e) => {
                                    fail = Some((
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        format!("part create: {e}"),
                                    ));
                                }
                            };
                        };
                    }
                    Frame::Chunk { seq, data } => {
                        if header_vote.is_none() {
                            fail = Some((
                                StatusCode::BAD_REQUEST,
                                "Header frame must be first".to_string(),
                            ));
                        } else if data.is_empty() {
                            fail =
                                Some((StatusCode::BAD_REQUEST, "empty Chunk payload".to_string()));
                        } else if data.len() > MAX_CHUNK_BYTES {
                            fail = Some((
                                StatusCode::BAD_REQUEST,
                                format!("Chunk {data} exceeds MAX_CHUNK_BYTES", data = data.len()),
                            ));
                        } else if seq != last_seq + 1 {
                            fail = Some((
                                StatusCode::BAD_REQUEST,
                                format!(
                                    "Chunk seq out of order: got {seq}, expected {last_seq}",
                                    last_seq = last_seq + 1
                                ),
                            ));
                        } else {
                            last_seq = seq;
                            total += data.len() as u64;
                            match check_total(total) {
                                Ok(()) => {}
                                Err(e) => {
                                    fail = Some((StatusCode::PAYLOAD_TOO_LARGE, e.msg));
                                }
                            };
                            if fail.is_none() {
                                crc.update(&data[..]);
                                // stream straight into `.part` (no whole-payload buffer)
                                match writer.as_mut() {
                                    Some(w) => {
                                        let wrote = w.write(&data[..]);
                                        match wrote.await {
                                            Ok(()) => {}
                                            Err(e) => {
                                                fail = Some((
                                                    StatusCode::INTERNAL_SERVER_ERROR,
                                                    format!("part write: {e}"),
                                                ));
                                            }
                                        };
                                    }
                                    None => {
                                        // Header arrived but its part could not be opened
                                        // (part create IO fault) - fail channel already set;
                                        // this branch only guards defensive consistency.
                                        fail = Some((
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            "part writer not open for Chunk frame".to_string(),
                                        ));
                                    }
                                };
                            };
                        };
                    }
                    Frame::End {
                        total_len,
                        crc: frame_crc,
                    } => {
                        if end_seen {
                            fail = Some((StatusCode::BAD_REQUEST, "End frame twice".to_string()));
                        } else if header_vote.is_none() {
                            fail = Some((
                                StatusCode::BAD_REQUEST,
                                "Header frame must be first".to_string(),
                            ));
                        } else {
                            end_seen = true;
                            end_total = total_len;
                            end_crc = frame_crc;
                        };
                    }
                },
            };
        }
    }
    if fail.is_none() && !end_seen {
        fail = Some((
            StatusCode::BAD_REQUEST,
            "stream ended without End frame (truncated transfer)".to_string(),
        ));
    }
    if fail.is_none() && total != end_total {
        fail = Some((
            StatusCode::BAD_REQUEST,
            format!("End total_len mismatch: received {total}, declared {end_total}"),
        ));
    }
    if fail.is_none() {
        let got_crc = crc.finish();
        if got_crc != end_crc {
            fail = Some((
                StatusCode::BAD_REQUEST,
                format!("crc mismatch: received {got_crc:08x}, declared {end_crc:08x}"),
            ));
        }
    }
    // validation passed: flush the streamed part, rename -> canonical
    let mut final_path: Option<std::path::PathBuf> = None;
    if fail.is_none() {
        match writer.as_mut() {
            Some(w) => {
                let flushed = w.flush();
                match flushed.await {
                    Ok(()) => {}
                    Err(e) => {
                        fail = Some((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("part flush: {e}"),
                        ));
                    }
                };
                if fail.is_none() {
                    match finalize_payload_part(s.snap_dir.clone(), part_name.clone()) {
                        Ok(p) => {
                            final_path = Some(p);
                        }
                        Err(e) => {
                            fail = Some((StatusCode::INTERNAL_SERVER_ERROR, format!("{e}")));
                        }
                    };
                };
            }
            None => {
                fail = Some((StatusCode::BAD_REQUEST, "Header frame missing".to_string()));
            }
        };
    }
    if let Some(f) = fail {
        // any failure after the `.part` was opened must not leave a half-written file
        if part_opened {
            abort_payload_part(s.snap_dir.clone(), part_name.clone());
        }
        return Err(f);
    }
    let vote: VoteOf<TypeConfig> = match header_vote {
        Some(v) => v,
        None => {
            return Err((StatusCode::BAD_REQUEST, "Header frame missing".to_string()));
        }
    };
    let meta: SnapshotMetaOf<TypeConfig> = match header_meta {
        Some(v) => v,
        None => {
            return Err((StatusCode::BAD_REQUEST, "Header frame missing".to_string()));
        }
    };
    let path: std::path::PathBuf = match final_path {
        Some(p) => p,
        None => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal: finalized payload path missing".to_string(),
            ));
        }
    };
    let snapshot = Snapshot {
        meta,
        snapshot: path.clone(),
    };
    let result = s.raft.install_full_snapshot(vote, snapshot).await;
    let response = match result {
        Ok(r) => Ok(r),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    };
    // install_full_snapshot persists its own canonical copy (or rejects before
    // persisting); the received file is best-effort removed either way.
    let _ = std::fs::remove_file(path);
    let response = response?;
    msgpack_resp(&response)
}
