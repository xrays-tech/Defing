//! P2b2: streaming snapshot transport protocol (replaces the P2a JSON full-snapshot bridge).
//!
//! Wire form (client->server, single HTTP request, Content-Type: application/msgpack):
//! concatenated frames, each `[u32 BE len][msgpack Frame]`:
//! - Header {vote, meta}: first frame; server then opens its `.part` file
//! - Chunk {seq, data}: seq 1..n; data <= MAX_CHUNK_BYTES (client rechunks
//!   option.snapshot_chunk_size(), clamped <= 1MiB)
//! - End {total_len, crc}: last frame; total_len = received byte count, crc = CRC32
//!   over the concatenated chunk payloads (IEEE reflected poly)
//!
//! Server streams: body data-stream parts -> FrameDecoder -> per Chunk append to
//! `.part` (never whole-payload buffer); on End validate total_len+crc -> rename ->
//! install_full_snapshot. Every failure path (truncated stream / crc mismatch /
//! oversize frame / ordering) removes `.part` and answers 4xx.
//!
//! Client streams: tokio fs File -> tokio_util ReaderStream -> FrameEmitter (frame
//! pump) -> reqwest Body::wrap_stream; per-request timeout = option.soft_ttl();
//! the openraft `cancel` future is honored via tokio::select! -> StreamingError::Closed.
//!
//! rmp-serde note: plain Vec<u8> serializes as a msgpack ARRAY (measured 1.49x
//! inflation, 64KiB -> 97653B); BIN is forced via BytesMode::ForceAll (measured
//! 65550B ~= payload+14), so chunk payloads travel as msgpack BIN blobs.

pub const MAX_CHUNK_BYTES: usize = 1024 * 1024; // option.snapshot_chunk_size() clamp
                                                // 帧上限 = 最大 chunk + 4KiB 信封余量（map/str/seq/bin32 头 ~22B；留足后即使
                                                // 未来给 Chunk 加字段也不会把合法 1MiB chunk 判成 4xx）
pub const MAX_FRAME_BYTES: usize = MAX_CHUNK_BYTES + 4096;
pub const MAX_SNAPSHOT_BYTES: usize = 640 * 1024 * 1024; // total ceiling (pairs route body limit)
pub const READ_BUF_BYTES: usize = 64 * 1024; // client pump read buffer

use std::io;

use openraft::type_config::alias::SnapshotMetaOf;
use openraft::type_config::alias::VoteOf;
use serde::Deserialize;
use serde::Serialize;

use crate::types::TypeConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Frame {
    Header {
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
    },
    Chunk {
        seq: u64,
        data: Vec<u8>,
    },
    End {
        total_len: u64,
        crc: u32,
    },
}

/// Protocol-level framing failure (oversize frame / msgpack decode / ordering) ->
/// server 4xx, client maps to StreamingError::Network.
#[derive(Debug)]
pub struct SnapshotProtoError {
    pub msg: String,
}

fn proto_err(msg: String) -> SnapshotProtoError {
    SnapshotProtoError { msg }
}

/// Encode one frame: BIN-forced msgpack body + BE u32 length prefix.
pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, SnapshotProtoError> {
    let mut wr: Vec<u8> = Vec::new();
    // P3f：命名（struct-map）编码。RPC 路径此前踩过 positional 编码在
    // `skip_serializing_if`/Option 字段上错位的坑（compat §13.5）；帧这边统一用 named，
    // 两端同 crate 编译，wire 变更无兼容负担，但杜绝同一类错位回归。
    let mut ser = rmp_serde::Serializer::new(&mut wr)
        .with_bytes(rmp_serde::config::BytesMode::ForceAll)
        .with_struct_map();
    let done = match frame.serialize(&mut ser) {
        Ok(()) => Ok(()),
        Err(e) => Err(proto_err(format!("msgpack encode: {e}"))),
    };
    done?;
    let n = wr.len() as u32;
    let mut out: Vec<u8> = vec![
        ((n >> 24) & 0xFF) as u8,
        ((n >> 16) & 0xFF) as u8,
        ((n >> 8) & 0xFF) as u8,
        (n & 0xFF) as u8,
    ];
    out.extend(wr);
    Ok(out)
}

/// Decode a full frame out of a msgpack byte slice (decoder calls this once per
/// complete `[len][body]` unit; msgpack Vec<u8> fields come back as BIN bytes).
pub fn decode_frame(body: &[u8]) -> Result<Frame, SnapshotProtoError> {
    match rmp_serde::from_slice(body) {
        Ok(f) => Ok(f),
        Err(e) => Err(proto_err(format!("msgpack decode: {e}"))),
    }
}

/// IEEE CRC32 (reflected poly 0xEDB88320) over chunk payload bytes; per-instance
/// 256-entry table. End frame carries finish() - client and server compute
/// independently and must agree.
pub struct Crc32 {
    pub table: [u32; 256],
    pub crc: u32,
}

impl Crc32 {
    pub fn init() -> Crc32 {
        let mut table = [0; 256];
        let mut i = 0;
        while i < 256 {
            let mut r = i as u32;
            for _ in 0..8 {
                r = if (r & 1) != 0 {
                    (r >> 1) ^ 0xEDB88320
                } else {
                    r >> 1
                };
            }
            table[i] = r;
            i += 1;
        }
        Crc32 {
            table,
            crc: 0xFFFFFFFF,
        }
    }

    pub fn update(self: &mut Crc32, data: &[u8]) {
        for b in data {
            self.crc = self.table[(self.crc as u8 ^ b) as usize] ^ (self.crc >> 8);
        }
    }

    pub fn finish(self: &Crc32) -> u32 {
        self.crc ^ 0xFFFFFFFF
    }
}

/// Server-side frame reader: feed body data-stream parts in, pull complete Frames
/// out (None = need more bytes). Frames larger than MAX_FRAME_BYTES fail fast -
/// bounds decoder memory against a slow/malformed peer regardless of body limit.
pub struct FrameDecoder {
    pub buf: Vec<u8>,
}

impl FrameDecoder {
    pub fn init() -> FrameDecoder {
        FrameDecoder { buf: Vec::new() }
    }

    pub fn feed(self: &mut FrameDecoder, part: &[u8]) {
        self.buf.extend(part);
    }

    pub fn next_frame(self: &mut FrameDecoder) -> Result<Option<Frame>, SnapshotProtoError> {
        if self.buf.len() < 4 {
            return Ok(None);
        }
        let n = (self.buf[0] as u32) << 24
            | (self.buf[1] as u32) << 16
            | (self.buf[2] as u32) << 8
            | self.buf[3] as u32;
        if n > MAX_FRAME_BYTES as u32 {
            // oversize frame: drop the buffer (caller aborts + cleans .part)
            self.buf.clear();
            return Err(proto_err(format!(
                "frame too large: {n} > {MAX_FRAME_BYTES}",
                n = n,
                MAX_FRAME_BYTES = MAX_FRAME_BYTES,
            )));
        }
        if self.buf.len() < 4 + n as usize {
            return Ok(None);
        }
        let body = &self.buf[4..4 + n as usize];
        match decode_frame(body) {
            Ok(f) => {
                self.buf.drain(0..4 + n as usize);
                Ok(Some(f))
            }
            Err(e) => {
                self.buf.clear();
                Err(e)
            }
        }
    }
}

/// Server-side streaming payload writer (P2b3): opens the `.part` file and pushes each
/// Chunk payload straight through via tokio AsyncWrite::poll_write loops - the handler
/// never accumulates the whole payload. Flush before rename; the caller owns part/final
/// path naming (store payload helpers) and cleans `.part` on any failure path.
pub struct ChunkWriter {
    pub file: tokio::fs::File,
    pub path: std::path::PathBuf,
}

impl ChunkWriter {
    pub async fn create(path: std::path::PathBuf) -> Result<ChunkWriter, io::Error> {
        let opened = tokio::fs::File::create(path.clone());
        let file = opened.await?;
        Ok(ChunkWriter { file, path })
    }

    /// Write `data` in full: loop poll_write until every byte is accepted. A
    /// Ready(0) is a stalled write - fail instead of spinning.
    pub async fn write(self: &mut ChunkWriter, data: &[u8]) -> Result<(), io::Error> {
        let mut done: usize = 0;
        while done < data.len() {
            let res = std::future::poll_fn(|cx| {
                tokio::io::AsyncWrite::poll_write(
                    std::pin::Pin::new(&mut self.file),
                    cx,
                    &data[done..],
                )
            });
            match res.await {
                Ok(0) => {
                    return Err(io::Error::other("write stalled: poll_write Ready(0)"));
                }
                Ok(n) => {
                    done += n;
                }
                Err(e) => {
                    return Err(e);
                }
            };
        }
        Ok(())
    }

    /// Durably flush before the caller renames `.part` to the canonical name.
    pub async fn flush(self: &mut ChunkWriter) -> Result<(), io::Error> {
        let res = std::future::poll_fn(|cx| {
            tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.file), cx)
        });
        let done = match res.await {
            Ok(()) => Ok(()),
            Err(e) => Err(e),
        };
        done?;
        Ok(())
    }
}

/// Total-bytes guard shared by both sides (protocol ceiling even if HTTP
/// limits are misconfigured).
pub fn check_total(total: u64) -> Result<(), SnapshotProtoError> {
    if total > MAX_SNAPSHOT_BYTES as u64 {
        return Err(proto_err(format!(
            "snapshot too large: {total} > {MAX_SNAPSHOT_BYTES}",
            total = total,
            MAX_SNAPSHOT_BYTES = MAX_SNAPSHOT_BYTES,
        )));
    }
    Ok(())
}

/// Client-side frame pump: streams the local payload file via tokio AsyncRead into
/// Header -> Chunk(seq 1.., <=chunk_size) -> End frames. Implements futures Stream
/// (Item = io::Result<Bytes>) so `reqwest::Body::wrap_stream` accepts it (TryStream
/// blanket over Stream<Item=Result<Ok,Error>>). No whole-payload buffering: at most
/// one chunk + one encode per emitted frame.
pub struct FrameEmitter {
    pub file: tokio::fs::File,
    /// pre-encoded Header frame (emitted first)
    pub header_frame: bytes::Bytes,
    /// encoded frames pending emission (End is queued at EOF)
    pub queue: std::collections::VecDeque<bytes::Bytes>,
    pub chunk_size: usize,
    /// raw bytes accumulated for the next Chunk frame
    pub carry: Vec<u8>,
    /// reusable read buffer backing AsyncRead::poll_read (fixed capacity)
    pub buf: Vec<u8>,
    pub seq: u64,
    pub total: u64,
    pub crc: Crc32,
    pub header_sent: bool,
    pub eof: bool,
    pub end_sent: bool,
    pub fatal: bool,
}

impl FrameEmitter {
    pub async fn open(
        path: std::path::PathBuf,
        vote: VoteOf<TypeConfig>,
        meta: SnapshotMetaOf<TypeConfig>,
        chunk_size: usize,
    ) -> Result<FrameEmitter, io::Error> {
        let opened = tokio::fs::File::open(path);
        let file = opened.await?;
        let enc = encode_frame(&Frame::Header { vote, meta });
        let header = match enc {
            Ok(v) => Ok(v),
            Err(e) => Err(io::Error::other(e.msg)),
        };
        let header = header?;
        let chunk = if chunk_size == 0 || chunk_size > MAX_CHUNK_BYTES {
            MAX_CHUNK_BYTES
        } else {
            chunk_size
        };
        Ok(FrameEmitter {
            file,
            header_frame: bytes::Bytes::from(header),
            queue: std::collections::VecDeque::new(),
            chunk_size: chunk,
            carry: Vec::new(),
            buf: vec![0u8; READ_BUF_BYTES],
            seq: 0,
            total: 0,
            crc: Crc32::init(),
            header_sent: false,
            eof: false,
            end_sent: false,
            fatal: false,
        })
    }
}

impl futures_util::stream::Stream for FrameEmitter {
    type Item = io::Result<bytes::Bytes>;

    fn poll_next(
        self: std::pin::Pin<&mut FrameEmitter>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<io::Result<bytes::Bytes>>> {
        let e = self.get_mut();
        loop {
            if !e.queue.is_empty() {
                match e.queue.pop_front() {
                    Some(f) => {
                        return std::task::Poll::Ready(Some(Ok(f)));
                    }
                    None => {
                        return std::task::Poll::Ready(None);
                    }
                };
            }
            if e.fatal {
                return std::task::Poll::Ready(None);
            }
            if !e.header_sent {
                e.header_sent = true;
                e.queue.push_back(e.header_frame.clone());
                continue;
            }
            if e.eof && e.end_sent {
                return std::task::Poll::Ready(None);
            }
            if e.eof {
                // EOF reached: flush any partial carry as the final chunk, then queue End
                if !e.carry.is_empty() {
                    e.seq += 1;
                    let part: Vec<u8> = e.carry.drain(0..e.carry.len()).collect();
                    e.crc.update(&part);
                    e.total += part.len() as u64;
                    let enc = encode_frame(&Frame::Chunk {
                        seq: e.seq,
                        data: part,
                    });
                    match enc {
                        Ok(v) => e.queue.push_back(bytes::Bytes::from(v)),
                        Err(err) => {
                            e.fatal = true;
                            return std::task::Poll::Ready(Some(Err(io::Error::other(err.msg))));
                        }
                    };
                }
                let enc = encode_frame(&Frame::End {
                    total_len: e.total,
                    crc: e.crc.finish(),
                });
                match enc {
                    Ok(v) => {
                        e.end_sent = true;
                        e.queue.push_back(bytes::Bytes::from(v));
                    }
                    Err(err) => {
                        e.fatal = true;
                        return std::task::Poll::Ready(Some(Err(io::Error::other(err.msg))));
                    }
                };
                continue;
            }
            // exactly one file read per poll (ReadBuf borrows e.buf, the read
            // targets e.file - disjoint fields, both reachable through e)
            let mut dst = tokio::io::ReadBuf::new(&mut e.buf[..]);
            // exactly one file read per poll
            let pulled = match tokio::io::AsyncRead::poll_read(
                std::pin::Pin::new(&mut e.file),
                cx,
                &mut dst,
            ) {
                std::task::Poll::Pending => return std::task::Poll::Pending,
                std::task::Poll::Ready(res) => match res {
                    Ok(()) => Ok(()),
                    Err(err) => Err(err),
                },
            };
            match pulled {
                Err(err) => {
                    e.fatal = true;
                    return std::task::Poll::Ready(Some(Err(err)));
                }
                Ok(()) => {
                    // copy whatever the read produced (may be empty at EOF)
                    e.carry.extend(dst.filled());
                    if dst.filled().is_empty() {
                        e.eof = true;
                        continue;
                    }
                }
            };
            if e.total + e.carry.len() as u64 > MAX_SNAPSHOT_BYTES as u64 {
                e.fatal = true;
                return std::task::Poll::Ready(Some(Err(io::Error::other(
                    "snapshot exceeds MAX_SNAPSHOT_BYTES",
                ))));
            }
            while e.carry.len() >= e.chunk_size {
                e.seq += 1;
                let part: Vec<u8> = e.carry.drain(0..e.chunk_size).collect();
                e.crc.update(&part);
                e.total += part.len() as u64;
                let enc = encode_frame(&Frame::Chunk {
                    seq: e.seq,
                    data: part,
                });
                match enc {
                    Ok(v) => e.queue.push_back(bytes::Bytes::from(v)),
                    Err(err) => {
                        e.fatal = true;
                        return std::task::Poll::Ready(Some(Err(io::Error::other(err.msg))));
                    }
                };
            }
            if !e.queue.is_empty() {
                continue;
            }
            return std::task::Poll::Pending;
        }
    }
}
