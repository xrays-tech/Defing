//! P2b2 协议层 unit tests (snapshot_stream codec, no HTTP involved): encode/decode
//! round trip with BIN payloads; FrameDecoder boundary behavior (frame split across
//! feeds, oversize frame fail-fast, msgpack garbage -> Err); check_total guard;
//! and the client pump FrameEmitter driven to EOF over a temp file, with the
//! emitted Header/Chunk*/End sequence decoded back to the original payload
//! (independent crc + total_len agreement = exactly what the server handler checks).

use dsh_raft::openraft::impls::leader_id_adv::LeaderId;
use dsh_raft::openraft::type_config::alias::SnapshotMetaOf;
use dsh_raft::openraft::type_config::alias::VoteOf;
use dsh_raft::openraft::SnapshotMeta;
use dsh_raft::openraft::StoredMembership;
use dsh_raft::openraft::Vote;
use dsh_raft::*;
use futures_util::stream::StreamExt;

fn a_meta() -> SnapshotMetaOf<TypeConfig> {
    SnapshotMeta {
        last_log_id: None,
        last_membership: StoredMembership::default(),
    }
}

fn a_vote() -> VoteOf<TypeConfig> {
    Vote {
        leader_id: LeaderId {
            term: 1,
            node_id: 1,
        },
        committed: false,
    }
}

#[tokio::test]
async fn frame_codec_roundtrip_bin() {
    // Chunk payload must survive as msgpack BIN (probe-measured: plain Vec<u8>
    // encodes as a decimal ARRAY, ~1.49x); decode returns the exact bytes.
    let payload: Vec<u8> = vec![0, 255, 128, 7, 255, 127];
    let enc = encode_frame(&Frame::Chunk {
        seq: 3,
        data: payload.clone(),
    });
    let bytes = match enc {
        Ok(v) => v,
        Err(e) => panic!("encode failed: {0}", e.msg),
    };
    // BE u32 length prefix must exactly frame the msgpack body
    let n = ((bytes[0] as u32) << 24)
        | ((bytes[1] as u32) << 16)
        | ((bytes[2] as u32) << 8)
        | (bytes[3] as u32);
    assert_eq!(
        n as usize + 4,
        bytes.len(),
        "length prefix must frame the body"
    );
    let dec = decode_frame(&bytes[4..]);
    let frame = match dec {
        Ok(f) => f,
        Err(e) => panic!("decode failed: {0}", e.msg),
    };
    match frame {
        Frame::Chunk { seq, data } => {
            assert_eq!(seq, 3, "seq preserved");
            assert_eq!(data, payload, "BIN payload round trip");
        }
        other => panic!("expected Chunk, got {other:?}"),
    };
}

#[tokio::test]
async fn decoder_splits_and_guards() {
    // 1) one frame split across two feeds (HTTP part boundaries are arbitrary)
    let mut dec = FrameDecoder::init();
    let enc = encode_frame(&Frame::Chunk {
        seq: 1,
        data: vec![1, 2, 3],
    });
    let bytes = match enc {
        Ok(v) => v,
        Err(e) => panic!("encode failed: {0}", e.msg),
    };
    dec.feed(&bytes[..2]);
    match dec.next_frame() {
        Ok(None) => {}
        Err(e) => panic!("unexpected early error: {0}", e.msg),
        Ok(Some(f)) => panic!("premature frame: {f:?}"),
    };
    dec.feed(&bytes[2..]);
    match dec.next_frame() {
        Ok(Some(Frame::Chunk { seq, data })) => {
            assert_eq!(seq, 1, "seq preserved");
            assert_eq!(data, vec![1, 2, 3], "data intact across split");
        }
        Err(e) => panic!("decode failed: {0}", e.msg),
        Ok(None) => panic!("frame should complete after second feed"),
        Ok(Some(other)) => panic!("unexpected frame: {other:?}"),
    };
    // 2) oversize length prefix -> Err immediately (decoder memory guard)
    let mut dec2 = FrameDecoder::init();
    let huge: Vec<u8> = vec![0xFF, 0xFF, 0xFF, 0xFF, 1, 2, 3, 4];
    dec2.feed(&huge[..]);
    match dec2.next_frame() {
        Err(e) => assert!(
            e.msg.contains("frame too large"),
            "expected oversize error, got {0}",
            e.msg
        ),
        Ok(_) => panic!("oversize frame must fail"),
    };
    // 3) complete but non-Frame msgpack -> decode Err
    let mut dec3 = FrameDecoder::init();
    dec3.feed(&vec![0, 0, 0, 1, 193][..]); // len=1, body=0xC1 (valid msgpack, not a Frame)
    match dec3.next_frame() {
        Err(e) => assert!(
            e.msg.contains("msgpack decode"),
            "expected decode error, got {0}",
            e.msg
        ),
        Ok(_) => panic!("garbage body must fail"),
    };
}

#[tokio::test]
async fn check_total_guard() {
    match check_total(MAX_SNAPSHOT_BYTES as u64) {
        Ok(()) => {}
        Err(e) => panic!("boundary must pass: {0}", e.msg),
    };
    match check_total(MAX_SNAPSHOT_BYTES as u64 + 1) {
        Err(e) => assert!(
            e.msg.contains("too large"),
            "expected oversize error, got {0}",
            e.msg
        ),
        Ok(()) => panic!("must reject > MAX_SNAPSHOT_BYTES"),
    };
}

#[tokio::test]
async fn emitter_pump_roundtrip() {
    // 3072-byte payload + 1024-byte chunk_size -> Header, Chunk(1..3), End.
    // Frames are decoded back independently (crc + total_len computed from
    // received Chunks) — mirrors the server handler's validation.
    let dir = std::env::temp_dir().join(format!("dsh-snap-frames-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("payload.bin");
    let mut payload: Vec<u8> = Vec::new();
    for i in 0..3072 {
        payload.push((i % 251) as u8);
    }
    let _ = std::fs::write(&path, &payload);

    let opened = FrameEmitter::open(path.clone(), a_vote(), a_meta(), 1024);
    let emitter = match opened.await {
        Ok(e) => e,
        Err(e) => panic!("emitter open failed: {e}"),
    };

    // drive the Stream to EOF, concatenating emitted wire bytes
    let mut stream = emitter;
    let mut collected: Vec<u8> = Vec::new();
    let mut more: bool = true;
    while more {
        match stream.next().await {
            None => {
                more = false;
            }
            Some(item) => match item {
                Ok(chunk) => {
                    collected.extend(&chunk[..]);
                }
                Err(e) => panic!("emitter io error: {e}"),
            },
        };
    }

    let mut dec = FrameDecoder::init();
    dec.feed(&collected[..]);
    match dec.next_frame() {
        Ok(Some(Frame::Header { vote, meta: _ })) => {
            assert_eq!(vote, a_vote(), "Header vote round trip");
        }
        Err(e) => panic!("header decode failed: {0}", e.msg),
        Ok(None) => panic!("header frame expected"),
        Ok(Some(other)) => panic!("expected Header, got {other:?}"),
    };
    let mut expect_seq: u64 = 1;
    let mut total: u64 = 0;
    let mut crc = Crc32::init();
    let mut received: Vec<u8> = Vec::new();
    let mut end_seen: bool = false;
    let mut end_total: u64 = 0;
    let mut end_crc: u32 = 0;
    let mut done: bool = false;
    while !done {
        match dec.next_frame() {
            Err(e) => panic!("frame decode failed: {0}", e.msg),
            Ok(None) => {
                done = true;
            }
            Ok(Some(frame)) => match frame {
                Frame::Chunk { seq, data } => {
                    assert_eq!(seq, expect_seq, "emitted Chunk seq must be 1,2,3...");
                    expect_seq += 1;
                    assert!(data.len() <= 1024, "chunk cap honored");
                    total += data.len() as u64;
                    crc.update(&data[..]);
                    received.extend(&data[..]);
                }
                Frame::End {
                    total_len,
                    crc: crc_declared,
                } => {
                    end_seen = true;
                    end_total = total_len;
                    end_crc = crc_declared;
                }
                other => panic!("unexpected trailing frame: {other:?}"),
            },
        };
    }
    assert!(end_seen, "End frame emitted last");
    assert_eq!(expect_seq, 4, "exactly 3 chunks of 1024");
    assert_eq!(received, payload, "pump payload round trip");
    assert_eq!(total, end_total, "End.total_len agrees");
    let mine = crc.finish();
    assert_eq!(mine, end_crc, "End.crc agrees with independent computation");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn chunk_writer_streams_to_part_file() {
    // P2b3 server side: ChunkWriter opens a `.part` path and writes payloads
    // incrementally (poll_write loop); after flush the file holds exactly the
    // concatenated chunks - the rename/install step is store-level and covered by
    // the http_snapshot e2e.
    let dir = std::env::temp_dir().join(format!("dsh-snap-writer-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&dir);
    let part = dir.join("payload.snap.part");

    let opened = ChunkWriter::create(part.clone());
    let mut wr = match opened.await {
        Ok(w) => w,
        Err(e) => panic!("writer create failed: {e}"),
    };
    let mut expect: Vec<u8> = Vec::new();
    for c in 0..4 {
        let mut block: Vec<u8> = Vec::new();
        for i in 0..4096 {
            block.push(((c * 31 + i) % 253) as u8);
        }
        let wrote = wr.write(&block);
        match wrote.await {
            Ok(()) => {}
            Err(e) => panic!("chunk write failed: {e}"),
        };
        expect.extend(&block[..]);
    }
    // empty write must be a no-op (End-only payloads never occur but guard covers it)
    let nothing: Vec<u8> = Vec::new();
    let empty = wr.write(&nothing);
    match empty.await {
        Ok(()) => {}
        Err(e) => panic!("empty write failed: {e}"),
    };
    let flushed = wr.flush();
    match flushed.await {
        Ok(()) => {}
        Err(e) => panic!("flush failed: {e}"),
    };

    let read = std::fs::read(&part);
    let on_disk = match read {
        Ok(v) => v,
        Err(e) => panic!("part file unreadable: {e}"),
    };
    assert_eq!(
        on_disk.len() as u64,
        expect.len() as u64,
        "no truncation/append drift"
    );
    assert_eq!(on_disk, expect, "streamed bytes equal concatenated chunks");
    let _ = std::fs::remove_file(part);
}
