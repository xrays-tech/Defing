# openraft 0.10.0-alpha.37 兼容 Spike（Phase 0）

Scope: migrate `dsh-raft` + downstream crates from openraft `0.9.25` to
`openraft = { version = "=0.10.0-alpha.37", features = ["serde"] }` in the isolated worktree
`/tmp/opencode/defing-openraft-010`（branch `migrate/openraft-010`, base `d629ec8`）.
Main repo `/home/alex/Projects/Defing` untouched; no commit/push; all builds use
`CARGO_TARGET_DIR=/tmp/opencode/openraft010-target`. Toolchain: rustc/cargo 1.98.0.

Outcome: workspace compiles; all workspace + dsh-api tests green; snapshot transport redesigned
as a single-RPC wire form; wire budget measured (JSON 4.00× inflation). Chunked/streaming
snapshot transport deferred to P2.

## 0. Decisions

- `SnapshotData = Vec<u8>` everywhere (store + network + handle bound).
  `store.rs` `generate_snapshot` = `serde_json::to_vec(dump_all())`; `install` =
  `serde_json::from_slice`.
- openraft 0.10 has **no chunked snapshot RPC** in the core protocol; V1
  `/raft/install-snapshot` chunk endpoints are out. `raft_http_server` exposes a single
  `/raft/full-snapshot` body `{vote, meta, snapshot: Vec<u8>}` → `Raft::install_full_snapshot`.
- `raft_router` uses `DefaultBodyLimit::max(256 MiB)` — covers ≤ ~64 MiB raw state under JSON.
  dev-single ~150 MB raw ⇒ ~600 MB JSON body ⇒ msgpack transport (rmp-serde, 1.00×) or
  ~640 MiB limit or P2 chunked/stream transport. See §6.
- `rmp-serde = "1"` added to `dsh-raft/Cargo.toml`; currently used only by the budget-experiment
  crate `/tmp/opencode/budget-exp` (path-deps worktree `dsh-core`). Keep as P2 wire-candidate or
  drop; not yet wired into `http_network`.
- `futures-util = "0.3"` required (`StreamExt`); the ancient `futures-util = "0.2"` squat crate
  lacks `Stream` — do not use.

## 1. Type / generic mapping (0.9.25 → 0.10.0-alpha.37)

| 0.9 | 0.10 | touched files |
|---|---|---|
| `declare_raft_types!(… SnapshotData = …)` | no `SnapshotData` in the macro; `SnapshotData` is an associated type on `RaftStateMachine` / `RaftSnapshotBuilder` / `RaftNetworkV2` / `NetSnapshot` | `types.rs`, `store.rs`, `network.rs` |
| `RaftHandle = Raft<TypeConfig>`（SM default `()`） | `Raft<TypeConfig, StateMachineStore>` — `()` no longer satisfies `RaftStateMachine` | `http_network.rs` |
| `Raft::new(…, N)` | bound `N::Network: NetSnapshot<TypeConfig, SnapshotData = Vec<u8>>` | `raft.rs` `new_raft_node` |
| `RaftMetrics<NID, N, …>` | `RaftMetrics<C>` | `tests/http_snapshot.rs` |
| `LogId<NodeId>` | `LogIdOf<C>` | `tests/http_snapshot.rs` |
| `CheckIsLeaderError<C>` | deprecated alias → `LinearizableReadError<C>` | `dsh-api/src/lib.rs` |
| `client_write` error `RaftError<…>` | `RaftError<C, ClientWriteError<C>>`; `e.forward_to_leader()` without type param | `dsh-api/src/lib.rs` |
| `ensure_linearizable(policy)` | `ensure_linearizable(impl Into<LinearizerOption>)`; strict read-index preserved via `ReadPolicy::ReadIndex` | `dsh-api/src/lib.rs` |
| `add_learner(id, node, blocking)` | same signature; membership changes are **serialized** — a prior uncommitted membership change yields `RaftError::APIError(ChangeMembershipError(InProgress{committed: None, membership_log_id: …}))` | `tests/cluster.rs` helper |
| `IOFlushed` / `log_io_completed` | `IOFlushed` / `io_completed(Ok(()))` | `store.rs` |
| `apply(Vec<Entry>)` | `apply(stream)` consuming `EntryResponder` — needs `futures_util::StreamExt`, `Stream` of `EntryResponder`; `EntryPayload` matched on **owned** `entry.payload` | `store.rs` |
| `LeaderId` term+index+… | `LeaderId { term, node_id }` struct literal | `store.rs` |

## 2. Snapshot wire design

- `openraft::Snapshot<…>` derives no serde in 0.10 (V2 network boundary never serializes it).
  HTTP transport uses **wire form** `FullSnapshotBody { vote, meta, snapshot: Vec<u8> }`
  (`http_network.rs`), rebuilt server-side via `FullSnapshotBody::to_snapshot()`
  → `install_full_snapshot(vote, SnapshotOf<TypeConfig, Vec<u8>>)` (`raft_http_server.rs`).
- `SnapshotMeta` keeps serde wire-compat; `snapshot_id` is a non-public field preserved as an
  empty slot in the wire form.
- `install_full_snapshot(vote, snapshot)` returns `Result<SnapshotResponse<C>, Fatal<C>>`.
- `LeaderElect`/`VoteOf`/`SnapshotMetaOf` type aliases used for the body.

## 3. Metrics / watch / membership

- `Raft::metrics()` returns a watch receiver; methods are trait methods on
  `openraft::rt::WatchReceiver` — import it or call sites fail.
- `watch.borrow()` is gone → `borrow_watched()`; `changed()` also on `WatchReceiver` and needs
  `mut` receiver. Touched: `raft.rs`, `dsh-observability/src/lib.rs`, `dsh-api/src/lib.rs`,
  `dsh-api/src/grpc.rs`, `dsh-cli/src/main.rs`.
- `metrics().membership_config` is `Arc<StoredMembership<C>>`; use `get_node(&id)` /
  `.membership()` clones (no tail-statement borrow tricks) — `leader_http_addr`, `dsh-api` hints.
- `m.committed` deprecated → `m.cluster_committed` in tests.

## 4. Store / apply notes

- Per-entry write lock inside `apply` (stream items resolved individually; `EntryResponder`
  `send(C::R)` kept for `WriteAck` payloads).
- `io_completed` replaces `log_io_completed`; keep flush ordering before responding.
- Test helpers convert entry vectors to streams (`stream::iter(entry_stream(…))`).

## 5. Network error semantics (deviation)

- Process-direct network (`network.rs`) now maps **remote** `RaftError` into
  `RPCError::Unreachable(Unreachable::new(e))` via a generic
  `wrap<E: std::error::Error + Send + Sync + 'static>`; typed remote errors (e.g.
  `RaftError::NodeNotExists`, remote `CheckIsLeaderError`) are no longer preserved on that path.
  Forward-to-leader UX is preserved because the leader hint is resolved locally (`leader_http_hint`
  in dsh-api) instead of decoded from remote `RPCError`. Revisit in P2 if typed remote errors matter.

## 6. Wire budget experiment（measured）

Crate: `/tmp/opencode/budget-exp`（path-dep worktree `dsh-core`; deps `serde_json`, `rmp-serde`;
`CARGO_TARGET_DIR=/tmp/opencode/openraft010-target`）. Method: N synthetic items
（`kek/item/{i}` key + 60,000-byte value, same shape as `http_snapshot` e2e）→
`restore_all` → `dump_all` → encode both forms. Debug profile.

| n | raw | serde_json | rmp-serde | json× | mp× | 1MiB chunks |
|---:|---:|---:|---:|---:|---:|---:|
| 10 | 600,100 B | 2,400,431 B | 600,151 B | 4.00 | 1.00 | 1 |
| 40 | 2,400,430 B | 9,601,811 B | 2,400,633 B | 4.00 | 1.00 | 3 |
| 1000 | 60,011,890 B | 240,048,671 B | 60,016,893 B | 4.00 | 1.00 | 58 |
| 2500 | 150,031,390 B | 600,126,671 B | 150,043,893 B | 4.00 | 1.00 | 144 |

Timings (debug, n=2500): dump 59 ms, restore 46 ms, json encode 10.7 s, msgpack encode 7.0 s.

Findings:
- JSON inflation is **exactly 4.00×** for binary `Vec<u8>` values (each byte as a decimal + comma
  through `serde_json`); ASCII key/value text does not change the ratio materially.
- msgpack (`rmp_serde::to_vec`) encodes bytes as bin → **1.00×**; a msgpack wire would shrink
  the full_snapshot RPC ~4× for snapshot data.
- Consequence: current 256 MiB axum body limit ⇒ max raw state ≈ 64 MiB under JSON transport.
  dev-single-scale state (~150 MB raw) needs: (a) msgpack wire, (b) ~640 MiB limit + client
  timeout/memory headroom, or (c) P2 chunked/stream transport. Spike keeps (b)-style limit with
  small-state e2e green; see open items.

## 7. Validation matrix

- `cargo check --workspace` — pass.
- `cargo check --workspace --tests` — pass.
- `cargo test --workspace --exclude dsh-api` — pass（`defing` 5, `dsh-core` 28 + integration
  suites incl `state_machine` 64, `dsh-raft` unit 5 + `cluster` 5 / `forward_hint` 1 /
  `http_snapshot` 1 / `snapshot_persist` 1, `dsh-observability` 5, others ok）.
- `cargo test -p dsh-api` — pass（unit 31; `cluster_join_idempotent` 2; `grpc_data_plane` 6;
  `http_global_admin` 15; `http_project_admin` 16; `http_project_clone` 6;
  `http_project_token` 5; `leader_write_forward` 5).
- Deprecation warnings cleaned: `CheckIsLeaderError` → `LinearizableReadError` in `dsh-api`.
- Infra note: `/tmp` is a 16 G tmpfs. A heavy `cargo test --workspace` hit
  `Disk quota exceeded` (redb test dirs + 7.7 G cargo target). Recovery used: `rm -rf` stale
  `/tmp/dsh-*` test dirs + `cargo clean`（freed ~9.6 G）; `http_snapshot` then passed.
  The mid-run `ld … Bus error` link failures on dsh-api test binaries were the same
  disk/pressure artifact — after cleanup `cargo test -p dsh-api` passes. Keep cleaning
  `/tmp/dsh-*` between heavy runs; do not touch other sessions' `/tmp/defing-storm.*`.
- rustc 1.98.0 quirks hit while writing the experiment: float format trait `f` removed
  (`{:.2f}` → `{:.2}`), and `const` requires an explicit type（`const CHUNK: usize = …`).

## 8. P2 open items（not done in spike）

1. Chunked/streaming snapshot transport（consume `option.snapshot_chunk_size()` /
   `soft_ttl()`）or msgpack full-snapshot wire（rmp 1.00× vs JSON 4.00×; body limit math above
   — P0 evidence in §9: a storm-scale full-snapshot POST is rejected with HTTP 413 at the
   256 MB `DefaultBodyLimit`, so the far-behind-follower path livelocks until fixed）.
2. Production membership-change retry: `add_learner` / `change_membership` can now return
   `InProgress` when a prior membership change is uncommitted — dsh-api/dsh-cli join paths do
   not retry yet（tests wait for `cluster_committed` instead）.
3. Preserve typed remote errors in the process-direct network instead of `RPCError::Unreachable`.
4. Decide fate of the `rmp-serde` dependency in `dsh-raft`（wire candidate vs drop）.

## 9. P0 0.9→0.10 兼容实测（storm data replay，2026-10-28）

Method: real 0.9.25 storm state copied from `/tmp/defing-storm.ug2lppru/n1..n5` into
`/home/alex/defing-p0-010/{probe,run}/n*`（source dirs untouched），probed with a standalone
offline crate `/tmp/opencode/p0-probe`（re-exported spike types; debug build; `--offline`）,
then the spike `defing` binary was started on the copies with their persisted
`--http/--raft/--grpc` addresses. Infra deviation: `CARGO_TARGET_DIR=/tmp/opencode/openraft010-target`
again hit `Disk quota exceeded` on the 16 G tmpfs, so these P0 artifacts were built/run with
`CARGO_TARGET_DIR=/home/alex/defing-p0-010/target`（scratch `TMPDIR` likewise on `/home`）.

### 9.1 存储/serde 兼容（deserialize 0.9 blobs into 0.10 types）

| blob（n1） | bytes | result |
| --- | --- | --- |
| `TBL_RAFT_META` `vote` | 56 | OK，re-ser byte-identical |
| `last_purged` | — | absent（0.9 never purged） |
| `last_applied` | 49 | OK |
| `membership` | 511 | OK，re-ser identical |
| raft-log first entry idx0 | 523 | `EntryOf` OK，re-ser identical |
| raft-log last entry idx2042 | 276 | OK |
| `TBL_SNAPSHOTS` | — | absent on n1 |
| `TBL_STATE` | keys=2072, kv=27,517,932 B | — |
| raft-log entries | 2043 | — |

n2（snapshot holder）: snapshot `meta` 628 B parses into `SnapshotMetaOf` but re-ser = 613 B — the
0.9 `snapshot_id` key is **silently dropped**（0.10 `SnapshotMeta` has no such field; unknown-field-
tolerant serde confirms the round-trip is otherwise lossless）. `data` blob = 151,595,393 B and parses
into `Vec<(Vec<u8>,Vec<u8>)>` in 2.1695 s: pairs=10101, raw_total=38,113,102 B → **the stored 0.9
snapshot payload is JSON-pairs inflated ~4.0× over raw KV**. n2 raft-log entries=4987,
`TBL_STATE` keys=14013, kv=42,756,861 B.

→ Storage schema + serde encodings are directly consumable by 0.10; no re-encode step needed
except awareness that `snapshot_id` is discarded. Genesis dump/redo is **not required for P0**
(state table + snapshot both parse); recorded as fallback only, not implemented.

### 9.2 Restore smoke（5 nodes, stored membership addresses）

Boot of all five restored nodes: leader=3, `last_log_index=14000`; a fresh write chain
(create project `p0check-010` → structure-draft → draft → publish) reached `last_log_index=14043`
and nodes 2/3/4/5 read back the marker at version 2. Node1 did not follow（see 9.4）.

Counter-case: restoring only 3 of 5 persisted members never established leadership
（no `granted leadership` line in any log; admin login POST times out）— membership must be
restored as the full stored set, or membership must be edited before boot. Upgrade runbook rule.

### 9.3 Full-snapshot install blocker（HTTP body limit）

n3 log shows the leader repeatedly streaming snapshots（"10-th / 11-th snapshot sending:
meta:{last_log:T3-N3.10001, …}"）then "snapshot transmission canceled by RaftCore"; n1 received
zero `append-entries` and zero `full-snapshot` RPCs and stays at `my_last_log_id T3-N3.2042`.
Direct probe: `POST /raft/full-snapshot` with a 300,000,000-byte body →
`HTTP/1.1 413 Payload Too Large`（axum `DefaultBodyLimit::max(256 MB)` on the raft router）.
Storm-scale snapshot body math: `Snapshot.snapshot` is `Vec<u8>`, serde_json encodes it as a
decimal-number array ≈4.0× → 151,595,393 B data ≈ 606 MB on the wire > 256 MB limit
→ far-behind followers **cannot ever be brought up**; observed as an endless snapshot retry
livelock. Options for P2: msgpack full-snapshot wire（measured 1.00× in §9.5 → ~151 MB fits the
256 MB limit）, chunking via `snapshot_chunk_size()`, or simply raising the limit (does not scale
with state growth — rejected as the only fix).

### 9.4 `SnapshotData` decision measurements

In-memory RSS during restore（VmRSS, debug binary）: node without snapshot 394,044 kB;
snapshot-holding nodes 491,908–691,468 kB — i.e. the 151.6 MB blob is held ≥3.2× simultaneously
（persisted + `current_snapshot` + build/serve copy）, violating design §8.5's "RSS ≤ 2×" rule for
storm-scale state. Wire: JSON 4.00× vs rmp 1.00×（same data; §6.3 numbers reconfirmed in 9.5）.
Measured position (superseded by §10.2): a raw msgpack wire only satisfies the body-limit rule;
it does **not** reduce RSS. The §8.5 RSS rule therefore requires final file-backed/PathBuf
SnapshotData; `Vec<u8>` is a P1 compile bridge only. Keep JSON only as a debug dump.

### 9.5 AppendEntries batch budget（60 KiB payload each, debug build）

| n | json | rmp | rmp/json | enc | rt | redb append+fsync |
| --- | --- | --- | --- | --- | --- | --- |
| 16 | 964,765 B | 961,347 B | 1.00 | 15.99 ms | 3.21 ms | 31.38 ms |
| 32 | 1,929,569 B | 1,922,691 B | 1.00 | 35.95 ms | 5.47 ms | 59.61 ms |
| 64 | 3,859,161 B | 3,845,379 B | 1.00 | 64.24 ms | 10.54 ms | 118.08 ms |

60 KiB-class command entries cost ~60 kB each in **both** codecs（string payloads dominate）→ JSON
transport stays; the 256 MB limit is only threatened by snapshot payloads, not by batched
commands. Repro: `cargo run --bin entries-budget`（`/tmp/opencode/budget-exp`）.

### 9.6 P0 verdict

0.10 spike is compatible at the storage/serde/boot level（real 0.9 state restores and serves
traffic). Alpha go/no-go is **conditional no**: shipping requires first fixing 9.3
（full-snapshot transport) plus the P2 items in §8; no data-format migration blocks the upgrade.

## 10. P0 gate remediation addendum (2026-10-28)

This addendum closes the Phase 0 review conditions without changing the measured results.

### 10.1 Data compatibility verdict
- C1: PASS. Real 0.9 blobs (vote/last_applied/membership/entries) deserialize into 0.10 types and
  re-serialize byte-identically; snapshot meta parses (only 0.9 `snapshot_id` is dropped).
- C2: PASS for data/storage/boot. All 5 restored nodes boot; leader=3; log advances; nodes 2–5
  serve a post-upgrade write. Node1 is stuck at 2042 because the 153MB snapshot becomes ~606MB JSON
  and hits the 256MiB body limit (413; §9.3). This is a **transport blocker**, not a data-format
  incompatibility. Old snapshot data parses (10101 pairs, 2.17s debug).
- C3 genesis-restore: **not required for data compatibility** (C1/C2 pass). It remains a fallback
  only; not implemented. Explicit decision: skip unless C1/C2 later fail.
- 3/5-only restore counter-case: leadership is never granted; runbook must restore the full stored
  membership set (or edit membership before boot). This is an operational rule, not a P0 failure.

### 10.2 SnapshotData decision (C4 — not final)
- Measured RSS 491–691MB vs 151.6MB blob = >=3.2×, violating design §8.5's RSS <=2× rule.
- **Decision per design §8.5: final `SnapshotData` = file-backed/PathBuf.** The measured >=3.2× RSS
  violates the <=2× rule, so `Vec<u8>` is **P1 compile-bridge only** and must not be treated as final.
  P2/P3 must implement file-backed snapshot data + streaming/chunked binary transport; no exception.
- 413 blocker: JSON single-RPC full-snapshot is rejected for production; P2/P3 must use streaming
  binary/chunked transport and per-frame/total-body semantics from design §8.3/§9.5.

### 10.3 C5 open items (must remain visible)
- Independent `new_heartbeat_client` / `new_snapshot_client` (P2) — current spike has only a shared
  `new_client`; RC#1 architecture fix is NOT implemented yet.
- `full_snapshot` must handle `cancel` with `tokio::select!` (current spike ignores it: the param is
  renamed `_cancel` — the earlier `let _ = cancel;` was removed with the clippy `let-future` cleanup,
  same no-op semantics; still P2 work to actually honor cancellation).
- Consume `RPCOption::soft_ttl()` / `snapshot_chunk_size()`; heartbeat reqwest timeout <= soft_ttl.
- Streaming/chunked or msgpack snapshot wire; 256MiB single-JSON limit is not acceptable.
- `apply` responder error path: drain/send pending responders before returning Err (P1).
- Membership `InProgress` retry in dsh-api/dsh-cli production join/promote/remove paths (P2).
- AppendEntries `soft_ttl(heartbeat)` budget/`stream_append` decision (C6 data collected; P2 decides).

### 10.4 Upgrade decision / runbook
- Original data upgrade is possible (no data migration blocks). Runbook must stop all nodes, restore
  full stored membership, and expect the 153MB snapshot transport blocker until P2/P3 fixes it.
- Alpha go/no-go remains **conditional no** until §10.2/§10.3 are implemented and storm passes.

## 11. P1 responder error-path（apply 的 IO/lock failure, 2026-10-28）

- Fix（`dsh-raft/src/store.rs`）：per-entry processing 的 no longer early-returns via `?`
  （stream Err / lock poison / `write_membership` / `write_last_applied` IO）。New private
  `apply_entry(&self, entry) -> (resp, events, hard)`：hard error 即 that entry's resp =
  `Err(DshError::internal("membership persist failed: …" / "last_applied persist failed: …"))`
  + `hard = Some(io_err)`。`apply`'s loop：collected successes stay in `pending_responses`;
  hard/stream Err → break; remaining stream entries drained with abort `Err(…internal("apply aborted
  (storage failure)"))` to their responders; then ONE unified flush（event broadcast + every
  `responder.send`）before `return Err(fatal)`。Contract per openraft `storage/v2/apply_responder.rs`
  docs：`send` must be called or `client_write` never returns. Happy path unchanged（still one
  fsync/entry per §8.7 per-entry txn; write lock still per-entry）.
- Injection hook（test-only, doc-hidden）：`StateMachineStore.set_fail_last_applied(n)`（第 N
  `write_last_applied` → IO Err；`0` = off; `Arc<AtomicU64>` fields so clones share it）。
- Tests: unit `store::tests::apply_mid_stream_error_flushes_collected_entries`（前 2 entries 的
  last_applied=2 persisted despite mid-stream Err）+ `apply_last_applied_failure_responds_then_recovers`
  （failure surfaces, no last_applied recorded, replay after disarm OK）; integration
  `tests/apply_error.rs::apply_error_path_returns_to_client`（`try_client_write` under 3 s timeout
  must RETURN; responder carries "last_applied persist failed"——0.9-style early `?` would hang
  forever）.
- P1 validation（`CARGO_TARGET_DIR=/home/alex/defing-p0-010/target`, `TMPDIR` scratch）：`cargo
  fmt --all -- --check` clean; `cargo check --workspace --all-targets` clean; `cargo test --workspace`
  40 suites ok / 0 failed（dsh-raft store 7 incl 2 new, apply_error 1, cluster 5, http_snapshot,
  snapshot_persist; dsh-api/dsh-core/…all green）; `cargo clippy --workspace --all-targets
  --all-features -- -D warnings` fails with 44 pre-existing spike lints（redundant-closure/let-future
  in compat bridges）— 0 from P1 code（net −3 lints fixed in touched code）.
- Clippy cleanup（same day）：42 的 `redundant_closure` autofixed via `cargo clippy --fix`（store.rs ×41、
  http_network.rs ×1）, 2 的 `non-binding let on a future` fixed by renaming `full_snapshot` 的
  `cancel` param → `_cancel`（same ignore semantics）; `cargo clippy --workspace --all-targets
  --all-features -- -D warnings` now green, fmt/check clean, `cargo test --workspace` 40 suites ok /
  0 failed.

## 12. P2 implementation status (2026-10-28: P2b2 + P2b3 landed; P3 chaos/stress NOT started)

Gate in worktree `/tmp/opencode/defing-openraft-010/server` (`CARGO_TARGET_DIR=/home/alex/defing-p0-010/target`,
`TMPDIR=/home/alex/defing-p0-010/scratch`): `cargo fmt --all -- --check`, `cargo check --workspace --all-targets`,
`cargo clippy --workspace --all-targets --all-features -- -D warnings`, `cargo test --workspace` — all green.

- **P2b2 (streaming wire)**: `crates/dsh-raft/src/snapshot_stream.rs` — frames
  `Header{vote,meta}` / `Chunk{seq,data}` / `End{total_len,crc}` on the wire as
  `[u32 BE len][msgpack Frame]`; `FrameDecoder` (fail-fast > `MAX_FRAME_BYTES`), `Crc32`
  (IEEE reflected), `FrameEmitter` (client rechunks ≤ 1 MiB, streams via `reqwest::Body::wrap_stream`);
  three independent reqwest clients (data / heartbeat / snapshot, snapshot timeout 300 s);
  msgpack AppendEntries + votes; `/raft/full-snapshot` route serves msgpack frames (JSON body path
  gone → the §9.3 64 MiB body-limit blocker is resolved by design). Tests:
  `crates/dsh-raft/tests/snapshot_frames.rs`.
- **P2b3a (server true streaming materialization)**: `ChunkWriter` opens the snapshot `.part`
  file at the Header frame and writes every Chunk payload immediately
  (`std::future::poll_fn` + `tokio::io::AsyncWrite::poll_write`; `Ready(0)` → io error, no spin;
  `poll_flush` before commit). Success path: flush → rename `.part` → `install_full_snapshot` →
  best-effort payload delete. Every failure path after `.part` opened calls
  `abort_payload_part`. `crates/dsh-raft/src/store.rs` helpers
  `snapshot_file_name` / `payload_part_path` / `finalize_payload_part` / `abort_payload_part` —
  the payload file name is minted exactly ONCE (`SNAP_SEQ` is global).
  Test `chunk_writer_streams_to_part_file` proves no truncation/append drift;
  `http_snapshot` learner catch-up still passes.
- **P2b3b (production membership retry)**: `crates/dsh-api/src/membership_retry.rs` —
  `MembershipOp {AddLearner, ReplaceVoters, Change}` + `membership_write`
  (≤ 8 tries, 250 ms apart); `is_membership_in_progress` absorbs ONLY
  `ClientWriteError::ChangeMembershipError(ChangeMembershipError::InProgress(..))`,
  `EmptyMembership` and other errors surface unchanged. `cluster_join` / `cluster_promote` /
  `cluster_remove` all go through `membership_write`; `dsh-cli` only calls those HTTP endpoints,
  so CLI paths are covered by the same fix.
  Evidence: `crates/dsh-api/tests/cluster_membership_retry.rs` —
  `membership_in_progress_predicate` (InProgress=true, EmptyMembership=false) and
  `concurrent_membership_ops_never_500` (4 concurrent join/remove proposals; contract = never 500).
  Note: a promote-to-phantom-voter e2e cannot commit in a single-node fixture (the new voter must
  ack the joint config), so the race test uses learner join/remove ops that commit on the real node.
- **Not done**: P3 153 MB voter-storm / chaos harness and stress numbers; P2 gate evidence is
  focused tests + full workspace verification, not a 153 MB stress run.

## 13. P3a 状态 (2026-10-28: A/C1/D implemented; C2 verified on real 0.9 storm data)

### 13.1 C1 — AppendEntries 字节预算（store-level cap）
- openraft 0.10 replication 读取是 `RaftLogReader::limited_get_log_entries(start,end)`
  (end-exclusive; default = `try_get_log_entries(start..end)`)。Short read legal — replication
  resumes at last returned index+1. Non-empty range MUST yield non-empty batch.
- 实测（release, budget-exp crate, ext4 FS with fsync 2.6-3.7ms）：
  - n=16 fat entries → msgpack 961347B, append+fsync 2.6-3.7ms
  - n=32 → 1922691B, ~4.1-5.6ms
  - n=64 → 3845379B, ~8-11ms
  - stored bytes ≈ msgpack bytes (1.00 ratio) → stored-bytes budget bounds the wire batch.
- Decision: `max_payload_entries=64` KEPT; new hard cap `MAX_BATCH_BYTES = 6<<20` (6 MiB)
  in `store.read_entries_limited` (both `limited_get_log_entries` override + raw helper).
  First oversized entry ships alone (never an empty batch).
- Config: `dev_config.enable_pre_vote = Some(true)` documented; raft.rs honors
  `openraft::leader_config::Leader.pre_vote`.

### 13.2 A — startup snapshot payload sweep
- `store.sweep_snapshot_payloads(db, dir)` at `StateMachineStore` ctor (after prepare_snap_dir):
  removes `*.part` residue; removes unreferenced canonical `*.snap` (registry `file` key =
  single JSON-encoded PathBuf); keeps foreign files; corrupt/unparseable registry read
  ABORTS sweep (nothing deleted). Returns `(parts_removed, orphans_removed)`.
- Verified by tests: `tests/snapshot_sweep.rs` (3 cases incl. corrupt-registry abort + ctor hook).

### 13.3 D — reqwest error taxonomy
- `http_network.map_send_err` / `map_stream_err`: `e.is_connect()` (DNS/refused/no-route) →
  `RPCError::Unreachable` / `StreamingError::Unreachable`; everything else (incl. client
  timeout on an established connection, mid-body, non-2xx) → `Network` (retry-immediately).
  Backoff remains openraft's decision.

### 13.4 C2 — boot 0.9 real storm data on the 0.10 spike（FRESH copies, c2run2/n1..n5）
- 5-node cluster from `cp -a run/` (single `dsh.redb` each, 0.9 layout incl. inline snapshot
  blob in raft metadata; node ids 1..5 on the stored 0.9 HTTP/Raft addresses).
- CLI now REQUIRES `--join-token` AND `--raft-token` (identical per cluster) for cluster mode.
- Results (fresh boot, fixed binary):
  - `migrated legacy inline snapshot blob to file` — ALL 5 nodes (P2a lazy migration path).
  - far-behind node 1: `Done install complete snapshot` (streamed full-snapshot install).
  - **0 msgpack decode failures** — see 13.5 below; replication converged all nodes
    (`last_log_index=14051`, lag≤1 across members).
  - write chain on leader 5 (login → DraftUpdate snp-5/dev → Publish, version 377) replicated
    and read back from follower node 1.
  - restart boot (c2run/ copies, prior mutated run): sweep kept exactly the 1 referenced
    145MB payload per node, 0 orphans — A + P2a interaction healthy.

### 13.5 P3a C2 bug found — msgpack positional desync（FIXED: `to_vec_named`）
- Symptom on 0.9 storm logs: `raft rpc msgpack decode failed … wrong msgpack marker FixMap(3)`
  on `/raft/append-entries` (server 400 → openraft NetworkError retry loop).
- Root cause: rmp_serde default `to_vec` encodes structs **positionally (array)**; model.rs
  `AuditEntry` (and friends) use `#[serde(skip_serializing_if = "Option::is_none")]` on
  `version/request_id/...` — skipped fields TRUNCATE the positional array → decoder desyncs
  mid-struct (lands on the `detail`/`result` map marker). 0.9 JSON (self-describing) never
  exposed it.
- Fix: BOTH wire ends use `rmp_serde::to_vec_named` (struct→map, field names):
  `http_network.post_msgpack` + `raft_http_server.msgpack_resp`. `from_slice` decodes
  map-structs natively; snapshot frame codec (no Option-bearing structs) unchanged.
  Wire grows by field names only; C1 6 MiB cap unaffected.
- Verified: fresh-copy C2 boot shows 0 decode failures + full replication (13.4).

## 13.6 P3 install 路径实测与整改（P3c/P3d/P3e/P3f）

> 复核要求：数字必须可溯源（方法、数据集、构建、命令、rev、二进制 sha）。

**方法**：`scripts/stress-leader-storm.sh --bin <release> --profile storm --snapshot-writes N
--chaos-during-snapshot --catchup-timeout 120 --max-install-seconds 30 --min-snapshot-bytes 100000000
--keep`，5 节点、数据目录在 btrfs（`TMPDIR` 指到项目内 `.tmp/tmp`），release 构建；
harness 在 summary 里记录 `git_rev` / `bin_sha256` / `TMPDIR` / `argv`（见 `p3d-storm4.log`）。
install 窗口 = `full-snapshot transfer start` → `install full snapshot` → `Done install complete snapshot`
（端到端 = transfer + install + apply）。

**根因与整改**（两处，均在 P2a 遗留的 compile-bridge 形态上）：

| # | 问题 | 整改 | 效果 |
|---|---|---|---|
| 1 | `dsh-core::restore_all` 逐 key `delete`/`put`，每个 key 一个 redb 事务（400MB 快照 ≈ 4 万次 fsync） | 走既有 `Store::write_batch`（单事务 + 一次 fsync），按 64MiB 分块 | **install 84.1s → 1.5s 量级**（主因） |
| 2 | `install_snapshot` 整份 `fs::read` + `from_slice`（两份全量驻留）并把 payload 再整份写一遍 | `serde_json::from_reader` 流式解析；文件级 `fs::copy`（不 rename 采纳，避免删掉 registry 自身 payload） | 峰值 RSS **2.05x → 1.3x**；`snapshots/` 只留 1 份 canonical 文件 |

**实测**（release + btrfs）：

| 轮次 | 快照 | install 端到端 | catch-up | victim 峰值 RSS | 备注 |
|---|---|---|---|---|---|
| 整改前（`56a3a6e`，A/B control，4k 写） | 197.6MB | 25.87s | 42.9s | 461MB (2.3x) | 同盘同构建对照 |
| 整改后（4k 写，同参数） | 174.2MB | **0.657s** | 20.0s | 263MB (1.5x) | 50x 级差异，排除数据/机器因素 |
| 整改后（6k 写，`p3d-storm4`） | 269.5MB | **1.779s**（transfer 0.747 + apply 1.031） | 10.2s | 368MB (1.37x) | C3/C4 断言全开 |
| 整改后（12k 写，`p3g-gate`，全项目比对判据） | 397.4MB | **2.727s** | 10.2s | 483.5MB (1.22x) | 5/5 kill 验证+换人，8 项目逐项相等 |

**C3/C4 证据口径**（harness 现在会断言，不再只报告）：
- C3：每次 leader kill（load 与 snapshot 阶段都算）都先确认目标**自认 leader**（`members[].is_leader`），
  击杀后、重启前观测是否真的换人（`p3d-storm4`/`p3g-gate`：5/5 验证、5/5 换人、0 missing）；
  install 窗口内 leader 视角 victim 心跳 age（`last_heartbeat_ms_ago`）在 120MB 档为 119–399ms，
  397MB 的 12k 跑为 1.742s（预算 3s）。
  口径：这是 leader 侧「最近一次确认心跳/复制」的 age；install 期间 follower 忙于收帧/应用会延后 ack，
  但 follower 的选举计时器由「收到请求」重置 —— 所以 age 1.7s 与「窗口内 victim 0 次选举」不矛盾，
  它证明的是心跳**没有整段停摆**（0.9 失败模式下该 age 会逼近整个 install 时长）。
- C4：`install_windows>=1`、端到端 <= `--max-install-seconds`、`snapshot_bytes >= --min-snapshot-bytes`、
  victim 必须 `is_voter`；churn 在 victim 的 **transfer 窗口一开**时注入；窗口内 victim
  选举/预投票必须为 0（计入 `trigger pre-vote`）。
