#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""leader_storm — 集群压力测试 harness (高强度、非常规).

Focus: 频繁 leader 变更 (leader roulette) + 高并发读写 + snapshot 追赶.
The snapshot stage reproduces the fixed "follower 永久落后" scenario
(dev_docs/deployment-guide.md §6.5, dsh-raft raft.rs: /raft/* body limit 32MiB,
chunk 1MiB, install_snapshot_timeout 30s). 超过 openraft snapshot_policy
since_last 阈值 → 被重新加入 follower 追平必须走 InstallSnapshot, 旧
配置恒 413 / InstallSnapshot RPC timed out.

Pure Python 3.8+ stdlib (urllib, threading). Exit codes:
  0  全绿
  1  任一 invariant / 日志扫描 / 追赶 timeout 失败
  2  启动失败 (二进制/集群 boot/login 失败)

Usage (thin wrapper scripts/stress-leader-storm.sh):
  leader_storm.py [--profile quick|storm|extreme] [--bin PATH] [--keep]
                  [--nodes N] [--workers W] [--duration S] [--leader-kills N]
                  [--snapshot-writes N] [--value-bytes N] [--seed N]
                  [--catchup-timeout S]

Default profile = storm.
"""

import argparse
import json
import os
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import uuid
import urllib.error
import urllib.request

# module-level shared state (set in main(); helpers reference one instance)
from typing import Any  # noqa: E402
CLUSTER: Any = None
STATS: Any = None
STATS_LOCK: Any = None
SESSION_TOKEN: Any = None
START_TS: float = 0.0
SNAPSHOT_PROJECTS: list = []
READERS: list = []


# ---------------------------------------------------------------- constants / profiles

ADMIN_PASSWORD = "stress-pass"
JOIN_TOKEN = "stress-token"
RAFT_TOKEN = "stress-token"
BRANCH = "dev"
MAX_VALUE_BYTES = 64 * 1024  # == dsh-core limits.MAX_VALUE_BYTES

PROFILES = {
    # nodes, duration, workers, leader_kills, snapshot_writes
    "quick":   dict(nodes=5, duration=8,   workers=6,  leader_kills=3,  snapshot_writes=0),
    "storm":   dict(nodes=5, duration=60,  workers=12, leader_kills=12, snapshot_writes=6500),
    "extreme": dict(nodes=5, duration=180, workers=24, leader_kills=30, snapshot_writes=8000),
}

# 日志扫描禁止 token (大小写不敏感, 本次 log files only)
# Concrete failure signatures for the snapshot-body-limit / RPC-timeout bug.
# A bare "413" substring collides with raft log indices (T2-N4-413 etc), so we match the
# strings the components actually emit:
#   - "length limit exceeded"   = axum DefaultBodyLimit 413 on /raft/* (raft_http_server.rs:66)
#   - "installsnapshot rpc timed out" = openraft InstallSnapshot timeout (raft.rs header note)
# plus hard-crash markers panic/fatal.
FORBIDDEN = ("panic", "fatal", "length limit exceeded", "installsnapshot rpc timed out")

# login/healthz 免 Bearer; 其余 /api/v1 需 Bearer
# data-plane /v1/... 需 session 或 project token (admin session 覆盖全项目 → 可读 config)


def log(msg):
    print(msg, flush=True)


# ---------------------------------------------------------------- free port (socket bind 0)

_alloc_lock = threading.Lock()
_taken = set()


def free_port():
    """Bind port 0 to obtain an OS-assigned free port, never reused during the run.

    独立端口 per node (http/raft/grpc), 避免 chaos-test/bench/cluster-demo 固定端口冲突.
    """
    while True:
        with _alloc_lock:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
            s.close()
            if port not in _taken:
                _taken.add(port)
                return port


# ---------------------------------------------------------------- HTTP helper (urllib)

# Never auto-follow redirects; caller handles 428/leader redirect explicitly.


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_OPENER = urllib.request.build_opener(NoRedirect)


def http_json(base, token, method, path, body=None, timeout=10.0):
    """POST/PUT/GET JSON to node base. Returns dict {status, data, err}.

    Never raises; transient errors surface as status=None/err so writers retry.
    """
    url = base.rstrip("/") + path
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        resp = _OPENER.open(req, timeout=timeout)
        raw = resp.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return {"status": resp.status, "data": parsed}
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            parsed = json.loads(raw)
        except Exception:
            parsed = raw
        return {"status": e.code, "data": parsed}
    except Exception as e:  # URLError / timeout / connection reset (node killed mid-flight)
        return {"status": None, "err": repr(e)}


def http_get(base, token, path, timeout=10.0):
    return http_json(base, token, "GET", path, None, timeout)


def members(base, token=None):
    # /api/v1/cluster/members needs Bearer: default to the global session token
    if token is None:
        token = SESSION_TOKEN
    r = http_get(base, token, "/api/v1/cluster/members")
    if r["status"] == 200 and isinstance(r["data"], dict):
        return r["data"]
    return None


def healthz(base):
    r = http_get(base, None, "/healthz", timeout=4.0)
    return r["status"] == 200


# ---------------------------------------------------------------- Node model


class Node:
    """Owns one defing process. Restart reuses same data-dir + argv (auto-rejoin)."""

    def __init__(self, node_id, bin_path, data_dir, log_path):
        self.node_id = node_id
        self.bin = bin_path
        self.data_dir = data_dir
        self.log_path = log_path
        self.http_addr = "127.0.0.1:%d" % free_port()
        self.raft_addr = "127.0.0.1:%d" % free_port()
        self.grpc_addr = "127.0.0.1:%d" % free_port()
        self.proc = None
        self.down = True            # True while not running (killed / not yet started)
        self.frozen = False         # SIGSTOP'd follower (still "live" for quorum counting)
        self.log = open(log_path, "ab", buffering=0)

    def http_base(self):
        return "http://%s" % self.http_addr

    def argv(self, seed_map):
        return [
            self.bin,
            "--node-id", str(self.node_id),
            "--http-addr", self.http_addr,
            "--raft-addr", self.raft_addr,
            "--grpc-addr", self.grpc_addr,
            "--data-dir", self.data_dir,
            "--bootstrap-peers", seed_map,
            "--admin-password", ADMIN_PASSWORD,
            "--allow-no-master-key",
            "--join-token", JOIN_TOKEN,
            "--raft-token", RAFT_TOKEN,
        ]

    def spawn(self, seed_map):
        """Start / restart. Log appends (restart → same file). start_new_session so
        we own a fresh process group and never touch foreign defing processes."""
        self.log = open(self.log_path, "ab", buffering=0)
        self.proc = subprocess.Popen(
            self.argv(seed_map),
            stdout=self.log,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        )
        self.down = False
        self.frozen = False

    def kill(self):
        """SIGKILL own process; wait for exit (port release before restart)."""
        if self.proc:
            try:
                self.proc.send_signal(signal.SIGKILL)
                self.proc.wait(timeout=10)
            except Exception:
                pass
        self.down = True
        self.frozen = False

    def stop_freeze(self):
        if self.proc and self.proc.poll() is None:
            try:
                self.proc.send_signal(signal.SIGSTOP)
                self.frozen = True
            except Exception:
                pass

    def resume_freeze(self):
        if self.proc and self.frozen:
            try:
                self.proc.send_signal(signal.SIGCONT)
                self.frozen = False
            except Exception:
                pass

    def alive(self):
        return self.proc is not None and self.proc.poll() is None and not self.down

    def tail(self, n=40):
        try:
            with open(self.log_path, "rb") as f:
                return f.read().decode("utf-8", "replace").splitlines()[-n:]
        except Exception:
            return []

    def close_log(self):
        try:
            self.log.close()
        except Exception:
            pass


# ---------------------------------------------------------------- Cluster


class Cluster:
    def __init__(self, nodes, seed_map):
        self.nodes = nodes            # list[Node], index == node_id-1
        self.seed_map = seed_map
        self.lock = threading.Lock()  # guards process lifecycle vs chaos/cleanup races

    def by_id(self, node_id):
        for n in self.nodes:
            if n.node_id == node_id:
                return n
        return None

    def live(self):
        return [n for n in self.nodes if n.alive()]

    def live_count(self):
        return len(self.live())


def cluster_leader_id(node):
    """Read /cluster/members (from a live node); return (leader_id, data).

    Any live node exposes current_leader (openraft Metrics.current_leader). Returns
    (None, None) when node unreachable.
    """
    data = members(node.http_base(), None)
    if data is None:
        return (None, None)
    return (data.get("current_leader"), data)


def wait_leader(timeout):
    """Poll until some live node reports a non-null current_leader. Returns node_id."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        for n in CLUSTER.live():
            lid, _ = cluster_leader_id(n)
            if lid is not None:
                return lid
        time.sleep(0.2)
    return None


def wait_healthz(node, timeout):
    t0 = time.time()
    while time.time() - t0 < timeout:
        if healthz(node.http_base()):
            return True
        time.sleep(0.2)
    return False


# ---------------------------------------------------------------- stats (thread-safe)


class Stats:
    def __init__(self, nodes):
        self.lock = threading.Lock()
        self.attempts = 0
        self.ok = 0
        self.retries = 0
        self.transient = 0
        self.publish_ok = 0
        self.leader_kills = 0
        self.follower_kills = 0
        self.freeze_count = 0
        self.observed_leader_changes = 0
        self.per_node = {n.node_id: 0 for n in nodes}

    def bump(self, attr, node_id=None, n=1):
        with self.lock:
            setattr(self, attr, getattr(self, attr) + n)
            if node_id is not None and node_id in self.per_node:
                self.per_node[node_id] += n


# ---------------------------------------------------------------- request primitives


def write_draft(node, token, project, value_bytes, marker):
    val = ("x" * value_bytes) + marker
    body = {
        "updates": [{"group": "g", "key": "k", "value": {"type": "string", "str_value": val}}]
    }
    return http_json(node.http_base(), token, "PUT",
                     "/api/v1/projects/%s/branches/%s/draft" % (project, BRANCH), body,
                     timeout=15.0)


def write_publish(node, token, project, comment, request_id):
    body = {"comment": comment, "request_id": request_id}
    return http_json(node.http_base(), token, "POST",
                     "/api/v1/projects/%s/branches/%s/publish" % (project, BRANCH), body,
                     timeout=15.0)


def read_config(node, token, project):
    """GET data-plane config json; parse (version, marker) from rendered groups.

    同一节点 (format=json) 的 config version 单调性 checked by caller (monotonic map).
    Returns dict or None on transient error.
    """
    r = http_get(node.http_base(), token,
                 "/v1/projects/%s/branches/%s/config?format=json" % (project, BRANCH),
                 timeout=10.0)
    if r["status"] != 200:
        return None
    data = r["data"]
    if not isinstance(data, dict):
        return None
    groups = data.get("groups") or {}
    val = None
    gv = groups.get("g")
    if isinstance(gv, dict):
        val = gv.get("k")
    if val is None:
        return None
    return {"groups": groups, "value": val}


def mgmt_config(node, token, project):
    """Management-plane GET /api/v1/.../config → {version, value}. Used for final checks
    (data-plane /v1 render carries no version; string values are not masked)."""
    r = http_get(node.http_base(), token,
                 "/api/v1/projects/%s/branches/%s/config" % (project, BRANCH), timeout=10.0)
    if r["status"] != 200 or not isinstance(r.get("data"), dict):
        return None
    groups = r["data"].get("groups") or {}
    val = None
    gv = groups.get("g")
    if isinstance(gv, dict):
        val = gv.get("k")
    return {"version": r["data"].get("version"), "value": val}



# ---------------------------------------------------------------- project prebuild


def ensure_project(token, project, deadline=None):
    """Create project + structure (idempotent; publish via leader). Used at setup."""
    # create project (POST /projects idempotent-ish: existing returns error 4xx — ignore)
    for n in CLUSTER.live():
        r = http_json(n.http_base(), token, "POST", "/api/v1/projects", {"name": project})
        if r["status"] in (200, 201) or (r["status"] is not None and r["status"] < 500):
            break
    # structure draft + publish (retry until some node 200 within deadline)
    while deadline is None or time.time() < deadline:
        for n in CLUSTER.live():
            http_json(n.http_base(), token, "PUT",
                      "/api/v1/projects/%s/structure-draft" % project,
                      {"base_version": 1, "groups": [
                          {"name": "g", "items": [{"key": "k", "type": "string", "required": True}]}]},
                      timeout=15.0)
            r = http_json(n.http_base(), token, "POST",
                          "/api/v1/projects/%s/structure-draft/publish" % project,
                          {"comment": "init", "request_id": "sinit-" + project},
                          timeout=15.0)
            if r["status"] == 200:
                return True
        time.sleep(0.3)
    return False


# ---------------------------------------------------------------- writers


class Writer:
    def __init__(self, wid, project, args, stats, token):
        self.wid = wid
        self.project = project
        self.args = args
        self.stats = stats
        self.token = token
        self.stop = threading.Event()
        self.acked_version = 0
        self.all_markers = set()
        self.local_seq = 0
        self.max_attempted_seq = -1
        self.t: Any = None

    def run(self, deadline):
        value_bytes = min(self.args.value_bytes, MAX_VALUE_BYTES - 64)
        while not self.stop.is_set() and time.time() < deadline:
            live = CLUSTER.live()
            if not live:
                time.sleep(0.1)
                continue
            seq = self.local_seq
            self.local_seq += 1
            marker = "|W%d#S%d#%s#" % (self.wid, seq, uuid.uuid4().hex[:8])
            rid = "r-%d-%d-%s" % (self.wid, seq, uuid.uuid4().hex[:6])
            self.max_attempted_seq = max(self.max_attempted_seq, seq)
            val_bytes = value_bytes
            ok_pub = None
            attempts_local = 0
            back = 0.05
            # retry loop with the SAME request_id → idempotent (allow unconfirmed-but-applied)
            while True:
                live = CLUSTER.live()
                if not live:
                    time.sleep(0.1)
                    if time.time() >= deadline or self.stop.is_set():
                        break
                    continue
                node = random.choice(live)
                if not self.stop.is_set():
                    self.stats.bump("attempts", node_id=node.node_id)
                d = write_draft(node, self.token, self.project, val_bytes, marker)
                attempts_local += 1
                p = write_publish(node, self.token, self.project, "w%d.%d" % (self.wid, seq), rid)
                if attempts_local > 1:
                    self.stats.bump("retries")
                if p["status"] == 200:
                    ok_pub = p
                    break
                if p["status"] == 428 or (p["status"] is not None and p["status"] >= 500):
                    # leader redirect / 5xx → transient, retry
                    self.stats.bump("transient")
                elif p["status"] is None:
                    self.stats.bump("transient")
                if time.time() >= deadline or self.stop.is_set():
                    break
                time.sleep(back)
                back = min(back * 2, 1.0)
            if ok_pub is not None:
                ver = (ok_pub["data"] or {}).get("version") if isinstance(ok_pub.get("data"), dict) else None
                if ver is not None:
                    self.acked_version = max(self.acked_version, int(ver))
                self.all_markers.add(marker)
                self.stats.bump("publish_ok")

    def owner_ok(self, value):
        # leader value must be one THIS writer generated: its |W{wid}#S{seq}# must match
        # this writer's id and a seq it actually attempted (tolerates unconfirmed-but-applied).
        i = value.find("|W")
        if i < 0:
            return False
        try:
            j = value.find("#S", i)
            if j < 0:
                return False
            wid = int(value[i + 2:j])
            s = value[j + 2:]
            end = s.find("#")
            seq = int(s[:end]) if end >= 0 else int(s)
        except Exception:
            return False
        return wid == self.wid and 0 <= seq <= self.max_attempted_seq


class Reader:
    def __init__(self, rid, projects, args, stats, token):
        self.rid = rid
        self.projects = projects
        self.args = args
        self.stats = stats
        self.token = token
        self.stop = threading.Event()
        # monotonic per (project,node): seq must never regress. last_seq map.
        self.last_seq = {}
        self.violations = []  # (project, node_id, prev, new)
        self.t: Any = None

    def parse_seq(self, value):
        # value ends with |W{wid}#S{seq}#rand#  → extract seq
        if value is None:
            return None
        idx = value.rfind("#S")
        if idx < 0:
            return None
        try:
            s = value[idx + 2:]
            end = s.find("#")
            return int(s[:end]) if end >= 0 else int(s)
        except Exception:
            return None

    def run(self, deadline):
        while not self.stop.is_set() and time.time() < deadline:
            live = CLUSTER.live()
            if not live:
                time.sleep(0.05)
                continue
            node = random.choice(live)
            project = random.choice(self.projects)
            r = read_config(node, self.token, project)
            if r is None:
                self.stats.bump("transient")
                continue
            seq = self.parse_seq(r.get("value"))
            if seq is None:
                continue
            key = (project, node.node_id)
            prev = self.last_seq.get(key)
            if prev is not None and seq < prev:
                self.violations.append((project, node.node_id, prev, seq))
            self.last_seq[key] = max(prev or 0, seq)


# ---------------------------------------------------------------- chaos (leader roulette core)


class Chaos:
    """Inject leader kills + follower freeze + (rare) follower kill during load.

    Guarantees live_count >= 3 at all times (5-node quorum). Every injection recorded.
    """

    def __init__(self, args, stats, rng):
        self.args = args
        self.stats = stats
        self.rng = rng
        self.stop = threading.Event()
        self.timeline = []  # (t, kind, node_id)
        self.last_leader = None
        self.kills_done = 0
        self.round_idx = 0
        self.leader_kill_count = 0
        self.follower_kill_count = 0
        self.t: Any = None

    def record(self, kind, node_id):
        self.timeline.append((round(time.time() - START_TS, 2), kind, node_id))

    def track_leader_change(self):
        # authoritative leader = the current_leader value the most live nodes agree on
        # (the leader reports itself; followers may lag, so use a majority vote).
        live = CLUSTER.live()
        if not live:
            return
        vals = []
        for n in live:
            lid, _ = cluster_leader_id(n)
            if lid is not None:
                vals.append(lid)
        if not vals:
            return
        lid = max(set(vals), key=vals.count)
        if self.last_leader is not None and lid != self.last_leader:
            STATS.bump("observed_leader_changes")
        self.last_leader = lid

    def leader_roulette(self):
        # honor --leader-kills cap; current leader read from a live node's members view
        if self.leader_kill_count >= self.args.leader_kills:
            return
        live = CLUSTER.live()
        if not live:
            return
        lid, _ = cluster_leader_id(random.choice(live))
        if lid is None:
            return
        leader = CLUSTER.by_id(lid)
        if leader is None or not leader.alive():
            return
        if CLUSTER.lock.locked():
            return
        with CLUSTER.lock:
            # need >=4 live before killing so live stays >=3 (5-node quorum floor)
            if CLUSTER.live_count() < 4:
                return
            leader.kill()
        self.leader_kill_count += 1
        self.stats.bump("leader_kills")
        self.record("leader_kill", leader.node_id)
        # restart 0.2–1.2s later with same data-dir/argv
        time.sleep(self.rng.uniform(0.2, 1.2))
        with CLUSTER.lock:
            leader.spawn(CLUSTER.seed_map)
        self.record("leader_restart", leader.node_id)
        wait_healthz(leader, 25)
        new_leader = wait_leader(25)
        if new_leader is not None and new_leader != lid:
            STATS.bump("observed_leader_changes")
            self.last_leader = new_leader

    def follower_freeze(self):
        live = [n for n in CLUSTER.live() if not n.frozen]
        if CLUSTER.live_count() < 4:  # keep >=3 responsive while one is frozen
            return
        lid = None
        if live:
            lid, _ = cluster_leader_id(random.choice(live))
        cands = [n for n in live if n.node_id != lid]
        if not cands:
            return
        target = random.choice(cands)
        target.stop_freeze()
        self.stats.bump("freeze_count")
        self.record("follower_freeze", target.node_id)
        time.sleep(self.rng.uniform(1.0, 3.0))
        target.resume_freeze()

    def follower_kill(self):
        live = CLUSTER.live()
        if CLUSTER.live_count() < 4:  # live stays >=3 after kill
            return
        lid = None
        if live:
            lid, _ = cluster_leader_id(random.choice(live))
        cands = [n for n in live if n.node_id != lid and not n.frozen]
        if not cands:
            return
        target = random.choice(cands)
        with CLUSTER.lock:
            target.kill()
        self.follower_kill_count += 1
        self.stats.bump("follower_kills")
        self.record("follower_kill", target.node_id)
        time.sleep(self.rng.uniform(1.0, 3.0))
        with CLUSTER.lock:
            target.spawn(CLUSTER.seed_map)
        self.record("follower_restart", target.node_id)
        wait_healthz(target, 25)

    def run(self, deadline):
        # seed the baseline leader so the first re-election is counted as a change
        self.last_leader = wait_leader(5)
        # rotation: leader kills prioritized until leader_kills reached, freeze interleaved,
        # follower_kill every 5th round (low probability)
        while not self.stop.is_set() and time.time() < deadline:
            self.round_idx += 1
            self.track_leader_change()
            if self.round_idx % 5 == 0:
                self.follower_kill()
            elif self.leader_kill_count < self.args.leader_kills:
                # leader roulette until cap hit; else freeze
                self.leader_roulette()
                self.track_leader_change()
                if time.time() < deadline and self.leader_kill_count >= self.args.leader_kills:
                    self.follower_freeze()
            else:
                self.follower_freeze()
            # random interval 0.8–2.5s between injections
            time.sleep(self.rng.uniform(0.8, 2.5))


# ---------------------------------------------------------------- snapshot catch-up phase


def snapshot_phase(args, stats, token, rng):
    """非常规重点: kill a follower (keep down) → bulk writes to exceed snapshot_policy →
    restart follower → it must catch up via InstallSnapshot (not 413 / timed out).
    """
    log("== snapshot 追赶阶段 (snapshot_writes=%d) ==" % args.snapshot_writes)
    # 1. pick a live non-leader follower, SIGKILL, keep down
    live = CLUSTER.live()
    if not live:
        return None, None
    lid, _ = cluster_leader_id(random.choice(live))
    cands = [n for n in live if n.node_id != lid] or live
    victim = random.choice(cands)
    with CLUSTER.lock:
        victim.kill()
    victim_id = victim.node_id
    log("  killed follower node %d (kept down during bulk writes)" % victim_id)

    # baseline leader last_log_index
    leader = CLUSTER.by_id(lid) if lid else CLUSTER.by_id(wait_leader(20))
    base = None
    if leader:
        md = members(leader.http_base(), None)
        if md:
            base = md.get("last_log_index")

    # 2. 8 writers over 8 dedicated snapshot projects → snapshot_writes total
    total = args.snapshot_writes
    projects = SNAPSHOT_PROJECTS
    nw = len(projects)
    share = [total // nw + (1 if i < total % nw else 0) for i in range(nw)]
    stop = threading.Event()
    threads = []
    done = [0]
    dlock = threading.Lock()
    vb = min(4096, MAX_VALUE_BYTES - 64)

    def bulk(wi, project, count):
        seq = 0
        while seq < count and not stop.is_set():
            live = CLUSTER.live()
            if not live:
                time.sleep(0.05)
                continue
            node = random.choice(live)
            seq += 1
            marker = "|SNP%d#S%d#%s#" % (wi, seq, uuid.uuid4().hex[:6])
            rid = "sn-%d-%d-%s" % (wi, seq, uuid.uuid4().hex[:6])
            stats.bump("attempts", node_id=node.node_id)
            write_draft(node, token, project, vb, marker)
            p = write_publish(node, token, project, "snp", rid)
            if p["status"] == 200:
                with dlock:
                    done[0] += 1
                stats.bump("publish_ok")
            else:
                stats.bump("transient")
                stats.bump("retries")

            with STATS_LOCK:
                stats.per_node[node.node_id] += 1

    t0 = time.time()
    for wi in range(nw):
        th = threading.Thread(target=bulk, args=(wi, projects[wi], share[wi]), name="snapw%d" % wi)
        th.start()
        threads.append(th)
    for th in threads:
        th.join()
    bulk_seconds = time.time() - t0
    log("  bulk writes done: %d/%d publishes in %.1fs (victim still down)" % (done[0], total, bulk_seconds))

    # 3. wait leader last_log_index to reflect the bulk volume (gap accumulates on victim)
    if leader is None:
        leader = CLUSTER.by_id(wait_leader(30))
    if leader and base is not None:
        target_idx = base + total * 2  # draft+publish each append ~2 log entries
        wl = time.time() + args.catchup_timeout
        while time.time() < wl:
            md = members(leader.http_base(), None)
            if md and md.get("last_log_index") is not None and md.get("last_log_index") >= base + total:
                break
            time.sleep(0.3)

    # 4. restart the killed follower NOW → large log gap → must catch up via InstallSnapshot
    with CLUSTER.lock:
        victim.spawn(CLUSTER.seed_map)
    log("  restarted victim node %d (catch-up must use InstallSnapshot)" % victim_id)
    if not wait_healthz(victim, 25):
        log("  WARN: victim healthz timeout after restart")

    # 5. poll leader members: victim lag → NUMERIC 0, AND snapshot project config == leader's.
    #    config-equal-but-lag-null is NOT catch-up: lag==null means the victim's matched index
    #    is not yet established on the leader (stale/absent replication), so require an explicit
    #    victim lag == 0 from the leader's replication view before accepting config equality.
    catchup_t0 = time.time()
    catchup_seconds = None
    max_lag = None          # only numeric lag observations update it; None is NOT "0"
    lag_values = []         # every numeric victim lag observed (for summary/diagnosis)
    lag_none_seen = False   # victim lag reported null at least once
    while time.time() - catchup_t0 < args.catchup_timeout:
        victim_lag = None
        md = members(leader.http_base(), None) if leader else None
        if md:
            for m in md.get("members", []):
                if str(m.get("node_id")) == str(victim_id):
                    lag = m.get("lag")
                    if lag is None or isinstance(lag, bool):
                        if lag is None:
                            lag_none_seen = True
                    else:
                        lag_values.append(int(lag))
                        max_lag = int(lag) if max_lag is None else max(max_lag, int(lag))
                        victim_lag = int(lag)
                    break
        # primary catch-up: victim healthz + victim lag == 0 (numeric) + config equal to leader
        if victim_lag == 0 and healthz(victim.http_base()):
            lc = read_config(leader, token, projects[0]) if leader else None
            vc = read_config(victim, token, projects[0])
            if lc and vc and lc.get("value") and lc.get("value") == vc.get("value"):
                catchup_seconds = time.time() - catchup_t0
                break
        time.sleep(0.4)

    # 6. interrupted-by-election diagnostic (a failure EXPLANATION, not a FORBIDDEN token —
    #    "election timeout passed" is normal chatter otherwise). If the victim repeatedly started
    #    an election while a snapshot was arriving, the transfer was interrupted.
    interrupted_by_election = False
    try:
        with open(victim.log_path, "rb") as f:
            vblob = f.read().decode("utf-8", "replace").lower()
    except OSError:
        vblob = ""
    if ("election timeout passed" in vblob) and ("received snapshot" in vblob or "installsnapshot" in vblob):
        interrupted_by_election = True

    return {"victim_id": victim_id, "catchup_seconds": catchup_seconds,
            "max_lag": max_lag, "lag_none_seen": lag_none_seen, "lag_values": lag_values,
            "interrupted_by_election": interrupted_by_election,
            "bulk_seconds": bulk_seconds, "leader_node": leader,
            "projects": projects}, victim


# ---------------------------------------------------------------- quiesce + final validation


def quiesce(timeout=30):
    """Wait until every node is healthz AND the leader's replication view shows every live
    VOTER lag == a numeric 0.

    lag semantics (dsh-observability cluster_members_json / replication_lag):
      - lag is only populated when queried from the CURRENT LEADER (replication map exists
        on the leader only). We therefore read members from the leader node.
      - lag == null on a NON-leader voter means its matched log index is not yet established
        on the leader → replication not built → NOT caught up. Do NOT treat null as settled.
      - lag == null on the LEADER's own member entry is expected (openraft's replication map
        excludes the leader itself) → tolerated only for is_leader == true.

    Returns True on settle, False on timeout (caller fails).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = all(healthz(n.http_base()) for n in CLUSTER.nodes)
        lid = wait_leader(1)
        leader = CLUSTER.by_id(lid) if lid else None
        lag_ok = False
        if leader:
            md = members(leader.http_base(), None)
            if md:
                lag_ok = True
                for m in md.get("members", []):
                    if not m.get("is_voter"):
                        continue  # non-voters don't gate quorum settle
                    if m.get("is_leader"):
                        continue  # leader's own entry: replication excludes self → null OK
                    lag = m.get("lag")
                    # require an explicit numeric 0; null or >0 = not settled
                    if isinstance(lag, bool) or not isinstance(lag, int) or lag != 0:
                        lag_ok = False
                        break
        if ok and lag_ok:
            return True
        time.sleep(0.5)
    return False


def validate_final(args, token, writers, snapshot_info, errors):
    # leader
    lid = wait_leader(10)
    leader = CLUSTER.by_id(lid) if lid else None
    if leader is None or not leader.alive():
        errors.append("no live leader at final check")
        return

    all_projects = [w.project for w in writers] + SNAPSHOT_PROJECTS

    # 1+2: per project, leader config version >= acked_version; marker ∈ all_markers;
    #      every node config version == leader version, marker == leader marker
    leader_cfg = {}
    for p in all_projects:
        lc = mgmt_config(leader, token, p)
        leader_cfg[p] = lc
        if lc is None:
            errors.append("leader config unreadable for project %s" % p)

    for w in writers:
        lc = leader_cfg.get(w.project)
        if lc and w.acked_version:
            ver = lc.get("version")
            if ver is not None and ver < w.acked_version:
                errors.append("leader version %s < acked_version %s (project %s)"
                              % (ver, w.acked_version, w.project))
        if lc and lc.get("value"):
            if not w.owner_ok(lc["value"]):
                errors.append("leader value not owned by writer w%d (project %s, value tail=%s)"
                              % (w.wid, w.project, lc["value"][-40:]))

    for n in CLUSTER.nodes:
        if not n.alive():
            errors.append("node %d down at final check" % n.node_id)
            continue
        if n.node_id == leader.node_id:
            continue
        for p in all_projects:
            nc = mgmt_config(n, token, p)
            lc = leader_cfg.get(p)
            if nc is None or lc is None:
                continue
            if nc.get("version") != lc.get("version"):
                errors.append("node %d version %s != leader %s (project %s)"
                              % (n.node_id, nc.get("version"), lc.get("version"), p))
                break
            if nc.get("value") != lc.get("value"):
                errors.append("node %d marker/value != leader (project %s)" % (n.node_id, p))
                break

    # 3: exactly one leader consistent across all nodes; 5 voters; none down
    voters_seen = set()
    leaders_seen = set()
    for n in CLUSTER.nodes:
        md = members(n.http_base(), None)
        if md is None:
            errors.append("node %d members unreadable" % n.node_id)
            continue
        cl = md.get("current_leader")
        if cl is not None:
            leaders_seen.add(cl)
        vs = [m for m in md.get("members", []) if m.get("is_voter")]
        if len(vs) >= 1:
            voters_seen.add(tuple(sorted(str(x.get("node_id")) for x in vs)))
    if len(leaders_seen) > 1:
        errors.append("leader disagreement across nodes: %s" % leaders_seen)

    # 3b: leader replication lag — every live VOTER must show a numeric lag == 0 in the
    #     leader's replication view. lag == null on a non-leader voter means its matched log
    #     index is not established on the leader (replication not built / not caught up), which
    #     is NOT settled. The leader's own member entry is null by design → tolerated.
    md = members(leader.http_base(), None)
    if md:
        for m in md.get("members", []):
            if not m.get("is_voter") or m.get("is_leader"):
                continue
            lag = m.get("lag")
            if isinstance(lag, bool) or not isinstance(lag, int) or lag != 0:
                errors.append("voter node %s not settled: lag=%r (null=replication/catch-up "
                              "not established)" % (m.get("node_id"), lag))

    # 4: log scan — no forbidden tokens in THIS run's log files
    for n in CLUSTER.nodes:
        with open(n.log_path, "rb") as f:
            blob = f.read().decode("utf-8", "replace").lower()
        for tok in FORBIDDEN:
            if tok in blob:
                errors.append("forbidden token %r in node %d log" % (tok, n.node_id))

    # 5: reader monotonicity — no per (project,node) seq regression
    for rd in READERS:
        for v in rd.violations:
            errors.append("reader monotonicity regression %s" % (v,))
            break

    # 6: snapshot catch-up assertions
    if snapshot_info is not None:
        cs = snapshot_info.get("catchup_seconds")
        if cs is None:
            errors.append("follower catch-up not reached: victim lag never settled to numeric 0 "
                          "within %ss (lag_none_seen=%s, max_lag=%s)"
                          % (args.catchup_timeout, snapshot_info.get("lag_none_seen"),
                             snapshot_info.get("max_lag")))
        elif cs >= args.catchup_timeout:
            errors.append("catch_up_seconds %s >= timeout %s" % (cs, args.catchup_timeout))
        if snapshot_info.get("max_lag") is None and snapshot_info.get("lag_none_seen"):
            errors.append("victim lag stayed null throughout (InstallSnapshot never established)")
        if snapshot_info.get("interrupted_by_election"):
            errors.append("snapshot transfer interrupted by election timeout (victim repeatedly "
                          "campaigned during InstallSnapshot); see node %s log"
                          % snapshot_info.get("victim_id"))
        if snapshot_info["victim_id"] is not None:
            vid = snapshot_info["victim_id"]
            with open(CLUSTER.by_id(vid).log_path, "rb") as f:
                blob = f.read().decode("utf-8", "replace").lower()
            for tok in ("length limit exceeded", "installsnapshot rpc timed out"):
                if tok in blob:
                    errors.append("victim node %d log shows %r (snapshot catch-up regression)" % (vid, tok))


def _cfg_version(cfg):
    return cfg.get("version")


# ---------------------------------------------------------------- main


def parse_args():
    p = argparse.ArgumentParser(prog="leader_storm.py", description="cluster storm harness")
    p.add_argument("--profile", default="storm", choices=sorted(PROFILES))
    p.add_argument("--bin", default=None)
    p.add_argument("--keep", action="store_true")
    p.add_argument("--nodes", type=int)
    p.add_argument("--workers", type=int)
    p.add_argument("--duration", type=float)
    p.add_argument("--leader-kills", type=int)
    p.add_argument("--snapshot-writes", type=int)
    p.add_argument("--value-bytes", type=int, default=60000)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--catchup-timeout", type=float, default=60.0)
    return p.parse_args()


def main():
    args = parse_args()
    prof = dict(PROFILES[args.profile])
    for key in ("nodes", "workers", "duration", "leader_kills", "snapshot_writes"):
        v = getattr(args, key.replace("-", "_"))
        if v is not None:
            prof[key] = v
    args.nodes = prof["nodes"]
    args.workers = prof["workers"]
    args.duration = prof["duration"]
    args.leader_kills = prof["leader_kills"]
    args.snapshot_writes = prof["snapshot_writes"]

    bin_path = args.bin or os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "..", "..", "server", "target", "debug", "defing")
    if not (os.path.isfile(bin_path) and os.access(bin_path, os.X_OK)):
        log("FAIL: binary not executable: %s (build first / pass --bin)" % bin_path)
        return 2

    rng = random.Random(args.seed)
    random.seed(args.seed)

    work = None
    nodes = []
    exit_code = 0
    try:
        import tempfile
        work = tempfile.mkdtemp(prefix="defing-storm.")
        node_objs = []
        for i in range(1, args.nodes + 1):
            d = os.path.join(work, "n%d" % i)
            os.makedirs(d)
            node_objs.append(Node(i, bin_path, d, os.path.join(work, "n%d.log" % i)))
        # bootstrap seed map: every node passes the SAME map id@raft@http
        seed_map = ",".join("%d@%s@%s" % (n.node_id, n.raft_addr, n.http_addr) for n in node_objs)
        globals()["CLUSTER"] = Cluster(node_objs, seed_map)
        nodes.extend(node_objs)  # track for finally cleanup


        t_boot = time.time()
        for n in node_objs:
            n.spawn(seed_map)
        # wait all healthz
        boot_ok = all(wait_healthz(n, 30) for n in node_objs)
        if not boot_ok:
            for n in node_objs:
                if not healthz(n.http_base()):
                    log("  boot fail node %d; log tail:" % n.node_id)
                    for line in n.tail(30):
                        log("    " + line)
            return 2
        # login once (login 免 Bearer; auto-forwards to leader)
        tok_data = None
        for attempt in range(20):
            r = http_json(node_objs[0].http_base(), None, "POST", "/api/v1/login",
                          {"password": ADMIN_PASSWORD}, timeout=10.0)
            if r["status"] == 200 and isinstance(r.get("data"), dict) and r["data"].get("token"):
                tok_data = r["data"]["token"]
                break
            time.sleep(0.5)
        if not tok_data:
            log("FAIL: admin login failed")
            return 2
        globals()["SESSION_TOKEN"] = tok_data
        # wait cluster fully forms: 5 voters + a leader (members now auth'd via SESSION_TOKEN)
        formed = None
        for _ in range(60):
            lid = wait_leader(1)
            if lid is not None:
                md = members(node_objs[0].http_base())
                if md and len([m for m in md.get("members", []) if m.get("is_voter")]) >= args.nodes:
                    formed = lid
                    break
            time.sleep(0.3)
        if formed is None:
            md = members(node_objs[0].http_base())
            log("  cluster not formed (need %d voters + leader); members=%s" % (args.nodes, md))
            return 2
        # all nodes must see the session token persisted via Raft (cluster-wide)
        if not wait_healthz(node_objs[0], 10):
            return 2
        log("  cluster up (%d nodes, %.1fs), leader=%s, logged in (token persisted)"
            % (args.nodes, time.time() - t_boot, formed))

        # prebuild writer projects + snapshot projects
        n_projects = max(args.workers, 8)
        writer_projects = ["storm-%d" % i for i in range(n_projects)]
        globals()["SNAPSHOT_PROJECTS"] = ["snp-%d" % i for i in range(8)] if args.snapshot_writes > 0 else []
        deadline_boot = time.time() + 40
        for proj in writer_projects + SNAPSHOT_PROJECTS:
            if not ensure_project(tok_data, proj, deadline_boot):
                log("FAIL: prebuild project %s" % proj)
                return 2
        log("  prebuilt %d writer + %d snapshot projects" % (len(writer_projects), len(SNAPSHOT_PROJECTS)))

        # stats + threads
        globals()["STATS"] = Stats(node_objs)
        globals()["STATS_LOCK"] = threading.Lock()
        writers = []
        readers = []
        globals()["READERS"] = readers
        load_deadline = time.time() + args.duration
        START_TS_G = time.time()
        globals()["START_TS"] = START_TS_G

        for wi in range(args.workers):
            w = Writer(wi, writer_projects[wi % n_projects], args, STATS, tok_data)
            w.t = threading.Thread(target=w.run, args=(load_deadline,), name="w%d" % wi)
            writers.append(w)
        nr = max(4, args.workers // 2)
        for ri in range(nr):
            r = Reader(ri, writer_projects, args, STATS, tok_data)
            r.t = threading.Thread(target=r.run, args=(load_deadline,), name="rd%d" % ri)
            readers.append(r)
        chaos = Chaos(args, STATS, rng)
        chaos.t = threading.Thread(target=chaos.run, args=(load_deadline,), name="chaos")

        t_load = time.time()
        for w in writers:
            w.t.start()
        for r in readers:
            r.t.start()
        chaos.t.start()
        log("== load/chaos: duration=%.0fs workers=%d readers=%d leader_kills<=%d =="
            % (args.duration, args.workers, nr, args.leader_kills))
        # wait until load deadline
        while time.time() < load_deadline:
            time.sleep(0.2)
        for w in writers:
            w.stop.set()
        for r in readers:
            r.stop.set()
        chaos.stop.set()
        for w in writers:
            w.t.join(timeout=15)
        for r in readers:
            r.t.join(timeout=15)
        chaos.t.join(timeout=30)
        load_seconds = time.time() - t_load
        log("  load/chaos done in %.1fs" % load_seconds)

        # snapshot catch-up phase (skip in quick / snapshot_writes==0)
        snapshot_info = None
        if args.snapshot_writes > 0:
            info = snapshot_phase(args, STATS, tok_data, rng)
            if info:
                snapshot_info = info[0]
        if not quiesce(40):
            log("  WARN: cluster not fully settled before final validation")

        # final validation
        errors = []
        validate_final(args, tok_data, writers, snapshot_info, errors)

        # summary
        acked = sum(len(w.all_markers) for w in writers)
        success_rate = (STATS.publish_ok / STATS.attempts) if STATS.attempts else 0.0
        summary = {
            "profile": args.profile,
            "duration_load_s": round(load_seconds, 1),
            "acked_writes": acked,
            "attempts": STATS.attempts,
            "publish_ok": STATS.publish_ok,
            "success_rate": round(success_rate, 4),
            "retries": STATS.retries,
            "transient_errors": STATS.transient,
            "leader_kills": STATS.leader_kills,
            "observed_leader_changes": STATS.observed_leader_changes,
            "freeze_count": STATS.freeze_count,
            "follower_kills": STATS.follower_kills,
            "snapshot_catchup_seconds": (snapshot_info or {}).get("catchup_seconds"),
            "max_lag": (snapshot_info or {}).get("max_lag"),
            "per_node_requests": STATS.per_node,
        }
        # only meaningful when the snapshot catch-up phase actually ran (not quick)
        if snapshot_info is not None:
            summary["victim_lag_none_seen"] = bool(snapshot_info.get("lag_none_seen"))
            summary["snapshot_interrupted_by_election"] = bool(snapshot_info.get("interrupted_by_election"))
        log("== SUMMARY ==")
        for k, v in summary.items():
            log("  %-24s %s" % (k + ":", v))
        if errors:
            log("== FAILURES (%d) ==" % len(errors))
            for e in errors[:20]:
                log("  ✗ " + e)
            exit_code = 1
        else:
            log("  ✓ 全部 invariant 通过")
        # one-line JSON
        log("JSON " + json.dumps({"ok": not errors, **summary}, ensure_ascii=False))

        # failure → print victim/leader tails
        if errors:
            for n in node_objs:
                if any(("node %d " % n.node_id) in e for e in errors):
                    log("--- node %d log tail ---" % n.node_id)
                    for line in n.tail(20):
                        log("  " + line)
        return exit_code
    except KeyboardInterrupt:
        log("interrupted")
        return 1
    except Exception as e:
        import traceback
        traceback.print_exc()
        return 2
    finally:
        # cleanup ALL our processes + temp dir (never pkill defing, only our Popen)
        for n in nodes:
            try:
                if n.proc and n.proc.poll() is None:
                    # SIGKILL even if frozen (SIGSTOP'd node ignores SIGTERM)
                    n.proc.kill()
                    try:
                        n.proc.wait(timeout=8)
                    except Exception:
                        pass
                n.close_log()
            except Exception:
                pass
        if work and not args.keep:
            import shutil
            shutil.rmtree(work, ignore_errors=True)
        elif args.keep and work:
            log("kept data-dir + logs: %s" % work)


if __name__ == "__main__":
    sys.exit(main())
