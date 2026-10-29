#!/usr/bin/env python3
"""leader_storm.py 的量测解析 + C3/C4 判定单测（stdlib unittest，无需额外依赖）。

复核（迁移/测试）要求：
- install 窗口 / 选举归属 / 快照字节 / VmHWM 这些「证据生产者」必须有解析单测；
- 「没走快照」「install 超预算」「快照太小」「churn 未覆盖窗口」「kill 没换人」必须判失败
  （负向用例），否则一个日志追平的绿跑就能冒充 C4。

运行： python3 -m unittest scripts/stress/test_leader_storm.py -v
"""

import os
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import leader_storm as ls  # noqa: E402


def write_log(lines):
    fd, path = tempfile.mkstemp(suffix=".log")
    with os.fdopen(fd, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path


def ts(sec, frac=0):
    """ISO 时间戳（故意跨秒，验证配对而不是取首尾）。"""
    return "2026-10-28T09:%02d:%02d.%06dZ" % (sec // 60, sec % 60, frac)


def args(**kw):
    base = dict(
        leader_kills=5,
        max_install_seconds=30.0,
        min_snapshot_bytes=100_000_000,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def stats(**kw):
    base = dict(
        leader_kills=0,
        leader_kill_targets_verified=0,
        leader_kill_target_not_leader=0,
        observed_leader_changes=0,
        leader_change_missing=0,
    )
    base.update(kw)
    return SimpleNamespace(**base)


class InstallWindowTest(unittest.TestCase):
    def test_complete_windows_take_last_pair(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(1),
            "%s INFO openraft: install full snapshot, meta: x" % ts(2),
            "%s INFO openraft: Done install complete snapshot, meta: x" % ts(4),
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(10),
            "%s INFO openraft: install full snapshot, meta: y" % ts(11, 500000),
            "%s INFO openraft: Done install complete snapshot, meta: y" % ts(13),
        ])
        node = SimpleNamespace(log_path=path)
        st = ls.install_window_stats(node)
        self.assertEqual(st["windows"], 2)
        # 最后一对：transfer 10s → done 13s，其中 transfer 段 1.5s、apply 段 1.5s
        self.assertAlmostEqual(st["end_to_end_seconds"], 3.0, places=3)
        self.assertAlmostEqual(st["transfer_seconds"], 1.5, places=3)
        self.assertAlmostEqual(st["apply_seconds"], 1.5, places=3)

    def test_unclosed_window_reports_no_windows(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(1),
            "%s INFO openraft: install full snapshot, meta: x" % ts(2),
        ])
        st = ls.install_window_stats(SimpleNamespace(log_path=path))
        self.assertEqual(st["windows"], 0)
        self.assertIsNone(st["end_to_end_seconds"])

    def test_missing_markers_are_none(self):
        path = write_log(["no snapshot here"])
        st = ls.install_window_stats(SimpleNamespace(log_path=path))
        self.assertIsNone(st["end_to_end_seconds"])
        self.assertEqual(st["windows"], 0)


class ElectionAttributionTest(unittest.TestCase):
    def test_election_inside_window_counts(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(1),
            "%s INFO openraft::core::raft_core: trigger election" % ts(2),
            "%s INFO openraft: Done install complete snapshot, meta: x" % ts(3),
        ])
        a = ls.election_attribution(path, 0)
        self.assertEqual(a["during"], 1)
        self.assertEqual(a["since_restart"], 1)
        self.assertEqual(a["pre_votes"], 0)

    def test_election_before_window_not_counted(self):
        path = write_log([
            "%s INFO openraft::core::raft_core: trigger election" % ts(1),
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(5),
            "%s INFO openraft: Done install complete snapshot, meta: x" % ts(6),
        ])
        a = ls.election_attribution(path, 0)
        self.assertEqual(a["during"], 0, "窗口外的选举只做诊断，不判失败")
        self.assertEqual(a["since_restart"], 1)

    def test_pre_vote_inside_window_counts(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(1),
            "%s INFO openraft::core::raft_core: trigger pre-vote" % ts(1, 500000),
            "%s INFO openraft: Done install complete snapshot, meta: x" % ts(3),
        ])
        a = ls.election_attribution(path, 0)
        self.assertEqual(a["during"], 1, "enable_pre_vote=true 时只 pre-vote 也必须计入")
        self.assertEqual(a["pre_votes"], 1)

    def test_unclosed_window_is_conservative(self):
        path = write_log([
            "%s INFO dsh_raft::snapshot: full-snapshot transfer start" % ts(1),
            "%s INFO openraft::core::raft_core: trigger election" % ts(2),
        ])
        a = ls.election_attribution(path, 0)
        self.assertEqual(a["unclosed"], 1)
        self.assertEqual(a["during"], 1, "未闭合窗口（安装被打断）必须保守计入")


class SnapshotBytesTest(unittest.TestCase):
    def test_picks_largest_snap(self):
        with tempfile.TemporaryDirectory() as d:
            snap = os.path.join(d, "snapshots")
            os.makedirs(snap)
            for name, size in [("a.snap", 10), ("b.snap", 40), ("x.part", 999)]:
                with open(os.path.join(snap, name), "wb") as f:
                    f.write(b"x" * size)
            got = ls.latest_snapshot_bytes(SimpleNamespace(data_dir=d))
            self.assertEqual(got, 40, "只认 *.snap，且取最大（.part 不算）")

    def test_missing_dir_is_none(self):
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(ls.latest_snapshot_bytes(SimpleNamespace(data_dir=d)))


def good_snapshot_info():
    return {
        "victim_id": 3,
        "install_windows": 1,
        "snapshot_install_seconds": 1.5,
        "snapshot_transfer_seconds": 0.7,
        "snapshot_apply_seconds": 0.8,
        "snapshot_bytes": 250_000_000,
    }


class C4AssertionsTest(unittest.TestCase):
    def test_good_case_passes(self):
        errs = ls.c4_errors(args(), good_snapshot_info(), True)
        self.assertEqual(errs, [])

    def test_no_snapshot_fails(self):
        info = good_snapshot_info()
        info["install_windows"] = 0
        errs = ls.c4_errors(args(), info, True)
        self.assertTrue(any("install_windows=0" in e for e in errs), errs)

    def test_over_budget_fails(self):
        info = good_snapshot_info()
        info["snapshot_install_seconds"] = 31.0
        errs = ls.c4_errors(args(), info, True)
        self.assertTrue(any("超过预算" in e for e in errs), errs)

    def test_too_small_snapshot_fails(self):
        info = good_snapshot_info()
        info["snapshot_bytes"] = 10
        errs = ls.c4_errors(args(), info, True)
        self.assertTrue(any("下限" in e for e in errs), errs)

    def test_learner_victim_fails(self):
        errs = ls.c4_errors(args(), good_snapshot_info(), False)
        self.assertTrue(any("不是 voter" in e for e in errs), errs)

    def test_missing_measurement_fails(self):
        info = good_snapshot_info()
        info["snapshot_install_seconds"] = None
        errs = ls.c4_errors(args(), info, True)
        self.assertTrue(any("无法量测" in e for e in errs), errs)


class C3AssertionsTest(unittest.TestCase):
    def test_good_case_passes(self):
        errs = ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=5,
                                         observed_leader_changes=5))
        self.assertEqual(errs, [])

    def test_no_kill_fails_when_requested(self):
        errs = ls.c3_errors(args(), stats(leader_kill_target_not_leader=9))
        self.assertTrue(any("一次都没杀到" in e for e in errs), errs)

    def test_unverified_target_fails(self):
        errs = ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=3,
                                         observed_leader_changes=5))
        self.assertTrue(any("确认目标是 leader" in e for e in errs), errs)

    def test_missing_leader_change_fails(self):
        errs = ls.c3_errors(args(), stats(leader_kills=5, leader_kill_targets_verified=5,
                                         observed_leader_changes=1, leader_change_missing=4))
        self.assertTrue(any("leader 变化" in e for e in errs), errs)

    def test_zero_kills_when_not_requested_is_fine(self):
        errs = ls.c3_errors(args(leader_kills=0), stats())
        self.assertEqual(errs, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
