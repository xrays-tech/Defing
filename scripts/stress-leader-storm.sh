#!/usr/bin/env bash
# Thin wrapper for the leader-storm cluster stress harness (scripts/stress/leader_storm.py).
# Checks/ builds the defing binary, then exec's the Python harness. Dynamic ports + temp
# data-dirs are handled inside Python (socket bind 0) — no fixed ports, no fixed tmp dirs
# (avoids collisions with chaos-test.sh / bench.sh / cluster-demo.sh).
#
# Usage: scripts/stress-leader-storm.sh [--profile quick|storm|extreme] [--bin PATH] [--keep] [extra flags...]
set -u

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PY="$HERE/stress/leader_storm.py"
DEFAULT_BIN="$ROOT/server/target/debug/defing"

if [ ! -f "$PY" ]; then
  echo "FAIL: harness not found: $PY" >&2
  exit 2
fi

# resolve --bin (so we can build it if missing); all args forwarded verbatim to Python
BIN=""
prev=""
for a in "$@"; do
  if [ "$prev" = "--bin" ]; then BIN="$a"; fi
  prev="$a"
done
[ -z "$BIN" ] && BIN="$DEFAULT_BIN"

if [ ! -x "$BIN" ]; then
  echo "binary not executable ($BIN) — building defing (debug) ..."
  if ! ( cd "$ROOT/server" && cargo build --bin defing ); then
    echo "FAIL: cargo build --bin defing failed" >&2
    exit 2
  fi
  BIN="$DEFAULT_BIN"
fi

exec python3 "$PY" "$@"
