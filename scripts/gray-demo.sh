#!/usr/bin/env bash
# G3 验收演示：灰度数据面三路解析 + watch 灰度事件（design/g3-dataplane.md）
# 流程：建项目→结构→稳定发布 v2→灰度发布→HTTP 三路身份解析→watch promote 补发事件→abort 回落
set -euo pipefail
BIN=${BIN:-/home/alex/Projects/Defing/server/target/debug/defing}
PORT=${PORT:-8385}
BASE=${BASE:-http://127.0.0.1:$PORT}

cleanup() { [ -n "${PID:-}" ] && kill $PID 2>/dev/null || true; }
trap cleanup EXIT

echo "== 启动 defing --dev-single =="
$BIN --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:$PORT >/tmp/dsh-gray.log 2>&1 &
PID=$!
for i in $(seq 1 20); do
  curl -sf $BASE/healthz >/dev/null && break
  sleep 0.5
done
curl -sf $BASE/healthz >/dev/null && echo "  healthz OK" || { echo "  healthz FAIL"; cat /tmp/dsh-gray.log; exit 1; }
TOKEN=$(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' -d '{"password":"admin123"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
AUTH="Authorization: Bearer $TOKEN"
# 数据面端点（/v1/...，含 snapshot/watch）需数据面 token（数据面 token 化后，管理会话不适用）
DEV_TOKEN=$(sed -n 's/.*开发数据面 token = \([a-f0-9]*\).*/\1/p' /tmp/dsh-gray.log)
[ -n "$DEV_TOKEN" ] || { echo "  dev token 未打印"; cat /tmp/dsh-gray.log; exit 1; }
DP_AUTH="Authorization: Bearer $DEV_TOKEN"
echo "  admin login ok"

echo "== 1. 创建项目 + 结构（redis.host 必填）=="
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects -H 'Content-Type: application/json' -d '{"name":"order-service"}' >/dev/null
curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/structure-draft -H 'Content-Type: application/json' -d '{
  "base_version": 1,
  "groups": [{"name":"redis","items":[{"key":"host","type":"string","required":true},{"key":"port","type":"int"}]}]
}' >/dev/null
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/structure-draft/publish -H 'Content-Type: application/json' -d '{"comment":"init","request_id":"s-1"}' >/dev/null
echo "  structure published (v1)"

echo "== 2. 稳定发布 dev（host=stable-host）=="
curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -H 'Content-Type: application/json' -d '{
  "updates":[{"group":"redis","key":"host","value":{"type":"string","str_value":"stable-host"}}]
}' >/dev/null
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -H 'Content-Type: application/json' -d '{"comment":"stable v2","request_id":"p-1"}' >/dev/null
echo "  stable v2 published"

echo "== 3. 编辑灰度草稿（host=gray-host）+ 灰度发布（规则 zone=cn-north-1）=="
curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -H 'Content-Type: application/json' -d '{
  "updates":[{"group":"redis","key":"host","value":{"type":"string","str_value":"gray-host"}}]
}' >/dev/null
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/gray-publish -H 'Content-Type: application/json' -d '{
  "rule":{"match_labels":[{"key":"zone","value":"cn-north-1"}]},
  "comment":"先给华北","request_id":"g-1"
}' | python3 -m json.tool
curl -sf -H "$AUTH" $BASE/api/v1/projects/order-service/branches/dev/gray-status | python3 -m json.tool

echo "== 4. 数据面三路解析（HTTP snapshot 带身份头）=="
echo "--- 华北（X-Dsh-Labels: zone=cn-north-1）→ 应返回 gray-host + gray=true:"
curl -sf -H "$DP_AUTH" $BASE/v1/projects/order-service/branches/dev/snapshot \
  -H 'X-Dsh-Instance: web-1' -H 'X-Dsh-Labels: zone=cn-north-1,svc=checkout' | tee /tmp/gray-north.json
echo
grep -q '"gray-host"' /tmp/gray-north.json && grep -q '"gray":true' /tmp/gray-north.json \
  && echo "  华北命中灰度 ✅" || { echo "  FAIL: 华北应读灰度"; exit 1; }

echo "--- 华南（zone=cn-south-1）→ 应返回 stable-host + gray=false:"
curl -sf -H "$DP_AUTH" $BASE/v1/projects/order-service/branches/dev/snapshot \
  -H 'X-Dsh-Instance: web-2' -H 'X-Dsh-Labels: zone=cn-south-1' | tee /tmp/gray-south.json
echo
grep -q '"stable-host"' /tmp/gray-south.json && grep -q '"gray":false' /tmp/gray-south.json \
  && echo "  华南未命中 ✅" || { echo "  FAIL: 华南应读稳定"; exit 1; }

echo "--- 无身份（旧客户端）→ 应返回 stable-host + gray=false（Q2）:"
curl -sf -H "$DP_AUTH" $BASE/v1/projects/order-service/branches/dev/snapshot | tee /tmp/gray-noid.json
echo
grep -q '"stable-host"' /tmp/gray-noid.json && grep -q '"gray":false' /tmp/gray-noid.json \
  && echo "  无身份不进灰度 ✅" || { echo "  FAIL: 无身份应读稳定"; exit 1; }

echo "== 5. watch：灰度转正补发事件（gray=true，version ≤ 客户端 last 仍投递，Q4）=="
curl -sN -H "$DP_AUTH" $BASE/v1/projects/order-service/branches/dev/watch >/tmp/gray-watch.out 2>/dev/null &
WPID=$!
sleep 0.5
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/gray-promote -H 'Content-Type: application/json' -d '{"comment":"全量","request_id":"pr-1"}' >/dev/null
sleep 1
kill $WPID 2>/dev/null || true
grep -q '"gray":true' /tmp/gray-watch.out && echo "  promote 补发事件 gray=true 已投递 ✅" \
  || { echo "  FAIL: promote 事件未收到"; cat /tmp/gray-watch.out; exit 1; }

echo "== 6. 转正后全量客户端读新稳定版（gray=false）=="
curl -sf -H "$AUTH" $BASE/api/v1/projects/order-service/branches/dev/gray-status | python3 -m json.tool
curl -sf -H "$DP_AUTH" $BASE/v1/projects/order-service/branches/dev/snapshot -H 'X-Dsh-Instance: web-1' -H 'X-Dsh-Labels: zone=cn-north-1' | tee /tmp/gray-promoted.json
echo
grep -q '"gray-host"' /tmp/gray-promoted.json && grep -q '"gray":false' /tmp/gray-promoted.json \
  && echo "  转正后全量读到 gray-host（灰度内容成为稳定版）✅" || { echo "  FAIL"; exit 1; }

echo
echo "ALL GRAY DEMO OK"
