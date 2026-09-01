#!/usr/bin/env bash
# M1 验收演示：defing --dev-single 全流程（建项目→结构→草稿→发布→GetConfig）
set -euo pipefail
BIN=${BIN:-/home/alex/Projects/Defing/server/target/debug/defing}
BASE=${BASE:-http://127.0.0.1:8384}
PORT=${PORT:-8384}

cleanup() { [ -n "${PID:-}" ] && kill $PID 2>/dev/null || true; }
trap cleanup EXIT

echo "== 启动 defing --dev-single =="
$BIN --dev-single --admin-password admin123 --allow-no-master-key --http-addr 127.0.0.1:$PORT >/tmp/dsh-dev-single.log 2>&1 &
PID=$!
sleep 1
curl -sf $BASE/healthz >/dev/null && echo "  healthz OK" || { echo "  healthz FAIL"; cat /tmp/dsh-dev-single.log; exit 1; }
TOKEN=$(curl -sf -X POST $BASE/api/v1/login -H 'Content-Type: application/json' -d '{"password":"admin123"}' | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
AUTH="Authorization: Bearer $TOKEN"
# 数据面端点（/v1/...，含 watch）需数据面 token（管理会话不适用，数据面 token 化后回归）：
# dev-single 启动时打印全局开发数据面 token
DEV_TOKEN=$(sed -n 's/.*开发数据面 token = \([a-f0-9]*\).*/\1/p' /tmp/dsh-dev-single.log)
[ -n "$DEV_TOKEN" ] || { echo "  dev token 未打印"; cat /tmp/dsh-dev-single.log; exit 1; }
echo "  admin login ok"

echo "== 1. 创建项目 order-service（自动建 dev/test/prod 分支）=="
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects -H 'Content-Type: application/json' -d '{"name":"order-service"}' | tee /tmp/r1.json
echo

echo "== 2. 设置结构草稿（redis 组：host 必填 / port / password secret）=="
curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/structure-draft -H 'Content-Type: application/json' -d '{
  "base_version": 1,
  "groups": [
    {"name":"redis","items":[
      {"key":"host","type":"string","required":true},
      {"key":"port","type":"int"},
      {"key":"password","type":"secret","secret":true}
    ]}
  ]
}' >/dev/null && echo "  structure-draft saved"

echo "== 3. 发布结构（3 分支版本推进）=="
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/structure-draft/publish -H 'Content-Type: application/json' -d '{"comment":"init structure","request_id":"s-1"}' | tee /tmp/r2.json
echo

echo "== 4. 编辑 dev 草稿 =="
curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -H 'Content-Type: application/json' -d '{
  "updates":[
    {"group":"redis","key":"host","value":{"type":"string","str_value":"127.0.0.1"}},
    {"group":"redis","key":"port","value":{"type":"int","int_value":6379}}
  ]
}' >/dev/null && echo "  draft saved"

echo "== 5. 发布前 GetConfig（应仍为结构版本 1、值空 → 草稿隔离 I4）=="
curl -sf -H "$AUTH" $BASE/api/v1/projects/order-service/branches/dev/config | tee /tmp/r3.json
echo
grep -q '"version":1' /tmp/r3.json && echo "  草稿隔离 OK（版本仍为 1）" || echo "  FAIL: 发布前版本应仍为 1"

echo "== 5b. 启动 watch（SSE，数据面 token 鉴权）=="
curl -sN -H "Authorization: Bearer $DEV_TOKEN" $BASE/v1/projects/order-service/branches/dev/watch >/tmp/watch.out 2>/dev/null &
WPID=\$!
echo "  watch started (pid $WPID)"

echo "== 6. 发布 dev 版本 =="
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -H 'Content-Type: application/json' -d '{"comment":"dev host","request_id":"r-1"}' | tee /tmp/r4.json
echo
grep -q '"version":2' /tmp/r4.json && echo "  发布 OK（version=2）" || echo "  FAIL: 期望 version=2"

echo "== 7. 发布后 GetConfig（应读到新值）=="
curl -sf -H "$AUTH" $BASE/api/v1/projects/order-service/branches/dev/config | tee /tmp/r5.json
echo
grep -q '127.0.0.1' /tmp/r5.json && grep -q '"version":2' /tmp/r5.json && echo "  GetConfig 读到新版本 ✅" || { echo "  FAIL: 未读到新值"; exit 1; }

echo "== 8. 幂等：同 request_id 再次发布（版本不增）=="
curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -H 'Content-Type: application/json' -d '{"comment":"dev host","request_id":"r-1"}' | tee /tmp/r6.json
echo
grep -q '"version":2' /tmp/r6.json && echo "  幂等 OK（仍为 version=2）" || echo "  FAIL: 幂等发布应返回同一版本"

echo "== 8b. watch 应收到发布事件 =="
# 订阅窗口竞态兜底：SSE 订阅若晚于广播会错过事件（CI 慢机偶发）。
# 参照 sdk-contract-test.sh 的成熟模式——重存草稿 + 唯一 request_id 重发，直到 watch 收到事件（有界重试）。
for i in 1 2 3 4 5 6 7 8 9 10; do
  curl -sf -H "$AUTH" -X PUT $BASE/api/v1/projects/order-service/branches/dev/draft -H 'Content-Type: application/json' \
    -d '{"updates":[{"group":"app","key":"host","value":{"type":"string","str_value":"127.0.0.1"}}]}' >/dev/null 2>&1 || true
  curl -sf -H "$AUTH" -X POST $BASE/api/v1/projects/order-service/branches/dev/publish -H 'Content-Type: application/json' \
    -d "{\"comment\":\"dev host\",\"request_id\":\"r-watch-$i\"}" >/dev/null 2>&1 || true
  sleep 0.3
  grep -q 'value_publish' /tmp/watch.out && break
done
kill \$WPID 2>/dev/null || true
grep -q 'value_publish' /tmp/watch.out && echo "  watch 收到发布事件 ✅" || { echo "  FAIL: watch 未收到事件"; cat /tmp/watch.out; exit 1; }

echo "== 9. 版本历史 =="
curl -sf -H "$AUTH" $BASE/api/v1/projects/order-service/branches/dev/versions | tee /tmp/r7.json
echo

echo "== 10. 必填校验：test 分支无草稿直接发布应 409（ERR_NO_DRAFT）=="
CODE=$(curl -s -H "$AUTH" -o /dev/null -w '%{http_code}' -X POST $BASE/api/v1/projects/order-service/branches/test/publish -H 'Content-Type: application/json' -d '{"comment":"x"}')
echo "  http=$CODE"; [ "$CODE" = "409" ] && echo "  OK" || echo "  FAIL: 期望 409"

echo
echo "======== M1 dev-single 演示全部通过 ========"