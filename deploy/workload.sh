#!/usr/bin/env bash
# workload.sh -- Simple workload driver for dynamo-kad cluster
#
# Usage: workload.sh <command> [args...]
#
# Requires: grpcurl, base64
# Sends gRPC requests to random cluster nodes via grpcurl.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROTO_DIR="${PROJECT_ROOT}/specs/v1"

# shellcheck source=cluster.conf
source "${SCRIPT_DIR}/cluster.conf"

# ── Helpers ──────────────────────────────────────────────────────────────

random_node() {
    echo "${NODES[$((RANDOM % ${#NODES[@]}))]}"
}

grpc() {
    local node="$1" service="$2"
    shift 2
    grpcurl -plaintext \
        -import-path "${PROTO_DIR}" \
        -proto kv.proto -proto common.proto \
        "$@" \
        "${node}:${GRPC_PORT}" \
        "${service}"
}

# PUT key value [node]
kv_put() {
    local key="$1" value="$2" node="${3:-$(random_node)}"
    local b64
    b64="$(echo -n "${value}" | base64)"
    echo "PUT  ${key} = ${value}  → ${node}"
    grpc "${node}" dynamo.kv.KvService/Put \
        -d "{\"key\":\"${key}\",\"value\":\"${b64}\"}"
}

# GET key [node]
kv_get() {
    local key="$1" node="${2:-$(random_node)}"
    echo "GET  ${key}  → ${node}"
    grpc "${node}" dynamo.kv.KvService/Get \
        -d "{\"key\":\"${key}\"}"
}

# DELETE key [node]
kv_delete() {
    local key="$1" node="${2:-$(random_node)}"
    echo "DEL  ${key}  → ${node}"
    grpc "${node}" dynamo.kv.KvService/Delete \
        -d "{\"key\":\"${key}\"}"
}

# ── Workloads ────────────────────────────────────────────────────────────

# Basic: single PUT then GET
test_basic() {
    echo "=== Basic PUT / GET ==="
    local key="test-$(date +%s)"
    local value="hello-dynamo-$(date +%s)"

    kv_put "${key}" "${value}"
    echo ""
    sleep 1
    kv_get "${key}"
}

# Cross-node: write to one node, read from a different one
test_cross_node() {
    echo "=== Cross-node replication test ==="
    local key="cross-$(date +%s)"
    local value="replicated-$(date +%s)"
    local write_node="${NODES[0]}"
    local read_node="${NODES[${#NODES[@]}-1]}"

    kv_put "${key}" "${value}" "${write_node}"
    echo ""
    echo "Waiting 2s for replication..."
    sleep 2
    kv_get "${key}" "${read_node}"
}

# Sequential: write N keys, then read them all back
test_sequential() {
    local count="${1:-20}"
    echo "=== Sequential workload: ${count} keys ==="

    local start_ns end_ns
    start_ns="$(date +%s%N 2>/dev/null || date +%s)"

    local ok=0 fail=0
    for i in $(seq 1 "${count}"); do
        if kv_put "seq-${i}" "val-${i}" > /dev/null 2>&1; then
            ((ok++)) || true
        else
            ((fail++)) || true
        fi
        if (( i % 10 == 0 )); then
            echo "  wrote ${i}/${count}..."
        fi
    done

    end_ns="$(date +%s%N 2>/dev/null || date +%s)"
    echo "Writes done.  OK: ${ok}  FAIL: ${fail}"

    echo "Verifying reads..."
    local read_ok=0 read_fail=0
    for i in $(seq 1 "${count}"); do
        local result
        result="$(kv_get "seq-${i}" 2>&1)" || true
        if echo "${result}" | grep -q '"success": true'; then
            ((read_ok++)) || true
        else
            ((read_fail++)) || true
        fi
    done
    echo "Read verification:  OK: ${read_ok}  FAIL: ${read_fail}  Total: ${count}"
}

# Mixed: interleaved PUT + GET
test_mixed() {
    local count="${1:-20}"
    echo "=== Mixed workload: ${count} operations ==="

    for i in $(seq 1 "${count}"); do
        local key="mix-$((RANDOM % (count / 2 + 1)))"
        if (( RANDOM % 3 == 0 )); then
            kv_get "${key}" > /dev/null 2>&1 && echo "  GET  ${key} OK" || echo "  GET  ${key} (not found or fail)"
        else
            kv_put "${key}" "v${i}" > /dev/null 2>&1 && echo "  PUT  ${key}=v${i} OK" || echo "  PUT  ${key} FAIL"
        fi
    done
}

# ── Dispatch ─────────────────────────────────────────────────────────────
case "${1:-help}" in
    basic)       test_basic ;;
    cross)       test_cross_node ;;
    sequential)  test_sequential "${2:-20}" ;;
    mixed)       test_mixed "${2:-20}" ;;
    put)         kv_put  "${2:?key}" "${3:?value}" "${4:-}" ;;
    get)         kv_get  "${2:?key}" "${3:-}" ;;
    delete)      kv_delete "${2:?key}" "${3:-}" ;;
    help|--help|-h)
        cat <<EOF
workload.sh — workload driver for dynamo-kad

COMMANDS
  basic                 Single PUT then GET
  cross                 Write to node 0, read from last node
  sequential [count]    Write N keys, read all back (default 20)
  mixed [count]         Interleaved random PUT/GET (default 20)
  put <key> <value>     Manual PUT
  get <key>             Manual GET
  delete <key>          Manual DELETE

EXAMPLES
  ./workload.sh basic
  ./workload.sh sequential 100
  ./workload.sh put hello world
  ./workload.sh get hello
EOF
        ;;
    *)
        echo "Unknown command: $1"; exit 1 ;;
esac
