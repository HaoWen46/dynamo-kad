#!/usr/bin/env bash
# experiment.sh -- Comprehensive experiments for dynamo-kad cluster
#
# Collects paper-ready data: latency, throughput, replication, fault tolerance,
# convergence, and metrics.
#
# Output: deploy/results/ directory with timestamped CSV/JSON files.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROTO_DIR="${PROJECT_ROOT}/specs/v1"

# shellcheck source=cluster.conf
source "${SCRIPT_DIR}/cluster.conf"

RESULTS_DIR="${SCRIPT_DIR}/results/$(date +%Y%m%d-%H%M%S)"
mkdir -p "${RESULTS_DIR}"

# ── Helpers ──────────────────────────────────────────────────────────────

log() { echo -e "\033[0;34m[EXP]\033[0m $*"; }
log_ok() { echo -e "\033[0;32m[OK]\033[0m  $*"; }
log_fail() { echo -e "\033[0;31m[FAIL]\033[0m $*"; }

ssh_node() {
    local ip="$1"; shift
    # shellcheck disable=SC2086
    ssh ${SSH_OPTS} "${SSH_USER}@${ip}" "$@"
}

# gRPC call helper.  Returns raw JSON.
grpc_call() {
    local node="$1" proto="$2" service="$3"
    shift 3
    grpcurl -plaintext \
        -import-path "${PROTO_DIR}" \
        -proto "${proto}" \
        "$@" \
        "${node}:${GRPC_PORT}" \
        "${service}" 2>&1
}

# PUT key value node — returns elapsed ms
timed_put() {
    local key="$1" value="$2" node="$3"
    local b64
    b64="$(echo -n "${value}" | base64)"
    local start_ns end_ns
    start_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
    local result
    result=$(grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
        -d "{\"key\":\"${key}\",\"value\":\"${b64}\"}" 2>&1) || true
    end_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    local success="false"
    echo "${result}" | grep -q '"success": true' && success="true"
    echo "${elapsed_ms},${success}"
}

# GET key node — returns elapsed ms
timed_get() {
    local key="$1" node="$2"
    local start_ns end_ns
    start_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
    local result
    result=$(grpc_call "${node}" kv.proto dynamo.kv.KvService/Get \
        -d "{\"key\":\"${key}\"}" 2>&1) || true
    end_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
    local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
    local success="false"
    echo "${result}" | grep -q '"success": true' && success="true"
    echo "${elapsed_ms},${success}"
}

random_node() {
    echo "${NODES[$((RANDOM % ${#NODES[@]}))]}"
}

# ── Experiment 1: Single-key Latency Profiling ──────────────────────────

exp_latency() {
    local n_ops="${1:-100}"
    log "Experiment 1: Latency profiling (${n_ops} PUTs + ${n_ops} GETs)"

    local csv="${RESULTS_DIR}/latency.csv"
    echo "op,key,node,latency_ms,success" > "${csv}"

    # PUTs to random nodes
    for i in $(seq 1 "${n_ops}"); do
        local node
        node=$(random_node)
        local result
        result=$(timed_put "lat-key-${i}" "lat-val-${i}" "${node}")
        echo "PUT,lat-key-${i},${node},${result}" >> "${csv}"
        if (( i % 20 == 0 )); then
            log "  PUT ${i}/${n_ops}"
        fi
    done

    # GETs from random nodes (for the same keys)
    for i in $(seq 1 "${n_ops}"); do
        local node
        node=$(random_node)
        local result
        result=$(timed_get "lat-key-${i}" "${node}")
        echo "GET,lat-key-${i},${node},${result}" >> "${csv}"
        if (( i % 20 == 0 )); then
            log "  GET ${i}/${n_ops}"
        fi
    done

    # Compute stats
    local put_latencies get_latencies
    put_latencies=$(grep "^PUT" "${csv}" | cut -d, -f4)
    get_latencies=$(grep "^GET" "${csv}" | cut -d, -f4)

    local put_stats get_stats
    put_stats=$(echo "${put_latencies}" | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
print(f'min={min(vals)},max={max(vals)},mean={statistics.mean(vals):.1f},median={statistics.median(vals):.1f},p95={sorted(vals)[int(len(vals)*0.95)]},p99={sorted(vals)[int(len(vals)*0.99)]},stdev={statistics.stdev(vals):.1f}')
")
    get_stats=$(echo "${get_latencies}" | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
print(f'min={min(vals)},max={max(vals)},mean={statistics.mean(vals):.1f},median={statistics.median(vals):.1f},p95={sorted(vals)[int(len(vals)*0.95)]},p99={sorted(vals)[int(len(vals)*0.99)]},stdev={statistics.stdev(vals):.1f}')
")

    local put_success get_success
    put_success=$(grep "^PUT" "${csv}" | grep ",true$" | wc -l | tr -d ' ')
    get_success=$(grep "^GET" "${csv}" | grep ",true$" | wc -l | tr -d ' ')

    cat > "${RESULTS_DIR}/latency_summary.txt" <<EOF
Latency Profiling Results
=========================
Operations: ${n_ops} PUTs + ${n_ops} GETs
Quorum: W=${KV_W}, R=${KV_R}, N=${KV_N}

PUT latency (ms): ${put_stats}
PUT success: ${put_success}/${n_ops}

GET latency (ms): ${get_stats}
GET success: ${get_success}/${n_ops}
EOF

    cat "${RESULTS_DIR}/latency_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 2: Throughput ────────────────────────────────────────────

exp_throughput() {
    local n_ops="${1:-200}"
    log "Experiment 2: Throughput (${n_ops} sequential PUTs)"

    local csv="${RESULTS_DIR}/throughput.csv"
    echo "op_num,latency_ms,success,cumulative_ops_per_sec" > "${csv}"

    local global_start
    global_start=$(python3 -c 'import time; print(int(time.time_ns()))')

    for i in $(seq 1 "${n_ops}"); do
        local node
        node=$(random_node)
        local result
        result=$(timed_put "tput-key-${i}" "tput-val-${i}-$(head -c 64 /dev/urandom | base64 | head -c 100)" "${node}")
        local now
        now=$(python3 -c 'import time; print(int(time.time_ns()))')
        local elapsed_total_ms=$(( (now - global_start) / 1000000 ))
        local ops_per_sec
        if [[ ${elapsed_total_ms} -gt 0 ]]; then
            ops_per_sec=$(python3 -c "print(f'{${i} / (${elapsed_total_ms}/1000):.1f}')")
        else
            ops_per_sec="inf"
        fi
        echo "${i},${result},${ops_per_sec}" >> "${csv}"
        if (( i % 50 == 0 )); then
            log "  ${i}/${n_ops}  (${ops_per_sec} ops/sec)"
        fi
    done

    local global_end
    global_end=$(python3 -c 'import time; print(int(time.time_ns()))')
    local total_ms=$(( (global_end - global_start) / 1000000 ))
    local final_ops_sec
    final_ops_sec=$(python3 -c "print(f'{${n_ops} / (${total_ms}/1000):.1f}')")

    local successes
    successes=$(grep ",true," "${csv}" | wc -l | tr -d ' ')

    cat > "${RESULTS_DIR}/throughput_summary.txt" <<EOF
Throughput Results
==================
Total operations: ${n_ops} PUTs
Total time: ${total_ms} ms
Throughput: ${final_ops_sec} ops/sec
Successes: ${successes}/${n_ops}
Quorum: W=${KV_W}, R=${KV_R}, N=${KV_N}
Value size: ~100 bytes
EOF

    cat "${RESULTS_DIR}/throughput_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 3: Replication Verification ──────────────────────────────

exp_replication() {
    local n_keys="${1:-20}"
    log "Experiment 3: Replication verification (${n_keys} keys, read from all ${#NODES[@]} nodes)"

    local csv="${RESULTS_DIR}/replication.csv"
    echo "key,write_node,read_node,latency_ms,found,consistent" > "${csv}"

    for i in $(seq 1 "${n_keys}"); do
        local write_node="${NODES[$((RANDOM % ${#NODES[@]}))]}"
        local key="repl-key-${i}"
        local value="repl-val-${i}-$(date +%s%N)"
        local b64
        b64="$(echo -n "${value}" | base64)"

        # Write to one node
        grpc_call "${write_node}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"${key}\",\"value\":\"${b64}\"}" > /dev/null 2>&1

        # Small delay for replication
        sleep 0.3

        # Read from every node
        for read_node in "${NODES[@]}"; do
            local start_ns end_ns
            start_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
            local result
            result=$(grpc_call "${read_node}" kv.proto dynamo.kv.KvService/Get \
                -d "{\"key\":\"${key}\"}" 2>&1) || true
            end_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
            local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))

            local found="false" consistent="false"
            if echo "${result}" | grep -q '"success": true'; then
                found="true"
                if echo "${result}" | grep -q "${b64}"; then
                    consistent="true"
                fi
            fi
            echo "${key},${write_node},${read_node},${elapsed_ms},${found},${consistent}" >> "${csv}"
        done
        if (( i % 5 == 0 )); then
            log "  ${i}/${n_keys} keys verified"
        fi
    done

    local total_reads found_count consistent_count
    total_reads=$(tail -n +2 "${csv}" | wc -l | tr -d ' ')
    found_count=$(grep ",true," "${csv}" | wc -l | tr -d ' ')
    consistent_count=$(grep ",true,true$" "${csv}" | wc -l | tr -d ' ')

    cat > "${RESULTS_DIR}/replication_summary.txt" <<EOF
Replication Verification Results
================================
Keys written: ${n_keys}
Nodes in cluster: ${#NODES[@]}
Total reads: ${total_reads}
Found (success=true): ${found_count}/${total_reads}
Consistent (correct value): ${consistent_count}/${total_reads}
Replication factor: N=${KV_N}
Read quorum: R=${KV_R}
EOF

    cat "${RESULTS_DIR}/replication_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 4: Fault Tolerance ───────────────────────────────────────

exp_fault_tolerance() {
    log "Experiment 4: Fault tolerance (kill nodes, verify quorum)"

    local csv="${RESULTS_DIR}/fault_tolerance.csv"
    echo "phase,nodes_alive,op,key,node,latency_ms,success" > "${csv}"

    # Phase 0: baseline — write keys with all 10 nodes
    log "  Phase 0: Baseline (10/10 nodes alive)"
    for i in $(seq 1 20); do
        local node
        node=$(random_node)
        local result
        result=$(timed_put "fault-key-${i}" "fault-val-${i}" "${node}")
        echo "baseline,10,PUT,fault-key-${i},${node},${result}" >> "${csv}"
    done
    log "  Baseline writes complete"

    # Phase 1: kill 1 non-seed node (should still work with W=2, N=3)
    local kill1="${NODES[9]}"  # 140.112.30.191
    log "  Phase 1: Killing 1 node (${kill1}), 9/10 alive"
    "${SCRIPT_DIR}/dynamoctl.sh" stop "${kill1}" > /dev/null 2>&1
    sleep 2

    for i in $(seq 1 20); do
        local node
        node="${NODES[$((RANDOM % 9))]}"  # only first 9
        local result
        result=$(timed_put "fault1-key-${i}" "fault1-val-${i}" "${node}")
        echo "kill_1,9,PUT,fault1-key-${i},${node},${result}" >> "${csv}"
    done
    for i in $(seq 1 20); do
        local node
        node="${NODES[$((RANDOM % 9))]}"
        local result
        result=$(timed_get "fault-key-${i}" "${node}")
        echo "kill_1,9,GET,fault-key-${i},${node},${result}" >> "${csv}"
    done

    # Phase 2: kill 2 more non-seed nodes (7/10 alive)
    local kill2="${NODES[8]}"  # 140.112.30.190
    local kill3="${NODES[7]}"  # 140.112.30.189
    log "  Phase 2: Killing 2 more nodes (${kill2}, ${kill3}), 7/10 alive"
    "${SCRIPT_DIR}/dynamoctl.sh" stop "${kill2}" > /dev/null 2>&1
    "${SCRIPT_DIR}/dynamoctl.sh" stop "${kill3}" > /dev/null 2>&1
    sleep 2

    for i in $(seq 1 20); do
        local node
        node="${NODES[$((RANDOM % 7))]}"  # only first 7
        local result
        result=$(timed_put "fault3-key-${i}" "fault3-val-${i}" "${node}")
        echo "kill_3,7,PUT,fault3-key-${i},${node},${result}" >> "${csv}"
    done
    for i in $(seq 1 20); do
        local node
        node="${NODES[$((RANDOM % 7))]}"
        local result
        result=$(timed_get "fault-key-${i}" "${node}")
        echo "kill_3,7,GET,fault-key-${i},${node},${result}" >> "${csv}"
    done

    # Phase 3: restore all nodes
    log "  Phase 3: Restoring killed nodes"
    "${SCRIPT_DIR}/dynamoctl.sh" start "${kill1}" > /dev/null 2>&1
    "${SCRIPT_DIR}/dynamoctl.sh" start "${kill2}" > /dev/null 2>&1
    "${SCRIPT_DIR}/dynamoctl.sh" start "${kill3}" > /dev/null 2>&1
    sleep 5

    for i in $(seq 1 20); do
        local node
        node=$(random_node)
        local result
        result=$(timed_get "fault-key-${i}" "${node}")
        echo "restored,10,GET,fault-key-${i},${node},${result}" >> "${csv}"
    done

    # Summarize
    local baseline_put kill1_put kill3_put
    baseline_put=$(grep "^baseline.*PUT.*,true$" "${csv}" | wc -l | tr -d ' ')
    kill1_put=$(grep "^kill_1.*PUT.*,true$" "${csv}" | wc -l | tr -d ' ')
    kill3_put=$(grep "^kill_3.*PUT.*,true$" "${csv}" | wc -l | tr -d ' ')
    local kill1_get kill3_get restored_get
    kill1_get=$(grep "^kill_1.*GET.*,true$" "${csv}" | wc -l | tr -d ' ')
    kill3_get=$(grep "^kill_3.*GET.*,true$" "${csv}" | wc -l | tr -d ' ')
    restored_get=$(grep "^restored.*GET.*,true$" "${csv}" | wc -l | tr -d ' ')

    cat > "${RESULTS_DIR}/fault_tolerance_summary.txt" <<EOF
Fault Tolerance Results
=======================
Quorum: W=${KV_W}, R=${KV_R}, N=${KV_N}

Phase                   Nodes Alive   PUT success   GET success
Baseline (all up)       10/10         ${baseline_put}/20         -
1 node killed           9/10          ${kill1_put}/20         ${kill1_get}/20
3 nodes killed          7/10          ${kill3_put}/20         ${kill3_get}/20
All restored            10/10         -              ${restored_get}/20
EOF

    cat "${RESULTS_DIR}/fault_tolerance_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 5: Cluster Convergence ───────────────────────────────────

exp_convergence() {
    log "Experiment 5: Cluster convergence (routing table completeness)"

    local csv="${RESULTS_DIR}/convergence.csv"
    echo "node,known_peers,expected_peers,complete" > "${csv}"

    local expected=$(( ${#NODES[@]} - 1 ))

    for ip in "${NODES[@]}"; do
        local result
        result=$(grpc_call "${ip}" admin.proto dynamo.admin.AdminService/ClusterView 2>&1) || true

        # Count unique addresses (filter out duplicates from seed placeholders)
        local known
        known=$(echo "${result}" | grep '"address"' | sort -u | wc -l | tr -d ' ')

        local complete="false"
        [[ ${known} -ge ${expected} ]] && complete="true"

        echo "${ip},${known},${expected},${complete}" >> "${csv}"
    done

    local fully_converged
    fully_converged=$(grep ",true$" "${csv}" | wc -l | tr -d ' ')

    cat > "${RESULTS_DIR}/convergence_summary.txt" <<EOF
Cluster Convergence Results
===========================
Total nodes: ${#NODES[@]}
Expected peers per node: ${expected}
Fully converged nodes: ${fully_converged}/${#NODES[@]}

$(cat "${csv}")
EOF

    cat "${RESULTS_DIR}/convergence_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 6: Metrics Snapshot ──────────────────────────────────────

exp_metrics() {
    log "Experiment 6: Metrics collection from all nodes"

    local csv="${RESULTS_DIR}/metrics.csv"
    echo "node,rpcs_sent,rpcs_received,kv_puts,kv_gets,kv_deletes,hints_stored,hints_delivered,read_repairs" > "${csv}"

    for ip in "${NODES[@]}"; do
        local metrics
        metrics=$(curl -s "http://${ip}:${METRICS_PORT}/metrics" 2>/dev/null) || true

        local rpcs_sent rpcs_recv kv_puts kv_gets kv_deletes hints_stored hints_delivered read_repairs
        rpcs_sent=$(echo "${metrics}" | grep "^dynamo_rpcs_sent_total " | awk '{print $2}' || echo 0)
        rpcs_recv=$(echo "${metrics}" | grep "^dynamo_rpcs_received_total " | awk '{print $2}' || echo 0)
        kv_puts=$(echo "${metrics}" | grep "^dynamo_kv_puts_total " | awk '{print $2}' || echo 0)
        kv_gets=$(echo "${metrics}" | grep "^dynamo_kv_gets_total " | awk '{print $2}' || echo 0)
        kv_deletes=$(echo "${metrics}" | grep "^dynamo_kv_deletes_total " | awk '{print $2}' || echo 0)
        hints_stored=$(echo "${metrics}" | grep "^dynamo_hints_stored_total " | awk '{print $2}' || echo 0)
        hints_delivered=$(echo "${metrics}" | grep "^dynamo_hints_delivered_total " | awk '{print $2}' || echo 0)
        read_repairs=$(echo "${metrics}" | grep "^dynamo_read_repairs_total " | awk '{print $2}' || echo 0)

        echo "${ip},${rpcs_sent},${rpcs_recv},${kv_puts},${kv_gets},${kv_deletes},${hints_stored},${hints_delivered},${read_repairs}" >> "${csv}"

        # Also save full prometheus dump per node
        echo "${metrics}" > "${RESULTS_DIR}/prometheus_${ip##*.}.txt"
    done

    # Also collect stats RPC
    local stats_csv="${RESULTS_DIR}/stats.csv"
    echo "node,routing_table_size,record_count,total_rpcs_sent,total_rpcs_received" > "${stats_csv}"

    for ip in "${NODES[@]}"; do
        local result
        result=$(grpc_call "${ip}" admin.proto dynamo.admin.AdminService/GetStats 2>&1) || true

        local rt_size records rpc_sent rpc_recv
        rt_size=$(echo "${result}" | grep -oE '"routingTableSize": "[0-9]+"' | grep -oE '[0-9]+' || echo 0)
        records=$(echo "${result}" | grep -oE '"recordCount": "[0-9]+"' | grep -oE '[0-9]+' || echo 0)
        rpc_sent=$(echo "${result}" | grep -oE '"totalRpcsSent": "[0-9]+"' | grep -oE '[0-9]+' || echo 0)
        rpc_recv=$(echo "${result}" | grep -oE '"totalRpcsReceived": "[0-9]+"' | grep -oE '[0-9]+' || echo 0)

        echo "${ip},${rt_size},${records},${rpc_sent},${rpc_recv}" >> "${stats_csv}"
    done

    log_ok "Metrics in ${csv}, stats in ${stats_csv}"
    log_ok "Full prometheus dumps in ${RESULTS_DIR}/prometheus_*.txt"
}

# ── Experiment 7: Per-node Latency (write to specific, read from specific) ──

exp_per_node_latency() {
    local n_ops="${1:-10}"
    log "Experiment 7: Per-node latency matrix (${n_ops} GETs per node pair)"

    # First write a set of keys
    local write_node="${NODES[0]}"
    for i in $(seq 1 "${n_ops}"); do
        local b64
        b64="$(echo -n "matrix-val-${i}" | base64)"
        grpc_call "${write_node}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"matrix-key-${i}\",\"value\":\"${b64}\"}" > /dev/null 2>&1
    done
    sleep 1

    local csv="${RESULTS_DIR}/per_node_latency.csv"
    echo "read_node,op_num,latency_ms,success" > "${csv}"

    for read_node in "${NODES[@]}"; do
        for i in $(seq 1 "${n_ops}"); do
            local result
            result=$(timed_get "matrix-key-${i}" "${read_node}")
            echo "${read_node},${i},${result}" >> "${csv}"
        done
        local avg
        avg=$(grep "^${read_node}," "${csv}" | cut -d, -f3 | python3 -c "
import sys
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
print(f'{sum(vals)/len(vals):.1f}')
" 2>/dev/null || echo "?")
        log "  ${read_node}: avg ${avg}ms"
    done

    log_ok "Results in ${csv}"
}

# ── Main dispatch ────────────────────────────────────────────────────────

run_all() {
    log "=========================================="
    log "dynamo-kad Experiment Suite"
    log "Cluster: ${#NODES[@]} nodes"
    log "Config: N=${KV_N} R=${KV_R} W=${KV_W}"
    log "Results: ${RESULTS_DIR}"
    log "=========================================="
    echo ""

    exp_convergence
    echo ""
    exp_latency "${1:-100}"
    echo ""
    exp_per_node_latency 10
    echo ""
    exp_throughput "${2:-200}"
    echo ""
    exp_replication 20
    echo ""
    exp_fault_tolerance
    echo ""
    exp_metrics
    echo ""

    log "=========================================="
    log "All experiments complete!"
    log "Results directory: ${RESULTS_DIR}"
    log "=========================================="
}

case "${1:-all}" in
    all)              run_all "${2:-100}" "${3:-200}" ;;
    latency)          exp_latency "${2:-100}" ;;
    throughput)       exp_throughput "${2:-200}" ;;
    replication)      exp_replication "${2:-20}" ;;
    fault)            exp_fault_tolerance ;;
    convergence)      exp_convergence ;;
    metrics)          exp_metrics ;;
    per-node)         exp_per_node_latency "${2:-10}" ;;
    help|--help|-h)
        cat <<EOF
experiment.sh — Experiment suite for dynamo-kad

COMMANDS
  all [n_lat] [n_tput]   Run all experiments (default: 100 lat, 200 tput)
  latency [n]            Latency profiling (default: 100 ops)
  throughput [n]          Throughput measurement (default: 200 ops)
  replication [n]         Replication verification (default: 20 keys)
  fault                   Fault tolerance (kills/restores nodes)
  convergence             Routing table convergence check
  metrics                 Collect Prometheus metrics from all nodes
  per-node [n]            Per-node latency matrix (default: 10 per node)
EOF
        ;;
    *)
        echo "Unknown: $1"; exit 1 ;;
esac
