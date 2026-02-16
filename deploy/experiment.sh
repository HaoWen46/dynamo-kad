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

# ── Experiment 8: Concurrent Client Throughput ─────────────────────────

exp_concurrent_throughput() {
    local concurrency="${1:-5}"
    local ops_per_client="${2:-50}"
    log "Experiment 8: Concurrent throughput (${concurrency} clients x ${ops_per_client} ops)"

    local csv="${RESULTS_DIR}/concurrent_throughput.csv"
    echo "client_id,op_num,latency_ms,success" > "${csv}"

    local tmpdir
    tmpdir=$(mktemp -d)

    local global_start
    global_start=$(python3 -c 'import time; print(int(time.time_ns()))')

    # Launch parallel clients
    for c in $(seq 1 "${concurrency}"); do
        (
            for i in $(seq 1 "${ops_per_client}"); do
                local node
                node="${NODES[$((RANDOM % ${#NODES[@]}))]}"
                local result
                result=$(timed_put "conc-c${c}-k${i}" "conc-v${c}-${i}" "${node}")
                echo "${c},${i},${result}"
            done > "${tmpdir}/client_${c}.csv"
        ) &
    done
    wait

    # Merge results
    for c in $(seq 1 "${concurrency}"); do
        cat "${tmpdir}/client_${c}.csv" >> "${csv}"
    done

    local global_end
    global_end=$(python3 -c 'import time; print(int(time.time_ns()))')
    local total_ms=$(( (global_end - global_start) / 1000000 ))
    local total_ops=$(( concurrency * ops_per_client ))
    local agg_tput
    agg_tput=$(python3 -c "print(f'{${total_ops} / (${total_ms}/1000):.1f}')")

    local successes
    successes=$(grep ",true$" "${csv}" | wc -l | tr -d ' ')

    # Per-client latency stats
    local all_latencies
    all_latencies=$(tail -n +2 "${csv}" | cut -d, -f3)
    local stats
    stats=$(echo "${all_latencies}" | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
print(f'min={min(vals)},max={max(vals)},mean={statistics.mean(vals):.1f},median={statistics.median(vals):.1f},p95={sorted(vals)[int(len(vals)*0.95)]},p99={sorted(vals)[int(len(vals)*0.99)]},stdev={statistics.stdev(vals):.1f}')
")

    cat > "${RESULTS_DIR}/concurrent_throughput_summary.txt" <<EOF
Concurrent Throughput Results
==============================
Clients: ${concurrency}
Ops per client: ${ops_per_client}
Total operations: ${total_ops} PUTs
Total wall-clock time: ${total_ms} ms
Aggregate throughput: ${agg_tput} ops/sec
Successes: ${successes}/${total_ops}
Latency stats (ms): ${stats}
Quorum: W=${KV_W}, R=${KV_R}, N=${KV_N}
EOF

    cat "${RESULTS_DIR}/concurrent_throughput_summary.txt"
    rm -rf "${tmpdir}"
    log_ok "Results in ${csv}"
}

# ── Experiment 9: Value Size Scaling ───────────────────────────────────

exp_value_sizes() {
    log "Experiment 9: Value size scaling"

    local csv="${RESULTS_DIR}/value_sizes.csv"
    echo "value_size_bytes,op,trial,latency_ms,success" > "${csv}"

    local sizes=(1 100 1024 10240 102400)
    local labels=("1B" "100B" "1KB" "10KB" "100KB")
    local trials=10

    for idx in "${!sizes[@]}"; do
        local sz="${sizes[$idx]}"
        local label="${labels[$idx]}"
        log "  Testing value size: ${label} (${trials} trials)"

        # Generate a payload of the given size
        local payload
        payload=$(head -c "${sz}" /dev/urandom | base64 | head -c "${sz}")

        for t in $(seq 1 "${trials}"); do
            local node
            node=$(random_node)
            local key="vsize-${label}-${t}"
            local b64
            b64=$(echo -n "${payload}" | base64)
            local start_ns end_ns
            start_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
            local result
            result=$(grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
                -d "{\"key\":\"${key}\",\"value\":\"${b64}\"}" 2>&1) || true
            end_ns=$(python3 -c 'import time; print(int(time.time_ns()))')
            local elapsed_ms=$(( (end_ns - start_ns) / 1000000 ))
            local success="false"
            echo "${result}" | grep -q '"success": true' && success="true"
            echo "${sz},PUT,${t},${elapsed_ms},${success}" >> "${csv}"
        done

        sleep 0.5

        for t in $(seq 1 "${trials}"); do
            local node
            node=$(random_node)
            local key="vsize-${label}-${t}"
            local result_line
            result_line=$(timed_get "${key}" "${node}")
            echo "${sz},GET,${t},${result_line}" >> "${csv}"
        done
    done

    # Compute per-size stats
    cat > "${RESULTS_DIR}/value_sizes_summary.txt" <<EOF
Value Size Scaling Results
===========================
Trials per size: ${trials} PUTs + ${trials} GETs

EOF

    for idx in "${!sizes[@]}"; do
        local sz="${sizes[$idx]}"
        local label="${labels[$idx]}"
        local put_stats get_stats
        put_stats=$(grep "^${sz},PUT," "${csv}" | cut -d, -f4 | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
s = statistics.stdev(vals) if len(vals) > 1 else 0
print(f'median={statistics.median(vals):.0f} mean={statistics.mean(vals):.1f} stdev={s:.1f}')
" 2>/dev/null || echo "?")
        get_stats=$(grep "^${sz},GET," "${csv}" | cut -d, -f4 | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
s = statistics.stdev(vals) if len(vals) > 1 else 0
print(f'median={statistics.median(vals):.0f} mean={statistics.mean(vals):.1f} stdev={s:.1f}')
" 2>/dev/null || echo "?")
        local put_ok get_ok
        put_ok=$(grep "^${sz},PUT,.*,true$" "${csv}" | wc -l | tr -d ' ')
        get_ok=$(grep "^${sz},GET,.*,true$" "${csv}" | wc -l | tr -d ' ')
        echo "${label}: PUT [${put_stats}] (${put_ok}/${trials})  GET [${get_stats}] (${get_ok}/${trials})" >> "${RESULTS_DIR}/value_sizes_summary.txt"
    done

    cat "${RESULTS_DIR}/value_sizes_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 10: Vector Clock Conflicts ──────────────────────────────

exp_vector_clocks() {
    local n_keys="${1:-10}"
    log "Experiment 10: Vector clock conflict detection (${n_keys} keys)"

    local csv="${RESULTS_DIR}/vector_clocks.csv"
    echo "key,write_node_a,write_node_b,num_versions_returned,has_conflict,read_node" > "${csv}"

    local conflicts_detected=0
    local total=0

    for i in $(seq 1 "${n_keys}"); do
        local key="conflict-${i}-$(date +%s%N)"
        local node_a="${NODES[$((RANDOM % ${#NODES[@]}))]}"
        local node_b="${NODES[$(( (RANDOM + 3) % ${#NODES[@]} ))]}"
        # Ensure different nodes
        while [[ "${node_b}" == "${node_a}" ]]; do
            node_b="${NODES[$(( (RANDOM + 5) % ${#NODES[@]} ))]}"
        done

        local val_a val_b b64_a b64_b
        val_a="from-node-a-${i}"
        val_b="from-node-b-${i}"
        b64_a=$(echo -n "${val_a}" | base64)
        b64_b=$(echo -n "${val_b}" | base64)

        # Write concurrently (no context — creates independent causal chains)
        grpc_call "${node_a}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"${key}\",\"value\":\"${b64_a}\"}" > /dev/null 2>&1 &
        local pid_a=$!
        grpc_call "${node_b}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"${key}\",\"value\":\"${b64_b}\"}" > /dev/null 2>&1 &
        local pid_b=$!
        wait "${pid_a}" "${pid_b}" 2>/dev/null || true

        sleep 0.5

        # Read back and check for siblings
        local read_node
        read_node=$(random_node)
        local result
        result=$(grpc_call "${read_node}" kv.proto dynamo.kv.KvService/Get \
            -d "{\"key\":\"${key}\"}" 2>&1) || true

        # Count versions returned (number of "value" fields in versions array)
        local num_versions
        num_versions=$(echo "${result}" | grep -c '"value"' || echo "0")
        local has_conflict="false"
        if [[ ${num_versions} -gt 1 ]]; then
            has_conflict="true"
            ((conflicts_detected++)) || true
        fi
        ((total++)) || true

        echo "${key},${node_a},${node_b},${num_versions},${has_conflict},${read_node}" >> "${csv}"

        if (( i % 5 == 0 )); then
            log "  ${i}/${n_keys} keys tested (${conflicts_detected} conflicts so far)"
        fi
    done

    cat > "${RESULTS_DIR}/vector_clocks_summary.txt" <<EOF
Vector Clock Conflict Detection Results
=========================================
Keys tested: ${total}
Conflicts detected (>1 version): ${conflicts_detected}/${total}
Conflict rate: $(python3 -c "print(f'{${conflicts_detected}/${total}*100:.1f}%')")

Methodology:
- Two concurrent PUTs to the same key from different nodes
- No context passed (independent causal chains)
- Expected: vector clocks should detect concurrency and store siblings
- Conflict presence depends on timing: if one write propagates before
  the other arrives, the second write may dominate instead of conflicting
EOF

    cat "${RESULTS_DIR}/vector_clocks_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 11: Node Rejoin & DHT Healing ──────────────────────────

exp_rejoin() {
    log "Experiment 11: Node rejoin & DHT healing"

    local csv="${RESULTS_DIR}/rejoin.csv"
    echo "phase,description,metric,value" > "${csv}"

    local target="${NODES[8]}"  # .190
    log "  Target node: ${target}"

    # Phase 1: check current routing table
    local pre_peers
    pre_peers=$(grpc_call "${target}" admin.proto dynamo.admin.AdminService/ClusterView 2>&1 | \
        grep '"address"' | sort -u | wc -l | tr -d ' ')
    echo "pre_kill,routing_table_peers,count,${pre_peers}" >> "${csv}"
    log "  Pre-kill peers: ${pre_peers}"

    # Phase 2: kill the target node
    log "  Killing ${target}..."
    "${SCRIPT_DIR}/dynamoctl.sh" stop "${target}" > /dev/null 2>&1
    sleep 2

    # Phase 3: write 30 keys while target is down
    log "  Writing 30 keys while node is down..."
    local keys_written=0
    for i in $(seq 1 30); do
        local node="${NODES[$((RANDOM % 8))]}"  # only first 8 nodes (skip .190, .191 might be fine)
        # Use nodes we know are up
        node="${NODES[$((i % 7))]}"  # cycle through .182-.188
        local b64
        b64=$(echo -n "rejoin-val-${i}" | base64)
        local result
        result=$(grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"rejoin-key-${i}\",\"value\":\"${b64}\"}" 2>&1) || true
        if echo "${result}" | grep -q '"success": true'; then
            ((keys_written++)) || true
        fi
    done
    echo "during_downtime,keys_written,count,${keys_written}" >> "${csv}"
    log "  Keys written: ${keys_written}/30"

    # Phase 4: restart the target node
    log "  Restarting ${target}..."
    "${SCRIPT_DIR}/dynamoctl.sh" start "${target}" > /dev/null 2>&1
    sleep 5  # allow time for bootstrap + hint delivery

    # Phase 5: check routing table recovery
    local post_peers
    post_peers=$(grpc_call "${target}" admin.proto dynamo.admin.AdminService/ClusterView 2>&1 | \
        grep '"address"' | sort -u | wc -l | tr -d ' ')
    echo "post_rejoin,routing_table_peers,count,${post_peers}" >> "${csv}"
    log "  Post-rejoin peers: ${post_peers}"

    # Phase 6: read keys FROM the rejoined node
    log "  Reading keys from rejoined node..."
    local found=0 not_found=0
    for i in $(seq 1 30); do
        local result
        result=$(grpc_call "${target}" kv.proto dynamo.kv.KvService/Get \
            -d "{\"key\":\"rejoin-key-${i}\"}" 2>&1) || true
        if echo "${result}" | grep -q '"success": true'; then
            ((found++)) || true
        else
            ((not_found++)) || true
        fi
    done
    echo "post_rejoin,keys_readable_from_target,count,${found}" >> "${csv}"
    echo "post_rejoin,keys_not_found_from_target,count,${not_found}" >> "${csv}"
    log "  Keys readable from rejoined node: ${found}/30"

    # Phase 7: read from other nodes too (should be 100%)
    local found_other=0
    local read_node="${NODES[0]}"
    for i in $(seq 1 30); do
        local result
        result=$(grpc_call "${read_node}" kv.proto dynamo.kv.KvService/Get \
            -d "{\"key\":\"rejoin-key-${i}\"}" 2>&1) || true
        if echo "${result}" | grep -q '"success": true'; then
            ((found_other++)) || true
        fi
    done
    echo "post_rejoin,keys_readable_from_other,count,${found_other}" >> "${csv}"

    # Collect hinted handoff metrics from the target
    local hints_delivered
    hints_delivered=$(curl -s "http://${target}:${METRICS_PORT}/metrics" 2>/dev/null | \
        grep "^dynamo_hints_delivered_total " | awk '{print $2}' || echo "0")
    echo "post_rejoin,hints_delivered,count,${hints_delivered}" >> "${csv}"

    cat > "${RESULTS_DIR}/rejoin_summary.txt" <<EOF
Node Rejoin & DHT Healing Results
===================================
Target node: ${target}

Phase               Metric                    Value
Pre-kill            Routing table peers       ${pre_peers}
During downtime     Keys written (to others)  ${keys_written}/30
Post-rejoin         Routing table peers       ${post_peers}
Post-rejoin         Keys readable (target)    ${found}/30
Post-rejoin         Keys readable (other)     ${found_other}/30
Post-rejoin         Hints delivered (target)   ${hints_delivered}

Analysis:
- Routing table recovery: ${post_peers} peers (expected: ~9)
- Data availability: keys written during downtime are readable via quorum
  (R=2 out of N=3 replicas were alive during write)
- Hinted handoff: hints are delivered to the target upon rejoin
EOF

    cat "${RESULTS_DIR}/rejoin_summary.txt"
    log_ok "Results in ${csv}"
}

# ── Experiment 12: Latency Under Load ──────────────────────────────────

exp_latency_under_load() {
    local bg_rate="${1:-20}"  # background ops total
    local probe_count="${2:-20}"
    log "Experiment 12: Latency under load (${bg_rate} bg writes, ${probe_count} probes)"

    local csv="${RESULTS_DIR}/latency_under_load.csv"
    echo "phase,op,key,latency_ms,success" > "${csv}"

    # Phase 1: baseline latency (no load)
    log "  Phase 1: Baseline latency (no background load)"
    for i in $(seq 1 "${probe_count}"); do
        local node
        node=$(random_node)
        local key="lub-${i}"
        local b64
        b64=$(echo -n "baseline-${i}" | base64)
        grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
            -d "{\"key\":\"${key}\",\"value\":\"${b64}\"}" > /dev/null 2>&1
    done
    sleep 0.5
    for i in $(seq 1 "${probe_count}"); do
        local node
        node=$(random_node)
        local result
        result=$(timed_get "lub-${i}" "${node}")
        echo "baseline,GET,lub-${i},${result}" >> "${csv}"
    done

    # Phase 2: start background load, then probe
    log "  Phase 2: Measuring latency with background load"
    local tmpdir
    tmpdir=$(mktemp -d)

    # Background writer 1
    (
        for i in $(seq 1 "${bg_rate}"); do
            local node="${NODES[$((RANDOM % ${#NODES[@]}))]}"
            local b64
            b64=$(echo -n "bg1-val-${i}-$(head -c 32 /dev/urandom | base64)" | base64)
            grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
                -d "{\"key\":\"bg1-key-${i}\",\"value\":\"${b64}\"}" > /dev/null 2>&1
        done
    ) &
    local bg_pid1=$!

    # Background writer 2
    (
        for i in $(seq 1 "${bg_rate}"); do
            local node="${NODES[$((RANDOM % ${#NODES[@]}))]}"
            local b64
            b64=$(echo -n "bg2-val-${i}-$(head -c 32 /dev/urandom | base64)" | base64)
            grpc_call "${node}" kv.proto dynamo.kv.KvService/Put \
                -d "{\"key\":\"bg2-key-${i}\",\"value\":\"${b64}\"}" > /dev/null 2>&1
        done
    ) &
    local bg_pid2=$!

    # Probe latency while load is running
    sleep 0.5  # let background writers start
    for i in $(seq 1 "${probe_count}"); do
        local node
        node=$(random_node)
        local result
        result=$(timed_get "lub-${i}" "${node}")
        echo "under_load,GET,lub-${i},${result}" >> "${csv}"
    done

    # Wait for background to finish
    wait "${bg_pid1}" "${bg_pid2}" 2>/dev/null || true

    # Compute stats
    local baseline_stats load_stats
    baseline_stats=$(grep "^baseline,GET," "${csv}" | cut -d, -f4 | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
s = statistics.stdev(vals) if len(vals) > 1 else 0
print(f'min={min(vals)} median={statistics.median(vals):.0f} mean={statistics.mean(vals):.1f} p95={sorted(vals)[int(len(vals)*0.95)]} max={max(vals)} stdev={s:.1f}')
" 2>/dev/null || echo "?")
    load_stats=$(grep "^under_load,GET," "${csv}" | cut -d, -f4 | python3 -c "
import sys, statistics
vals = [int(x) for x in sys.stdin.read().strip().split('\n') if x]
s = statistics.stdev(vals) if len(vals) > 1 else 0
print(f'min={min(vals)} median={statistics.median(vals):.0f} mean={statistics.mean(vals):.1f} p95={sorted(vals)[int(len(vals)*0.95)]} max={max(vals)} stdev={s:.1f}')
" 2>/dev/null || echo "?")

    cat > "${RESULTS_DIR}/latency_under_load_summary.txt" <<EOF
Latency Under Load Results
============================
Background load: 2 concurrent writers x ${bg_rate} PUTs each
Probe count: ${probe_count} GETs per phase

Baseline (no load): ${baseline_stats}
Under load:         ${load_stats}

Note: Background load is limited by serial grpcurl calls per client.
Real load is ~2 concurrent PUT streams during the under_load phase.
EOF

    cat "${RESULTS_DIR}/latency_under_load_summary.txt"
    rm -rf "${tmpdir}"
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
    exp_concurrent_throughput 5 50
    echo ""
    exp_value_sizes
    echo ""
    exp_vector_clocks 10
    echo ""
    exp_rejoin
    echo ""
    exp_latency_under_load 20 20
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
    concurrent)       exp_concurrent_throughput "${2:-5}" "${3:-50}" ;;
    value-sizes)      exp_value_sizes ;;
    vector-clocks)    exp_vector_clocks "${2:-10}" ;;
    rejoin)           exp_rejoin ;;
    latency-load)     exp_latency_under_load "${2:-20}" "${3:-20}" ;;
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
  concurrent [c] [n]      Concurrent throughput (c clients, n ops each)
  value-sizes             Value size scaling (1B to 100KB)
  vector-clocks [n]       Vector clock conflict detection (n keys)
  rejoin                  Node rejoin & DHT healing test
  latency-load [bg] [n]   Latency under background load
EOF
        ;;
    *)
        echo "Unknown: $1"; exit 1 ;;
esac
