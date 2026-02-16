#!/usr/bin/env python3
"""
dynamo-kad evaluation visualization.

Generates publication-quality figures from experiment CSV data.
Output: deploy/plots/figures/<name>.png

Usage:
    cd deploy/plots && uv run main.py
"""

import csv
import statistics
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# ── Paths ──────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR.parent / "results" / "20260217-030352"
FIG_DIR = SCRIPT_DIR / "figures"
FIG_DIR.mkdir(exist_ok=True)

# ── Style ──────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.figsize": (7, 4.5),
    "figure.dpi": 200,
    "font.size": 11,
    "font.family": "serif",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "savefig.bbox": "tight",
    "savefig.pad_inches": 0.15,
})

COLORS = {
    "put": "#2563eb",      # blue
    "get": "#dc2626",      # red
    "accent": "#059669",   # green
    "orange": "#ea580c",
    "purple": "#7c3aed",
    "gray": "#6b7280",
    "dark": "#1f2937",
}


def load_csv(name: str) -> list[dict]:
    with open(DATA_DIR / name) as f:
        return list(csv.DictReader(f))


def save(fig, name: str):
    path = FIG_DIR / f"{name}.png"
    fig.savefig(path)
    plt.close(fig)
    print(f"  saved {path.relative_to(SCRIPT_DIR)}")


# ── Figure 1: PUT vs GET Latency Distribution (Histogram) ─────────────

def fig_latency_distribution():
    rows = load_csv("latency.csv")
    put_lat = [int(r["latency_ms"]) for r in rows if r["op"] == "PUT"]
    get_lat = [int(r["latency_ms"]) for r in rows if r["op"] == "GET"]

    fig, ax = plt.subplots()
    bins = np.arange(55, 200, 3)
    ax.hist(put_lat, bins=bins, alpha=0.7, label=f"PUT (n={len(put_lat)})",
            color=COLORS["put"], edgecolor="white", linewidth=0.5)
    ax.hist(get_lat, bins=bins, alpha=0.7, label=f"GET (n={len(get_lat)})",
            color=COLORS["get"], edgecolor="white", linewidth=0.5)

    put_med = statistics.median(put_lat)
    get_med = statistics.median(get_lat)
    ax.axvline(put_med, color=COLORS["put"], linestyle="--", linewidth=1.5,
               label=f"PUT median={put_med:.0f} ms")
    ax.axvline(get_med, color=COLORS["get"], linestyle="--", linewidth=1.5,
               label=f"GET median={get_med:.0f} ms")

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("Count")
    ax.set_title("PUT vs GET Latency Distribution (100 ops each)")
    ax.legend(fontsize=9)
    ax.set_xlim(55, 200)
    save(fig, "01_latency_distribution")


# ── Figure 2: Per-Node Latency (Bar chart) ────────────────────────────

def fig_per_node_latency():
    rows = load_csv("per_node_latency.csv")
    nodes = sorted(set(r["read_node"] for r in rows))
    node_labels = [n.split(".")[-1] for n in nodes]

    node_lats: dict[str, list[int]] = {}
    for r in rows:
        node_lats.setdefault(r["read_node"], []).append(int(r["latency_ms"]))

    means = [statistics.mean(node_lats[n]) for n in nodes]
    stds = [statistics.stdev(node_lats[n]) for n in nodes]

    fig, ax = plt.subplots()
    x = np.arange(len(nodes))
    ax.bar(x, means, yerr=stds, width=0.6, color=COLORS["put"], edgecolor="white",
           linewidth=0.5, capsize=3, alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([f".{l}" for l in node_labels], fontsize=9)
    ax.set_xlabel("Node (140.112.30.x)")
    ax.set_ylabel("Average GET Latency (ms)")
    ax.set_title("Per-Node GET Latency (10 reads each)")
    ax.set_ylim(50, 85)
    ax.axhline(statistics.mean(means), color=COLORS["gray"], linestyle=":", linewidth=1,
               label=f"Cluster mean = {statistics.mean(means):.1f} ms")
    ax.legend(fontsize=9)
    save(fig, "02_per_node_latency")


# ── Figure 3: Concurrent Throughput Scaling ────────────────────────────

def fig_concurrency_scaling():
    rows = load_csv("concurrency_scaling.csv")
    clients = [int(r["clients"]) for r in rows]
    throughput = [float(r["throughput_ops_sec"]) for r in rows]

    base = throughput[0]
    ideal = [base * c for c in clients]

    fig, ax = plt.subplots()
    ax.plot(clients, throughput, "o-", color=COLORS["put"], linewidth=2,
            markersize=8, label="Measured", zorder=3)
    ax.plot(clients, ideal, "--", color=COLORS["gray"], linewidth=1.5,
            label="Ideal linear scaling", alpha=0.7)

    for i, (c, t) in enumerate(zip(clients, throughput)):
        offset = 8 if i < 4 else -12
        ax.annotate(f"{t:.1f}", (c, t), textcoords="offset points",
                    xytext=(0, offset), ha="center", fontsize=9, color=COLORS["put"])

    ax.set_xlabel("Concurrent Clients")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Throughput Scaling with Concurrent Clients")
    ax.set_xticks(clients)
    ax.legend(fontsize=9)
    ax.set_ylim(0, max(ideal) * 1.1)
    save(fig, "03_concurrency_scaling")


# ── Figure 4: Value Size Scaling ──────────────────────────────────────

def fig_value_size_scaling():
    rows = load_csv("value_sizes.csv")

    sizes = sorted(set(int(r["value_size_bytes"]) for r in rows))
    size_labels = {1: "1B", 100: "100B", 1024: "1KB", 10240: "10KB", 102400: "100KB"}

    put_med, get_med = [], []
    put_err, get_err = [], []

    for sz in sizes:
        pvals = [int(r["latency_ms"]) for r in rows
                 if int(r["value_size_bytes"]) == sz and r["op"] == "PUT"]
        gvals = [int(r["latency_ms"]) for r in rows
                 if int(r["value_size_bytes"]) == sz and r["op"] == "GET"]
        put_med.append(statistics.median(pvals))
        get_med.append(statistics.median(gvals))
        put_err.append(statistics.stdev(pvals) if len(pvals) > 1 else 0)
        get_err.append(statistics.stdev(gvals) if len(gvals) > 1 else 0)

    fig, ax = plt.subplots()
    x = np.arange(len(sizes))
    w = 0.35
    ax.bar(x - w / 2, put_med, w, yerr=put_err, label="PUT",
           color=COLORS["put"], edgecolor="white", linewidth=0.5, capsize=3, alpha=0.85)
    ax.bar(x + w / 2, get_med, w, yerr=get_err, label="GET",
           color=COLORS["get"], edgecolor="white", linewidth=0.5, capsize=3, alpha=0.85)

    ax.set_xticks(x)
    ax.set_xticklabels([size_labels[s] for s in sizes])
    ax.set_xlabel("Value Size")
    ax.set_ylabel("Median Latency (ms)")
    ax.set_title("Latency vs Value Size (10 trials each)")
    ax.set_ylim(bottom=0)
    ax.legend(fontsize=9)
    save(fig, "04_value_size_scaling")


# ── Figure 5: Fault Tolerance ─────────────────────────────────────────

def fig_fault_tolerance():
    rows = load_csv("fault_tolerance.csv")

    phases = [
        ("baseline", "10/10\n(baseline)"),
        ("kill_1",   "9/10\n(1 killed)"),
        ("kill_3",   "7/10\n(3 killed)"),
        ("restored", "10/10\n(restored)"),
    ]

    put_rates: list[float | None] = []
    get_rates: list[float | None] = []
    for phase_key, _ in phases:
        pr = [r for r in rows if r["phase"] == phase_key and r["op"] == "PUT"]
        gr = [r for r in rows if r["phase"] == phase_key and r["op"] == "GET"]
        if pr:
            put_rates.append(sum(1 for r in pr if r["success"] == "true") / len(pr) * 100)
        else:
            put_rates.append(None)
        if gr:
            get_rates.append(sum(1 for r in gr if r["success"] == "true") / len(gr) * 100)
        else:
            get_rates.append(None)

    fig, ax = plt.subplots()
    x = np.arange(len(phases))
    w = 0.35

    put_x = [i for i, v in enumerate(put_rates) if v is not None]
    put_v = [v for v in put_rates if v is not None]
    get_x = [i for i, v in enumerate(get_rates) if v is not None]
    get_v = [v for v in get_rates if v is not None]

    ax.bar([i - w / 2 for i in put_x], put_v, w, label="PUT",
           color=COLORS["put"], edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.bar([i + w / 2 for i in get_x], get_v, w, label="GET",
           color=COLORS["get"], edgecolor="white", linewidth=0.5, alpha=0.85)

    for xi, vi in zip(put_x, put_v):
        ax.text(xi - w / 2, vi + 1, f"{vi:.0f}%", ha="center", va="bottom",
                fontsize=9, color=COLORS["put"])
    for xi, vi in zip(get_x, get_v):
        ax.text(xi + w / 2, vi + 1, f"{vi:.0f}%", ha="center", va="bottom",
                fontsize=9, color=COLORS["get"])

    ax.set_xticks(x)
    ax.set_xticklabels([label for _, label in phases], fontsize=9)
    ax.set_xlabel("Cluster State (nodes alive)")
    ax.set_ylabel("Success Rate (%)")
    ax.set_title("Fault Tolerance: Success Rate Under Node Failures")
    ax.set_ylim(0, 115)
    ax.legend(fontsize=9)
    save(fig, "05_fault_tolerance")


# ── Figure 6: Latency Under Load (Box Plot) ───────────────────────────

def fig_latency_under_load():
    rows = load_csv("latency_under_load.csv")
    baseline = [int(r["latency_ms"]) for r in rows if r["phase"] == "baseline"]
    loaded = [int(r["latency_ms"]) for r in rows if r["phase"] == "under_load"]

    fig, ax = plt.subplots()

    bp = ax.boxplot(
        [baseline, loaded],
        tick_labels=["No Load", "Under Load\n(2 bg writers)"],
        patch_artist=True,
        widths=0.5,
        medianprops=dict(color=COLORS["dark"], linewidth=2),
    )

    bp["boxes"][0].set_facecolor(COLORS["put"])
    bp["boxes"][0].set_alpha(0.5)
    bp["boxes"][1].set_facecolor(COLORS["orange"])
    bp["boxes"][1].set_alpha(0.5)

    # Overlay individual points
    rng = np.random.default_rng(42)
    for i, data in enumerate([baseline, loaded], 1):
        jitter = rng.uniform(-0.08, 0.08, len(data))
        ax.scatter(np.full(len(data), i) + jitter, data,
                   color=COLORS["dark"], s=20, alpha=0.5, zorder=3)

    ax.set_ylabel("GET Latency (ms)")
    ax.set_title("Read Latency Stability Under Concurrent Write Load")

    for i, data in enumerate([baseline, loaded], 1):
        med = statistics.median(data)
        ax.annotate(f"median = {med:.0f} ms", (i, med), xytext=(40, -5),
                    textcoords="offset points", fontsize=9, color=COLORS["dark"],
                    arrowprops=dict(arrowstyle="-", color=COLORS["gray"], lw=0.8))

    save(fig, "06_latency_under_load")


# ── Figure 7: Serial Throughput Warmup Curve ──────────────────────────

def fig_throughput_over_time():
    rows = load_csv("throughput.csv")
    ops = [int(r["op_num"]) for r in rows]
    cum_tput = []
    for r in rows:
        try:
            cum_tput.append(float(r["cumulative_ops_per_sec"]))
        except ValueError:
            cum_tput.append(float("nan"))

    latencies = [int(r["latency_ms"]) for r in rows]

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()

    ax1.plot(ops, cum_tput, color=COLORS["put"], linewidth=1.5, label="Cumulative throughput")
    ax1.set_xlabel("Operation #")
    ax1.set_ylabel("Cumulative Throughput (ops/sec)", color=COLORS["put"])
    ax1.tick_params(axis="y", labelcolor=COLORS["put"])
    valid = [v for v in cum_tput if not np.isnan(v)]
    ax1.set_ylim(0, max(valid) * 1.3 if valid else 10)

    # Cap latency outliers for readability (mark outliers with a triangle)
    cap = 500
    capped = [min(l, cap) for l in latencies]
    outlier_idx = [i for i, l in enumerate(latencies) if l > cap]
    normal_idx = [i for i, l in enumerate(latencies) if l <= cap]
    ax2.scatter([ops[i] for i in normal_idx], [capped[i] for i in normal_idx],
                color=COLORS["get"], s=6, alpha=0.4, label="Per-op latency")
    if outlier_idx:
        ax2.scatter([ops[i] for i in outlier_idx], [cap] * len(outlier_idx),
                    color=COLORS["get"], s=30, alpha=0.8, marker="^",
                    label=f"Outlier (>{cap} ms, n={len(outlier_idx)})")
    ax2.set_ylabel("Per-Operation Latency (ms)", color=COLORS["get"])
    ax2.tick_params(axis="y", labelcolor=COLORS["get"])
    ax2.set_ylim(0, cap * 1.15)

    # Fix right spine visibility for dual axis
    ax2.spines["right"].set_visible(True)

    ax1.set_title("Serial Throughput Warmup (200 PUTs)")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, fontsize=9, loc="center right")

    save(fig, "07_throughput_over_time")


# ── Figure 8: Cluster Metrics Distribution ─────────────────────────────

def fig_metrics_distribution():
    rows = load_csv("metrics.csv")
    nodes = [r["node"].split(".")[-1] for r in rows]
    kv_puts = [int(r["kv_puts"]) for r in rows]
    kv_gets = [int(r["kv_gets"]) for r in rows]
    hints_stored = [int(r["hints_stored"]) for r in rows]
    hints_delivered = [int(r["hints_delivered"]) for r in rows]
    read_repairs = [int(r["read_repairs"]) for r in rows]

    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

    # Left: KV operations per node
    ax = axes[0]
    x = np.arange(len(nodes))
    w = 0.35
    ax.bar(x - w / 2, kv_puts, w, label="PUTs", color=COLORS["put"],
           edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.bar(x + w / 2, kv_gets, w, label="GETs", color=COLORS["get"],
           edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels([f".{n}" for n in nodes], fontsize=9)
    ax.set_xlabel("Node (140.112.30.x)")
    ax.set_ylabel("Operations")
    ax.set_title("KV Operations per Node")
    ax.legend(fontsize=9)

    # Right: Anti-entropy metrics
    ax = axes[1]
    ax.bar(x - w / 2, hints_stored, w, label="Hints stored",
           color=COLORS["orange"], edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.bar(x + w / 2, hints_delivered, w, label="Hints delivered",
           color=COLORS["accent"], edgecolor="white", linewidth=0.5, alpha=0.85)
    ax.plot(x, read_repairs, "D-", color=COLORS["purple"], linewidth=1.5,
            markersize=5, label="Read repairs", zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels([f".{n}" for n in nodes], fontsize=9)
    ax.set_xlabel("Node (140.112.30.x)")
    ax.set_ylabel("Count")
    ax.set_title("Anti-Entropy: Hints & Read Repairs")
    ax.legend(fontsize=9)

    fig.suptitle("Cluster-Wide Prometheus Metrics", fontsize=13, y=1.02)
    fig.tight_layout()
    save(fig, "08_cluster_metrics")


# ── Main ───────────────────────────────────────────────────────────────

def main():
    print(f"Data:    {DATA_DIR}")
    print(f"Output:  {FIG_DIR}")
    print()

    print("Generating figures...")
    fig_latency_distribution()
    fig_per_node_latency()
    fig_concurrency_scaling()
    fig_value_size_scaling()
    fig_fault_tolerance()
    fig_latency_under_load()
    fig_throughput_over_time()
    fig_metrics_distribution()

    print(f"\nDone -- {len(list(FIG_DIR.glob('*.png')))} figures in {FIG_DIR.relative_to(SCRIPT_DIR)}/")


if __name__ == "__main__":
    main()
