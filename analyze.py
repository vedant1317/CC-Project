"""
analyze.py
Reads results/results.csv and produces:
  - A formatted summary table in the terminal
  - results/results.png — grouped bar chart of P99 latency per scenario per database

Run: python analyze.py
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

CSV_PATH = os.path.join("results", "results.csv")
PNG_PATH = os.path.join("results", "results.png")

# ── Load data ─────────────────────────────────────────────────────────

df = pd.read_csv(CSV_PATH)
print("\n" + "=" * 65)
print("  DATABASE BENCHMARK RESULTS — SUMMARY")
print("=" * 65)
print(df[["scenario", "db", "avg", "p50", "p95", "p99"]].to_string(index=False))

# ── Winner per scenario ───────────────────────────────────────────────

print("\n" + "=" * 65)
print("  WINNERS (lowest avg latency per scenario)")
print("=" * 65)
for scenario in df["scenario"].unique():
    subset = df[df["scenario"] == scenario]
    winner = subset.loc[subset["avg"].idxmin()]
    print(f"  {scenario:<20}  →  {winner['db']:<12}  avg={winner['avg']:.3f}ms")

# ── P99 grouped bar chart ─────────────────────────────────────────────

DB_COLORS = {
    "postgres": "#60a5fa",
    "dynamo":   "#34d399",
    "mongodb":  "#fb923c",
}

scenarios = df["scenario"].unique().tolist()
dbs       = sorted(df["db"].unique().tolist())

x     = np.arange(len(scenarios))
width = 0.25
n_dbs = len(dbs)

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.patch.set_facecolor("#0f1117")

for ax_idx, metric in enumerate(["p99", "avg"]):
    ax = axes[ax_idx]
    ax.set_facecolor("#1e2130")

    for i, db in enumerate(dbs):
        values = []
        for s in scenarios:
            row = df[(df["scenario"] == s) & (df["db"] == db)]
            values.append(row[metric].values[0] if not row.empty else 0)

        offset = (i - n_dbs / 2 + 0.5) * width
        bars = ax.bar(x + offset, values, width,
                      label=db,
                      color=DB_COLORS.get(db, "#888888"),
                      alpha=0.85,
                      edgecolor="#0f1117",
                      linewidth=0.5)

        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.5,
                    f"{val:.1f}",
                    ha="center", va="bottom",
                    fontsize=6.5, color="#94a3b8"
                )

    title_label = "P99 Latency" if metric == "p99" else "Average Latency"
    ax.set_title(f"{title_label} by Scenario", color="#e2e8f0", fontsize=11, pad=10)
    ax.set_ylabel("Latency (ms)", color="#64748b", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([s.replace("_", "\n") for s in scenarios],
                       color="#94a3b8", fontsize=8)
    ax.yaxis.set_tick_params(labelcolor="#64748b")
    ax.tick_params(colors="#64748b")
    ax.spines[:].set_color("#2d3148")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f ms"))
    ax.grid(axis="y", color="#2d3148", linewidth=0.5, linestyle="--")
    ax.legend(
        facecolor="#1a1f2e", edgecolor="#2d3148",
        labelcolor="#94a3b8", fontsize=8
    )

plt.suptitle(
    "PostgreSQL vs DynamoDB vs MongoDB — Local Benchmark",
    color="#e2e8f0", fontsize=13, fontweight="bold", y=1.01
)
plt.tight_layout()
plt.savefig(PNG_PATH, dpi=140, bbox_inches="tight", facecolor="#0f1117")
print(f"\n[OK] Chart saved to {PNG_PATH}")
# plt.show()
