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
    "postgres": "#2563eb",
    "dynamo":   "#059669",
    "mongodb":  "#ea580c",
}

scenarios = df["scenario"].unique().tolist()
dbs       = sorted(df["db"].unique().tolist())

x     = np.arange(len(scenarios))
width = 0.25
n_dbs = len(dbs)

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.patch.set_facecolor("#ffffff")

for ax_idx, metric in enumerate(["p99", "avg"]):
    ax = axes[ax_idx]
    ax.set_facecolor("#ffffff")

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
                      edgecolor="#ffffff",
                      linewidth=0.7)

        for bar, val in zip(bars, values):
            if val > 0:
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height() + 0.5,
                    f"{val:.1f}",
                    ha="center", va="bottom",
                    fontsize=6.5, color="#334155"
                )

    title_label = "P99 Latency" if metric == "p99" else "Average Latency"
    ax.set_title(f"{title_label} by Scenario", color="#0f172a", fontsize=11, pad=10)
    ax.set_ylabel("Latency (ms)", color="#475569", fontsize=9)
    ax.set_xticks(x)
    ax.set_xticklabels([s.replace("_", "\n") for s in scenarios],
                       color="#475569", fontsize=8)
    ax.yaxis.set_tick_params(labelcolor="#475569")
    ax.tick_params(colors="#475569")
    ax.spines[:].set_color("#cbd5e1")
    ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f ms"))
    ax.grid(axis="y", color="#e2e8f0", linewidth=0.7, linestyle="--")
    ax.legend(
        facecolor="#ffffff", edgecolor="#cbd5e1",
        labelcolor="#334155", fontsize=8
    )

plt.suptitle(
    "PitLane — PostgreSQL vs DynamoDB vs MongoDB",
    color="#0f172a", fontsize=13, fontweight="bold", y=1.01
)
plt.tight_layout()
plt.savefig(PNG_PATH, dpi=140, bbox_inches="tight", facecolor="#ffffff")
print(f"\n[OK] Chart saved to {PNG_PATH}")
# plt.show()
