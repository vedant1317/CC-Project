"""
analyze.py
Reads results/results.csv and prints a decision-oriented benchmark summary.
Also generates results/results.png with latency and cost views.

Run: python analyze.py
"""

import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

CSV_PATH = os.path.join("results", "results.csv")
PNG_PATH = os.path.join("results", "results.png")

DB_COLORS = {
    "postgres": "#2563eb",
    "dynamo": "#059669",
    "mongodb": "#ea580c",
}

NUMERIC_COLUMNS = [
    "avg",
    "p50",
    "p95",
    "p99",
    "min",
    "max",
    "throughput_ops_s",
    "error_rate_pct",
    "cpu_avg_pct",
    "mem_avg_mb",
    "cost_index",
    "decision_score",
]


def main() -> None:
    df = pd.read_csv(CSV_PATH)
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")

    print("\n" + "=" * 78)
    print("  PITLANE RESULTS - AUTOMATED DB SELECTION SUMMARY")
    print("=" * 78)

    summary_cols = [
        "scenario",
        "db",
        "avg",
        "p95",
        "p99",
        "throughput_ops_s",
        "cpu_avg_pct",
        "mem_avg_mb",
        "cost_index",
    ]
    summary_cols = [col for col in summary_cols if col in df.columns]
    print(df[summary_cols].to_string(index=False))

    print("\n" + "=" * 78)
    print("  WINNERS BY SCENARIO")
    print("=" * 78)
    for scenario in df["scenario"].unique():
        subset = df[df["scenario"] == scenario]
        fastest = subset.loc[subset["avg"].idxmin()]["db"]

        if "cost_index" in subset.columns:
            cheapest = subset.loc[subset["cost_index"].idxmin()]["db"]
        else:
            cheapest = "n/a"

        if "decision_score" in subset.columns:
            recommended = subset.loc[subset["decision_score"].idxmin()]["db"]
        elif "recommended" in subset.columns:
            rec_rows = subset[subset["recommended"] == True]  # noqa: E712
            recommended = rec_rows.iloc[0]["db"] if not rec_rows.empty else fastest
        else:
            recommended = fastest

        print(
            f"  {scenario:<20} fastest={fastest:<10} "
            f"cheapest={cheapest:<10} recommended={recommended}"
        )

    _render_chart(df)
    print(f"\n[OK] Chart saved to {PNG_PATH}")


def _render_chart(df: pd.DataFrame) -> None:
    scenarios = df["scenario"].unique().tolist()
    dbs = sorted(df["db"].unique().tolist())

    x = np.arange(len(scenarios))
    width = 0.75 / max(len(dbs), 1)
    n_dbs = len(dbs)

    right_metric = "cost_index" if "cost_index" in df.columns else "avg"
    right_title = "Operational Cost Index" if right_metric == "cost_index" else "Average Latency"
    right_ylabel = "Cost Index" if right_metric == "cost_index" else "Latency (ms)"

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.patch.set_facecolor("#ffffff")

    for axis_index, metric in enumerate(["p99", right_metric]):
        ax = axes[axis_index]
        ax.set_facecolor("#ffffff")

        for i, db in enumerate(dbs):
            values = []
            for scenario in scenarios:
                row = df[(df["scenario"] == scenario) & (df["db"] == db)]
                values.append(float(row[metric].values[0]) if not row.empty else 0)

            offset = (i - n_dbs / 2 + 0.5) * width
            bars = ax.bar(
                x + offset,
                values,
                width,
                label=db,
                color=DB_COLORS.get(db, "#888888"),
                alpha=0.86,
                edgecolor="#ffffff",
                linewidth=0.8,
            )

            for bar, value in zip(bars, values):
                if value <= 0:
                    continue
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    bar.get_height(),
                    f"{value:.2f}",
                    ha="center",
                    va="bottom",
                    fontsize=6.5,
                    color="#334155",
                )

        if metric == "p99":
            ax.set_title("P99 Latency by Scenario", color="#0f172a", fontsize=11, pad=10)
            ax.set_ylabel("Latency (ms)", color="#475569", fontsize=9)
            ax.yaxis.set_major_formatter(mticker.FormatStrFormatter("%.0f ms"))
        else:
            ax.set_title(f"{right_title} by Scenario", color="#0f172a", fontsize=11, pad=10)
            ax.set_ylabel(right_ylabel, color="#475569", fontsize=9)

        ax.set_xticks(x)
        ax.set_xticklabels(
            [scenario.replace("_", "\n") for scenario in scenarios],
            color="#475569",
            fontsize=8,
        )
        ax.tick_params(colors="#475569")
        ax.spines[:].set_color("#cbd5e1")
        ax.grid(axis="y", color="#e2e8f0", linewidth=0.7, linestyle="--")
        ax.legend(facecolor="#ffffff", edgecolor="#cbd5e1", labelcolor="#334155", fontsize=8)

    plt.suptitle(
        "PitLane - Automated DB Selection and Regression Analytics",
        color="#0f172a",
        fontsize=13,
        fontweight="bold",
        y=1.01,
    )
    plt.tight_layout()
    plt.savefig(PNG_PATH, dpi=140, bbox_inches="tight", facecolor="#ffffff")


if __name__ == "__main__":
    main()
