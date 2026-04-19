"""
run.py
Workload-driven benchmark runner with concurrency, resource telemetry,
and optional regression checks.
"""

from __future__ import annotations

import argparse
import csv
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd

from benchmark import (
    DB_CONTAINER_MAP,
    DEFAULT_STATUSES,
    SCENARIO_COMPATIBILITY,
    dynamo_bulk_write,
    dynamo_point_lookup,
    execute_custom_query,
    get_sample_user_ids,
    mongo_aggregation,
    mongo_bulk_write,
    mongo_complex_join,
    mongo_point_lookup,
    mongo_range_query,
    pg_aggregation,
    pg_bulk_write,
    pg_complex_join,
    pg_point_lookup,
    pg_range_query,
    summarize_latencies_ms,
)
from telemetry import DockerStatsSampler
from workload import (
    WorkloadValidationError,
    build_token_resolver,
    load_workload,
    resolve_placeholders,
)


BUILTIN_DISPATCH: Dict[str, Dict[str, Callable[..., float]]] = {
    "point_lookup": {
        "postgres": pg_point_lookup,
        "dynamo": dynamo_point_lookup,
        "mongodb": mongo_point_lookup,
    },
    "complex_join": {
        "postgres": pg_complex_join,
        "mongodb": mongo_complex_join,
    },
    "bulk_write": {
        "postgres": pg_bulk_write,
        "dynamo": dynamo_bulk_write,
        "mongodb": mongo_bulk_write,
    },
    "range_query": {
        "postgres": pg_range_query,
        "mongodb": mongo_range_query,
    },
    "aggregation": {
        "postgres": pg_aggregation,
        "mongodb": mongo_aggregation,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PitLane concurrent benchmark runner")
    parser.add_argument(
        "--workload",
        default="workloads/default_workload.json",
        help="Path to workload JSON file.",
    )
    parser.add_argument(
        "--output",
        default="results/results.csv",
        help="Output CSV path for benchmark metrics.",
    )
    parser.add_argument("--seed", type=int, default=99, help="Random seed for repeatable runs.")
    parser.add_argument(
        "--sample-users",
        type=int,
        default=500,
        help="Number of user IDs to preload for dynamic placeholders.",
    )
    parser.add_argument(
        "--baseline",
        default=None,
        help="Optional baseline CSV path for regression checks.",
    )
    parser.add_argument(
        "--regression-p95-threshold",
        type=float,
        default=15.0,
        help="Allowed p95 regression percentage before failing.",
    )
    parser.add_argument(
        "--regression-cost-threshold",
        type=float,
        default=20.0,
        help="Allowed cost-index regression percentage before failing.",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with a non-zero code when regression thresholds are exceeded.",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Run a lightweight validation pass by capping runs/concurrency.",
    )
    parser.add_argument(
        "--smoke-max-runs",
        type=int,
        default=20,
        help="Maximum per-scenario runs when --smoke is enabled.",
    )
    parser.add_argument(
        "--smoke-max-concurrency",
        type=int,
        default=8,
        help="Maximum per-scenario concurrency when --smoke is enabled.",
    )
    parser.add_argument(
        "--smoke-max-warmup-runs",
        type=int,
        default=1,
        help="Maximum per-scenario warmup runs when --smoke is enabled.",
    )
    return parser.parse_args()


def run_load(
    request_fn: Callable[[], float],
    runs: int,
    concurrency: int,
    warmup_runs: int,
) -> tuple[Dict[str, Any], List[str]]:
    """Execute one scenario/db pair under concurrent load."""
    warmup_errors = 0
    for _ in range(warmup_runs):
        try:
            request_fn()
        except Exception:
            warmup_errors += 1

    latencies_ms: List[float] = []
    errors: List[str] = []

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(request_fn) for _ in range(runs)]
        for future in as_completed(futures):
            try:
                elapsed_seconds = future.result()
                latencies_ms.append(elapsed_seconds * 1000)
            except Exception as exc:
                errors.append(str(exc))

    wall_time = max(time.perf_counter() - started, 1e-9)
    stats = summarize_latencies_ms(latencies_ms)
    stats.update(
        {
            "runs": runs,
            "warmup_runs": warmup_runs,
            "warmup_errors": warmup_errors,
            "successful_runs": len(latencies_ms),
            "errors": len(errors),
            "error_rate_pct": round((len(errors) / runs) * 100, 3),
            "throughput_ops_s": round(len(latencies_ms) / wall_time, 3),
            "wall_time_s": round(wall_time, 3),
        }
    )
    return stats, errors


def _build_runtime_context(sample_users: int) -> Dict[str, Any]:
    print("Loading sample user IDs from PostgreSQL...")
    user_ids = get_sample_user_ids(limit=sample_users)
    if not user_ids:
        raise RuntimeError("No users found. Seed data first with 'python seed.py'.")

    print(f"  Loaded {len(user_ids)} user IDs")
    return {
        "user_ids": user_ids,
        "statuses": list(DEFAULT_STATUSES),
        "range_start": datetime(2025, 1, 1),
        "range_end": datetime(2025, 6, 1),
    }


def _apply_smoke_profile(
    workload: Dict[str, Any],
    max_runs: int,
    max_concurrency: int,
    max_warmup_runs: int,
) -> None:
    if max_runs <= 0:
        raise ValueError("--smoke-max-runs must be a positive integer.")
    if max_concurrency <= 0:
        raise ValueError("--smoke-max-concurrency must be a positive integer.")
    if max_warmup_runs < 0:
        raise ValueError("--smoke-max-warmup-runs must be a non-negative integer.")

    defaults = workload.get("defaults", {})
    defaults["runs"] = min(int(defaults.get("runs", max_runs)), max_runs)
    defaults["concurrency"] = min(
        int(defaults.get("concurrency", max_concurrency)),
        max_concurrency,
    )
    defaults["warmup_runs"] = min(
        int(defaults.get("warmup_runs", max_warmup_runs)),
        max_warmup_runs,
    )

    for scenario in workload.get("scenarios", []):
        scenario["runs"] = min(int(scenario["runs"]), max_runs)
        scenario["concurrency"] = min(int(scenario["concurrency"]), max_concurrency)
        scenario["warmup_runs"] = min(int(scenario["warmup_runs"]), max_warmup_runs)


def _build_request_fn(scenario: Dict[str, Any], db: str, context: Dict[str, Any]) -> Callable[[], float]:
    if scenario["type"] == "builtin":
        return _build_builtin_request_fn(scenario, db, context)
    return _build_custom_request_fn(scenario, db, context)


def _build_builtin_request_fn(
    scenario: Dict[str, Any],
    db: str,
    context: Dict[str, Any],
) -> Callable[[], float]:
    operation = scenario["builtin"]
    params = scenario.get("params", {})

    if db not in SCENARIO_COMPATIBILITY.get(operation, set()):
        raise ValueError(
            f"Scenario '{scenario['id']}' operation '{operation}' is not supported on {db}."
        )

    resolver = build_token_resolver(context)
    handler = BUILTIN_DISPATCH[operation][db]

    if operation in {"point_lookup", "complex_join"}:
        user_token = params.get("user_id", "$random_user_id")

        def _run_user_lookup() -> float:
            user_id = str(resolve_placeholders(user_token, resolver))
            return handler(user_id)

        return _run_user_lookup

    if operation == "bulk_write":
        batch_size = int(params.get("batch_size", 100))
        user_token = params.get("user_id", "$random_user_id")
        total_token = params.get("total", "$random_total")
        status_token = params.get("status", "$random_status")

        def _run_bulk_write() -> float:
            batch = [
                {
                    "user_id": str(resolve_placeholders(user_token, resolver)),
                    "total": float(resolve_placeholders(total_token, resolver)),
                    "status": str(resolve_placeholders(status_token, resolver)),
                }
                for _ in range(batch_size)
            ]
            return handler(batch)

        return _run_bulk_write

    if operation == "range_query":
        start_raw = params.get("start_date", "$range_start_iso")
        end_raw = params.get("end_date", "$range_end_iso")

        def _run_range_query() -> float:
            start_value = resolve_placeholders(start_raw, resolver)
            end_value = resolve_placeholders(end_raw, resolver)
            return handler(_parse_datetime(start_value), _parse_datetime(end_value))

        return _run_range_query

    if operation == "aggregation":
        return handler

    raise ValueError(f"Unsupported builtin operation '{operation}'.")


def _build_custom_request_fn(
    scenario: Dict[str, Any],
    db: str,
    context: Dict[str, Any],
) -> Callable[[], float]:
    mock_queries = scenario.get("mock_queries", {})
    if db not in mock_queries:
        raise ValueError(
            f"Custom scenario '{scenario['id']}' has no mock query definition for '{db}'."
        )

    resolver = build_token_resolver(context)
    query_template = mock_queries[db]

    def _run_custom_query() -> float:
        query_payload = resolve_placeholders(query_template, resolver)
        return execute_custom_query(db, query_payload)

    return _run_custom_query


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value

    text = str(value)
    if text.endswith("Z"):
        text = text.replace("Z", "+00:00")

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d")


def _cost_index(row: Dict[str, Any]) -> float:
    cpu_component = float(row.get("cpu_avg_pct") or 0.0)
    mem_component = float(row.get("mem_avg_mb") or 0.0)
    throughput = max(float(row.get("throughput_ops_s") or 0.0), 0.001)
    return round(((0.6 * cpu_component) + (0.4 * mem_component)) / throughput, 6)


def _annotate_decision_scores(rows: List[Dict[str, Any]]) -> None:
    scenarios = sorted({row["scenario"] for row in rows})

    for scenario_name in scenarios:
        subset = [row for row in rows if row["scenario"] == scenario_name]
        latency_values = [float(row["avg"]) for row in subset]
        cost_values = [float(row["cost_index"]) for row in subset]

        latency_min, latency_max = min(latency_values), max(latency_values)
        cost_min, cost_max = min(cost_values), max(cost_values)

        for row in subset:
            latency_norm = _normalize(float(row["avg"]), latency_min, latency_max)
            cost_norm = _normalize(float(row["cost_index"]), cost_min, cost_max)
            row["latency_norm"] = round(latency_norm, 6)
            row["cost_norm"] = round(cost_norm, 6)
            row["decision_score"] = round((0.7 * latency_norm) + (0.3 * cost_norm), 6)

        fastest = min(subset, key=lambda item: item["avg"])["db"]
        cheapest = min(subset, key=lambda item: item["cost_index"])["db"]
        recommended = min(subset, key=lambda item: item["decision_score"])["db"]

        for row in subset:
            row["latency_winner"] = row["db"] == fastest
            row["cost_winner"] = row["db"] == cheapest
            row["recommended"] = row["db"] == recommended


def _normalize(value: float, lower: float, upper: float) -> float:
    if upper <= lower:
        return 0.0
    return (value - lower) / (upper - lower)


def _evaluate_regressions(
    current_csv_path: Path,
    baseline_csv_path: Path,
    p95_threshold_pct: float,
    cost_threshold_pct: float,
) -> List[Dict[str, Any]]:
    current_df = pd.read_csv(current_csv_path)
    baseline_df = pd.read_csv(baseline_csv_path)

    merged = current_df.merge(
        baseline_df,
        on=["scenario", "db"],
        suffixes=("_current", "_baseline"),
        how="inner",
    )
    if merged.empty:
        return []

    regressions = []
    for _, row in merged.iterrows():
        p95_delta = _pct_change(row.get("p95_current"), row.get("p95_baseline"))

        cost_delta = None
        if "cost_index_current" in merged.columns and "cost_index_baseline" in merged.columns:
            cost_delta = _pct_change(row.get("cost_index_current"), row.get("cost_index_baseline"))

        p95_failed = p95_delta is not None and p95_delta > p95_threshold_pct
        cost_failed = cost_delta is not None and cost_delta > cost_threshold_pct

        if p95_failed or cost_failed:
            regressions.append(
                {
                    "scenario": row["scenario"],
                    "db": row["db"],
                    "p95_delta_pct": round(p95_delta or 0.0, 3),
                    "cost_delta_pct": round(cost_delta or 0.0, 3) if cost_delta is not None else None,
                    "p95_threshold_pct": p95_threshold_pct,
                    "cost_threshold_pct": cost_threshold_pct,
                }
            )

    return regressions


def _pct_change(current: Any, baseline: Any) -> float | None:
    try:
        current_value = float(current)
        baseline_value = float(baseline)
    except (TypeError, ValueError):
        return None

    if baseline_value == 0:
        return 0.0 if current_value == 0 else 100.0
    return ((current_value - baseline_value) / baseline_value) * 100


def _save_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "workload",
        "workload_version",
        "scenario",
        "db",
        "scenario_type",
        "runs",
        "concurrency",
        "warmup_runs",
        "warmup_errors",
        "successful_runs",
        "errors",
        "error_rate_pct",
        "throughput_ops_s",
        "wall_time_s",
        "avg",
        "p50",
        "p95",
        "p99",
        "min",
        "max",
        "cpu_avg_pct",
        "cpu_p95_pct",
        "cpu_max_pct",
        "mem_avg_mb",
        "mem_p95_mb",
        "mem_max_mb",
        "resource_samples",
        "resource_errors",
        "cost_index",
        "latency_norm",
        "cost_norm",
        "decision_score",
        "latency_winner",
        "cost_winner",
        "recommended",
    ]

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file_obj:
        writer = csv.DictWriter(file_obj, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    random.seed(args.seed)

    total_started = time.time()
    output_path = Path(args.output)

    try:
        workload = load_workload(args.workload)
    except WorkloadValidationError as exc:
        print(f"[ERR] Invalid workload: {exc}")
        return 1

    if args.smoke:
        try:
            _apply_smoke_profile(
                workload,
                max_runs=args.smoke_max_runs,
                max_concurrency=args.smoke_max_concurrency,
                max_warmup_runs=args.smoke_max_warmup_runs,
            )
        except ValueError as exc:
            print(f"[ERR] Invalid smoke configuration: {exc}")
            return 1

    try:
        context = _build_runtime_context(sample_users=args.sample_users)
    except Exception as exc:
        print(f"[ERR] Unable to build runtime context: {exc}")
        return 1

    if args.smoke:
        print(
            "[INFO] Smoke mode enabled: "
            f"runs<={args.smoke_max_runs}, "
            f"concurrency<={args.smoke_max_concurrency}, "
            f"warmup<={args.smoke_max_warmup_runs}."
        )

    print("\n" + "=" * 76)
    print("PitLane Automated DB Selection + Regression Runner")
    print(f"Workload: {workload['name']} (v{workload['version']})")
    print(f"Source:   {workload['source_path']}")
    print("=" * 76)

    results: List[Dict[str, Any]] = []
    sample_interval = float(workload["defaults"]["sample_interval_seconds"])

    for scenario in workload["scenarios"]:
        scenario_id = scenario["id"]
        print("\n" + "-" * 76)
        print(
            f"Scenario: {scenario_id} | type={scenario['type']} "
            f"runs={scenario['runs']} concurrency={scenario['concurrency']}"
        )
        print("-" * 76)

        for db in scenario["targets"]:
            try:
                request_fn = _build_request_fn(scenario, db, context)
            except Exception as exc:
                print(f"  [SKIP] {db:<10} {exc}")
                continue

            sampler = DockerStatsSampler(
                container_name=DB_CONTAINER_MAP.get(db, ""),
                sample_interval_seconds=sample_interval,
            )

            sampler.start()
            try:
                metrics, errors = run_load(
                    request_fn=request_fn,
                    runs=scenario["runs"],
                    concurrency=scenario["concurrency"],
                    warmup_runs=scenario["warmup_runs"],
                )
            finally:
                sampler.stop()

            resource_metrics = sampler.summary()

            row: Dict[str, Any] = {
                "workload": workload["name"],
                "workload_version": workload["version"],
                "scenario": scenario_id,
                "db": db,
                "scenario_type": scenario["type"],
                "runs": scenario["runs"],
                "concurrency": scenario["concurrency"],
                **metrics,
                **resource_metrics,
            }
            row["cost_index"] = _cost_index(row)
            results.append(row)

            print(
                f"  {db:<10} avg={row['avg']:>8.3f}ms "
                f"p95={row['p95']:>8.3f}ms "
                f"ops/s={row['throughput_ops_s']:>8.2f} "
                f"cpu={float(row.get('cpu_avg_pct') or 0):>6.2f}% "
                f"mem={float(row.get('mem_avg_mb') or 0):>8.2f}MB "
                f"cost={row['cost_index']:>8.4f} "
                f"errors={row['errors']}"
            )

            if errors:
                print(f"    Sample error: {errors[0]}")

    if not results:
        print("[ERR] No benchmarks were executed. Check workload definitions and DB setup.")
        return 1

    _annotate_decision_scores(results)
    _save_csv(output_path, results)

    print("\n" + "=" * 76)
    print("Decision Summary")
    print("=" * 76)

    for scenario_name in sorted({row["scenario"] for row in results}):
        subset = [row for row in results if row["scenario"] == scenario_name]
        fastest = min(subset, key=lambda item: item["avg"])["db"]
        cheapest = min(subset, key=lambda item: item["cost_index"])["db"]
        recommended = min(subset, key=lambda item: item["decision_score"])["db"]
        print(
            f"  {scenario_name:<20} fastest={fastest:<9} "
            f"cheapest={cheapest:<9} recommended={recommended}"
        )

    if args.baseline:
        baseline_path = Path(args.baseline)
        if baseline_path.exists():
            regressions = _evaluate_regressions(
                current_csv_path=output_path,
                baseline_csv_path=baseline_path,
                p95_threshold_pct=args.regression_p95_threshold,
                cost_threshold_pct=args.regression_cost_threshold,
            )

            if regressions:
                regressions_path = output_path.parent / "regressions.csv"
                with regressions_path.open("w", newline="", encoding="utf-8") as file_obj:
                    writer = csv.DictWriter(file_obj, fieldnames=list(regressions[0].keys()))
                    writer.writeheader()
                    writer.writerows(regressions)

                print("\n[WARN] Regression thresholds exceeded:")
                for item in regressions:
                    print(
                        f"  {item['scenario']} / {item['db']} "
                        f"p95_delta={item['p95_delta_pct']}% "
                        f"cost_delta={item['cost_delta_pct']}%"
                    )
                print(f"  Details written to {regressions_path}")

                if args.fail_on_regression:
                    print("[ERR] Failing run due to regression threshold violations.")
                    return 2
            else:
                print("\n[OK] No regressions found against baseline.")
        else:
            print(f"\n[WARN] Baseline file not found: {baseline_path}")

    print("\n" + "=" * 76)
    print(f"[OK] Results saved to {output_path}")
    print(f"[OK] Total benchmark time: {time.time() - total_started:.1f}s")
    print("[OK] Next: python analyze.py  OR  uvicorn dashboard.main:app --reload")
    print("=" * 76)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
