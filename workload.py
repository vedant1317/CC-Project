"""
workload.py
Load, validate, and resolve PitLane workload JSON definitions.
"""

from __future__ import annotations

import json
import random
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Mapping

SUPPORTED_DATABASES = {"postgres", "dynamo", "mongodb"}
TOKEN_PATTERN = re.compile(r"\$[A-Za-z0-9_]+")
DEFAULT_WORKLOAD_PATH = Path("workloads/default_workload.json")

BUILTIN_COMPATIBILITY = {
    "point_lookup": {"postgres", "dynamo", "mongodb"},
    "complex_join": {"postgres", "mongodb"},
    "bulk_write": {"postgres", "dynamo", "mongodb"},
    "range_query": {"postgres", "mongodb"},
    "aggregation": {"postgres", "mongodb"},
}

QUERY_KIND_ALIASES = {
    "postgres": {"sql", "postgres", "postgresql"},
    "mongodb": {"mongo", "mongodb"},
    "dynamo": {"dynamo", "dynamodb"},
}

POSTGRES_CUSTOM_MODES = {"fetchone", "fetchall", "execute", "executemany"}
MONGO_CUSTOM_OPERATIONS = {"find_one", "find", "aggregate", "insert_one", "insert_many"}
DYNAMO_CUSTOM_OPERATIONS = {"get_item", "put_item", "query"}


class WorkloadValidationError(ValueError):
    """Raised when a workload JSON definition is invalid."""


def load_workload(path: str | None = None) -> Dict[str, Any]:
    """Read a workload file from disk and return a normalized dict."""
    workload_path = Path(path) if path else DEFAULT_WORKLOAD_PATH
    if not workload_path.exists():
        raise WorkloadValidationError(f"Workload file not found: {workload_path}")

    try:
        raw = json.loads(workload_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise WorkloadValidationError(
            f"Invalid JSON in workload file {workload_path}: {exc}"
        ) from exc

    normalized = normalize_workload(raw, source_path=workload_path)
    normalized["source_path"] = str(workload_path)
    return normalized


def normalize_workload(raw: Any, source_path: Path) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise WorkloadValidationError("Workload root must be a JSON object.")

    defaults = _normalize_defaults(raw.get("defaults", {}))
    scenarios_raw = raw.get("scenarios")
    if not isinstance(scenarios_raw, list) or not scenarios_raw:
        raise WorkloadValidationError("Workload must include a non-empty 'scenarios' array.")

    seen_ids = set()
    scenarios = []
    for index, scenario_raw in enumerate(scenarios_raw, start=1):
        scenario = _normalize_scenario(scenario_raw, index=index, defaults=defaults)
        scenario_id = scenario["id"]
        if scenario_id in seen_ids:
            raise WorkloadValidationError(f"Duplicate scenario id '{scenario_id}'.")
        seen_ids.add(scenario_id)
        scenarios.append(scenario)

    return {
        "name": str(raw.get("name") or source_path.stem),
        "description": str(raw.get("description", "")).strip(),
        "version": str(raw.get("version", "1.0")),
        "schema": raw.get("schema", {}),
        "defaults": defaults,
        "scenarios": scenarios,
    }


def _normalize_defaults(raw_defaults: Any) -> Dict[str, Any]:
    if raw_defaults is None:
        raw_defaults = {}
    if not isinstance(raw_defaults, dict):
        raise WorkloadValidationError("Workload 'defaults' must be an object.")

    return {
        "runs": _coerce_positive_int(raw_defaults.get("runs", 200), "defaults.runs"),
        "concurrency": _coerce_positive_int(
            raw_defaults.get("concurrency", 16), "defaults.concurrency"
        ),
        "warmup_runs": _coerce_non_negative_int(
            raw_defaults.get("warmup_runs", 5), "defaults.warmup_runs"
        ),
        "sample_interval_seconds": _coerce_positive_float(
            raw_defaults.get("sample_interval_seconds", 0.5),
            "defaults.sample_interval_seconds",
        ),
    }


def _normalize_scenario(raw_scenario: Any, index: int, defaults: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw_scenario, dict):
        raise WorkloadValidationError(f"Scenario #{index} must be an object.")

    scenario_id = str(raw_scenario.get("id", "")).strip()
    if not scenario_id:
        raise WorkloadValidationError(f"Scenario #{index} is missing required field 'id'.")

    scenario_type = str(raw_scenario.get("type", "builtin")).strip().lower()
    if scenario_type not in {"builtin", "custom"}:
        raise WorkloadValidationError(
            f"Scenario '{scenario_id}' has invalid type '{scenario_type}'. "
            "Expected 'builtin' or 'custom'."
        )

    targets = raw_scenario.get("targets", ["postgres", "dynamo", "mongodb"])
    if not isinstance(targets, list) or not targets:
        raise WorkloadValidationError(f"Scenario '{scenario_id}' must define non-empty 'targets'.")

    normalized_targets = []
    seen_targets = set()
    for target in targets:
        db_name = str(target).strip().lower()
        if db_name not in SUPPORTED_DATABASES:
            raise WorkloadValidationError(
                f"Scenario '{scenario_id}' uses unsupported database '{target}'."
            )
        if db_name in seen_targets:
            raise WorkloadValidationError(
                f"Scenario '{scenario_id}' has duplicate target '{db_name}'."
            )
        seen_targets.add(db_name)
        normalized_targets.append(db_name)

    runs = _coerce_positive_int(raw_scenario.get("runs", defaults["runs"]), f"{scenario_id}.runs")
    concurrency = _coerce_positive_int(
        raw_scenario.get("concurrency", defaults["concurrency"]),
        f"{scenario_id}.concurrency",
    )
    warmup_runs = _coerce_non_negative_int(
        raw_scenario.get("warmup_runs", defaults["warmup_runs"]),
        f"{scenario_id}.warmup_runs",
    )

    params = raw_scenario.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise WorkloadValidationError(f"Scenario '{scenario_id}' has invalid 'params' (must be object).")

    scenario: Dict[str, Any] = {
        "id": scenario_id,
        "description": str(raw_scenario.get("description", "")).strip(),
        "type": scenario_type,
        "targets": normalized_targets,
        "runs": runs,
        "concurrency": concurrency,
        "warmup_runs": warmup_runs,
        "params": params,
    }

    if scenario_type == "builtin":
        builtin_name = str(raw_scenario.get("builtin", scenario_id)).strip().lower()
        _validate_builtin_contract(scenario_id, builtin_name, normalized_targets)

        scenario["builtin"] = builtin_name
        scenario["mock_queries"] = {}
    else:
        mock_queries = raw_scenario.get("mock_queries")
        if not isinstance(mock_queries, dict) or not mock_queries:
            raise WorkloadValidationError(
                f"Custom scenario '{scenario_id}' must define non-empty 'mock_queries'."
            )
        _validate_custom_contract(scenario_id, normalized_targets, mock_queries)

        scenario["builtin"] = ""
        scenario["mock_queries"] = mock_queries

    return scenario


def _validate_builtin_contract(scenario_id: str, builtin_name: str, targets: list[str]) -> None:
    if builtin_name not in BUILTIN_COMPATIBILITY:
        supported = ", ".join(sorted(BUILTIN_COMPATIBILITY))
        raise WorkloadValidationError(
            f"Scenario '{scenario_id}' uses unknown builtin '{builtin_name}'. "
            f"Supported builtins: {supported}."
        )

    unsupported_targets = [db for db in targets if db not in BUILTIN_COMPATIBILITY[builtin_name]]
    if unsupported_targets:
        unsupported = ", ".join(sorted(unsupported_targets))
        raise WorkloadValidationError(
            f"Scenario '{scenario_id}' builtin '{builtin_name}' does not support target(s): {unsupported}."
        )


def _validate_custom_contract(
    scenario_id: str,
    targets: list[str],
    mock_queries: Mapping[str, Any],
) -> None:
    missing_targets = [db for db in targets if db not in mock_queries]
    if missing_targets:
        missing = ", ".join(sorted(missing_targets))
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' is missing mock_queries for target(s): {missing}."
        )

    for db in targets:
        query_spec = mock_queries.get(db)
        if not isinstance(query_spec, dict):
            raise WorkloadValidationError(
                f"Custom scenario '{scenario_id}' target '{db}' must define an object query spec."
            )

        _validate_query_kind(scenario_id, db, query_spec)

        if db == "postgres":
            _validate_postgres_query_spec(scenario_id, query_spec)
        elif db == "mongodb":
            _validate_mongo_query_spec(scenario_id, query_spec)
        elif db == "dynamo":
            _validate_dynamo_query_spec(scenario_id, query_spec)


def _validate_query_kind(scenario_id: str, db: str, query_spec: Mapping[str, Any]) -> None:
    kind_raw = query_spec.get("kind")
    if kind_raw is None:
        return

    kind = str(kind_raw).strip().lower()
    allowed = QUERY_KIND_ALIASES[db]
    if kind not in allowed:
        expected = ", ".join(sorted(allowed))
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' target '{db}' has invalid kind '{kind_raw}'. "
            f"Expected one of: {expected}."
        )


def _validate_postgres_query_spec(scenario_id: str, query_spec: Mapping[str, Any]) -> None:
    query = query_spec.get("query")
    if not isinstance(query, str) or not query.strip():
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' postgres query must include a non-empty 'query'."
        )

    mode = str(query_spec.get("mode", "fetchall")).lower()
    if mode not in POSTGRES_CUSTOM_MODES:
        supported = ", ".join(sorted(POSTGRES_CUSTOM_MODES))
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' postgres mode '{mode}' is invalid. "
            f"Supported modes: {supported}."
        )

    if mode == "executemany" and not isinstance(query_spec.get("param_list"), list):
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' postgres mode 'executemany' requires 'param_list' list."
        )


def _validate_mongo_query_spec(scenario_id: str, query_spec: Mapping[str, Any]) -> None:
    collection = query_spec.get("collection")
    if not isinstance(collection, str) or not collection.strip():
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' mongodb query must include non-empty 'collection'."
        )

    operation = str(query_spec.get("operation", "find_one")).lower()
    if operation not in MONGO_CUSTOM_OPERATIONS:
        supported = ", ".join(sorted(MONGO_CUSTOM_OPERATIONS))
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' mongodb operation '{operation}' is invalid. "
            f"Supported operations: {supported}."
        )

    if operation == "insert_many" and not isinstance(query_spec.get("documents"), list):
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' mongodb operation 'insert_many' requires 'documents' list."
        )


def _validate_dynamo_query_spec(scenario_id: str, query_spec: Mapping[str, Any]) -> None:
    operation = str(query_spec.get("operation", "get_item")).lower()
    if operation not in DYNAMO_CUSTOM_OPERATIONS:
        supported = ", ".join(sorted(DYNAMO_CUSTOM_OPERATIONS))
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' dynamo operation '{operation}' is invalid. "
            f"Supported operations: {supported}."
        )

    if operation == "get_item" and not isinstance(query_spec.get("key"), dict):
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' dynamo operation 'get_item' requires 'key' object."
        )

    if operation == "put_item" and not isinstance(query_spec.get("item"), dict):
        raise WorkloadValidationError(
            f"Custom scenario '{scenario_id}' dynamo operation 'put_item' requires 'item' object."
        )

    if operation == "query":
        key_condition = query_spec.get("key_condition")
        if not isinstance(key_condition, dict):
            raise WorkloadValidationError(
                f"Custom scenario '{scenario_id}' dynamo operation 'query' requires 'key_condition' object."
            )
        if "pk_value" not in key_condition:
            raise WorkloadValidationError(
                f"Custom scenario '{scenario_id}' dynamo query requires key_condition.pk_value."
            )


def build_token_resolver(context: Mapping[str, Any]) -> Callable[[str], Any]:
    user_ids = list(context.get("user_ids", []))
    statuses = list(context.get("statuses", ["pending", "processing", "shipped", "delivered"]))

    range_start = context.get("range_start")
    range_end = context.get("range_end")

    def _resolve_token(token: str) -> Any:
        if token == "$uuid":
            return str(uuid.uuid4())

        if token == "$now_iso":
            return datetime.now(timezone.utc).isoformat()

        if token == "$today_iso":
            return datetime.now(timezone.utc).date().isoformat()

        if token == "$random_user_id":
            if not user_ids:
                raise WorkloadValidationError(
                    "Token '$random_user_id' was used but no user_ids are available."
                )
            return random.choice(user_ids)

        if token == "$random_status":
            return random.choice(statuses)

        if token == "$random_total":
            return round(random.uniform(10, 500), 2)

        if token == "$range_start_iso":
            if range_start is None:
                raise WorkloadValidationError("Token '$range_start_iso' is unavailable in current context.")
            return _datetime_to_iso(range_start)

        if token == "$range_end_iso":
            if range_end is None:
                raise WorkloadValidationError("Token '$range_end_iso' is unavailable in current context.")
            return _datetime_to_iso(range_end)

        if token.startswith("$randint_"):
            bounds = token.removeprefix("$randint_").split("_", 1)
            if len(bounds) != 2:
                raise WorkloadValidationError(
                    f"Invalid randint token '{token}'. Expected format: $randint_<min>_<max>."
                )
            lower, upper = int(bounds[0]), int(bounds[1])
            return random.randint(lower, upper)

        if token.startswith("$randfloat_"):
            bounds = token.removeprefix("$randfloat_").split("_", 1)
            if len(bounds) != 2:
                raise WorkloadValidationError(
                    f"Invalid randfloat token '{token}'. Expected format: $randfloat_<min>_<max>."
                )
            lower, upper = float(bounds[0]), float(bounds[1])
            return round(random.uniform(lower, upper), 4)

        raise WorkloadValidationError(f"Unsupported token '{token}' in workload payload.")

    return _resolve_token


def resolve_placeholders(payload: Any, token_resolver: Callable[[str], Any]) -> Any:
    """Recursively resolve supported token placeholders from any JSON-like payload."""
    if isinstance(payload, dict):
        return {key: resolve_placeholders(value, token_resolver) for key, value in payload.items()}

    if isinstance(payload, list):
        return [resolve_placeholders(item, token_resolver) for item in payload]

    if isinstance(payload, str):
        return _resolve_string(payload, token_resolver)

    return payload


def _resolve_string(value: str, token_resolver: Callable[[str], Any]) -> Any:
    tokens = TOKEN_PATTERN.findall(value)
    if not tokens:
        return value

    if len(tokens) == 1 and value == tokens[0]:
        return token_resolver(tokens[0])

    resolved = value
    for token in tokens:
        replacement = token_resolver(token)
        resolved = resolved.replace(token, str(replacement))
    return resolved


def _datetime_to_iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _coerce_positive_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or value <= 0:
        raise WorkloadValidationError(f"{field_name} must be a positive integer.")
    return value


def _coerce_non_negative_int(value: Any, field_name: str) -> int:
    if not isinstance(value, int) or value < 0:
        raise WorkloadValidationError(f"{field_name} must be a non-negative integer.")
    return value


def _coerce_positive_float(value: Any, field_name: str) -> float:
    if not isinstance(value, (int, float)) or value <= 0:
        raise WorkloadValidationError(f"{field_name} must be a positive number.")
    return float(value)
