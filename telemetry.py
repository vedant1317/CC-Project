"""
telemetry.py
Collect lightweight Docker CPU and memory samples during benchmark runs.
"""

from __future__ import annotations

import json
import re
import statistics
import subprocess
import threading
import time
from typing import Dict, List, Optional

MEMORY_PATTERN = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([A-Za-z]+)\s*$")


class DockerStatsSampler:
    """Poll docker stats for a container and summarize CPU/memory usage."""

    def __init__(self, container_name: str, sample_interval_seconds: float = 0.5):
        self.container_name = container_name
        self.sample_interval_seconds = max(sample_interval_seconds, 0.2)
        self.samples: List[Dict[str, float]] = []
        self.errors: List[str] = []

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if not self.container_name:
            return
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._collect_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3.0)

    def _collect_loop(self) -> None:
        while not self._stop_event.is_set():
            sample = self._sample_once()
            if sample:
                self.samples.append(sample)
            self._stop_event.wait(self.sample_interval_seconds)

    def _sample_once(self) -> Optional[Dict[str, float]]:
        cmd = [
            "docker",
            "stats",
            self.container_name,
            "--no-stream",
            "--format",
            "{{json .}}",
        ]

        try:
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=8,
                check=False,
            )
        except Exception as exc:
            self.errors.append(str(exc))
            return None

        if proc.returncode != 0:
            message = (proc.stderr or proc.stdout or "docker stats failed").strip()
            self.errors.append(message)
            return None

        line = proc.stdout.strip()
        if not line:
            return None

        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            self.errors.append(f"Unable to parse docker stats payload: {line}")
            return None

        cpu_pct = _parse_percent(row.get("CPUPerc", "0%"))
        mem_usage = str(row.get("MemUsage", "0MiB / 0MiB")).split("/")[0].strip()
        mem_mb = _parse_memory_to_mb(mem_usage)

        return {
            "cpu_pct": cpu_pct,
            "mem_mb": mem_mb,
        }

    def summary(self) -> Dict[str, Optional[float]]:
        cpu_values = [item["cpu_pct"] for item in self.samples if item.get("cpu_pct") is not None]
        mem_values = [item["mem_mb"] for item in self.samples if item.get("mem_mb") is not None]

        return {
            "cpu_avg_pct": _rounded_mean(cpu_values),
            "cpu_p95_pct": _rounded_percentile(cpu_values, 0.95),
            "cpu_max_pct": _rounded_max(cpu_values),
            "mem_avg_mb": _rounded_mean(mem_values),
            "mem_p95_mb": _rounded_percentile(mem_values, 0.95),
            "mem_max_mb": _rounded_max(mem_values),
            "resource_samples": len(self.samples),
            "resource_errors": len(self.errors),
        }


def _parse_percent(raw_value: str) -> float:
    cleaned = str(raw_value).strip().replace("%", "")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def _parse_memory_to_mb(raw_value: str) -> float:
    match = MEMORY_PATTERN.match(raw_value)
    if not match:
        return 0.0

    value = float(match.group(1))
    unit = match.group(2).lower()

    multiplier = {
        "b": 1 / (1024 * 1024),
        "kb": 1 / 1024,
        "kib": 1 / 1024,
        "mb": 1.0,
        "mib": 1.0,
        "gb": 1024.0,
        "gib": 1024.0,
        "tb": 1024.0 * 1024.0,
        "tib": 1024.0 * 1024.0,
    }.get(unit, 1.0)

    return value * multiplier


def _rounded_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(statistics.mean(values), 3)


def _rounded_percentile(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None

    ordered = sorted(values)
    index = int((len(ordered) - 1) * percentile)
    return round(ordered[index], 3)


def _rounded_max(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return round(max(values), 3)
