#!/usr/bin/env python3
"""Criterion baseline gate for tracing-flare.

Criterion reports relative changes but does not fail a process on regression. This script
reads the machine-readable `estimates.json` files and compares the mean wall time of each
benchmark against a committed baseline.

Usage:
    # From the package root, after
    # `cargo bench --features compression-gzip --bench flare -- --noplot`
    python bench/gate.py --threshold 0.10

    # Refresh the baseline after an intentional performance change
    python bench/gate.py --update

Exit codes:
    0  all benchmarks within threshold (or baseline updated)
    1  regression / missing benchmark / malformed data
    2  usage or filesystem error
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

SCHEMA = 2
SCRIPT_DIR = Path(__file__).resolve().parent
BENCH_DIR = SCRIPT_DIR
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CRITERION = REPO_ROOT / "target" / "criterion"
DEFAULT_BASELINE = BENCH_DIR / "baseline.json"
DEFAULT_POLICY = BENCH_DIR / "policy.json"
BENCH_SOURCE = REPO_ROOT / "benches" / "flare.rs"
THRESHOLD = 0.10


def collect(criterion_dir: Path) -> dict[str, dict[str, float]]:
    """Return benchmark id -> {mean, lower, upper} nanoseconds (95% CI)."""
    results: dict[str, dict[str, float]] = {}
    if not criterion_dir.is_dir():
        raise FileNotFoundError(f"criterion dir not found: {criterion_dir}")
    for estimates in criterion_dir.rglob("new/estimates.json"):
        # target/criterion/<group>/<bench>/new/estimates.json
        benchmark_dir = estimates.parent.parent
        benchmark_id = benchmark_dir.relative_to(criterion_dir).as_posix()
        data = json.loads(estimates.read_text(encoding="utf-8"))
        mean = data.get("mean", {})
        point = mean.get("point_estimate")
        if point is None:
            raise ValueError(f"missing mean.point_estimate in {estimates}")
        interval = mean.get("confidence_interval") or {}
        results[benchmark_id] = {
            "mean": float(point),
            "lower": float(interval.get("lower_bound", point)),
            "upper": float(interval.get("upper_bound", point)),
        }
    if not results:
        raise ValueError(f"no benchmark results under {criterion_dir}")
    return results


def machine_info() -> dict[str, object]:
    return {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
        "python": platform.python_version(),
    }


def machine_signature(machine: object) -> tuple[object, ...] | None:
    """Return stable hardware fields; ignore Python version drift."""
    if not isinstance(machine, dict):
        return None
    return (
        machine.get("system"),
        machine.get("release"),
        machine.get("machine"),
        machine.get("processor"),
        machine.get("cpu_count"),
    )


def discover_benchmarks(source: Path) -> set[str]:
    """Extract e2e benchmark ids from benches/flare.rs.

    Criterion output is not authoritative when a benchmark is missing from a stale
    build, so compare the source declarations too. `bench_case` call sites carry
    literal ids while the generic helper uses a variable.
    """
    text = source.read_text(encoding="utf-8")
    ids = set(re.findall(r'bench_function\(\s*"([^"]+)"', text))
    ids.update(re.findall(r'bench_case\(\s*c\s*,\s*"([^"]+)"', text, re.S))
    return {f"e2e/{name}" for name in ids}


def load_baseline(path: Path) -> dict:
    data = json.loads(path.read_text(encoding="utf-8"))
    schema = data.get("schema")
    if schema not in (1, SCHEMA):
        raise ValueError(f"unsupported baseline schema in {path}: {schema!r}")
    benchmarks = data.get("benchmarks")
    if not isinstance(benchmarks, dict) or not benchmarks:
        raise ValueError(f"baseline has no benchmarks: {path}")
    # Schema 1 stored bare mean nanoseconds; normalize to the schema-2 shape.
    if schema == 1:
        benchmarks = {
            name: {"mean": float(value), "lower": float(value), "upper": float(value)}
            for name, value in benchmarks.items()
        }
    return {"schema": schema, "machine": data.get("machine"), "benchmarks": benchmarks}


def write_baseline(path: Path, results: dict[str, dict[str, float]]) -> None:
    payload = {
        "schema": SCHEMA,
        "created": datetime.now(timezone.utc).isoformat(),
        "machine": machine_info(),
        "benchmarks": dict(sorted(results.items())),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def load_policy(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    advisory = data.get("advisory", [])
    if not isinstance(advisory, list):
        raise ValueError(f"policy advisory must be a list: {path}")
    return {str(name) for name in advisory}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--criterion-dir", type=Path, default=DEFAULT_CRITERION)
    parser.add_argument("--baseline", type=Path, default=DEFAULT_BASELINE)
    parser.add_argument("--policy", type=Path, default=DEFAULT_POLICY)
    parser.add_argument(
        "--threshold",
        type=float,
        default=THRESHOLD,
        help="relative regression allowed before failing (default: 0.10 = 10%%)",
    )
    parser.add_argument("--update", action="store_true", help="write the current results as the new baseline")
    parser.add_argument(
        "--allow-missing",
        action="store_true",
        help="do not fail when a baseline benchmark produced no result",
    )
    parser.add_argument(
        "--point-only",
        action="store_true",
        help="fail on point-estimate regression even when confidence intervals overlap",
    )
    parser.add_argument(
        "--allow-machine-mismatch",
        action="store_true",
        help="compare anyway when the baseline was recorded on different hardware",
    )
    args = parser.parse_args()

    try:
        current = collect(args.criterion_dir)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"gate: cannot read benchmark results: {error}", file=sys.stderr)
        return 2

    try:
        defined = discover_benchmarks(BENCH_SOURCE)
    except OSError as error:
        print(f"gate: cannot read benchmark source: {error}", file=sys.stderr)
        return 2

    if args.update:
        if not args.criterion_dir.is_dir():
            print(
                "gate: refusing to update baseline: no criterion output "
                "(run cargo bench first)",
                file=sys.stderr,
            )
            return 2
        missing = sorted(defined - set(current))
        if missing:
            print(
                "gate: refusing partial baseline update; missing results: "
                + ", ".join(missing),
                file=sys.stderr,
            )
            return 2
        write_baseline(args.baseline, current)
        print(f"gate: wrote {len(current)} benchmarks to {args.baseline}")
        return 0

    try:
        baseline = load_baseline(args.baseline)
        advisory = load_policy(args.policy)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"gate: cannot read baseline/policy: {error}", file=sys.stderr)
        print("gate: run with --update to create it after an intentional change", file=sys.stderr)
        return 2

    baseline_machine = baseline.get("machine")
    if machine_signature(baseline_machine) != machine_signature(machine_info()):
        message = (
            f"baseline machine mismatch: recorded={baseline_machine!r}, "
            f"current={machine_info()!r}"
        )
        if not args.allow_machine_mismatch:
            print(f"gate: {message}", file=sys.stderr)
            print(
                "gate: thresholds are not comparable across machines; regenerate "
                "with --update or pass --allow-machine-mismatch",
                file=sys.stderr,
            )
            return 1
        print(f"gate: warning: {message}")

    expected: dict[str, dict[str, float]] = baseline["benchmarks"]
    unbaselined = sorted(defined - set(expected))
    if unbaselined:
        print(
            "gate: FAILED: benchmarks defined but not baselined: "
            + ", ".join(unbaselined),
            file=sys.stderr,
        )
        return 1
    failures: list[str] = []
    noisy: list[str] = []
    advisory_hits: list[str] = []
    rows: list[tuple[str, dict[str, float], dict[str, float], float]] = []
    for name, base in sorted(expected.items()):
        new = current.get(name)
        if new is None:
            if not args.allow_missing:
                failures.append(f"{name}: missing from criterion output")
            continue
        base_mean = float(base["mean"])
        new_mean = float(new["mean"])
        delta = (new_mean - base_mean) / base_mean if base_mean else 0.0
        rows.append((name, base, new, delta))
        if delta > args.threshold:
            # A point-estimate regression inside overlapping confidence intervals is
            # machine noise, not a signal. Require the new lower bound to exceed the
            # baseline upper bound (or --point-only for strict mode).
            significant = float(new["lower"]) > float(base["upper"])
            message = (
                f"{name}: {new_mean / 1e6:.3f} ms vs baseline {base_mean / 1e6:.3f} ms "
                f"(+{delta * 100:.1f}%, threshold {args.threshold * 100:.0f}%)"
            )
            if significant or args.point_only:
                if name in advisory:
                    advisory_hits.append(message)
                else:
                    failures.append(message)
            else:
                noisy.append(message)

    print(f"{'benchmark':32} {'baseline':>12} {'current':>12} {'delta':>9}")
    for name, base, new, delta in rows:
        print(
            f"{name:32} {base['mean'] / 1e6:>10.3f}ms {new['mean'] / 1e6:>10.3f}ms "
            f"{delta * 100:>+8.1f}%"
        )

    extra = sorted(set(current) - set(expected))
    if extra:
        print(f"note: {len(extra)} new benchmark(s) not in baseline: {', '.join(extra)}")
    if noisy:
        print("note: point-estimate regression within confidence intervals (treated as noise):")
        for message in noisy:
            print(f"  - {message}")
    if advisory_hits:
        print("note: advisory regressions (disk-bound benchmarks; not gate-blocking):")
        for message in advisory_hits:
            print(f"  - {message}")

    if failures:
        print("\ngate: FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1
    print("\ngate: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
