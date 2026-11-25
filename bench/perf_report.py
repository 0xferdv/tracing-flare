#!/usr/bin/env python3
"""串行采样 tracing-flare 的 release 吞吐，输出中位数/P95。

`tests/throughput.rs` 有 10 个 `#[ignore]` 基准。直接运行 `cargo test ... --ignored`
会让多个基准共享 CPU，数字会被系统性压低。本脚本强制 `--test-threads=1`，
多次运行同一套基准并汇总每个场景的 rate。

用法（包根目录）：

    python bench/perf_report.py
    python bench/perf_report.py --runs 5 --json bench/perf.json

退出码：
    0  采样成功
    1  cargo 失败或没有解析到指标
    2  参数/文件错误
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import median

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_JSON = SCRIPT_DIR / "perf_report.json"

RATE_RE = re.compile(r"(?:^|[.\s])(?P<label>[a-z0-9_().-]+): records=(?P<records>\d+) .*rate=(?P<rate>\d+)/s")
LEVEL_RE = re.compile(r"(?:^|[.\s])level=(?P<label>[a-z0-9_-]+) records=(?P<records>\d+) .*rate=(?P<rate>\d+)/s")


def parse_output(text: str) -> dict[str, dict[str, int]]:
    """Parse benchmark lines from cargo test output."""
    results: dict[str, dict[str, int]] = {}
    for line in text.splitlines():
        match = RATE_RE.search(line)
        if match:
            label = match.group("label")
            results[label] = {
                "records": int(match.group("records")),
                "rate": int(match.group("rate")),
            }
            continue
        match = LEVEL_RE.search(line)
        if match:
            results[f"production/{match.group('label')}"] = {
                "records": int(match.group("records")),
                "rate": int(match.group("rate")),
            }
    return results


def percentile(values: list[float], ratio: float) -> float:
    """Nearest-rank percentile for a small sample."""
    ordered = sorted(values)
    if not ordered:
        return math.nan
    index = max(0, min(len(ordered) - 1, math.ceil(ratio * len(ordered)) - 1))
    return ordered[index]


def run_once(args: argparse.Namespace) -> tuple[dict[str, dict[str, int]], str]:
    command = [
        "cargo",
        "test",
        "--release",
        "--test",
        "throughput",
    ]
    if args.mimalloc:
        command.extend(["--features", "mimalloc"])
    command.extend(["--", "--ignored", "--nocapture", "--test-threads=1"])
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if completed.returncode != 0:
        print(completed.stdout, file=sys.stderr)
        raise RuntimeError(f"cargo test failed with exit code {completed.returncode}")
    return parse_output(completed.stdout), " ".join(command)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=3, help="串行重复次数（默认 3）")
    parser.add_argument(
        "--mimalloc",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="是否启用 mimalloc feature（默认启用）",
    )
    parser.add_argument("--json", type=Path, default=None, help="可选：写 JSON 报告")
    args = parser.parse_args()
    if args.runs < 1:
        print("perf_report: --runs must be >= 1", file=sys.stderr)
        return 2

    samples: dict[str, list[float]] = {}
    command = ""
    try:
        for run in range(1, args.runs + 1):
            print(f"perf_report: run {run}/{args.runs}", file=sys.stderr)
            parsed, command = run_once(args)
            if not parsed:
                print("perf_report: no benchmark rates parsed", file=sys.stderr)
                return 1
            for label, values in parsed.items():
                samples.setdefault(label, []).append(float(values["rate"]))
    except (OSError, RuntimeError) as error:
        print(f"perf_report: {error}", file=sys.stderr)
        return 1

    report = {
        "created": datetime.now(timezone.utc).isoformat(),
        "machine": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
        },
        "command": command,
        "runs": args.runs,
        "mimalloc": args.mimalloc,
        "benchmarks": {},
    }
    print(f"{'benchmark':24} {'median':>12} {'p95':>12} {'min':>12} {'max':>12}")
    for label in sorted(samples):
        values = samples[label]
        row = {
            "median": median(values),
            "p95": percentile(values, 0.95),
            "min": min(values),
            "max": max(values),
            "samples": values,
        }
        report["benchmarks"][label] = row
        print(
            f"{label:24} {row['median']:>12.0f} {row['p95']:>12.0f} "
            f"{row['min']:>12.0f} {row['max']:>12.0f}"
        )

    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(
            json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
        print(f"perf_report: wrote {args.json}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
