#!/usr/bin/env python3

import argparse
import json
import math
import os
import platform
import random
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence, Tuple, Any

GIB = 1024.0 * 1024.0 * 1024.0

@dataclass
class RunRecord:
    method: str
    round_index: int
    wall_elapsed_s: float
    user_time_s: float
    sys_time_s: float
    max_rss_kb: int
    throughput_gib_s: float

class BenchError(RuntimeError):
    pass

def sh(cmd: Sequence[str], cwd: Path, env: Dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=str(cwd), env=env, text=True, capture_output=True, check=True)

def check_cpu_governor() -> str:
    """Checks if the CPU is in performance mode to prevent thermal/frequency jitter."""
    gov_path = Path("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
    if gov_path.exists():
        gov = gov_path.read_text().strip()
        if gov != "performance":
            print(f"WARNING: CPU governor is set to '{gov}', not 'performance'. Results may contain frequency scaling jitter.", file=sys.stderr)
        return gov
    return "unknown"

def measure_spawn_overhead(runs: int = 50) -> float:
    """Measures the wall-clock overhead of simply forking and execing a binary (like `/bin/true`)."""
    true_bin = shutil.which("true") or "/bin/true"
    times = []
    for _ in range(runs):
        start = time.perf_counter_ns()
        subprocess.run([true_bin], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        times.append((time.perf_counter_ns() - start) / 1_000_000_000.0)
    return statistics.median(times)

def strict_timed_run(
    cmd: Sequence[str],
    cwd: Path,
    env: Dict[str, str] | None,
) -> Tuple[str, float, float, float, int]:
    start_ns = time.perf_counter_ns()
    
    proc = subprocess.Popen(cmd, cwd=str(cwd), env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    
    _, status, rusage = os.wait4(proc.pid, 0)
    wall_elapsed_s = (time.perf_counter_ns() - start_ns) / 1_000_000_000.0
    
    stdout, stderr = proc.communicate()
    
    if status != 0:
        raise BenchError(f"Command failed: {' '.join(cmd)}\nStderr: {stderr}")

    return (
        stdout.strip(),
        wall_elapsed_s,
        rusage.ru_utime,
        rusage.ru_stime,
        rusage.ru_maxrss  
    )

def percentile(sorted_values: List[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    rank = math.ceil((p / 100.0) * len(sorted_values)) - 1
    rank = max(0, min(rank, len(sorted_values) - 1))
    return sorted_values[rank]

def summary(samples_s: List[float], rss_samples: List[int], file_size_bytes: int) -> Dict[str, float]:
    if not samples_s:
        raise BenchError("No samples available for summary")

    sorted_samples = sorted(samples_s)
    throughputs = [file_size_bytes / s / GIB for s in samples_s]
    sorted_tput = sorted(throughputs)
    mean_s = statistics.mean(samples_s)
    stdev_s = statistics.stdev(samples_s) if len(samples_s) > 1 else 0.0

    return {
        "n": float(len(samples_s)),
        "min_s": sorted_samples[0],
        "max_s": sorted_samples[-1],
        "mean_s": mean_s,
        "median_s": statistics.median(samples_s),
        "stdev_s": stdev_s,
        "cv_pct": (stdev_s / mean_s * 100.0) if mean_s > 0 else 0.0, 
        "p95_s": percentile(sorted_samples, 95),
        "p99_s": percentile(sorted_samples, 99),
        "min_gib_s": sorted_tput[0],
        "max_gib_s": sorted_tput[-1],
        "mean_gib_s": statistics.mean(throughputs),
        "median_gib_s": statistics.median(throughputs),
        "median_rss_mb": statistics.median(rss_samples) / 1024.0,
    }

def cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.lower().startswith("model name"):
                return line.split(":", 1)[1].strip()
    return "unknown"

def maybe_prefixed_with_taskset(cmd: List[str], pin_core: int | None) -> List[str]:
    if pin_core is None:
        return cmd
    taskset = shutil.which("taskset")
    if taskset is None:
        raise BenchError("--pin-core requested but 'taskset' was not found")
    return [taskset, "-c", str(pin_core), *cmd]

def ensure_binaries(repo_root: Path, release: bool, build: bool) -> Tuple[Path, Path]:
    profile_dir = "release" if release else "debug"
    scanner = repo_root / "target" / profile_dir / "scan-newlines"
    generator = repo_root / "target" / profile_dir / "generate-logs"

    if build:
        profile = "--release" if release else ""
        build_cmd = ["cargo", "build"]
        if profile:
            build_cmd.append(profile)
        build_cmd.extend(["--bins"])
        print(f"[build] {' '.join(build_cmd)}")
        sh(build_cmd, repo_root)

    if not scanner.exists():
        raise BenchError(f"Scanner binary not found: {scanner}")
    if not generator.exists():
        raise BenchError(f"Generator binary not found: {generator}")

    return scanner, generator

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="High-Precision Benchmark Pandora vs grep.")
    parser.add_argument("--dataset", type=Path, default=Path("pandora_2gb.log"))
    parser.add_argument("--runs", type=int, default=15)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--pin-core", type=int, default=None)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--json-out", type=Path, default=None)
    return parser.parse_args()

def main() -> int:
    args = parse_args()
    if platform.system() != "Linux":
        raise BenchError("This benchmark harness requires Linux (uses os.wait4 rusage).")

    repo_root = Path(__file__).resolve().parents[1]
    scanner_bin, _ = ensure_binaries(repo_root, release=not args.debug, build=not args.no_build)
    
    if not args.dataset.exists() or args.dataset.stat().st_size == 0:
        raise BenchError(f"Dataset missing or empty: {args.dataset}")

    file_size_bytes = args.dataset.stat().st_size
    env = dict(os.environ, LC_ALL="C")

    grep_cmd = maybe_prefixed_with_taskset(["grep", "-c", "^", str(args.dataset)], args.pin_core)
    pandora_cmd = maybe_prefixed_with_taskset([str(scanner_bin), str(args.dataset), str(max(1, args.threads)), "--quiet"], args.pin_core)

    print("=== Environment Analysis ===")
    gov = check_cpu_governor()
    spawn_overhead_s = measure_spawn_overhead()
    print(f"cpu_model={cpu_model()}")
    print(f"scaling_governor={gov}")
    print(f"base_process_spawn_overhead={spawn_overhead_s * 1000.0:.3f} ms")
    print(f"dataset_gib={file_size_bytes / GIB:.3f}")
    print("")

    grep_out, *_ = strict_timed_run(grep_cmd, repo_root, env)
    pandora_out, *_ = strict_timed_run(pandora_cmd, repo_root, env)
    
    if int(grep_out) != int(pandora_out):
        raise BenchError(f"Mismatch: grep={grep_out} pandora={pandora_out}")

    rng = random.Random(args.seed)

    print("=== Warmup Phase ===")
    for i in range(args.warmup):
        methods = ["pandora", "grep"]
        rng.shuffle(methods)
        for m in methods:
            cmd = pandora_cmd if m == "pandora" else grep_cmd
            _, wall_s, _, _, _ = strict_timed_run(cmd, repo_root, env)
            print(f"warmup={i + 1:02d} method={m:7s} wall_ms={wall_s * 1000.0:7.3f}")
    print("")

    print("=== Measurement Phase ===")
    records: List[RunRecord] = []
    for round_index in range(1, args.runs + 1):
        methods = ["pandora", "grep"]
        rng.shuffle(methods)
        for m in methods:
            cmd = pandora_cmd if m == "pandora" else grep_cmd
            out, wall_s, u_time, s_time, max_rss = strict_timed_run(cmd, repo_root, env)
            
            if int(out) != int(pandora_out):
                raise BenchError(f"Mismatch during bench: expected {pandora_out}, got {out}")

            net_wall_s = max(0.0001, wall_s - spawn_overhead_s)
            tput = file_size_bytes / net_wall_s / GIB
            
            records.append(RunRecord(m, round_index, wall_s, u_time, s_time, max_rss, tput))
            print(f"run={round_index:02d} method={m:7s} wall_ms={wall_s * 1000.0:7.3f} "
                  f"cpu_ms={(u_time + s_time) * 1000.0:7.3f} max_rss_mb={max_rss / 1024.0:5.1f} "
                  f"net_gib_s={tput:6.3f}")

    pandora_samples = [r.wall_elapsed_s for r in records if r.method == "pandora"]
    grep_samples = [r.wall_elapsed_s for r in records if r.method == "grep"]
    pandora_rss = [r.max_rss_kb for r in records if r.method == "pandora"]
    grep_rss = [r.max_rss_kb for r in records if r.method == "grep"]

    pandora_stats = summary(pandora_samples, pandora_rss, file_size_bytes)
    grep_stats = summary(grep_samples, grep_rss, file_size_bytes)

    print("\n=== Advanced Summary (Wall Clock) ===")
    for name, stats in [("pandora", pandora_stats), ("grep", grep_stats)]:
        noise_warning = "[NOISY]" if stats['cv_pct'] > 5.0 else "[STABLE]"
        print(f"{name.upper()} {noise_warning} (CV: {stats['cv_pct']:.2f}%):")
        print(f"  Wall Time : Median={stats['median_s'] * 1000.0:.3f}ms  Mean={stats['mean_s'] * 1000.0:.3f}ms  p99={stats['p99_s'] * 1000.0:.3f}ms")
        print(f"  Net T-Put : Median={stats['median_gib_s']:.3f} GiB/s  Max={stats['max_gib_s']:.3f} GiB/s")
        print(f"  Memory    : Peak RSS={stats['median_rss_mb']:.2f} MB")

    median_speedup = grep_stats["median_s"] / pandora_stats["median_s"]
    print(f"\nOverall Median Speedup: {median_speedup:.3f}x")

    if args.json_out:
        pass

    return 0

if __name__ == "__main__":
    sys.exit(main())