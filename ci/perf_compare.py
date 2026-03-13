#!/usr/bin/env python3
"""Compare k6/stroppy TPC-C benchmark results between base and head branches.

Parses k6 summary JSON files (produced by stroppy-action with --summary-export).
Each file is a single JSON object: {"metrics": {"metric_name": {"avg":..., "med":..., ...}}}
"""

import argparse
import glob
import json
import os
import statistics
import sys


def parse_k6_summary(filepath):
    """Parse k6 summary JSON results file.

    Returns a flat dict of normalized metric values suitable for comparison.
    """
    with open(filepath) as f:
        data = json.load(f)

    raw_metrics = data.get("metrics", {})
    if not raw_metrics:
        return {}

    metrics = {}

    # iteration_duration — latency metrics (ms)
    it = raw_metrics.get("iteration_duration", {})
    if it:
        metrics["avg_duration_ms"] = it.get("avg", 0)
        metrics["med_duration_ms"] = it.get("med", 0)
        metrics["p90_duration_ms"] = it.get("p(90)", 0)
        metrics["p95_duration_ms"] = it.get("p(95)", 0)

    # iterations — throughput counter
    iters = raw_metrics.get("iterations", {})
    if iters:
        metrics["total_iterations"] = iters.get("count", 0)
        metrics["total_iterations_rate"] = iters.get("rate", 0)

    # run_query_duration — query latency
    qd = raw_metrics.get("run_query_duration", {})
    if qd:
        metrics["query_avg_ms"] = qd.get("avg", 0)
        metrics["query_p90_ms"] = qd.get("p(90)", 0)
        metrics["query_p95_ms"] = qd.get("p(95)", 0)

    # run_query_count — query throughput
    qc = raw_metrics.get("run_query_count", {})
    if qc:
        metrics["query_rate"] = qc.get("rate", 0)

    return metrics


def find_result_files(results_dir, num_runs, scale_factor=None):
    """Find stroppy JSON result files in the download directory.

    stroppy-action artifacts are downloaded as:
      results-dir/perf-results-{branch}-sf{SF}-{N}/stroppy-results.json
    If scale_factor is given, only match directories containing that sf.
    """
    pattern = os.path.join(results_dir, "**", "stroppy-results.json")
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        files = sorted(glob.glob(os.path.join(results_dir, "*.json")))
    if scale_factor is not None:
        sf_tag = f"-sf{scale_factor}-"
        files = [f for f in files if sf_tag in f]
    return files[:num_runs]


def load_run_results(results_dir, num_runs, scale_factor=None):
    """Load and parse all result files from a results directory."""
    files = find_result_files(results_dir, num_runs, scale_factor)
    all_metrics = []
    for filepath in files:
        print(f"Parsing: {filepath}", file=sys.stderr)
        metrics = parse_k6_summary(filepath)
        if metrics:
            all_metrics.append(metrics)
        else:
            print(f"Warning: no metrics found in {filepath}", file=sys.stderr)
    return all_metrics


def compute_medians(all_metrics):
    """Compute median values across all runs for each metric."""
    if not all_metrics:
        return {}
    keys = set()
    for m in all_metrics:
        keys.update(m.keys())
    medians = {}
    for key in sorted(keys):
        values = [m[key] for m in all_metrics if key in m]
        if values:
            medians[key] = statistics.median(values)
    return medians


def format_change(base_val, head_val, lower_is_better=False):
    """Format percentage change with direction indicator."""
    if base_val == 0:
        return "N/A"
    change = (head_val - base_val) / base_val * 100
    sign = "+" if change > 0 else ""
    if lower_is_better:
        indicator = " :white_check_mark:" if change < -2 else (" :warning:" if change > 2 else "")
    else:
        indicator = " :white_check_mark:" if change > 2 else (" :warning:" if change < -2 else "")
    return f"{sign}{change:.1f}%{indicator}"


def format_value(value, is_rate=False):
    """Format a metric value for display."""
    if is_rate:
        return f"{value:.1f}/s"
    return f"{value:.1f}ms"


def generate_markdown(base_medians, head_medians, config):
    """Generate markdown comparison table."""
    lines = []
    lines.append("## Performance Test Results (TPC-C)")
    lines.append("")
    lines.append("| Metric | Base | Head | Change |")
    lines.append("|--------|------|------|--------|")

    # Throughput metrics (higher is better)
    rate_metrics = [
        ("total_iterations_rate", "Iterations/s"),
        ("query_rate", "Queries/s"),
    ]

    for key, label in rate_metrics:
        base_val = base_medians.get(key)
        head_val = head_medians.get(key)
        if base_val is not None and head_val is not None:
            lines.append(
                f"| {label} | {format_value(base_val, is_rate=True)} "
                f"| {format_value(head_val, is_rate=True)} "
                f"| {format_change(base_val, head_val)} |"
            )

    # Count metric
    base_iters = base_medians.get("total_iterations")
    head_iters = head_medians.get("total_iterations")
    if base_iters is not None and head_iters is not None:
        lines.append(
            f"| Total iterations | {int(base_iters)} "
            f"| {int(head_iters)} "
            f"| {format_change(base_iters, head_iters)} |"
        )

    # Duration metrics (lower is better)
    duration_metrics = [
        ("avg_duration_ms", "Avg iteration duration"),
        ("med_duration_ms", "Median iteration duration"),
        ("p90_duration_ms", "P90 iteration duration"),
        ("p95_duration_ms", "P95 iteration duration"),
        ("query_avg_ms", "Avg query duration"),
        ("query_p90_ms", "P90 query duration"),
        ("query_p95_ms", "P95 query duration"),
    ]

    for key, label in duration_metrics:
        base_val = base_medians.get(key)
        head_val = head_medians.get(key)
        if base_val is not None and head_val is not None and (base_val > 0 or head_val > 0):
            lines.append(
                f"| {label} | {format_value(base_val)} "
                f"| {format_value(head_val)} "
                f"| {format_change(base_val, head_val, lower_is_better=True)} |"
            )

    lines.append("")
    lines.append(
        f"**Config**: {config['runs']} runs, {config['duration']} each, "
        f"scale_factor={config['scale_factor']}"
    )
    lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Compare TPC-C benchmark results")
    parser.add_argument("--base-dir", required=True, help="Directory with base branch results")
    parser.add_argument("--head-dir", required=True, help="Directory with head branch results")
    parser.add_argument("--runs", type=int, default=5, help="Number of benchmark runs")
    parser.add_argument("--duration", default="10m", help="Duration per run")
    parser.add_argument("--scale-factor", default="1",
                        help="TPC-C scale factor (comma-separated for multiple)")
    parser.add_argument("--output", default="comment.md", help="Output markdown file")
    args = parser.parse_args()

    scale_factors = [s.strip() for s in args.scale_factor.split(",")]
    sections = []

    for sf in scale_factors:
        base_metrics = load_run_results(args.base_dir, args.runs, scale_factor=sf)
        head_metrics = load_run_results(args.head_dir, args.runs, scale_factor=sf)

        if not base_metrics:
            print(f"Error: no base branch results found for scale_factor={sf}", file=sys.stderr)
            sys.exit(1)
        if not head_metrics:
            print(f"Error: no head branch results found for scale_factor={sf}", file=sys.stderr)
            sys.exit(1)

        base_medians = compute_medians(base_metrics)
        head_medians = compute_medians(head_metrics)

        config = {
            "runs": args.runs,
            "duration": args.duration,
            "scale_factor": sf,
        }

        sections.append(generate_markdown(base_medians, head_medians, config))

    markdown = "\n---\n\n".join(sections)
    with open(args.output, "w") as f:
        f.write(markdown)

    print(markdown)


if __name__ == "__main__":
    main()
