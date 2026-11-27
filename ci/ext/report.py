#!/usr/bin/env python3
"""Build results.json and report.md from run.py output.

Verdicts come from pg_regress (`ok` / `not ok`) and are never altered.  For each
failed test the raw diff hunk is saved, then run through the repository's
existing ci/filter_regression_diff.py to see whether the difference is already
known OrioleDB-vs-heap drift; the result is shown alongside the raw verdict,
never instead of it.  PATTERNS below labels the difference with a class.
"""
import argparse
import glob
import json
import os
import re
import subprocess
import sys
from collections import Counter, OrderedDict
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
RESULT_RE = re.compile(r"^(ok|not ok)\s+(\d+)\s+[-+]\s+(\S+)\s+(\d+) ms")

# Divergence classes.  Regexes are matched against the added/removed lines of
# a failed test's diff.  They annotate; they never change a verdict.  Add a
# class when a new kind of OrioleDB-vs-heap difference shows up.
PATTERNS = {
    "environment": [
        r"could not load library .*: Error loading shared library",
        r'language "plperl(u)?" does not exist',
        r'language "plpython3?u?" does not exist',
        r"^skipped \(",
        r"cargo not available",
    ],
    "bridging-notice": [
        r"index bridging is enabled for orioledb table",
        r"is supported only via index bridging for OrioleDB table",
        r"Options: index_bridging=true",
    ],
    "heap-introspection": [
        r"orioledb tuples does not have system attribute",
        r"Not implemented: orioledb_tuple_tid_valid",
        r"orioledb does not support TID scan",
        r"is not a heap",
        r"could not open file .*(_vm|_fsm)",
        r"block number \d+ is out of range",
        r'"[a-z0-9_]+" is not a table, materialized view, or TOAST table',
        r"could not read blocks? \d+",
        r"only heap AM is supported",
    ],
    "unsupported-feature": [
        r"orioledb tables? does not support",
        r'orioledb table "[^"]+" does not support',
        r"cannot use PREPARE TRANSACTION in transaction that uses orioledb table",
        r"OrioleDB does not support prepared transactions",
        r"unsupported alter table subcommand",
        r"will be implemented in future",
        r"is not supported for OrioleDB tables",
        r"orioledb does not support SERIALIZABLE",
        r"cursor can only scan forward",
        r"cannot REINDEX CONCURRENTLY primary index",
        r"exceeds orioledb maximum \d+",
        r"is not supported for OrioleDB tables yet",
    ],
    "plan-shape": [
        r"Custom Scan \(o_scan\)",
        r"Bitmap heap scan$",
    ],
    "no-index-only-scan": [
        r"Index Only Scan using",
    ],
    "access-method-listing": [
        r"access method orioledb",
        r"orioledb_tableam_handler",
        r"^\+?\s*orioledb\s*\|",
    ],
    "harness-limitation": [
        r'could not access file ".*regress',
        r"regress\.so",
        r"regress_setenv",
    ],
    "orioledb-objects-in-public": [
        r"\| orioledb_\w+ ",
        r"fetch_read_page_checkpoint_stats",
        r"pg_stopevent",
        r"^\s*orioledb(_\w+)?\s*$",
        r"Extra (views|extensions|tables|functions):",
        r'"name": "orioledb',
        r'"orioledb[A-Z]\w*"',
    ],
    "logical-decoding-output": [
        r"UPDATE: old-key: .* new-tuple:",
        r"opening a streamed block for transaction",
        r"streaming (change|message) for transaction",
    ],
    "network": [
        r"Could not resolve host",
        r"Couldn't connect to server",
        r"Connection refused",
        r"Failed to connect to",
        r"Timeout was reached",
    ],
    "engine-error": [
        r"attempted to delete invisible tuple",
        r"cache lookup failed for relation",
        r"could not open relation with OID \d+",
        r"undo record was cleaned concurrently",
        r"PANIC:",
    ],
}


def parse_regression_out(path):
    tests = OrderedDict()
    if not os.path.exists(path):
        return tests
    for line in open(path, errors="replace"):
        m = RESULT_RE.match(line)
        if m:
            tests[m.group(3)] = {"name": m.group(3), "raw": "pass" if m.group(1) == "ok" else "fail",
                                 "duration_ms": int(m.group(4))}
    return tests


def split_diffs(path):
    """regression.diffs -> {test: diff_text}; the results/<test>.out path names the test."""
    out = {}
    if not os.path.exists(path):
        return out
    cur, buf = None, []
    for line in open(path, errors="replace"):
        if line.startswith("diff "):
            if cur:
                out[cur] = "".join(buf)
            cur, buf = None, [line]
            m = re.search(r"/results/([^/\s]+)\.out", line)
            cur = m.group(1) if m else f"unknown_{len(out)}"
        else:
            buf.append(line)
    if cur:
        out[cur] = "".join(buf)
    return out


def run_filter(filter_script, diff_path):
    if not filter_script or not os.path.exists(filter_script):
        return None, "filter script not available"
    r = subprocess.run([sys.executable, filter_script, "--diff", diff_path],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if r.returncode != 0:
        return None, (r.stderr.strip().splitlines() or ["filter failed"])[-1]
    return r.stdout.strip(), None


def classify(diff_text):
    changed = [l[1:] for l in diff_text.splitlines() if l[:1] in "+-" and not l.startswith(("+++", "---"))]
    classes = []
    for cls, regexes in PATTERNS.items():
        if any(re.search(rx, l) for rx in regexes for l in changed):
            classes.append(cls)
    return classes


def status_for(test):
    if test["raw"] == "pass":
        return "compatible"
    if test.get("filter_error"):
        return "fail"
    if test.get("residual") == "":
        return "known-drift"
    if "environment" in test.get("divergences", []):
        return "environment"
    if "unsupported-feature" in test.get("divergences", []) or "heap-introspection" in test.get("divergences", []):
        return "unsupported"
    return "fail"


def build(results_dir, filter_script, manifest=None):
    # One run.json, or one run-group<N>.json per server configuration.
    # Oldest first so a later re-run of one suite (run.py --only) replaces its
    # earlier entry instead of appearing twice.
    paths = sorted(glob.glob(os.path.join(results_dir, "run*.json")), key=os.path.getmtime)
    runs = [json.load(open(p)) for p in paths]
    if not runs:
        raise SystemExit(f"no run*.json under {results_dir}")
    run = dict(runs[0])
    latest = OrderedDict()
    for r in runs:
        for m in r["suites"]:
            latest[m["name"]] = m
    run["suites"] = list(latest.values())
    # Suites the manifest skips never reach a group; list them anyway so the
    # table accounts for every manifest entry.
    seen = {m["name"] for m in run["suites"]}
    if manifest and os.path.exists(manifest):
        for e in json.load(open(manifest))["extensions"]:
            if e.get("skip") and e["name"] not in seen:
                run["suites"].append({"name": e["name"], "kind": e["kind"], "tag": e.get("tag"),
                                      "status": "skipped", "reason": e["skip"], "notes": e.get("notes", "")})
    suites = []
    for meta in run["suites"]:
        name = meta["name"]
        d = os.path.join(results_dir, name)
        suite = {"name": name, "kind": meta["kind"],
                 "tag": meta.get("tag") if meta["kind"] == "external" else run.get("pg_tag"),
                 "server_settings": meta.get("server_settings"), "server_crash": meta.get("server_crash", False),
                 "status": meta["status"], "reason": meta.get("reason"), "notes": meta.get("notes", ""),
                 "runner": meta.get("runner", "pg_regress"),
                 "no_installcheck": meta.get("no_installcheck", False), "tests": []}
        # pg_regress removes regression.out (and regression.diffs) when every test
        # passed, so fall back to the captured stdout, which has the same lines.
        tests = parse_regression_out(os.path.join(d, "regression.out"))
        if not tests:
            tests = parse_regression_out(os.path.join(d, "pg_regress.log"))
        diffs = split_diffs(os.path.join(d, "regression.diffs"))
        os.makedirs(os.path.join(d, "diffs"), exist_ok=True)
        for tname, t in tests.items():
            if tname in diffs:
                p = os.path.join(d, "diffs", f"{tname}.diff")
                open(p, "w").write(diffs[tname])
                t["diff"] = os.path.relpath(p, results_dir)
                t["divergences"] = classify(diffs[tname])
                residual, err = run_filter(filter_script, p)
                if err:
                    t["filter_error"] = err
                else:
                    t["residual"] = residual
                    if residual:
                        rp = os.path.join(d, "diffs", f"{tname}.residual.diff")
                        open(rp, "w").write(residual + "\n")
                        t["residual_diff"] = os.path.relpath(rp, results_dir)
            elif t["raw"] == "fail":
                t["divergences"] = ["no-diff-recorded"]
            t["status"] = status_for(t)
            t.pop("residual", None)
            suite["tests"].append(t)
        c = Counter(t["status"] for t in suite["tests"])
        suite["summary"] = {"tests": len(suite["tests"]), "raw_pass": sum(1 for t in suite["tests"] if t["raw"] == "pass"),
                            "compatible": c["compatible"], "known_drift": c["known-drift"],
                            "unsupported": c["unsupported"], "environment": c["environment"], "fail": c["fail"],
                            "pass_after_filter": c["compatible"] + c["known-drift"],
                            "divergences": sorted({x for t in suite["tests"] for x in t.get("divergences", [])})}
        suites.append(suite)
    return {"generated": datetime.now(timezone.utc).isoformat(), "pg_tag": run.get("pg_tag"),
            "server": run["server"], "settings": run["settings"], "suites": suites}


ICON = {"compatible": "✅", "known-drift": "🟡", "unsupported": "🚫", "environment": "🔧", "fail": "❌"}


def md_escape(s):
    return (s or "").replace("|", "\\|").replace("\n", " ")


def write_markdown(res, path, artifact_prefix=""):
    L = []
    srv = res["server"]
    L.append("## OrioleDB extension compatibility report\n")
    L.append(f"**Server:** `{srv['version']}`  ")
    L.append(f"**OrioleDB:** `{srv.get('orioledb_version')}` commit `{srv.get('orioledb_commit')}`  ")
    L.append(f"**PostgreSQL source tag:** `{res['pg_tag']}`  ")
    L.append(f"**Generated:** {res['generated']}  ")
    L.append("**Server settings applied:** `default_table_access_method=orioledb` with `orioledb` preloaded; "
             "suites whose own temp-config asks for more start-time settings ran on a server configured that way "
             "(listed in the *Server* column).\n")
    L.append("Suites are the extensions' own upstream regression tests, unmodified. A test counts as **same as heap** when "
             "its output matches upstream's expected file exactly. **Expected drift** is a test whose only differences are "
             "ones OrioleDB already documents, such as `Custom Scan (o_scan)` in an EXPLAIN or the `index bridging is enabled` "
             "NOTICE; the repository's existing `ci/filter_regression_diff.py` decides that, never this harness. "
             "🚫 **documented limit**: the diff names a feature OrioleDB does not support, or heap-only page inspection. "
             "🔧 **image problem**: the test image is missing something (a shared library, a procedural language), not OrioleDB. "
             "❌ **other failure**: everything else, and the part worth reading.\n")

    ran = [s for s in res["suites"] if s["status"] == "ran"]
    tot = Counter()
    for s in ran:
        for k in ("tests", "raw_pass", "pass_after_filter", "compatible", "known_drift", "unsupported", "environment", "fail"):
            tot[k] += s["summary"][k]
    fully_raw = sum(1 for s in ran if s["summary"]["tests"] and s["summary"]["raw_pass"] == s["summary"]["tests"])
    fully_filt = sum(1 for s in ran if s["summary"]["tests"] and s["summary"]["pass_after_filter"] == s["summary"]["tests"])
    other = Counter(s["status"] for s in res["suites"] if s["status"] != "ran")
    L.append("### Totals\n")
    L.append(f"- Extensions run: **{len(ran)}** of {len(res['suites'])}"
             + (" (" + ", ".join(f"{v} {k}" for k, v in sorted(other.items())) + ")" if other else ""))
    L.append(f"- Extensions where every test is the same as heap: **{fully_raw}/{len(ran)}**; "
             f"counting expected drift as fine: **{fully_filt}/{len(ran)}**")
    crashed = [s["name"] for s in res["suites"] if s.get("server_crash")]
    if crashed:
        L.append(f"- 💥 **Server crashed** during: **{', '.join(crashed)}** (see the server log for that group in the artifact)")
    L.append(f"- Tests: **{tot['tests']}** total; **{tot['raw_pass']}** same as heap; **{tot['known_drift']}** expected drift; "
             f"**{tot['unsupported']}** documented limit; **{tot['environment']}** image problem; **{tot['fail']}** other failure\n")

    L.append("### Extensions\n")
    def server_cell(s):
        extra = {k: v for k, v in (s.get("server_settings") or {}).items()
                 if not (k == "default_table_access_method" or (k == "shared_preload_libraries" and v == "orioledb"))}
        return md_escape(", ".join(f"{k}={v}" for k, v in extra.items())) or "plain"
    def kind_cell(s):
        r = s.get("runner", "pg_regress")
        return s["kind"] if r == "pg_regress" else f"{s['kind']} ({r})"
    L.append("| Extension | Kind | Version | Server | Tests | Same as heap | Same or expected drift | 🚫 Limit | 🔧 Image | ❌ Other | What differs | Notes |")
    L.append("|---|---|---|---|---:|---:|---:|---:|---:|---:|---|---|")
    for s in res["suites"]:
        if s["status"] != "ran":
            L.append(f"| {s['name']} | {kind_cell(s)} | {s['tag'] or ''} | {server_cell(s)} | — | — | — | — | — | — | *{s['status']}* | {md_escape(s.get('reason') or s['notes'])} |")
            continue
        sm = s["summary"]
        if s.get("server_crash"):
            sm = dict(sm, divergences=["💥 server crashed during suite"] + sm["divergences"])
        L.append(f"| {s['name']} | {kind_cell(s)} | {s['tag'] or ''} | {server_cell(s)} | {sm['tests']} | {sm['raw_pass']} | {sm['pass_after_filter']} | "
                 f"{sm['unsupported']} | {sm['environment']} | {sm['fail']} | {', '.join(sm['divergences'])} | {md_escape(s['notes'])} |")
    L.append("")

    L.append("### Per-test results\n")
    for s in res["suites"]:
        if s["status"] != "ran" or not s["tests"]:
            continue
        sm = s["summary"]
        L.append(f"<details><summary><b>{s['name']}</b> — {sm['raw_pass']}/{sm['tests']} same as heap, {sm['pass_after_filter']}/{sm['tests']} counting expected drift</summary>\n")
        L.append("| # | Test | pg_regress | Status | What differs | Duration | Diff |")
        L.append("|---:|---|---|---|---|---:|---|")
        for i, t in enumerate(s["tests"], 1):
            diff = f"[diff]({artifact_prefix}{t['diff']})" if t.get("diff") else ""
            if t.get("residual_diff"):
                diff += f" [residual]({artifact_prefix}{t['residual_diff']})"
            div = ", ".join(t.get("divergences", []))
            if t.get("filter_error"):
                div = (div + "; " if div else "") + f"drift filter error: {md_escape(t['filter_error'])}"
            L.append(f"| {i} | {t['name']} | {t['raw']} | {ICON[t['status']]} {t['status']} | "
                     f"{div} | {t['duration_ms']} ms | {diff} |")
        L.append("\n</details>\n")
    open(path, "w").write("\n".join(L) + "\n")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", default=os.path.join(HERE, "work", "results"))
    ap.add_argument("--filter-script", default=os.path.join(HERE, "..", "filter_regression_diff.py"))
    ap.add_argument("--manifest", default=os.path.join(HERE, "extensions.json"))
    ap.add_argument("--artifact-prefix", default="", help="URL prefix for diff links in the markdown")
    ap.add_argument("--step-summary", action="store_true", help="also append report.md to $GITHUB_STEP_SUMMARY")
    args = ap.parse_args()

    res = build(args.results, args.filter_script, args.manifest)
    json.dump(res, open(os.path.join(args.results, "results.json"), "w"), indent=2)
    write_markdown(res, os.path.join(args.results, "report.md"), args.artifact_prefix)
    if args.step_summary and os.environ.get("GITHUB_STEP_SUMMARY"):
        open(os.environ["GITHUB_STEP_SUMMARY"], "a").write(open(os.path.join(args.results, "report.md")).read())
    ran = [s for s in res["suites"] if s["status"] == "ran"]
    print(f"{len(ran)} suites ran; results.json and report.md written to {args.results}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
