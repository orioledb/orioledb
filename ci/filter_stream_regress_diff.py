#!/usr/bin/env python3
# coding: utf-8

# This script filters out diff output where both files contain the same lines
# but in a different order. This is used for filtering pg_dumpall diffs between
# primary and standby in streaming replication tests, where OrioleDB may
# produce the same data in a different order.
#
# Expects unified diff format (diff -u).

from unidiff import PatchSet
import argparse
import sys

ap = argparse.ArgumentParser(
    description="Filter out order-only differences from diff output")
ap.add_argument("--diff", "-d", help="path to diff output file")
args = ap.parse_args()

if args.diff:
    with open(args.diff, "r", encoding="utf-8", errors="replace") as f:
        diff_text = f.read()
else:
    diff_text = sys.stdin.read()

patch_set = PatchSet(diff_text)
result = PatchSet([])

removed = []
added = []

for patched_file in list(patch_set):
    for hunk in patched_file:
        removed += [line.value for line in hunk if line.is_removed]
        added += [line.value for line in hunk if line.is_added]

if sorted(removed) != sorted(added):
    sys.exit(1)
else:
    sys.exit(0)
