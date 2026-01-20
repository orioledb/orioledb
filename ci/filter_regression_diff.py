#!/usr/bin/env python3
# coding: utf-8

from unidiff import PatchSet
import argparse
import sys
import io
import re
import os

ap = argparse.ArgumentParser(description="Найти первую отличающуюся строку по permutation-блокам .out-файлов из unified diff.")
ap.add_argument("--diff", "-d", help="Путь к файлу diff (если не указан — читаем из stdin).")
args = ap.parse_args()

if args.diff:
	with open(args.diff, "r", encoding="utf-8", errors="replace") as f:
		diff_text = f.read()
else:
	diff_text = sys.stdin.read()

patch_set = PatchSet(diff_text)

allowedRegexes = {
	r"ERROR:  orioledb does not support SERIALIZABLE isolation level": ["*"],
	r"ERROR:  tuple to be locked has its primary key changed due to concurrent update": ["*"],
	r"ERROR:  Not implemented: orioledb_tuple_tid_valid": ["*"],
	r"ERROR:  REINDEX CONCURRENTLY is not supported for orioledb tables yet": ["*"],
	r"ERROR:  orioledb tables does not support CLUSTER": ["*"],
	r"ERROR:  orioledb table \"[a-z0-9_]+\" does not support VACUUM FULL": ["*"],
	r"ERROR:  cannot use PREPARE TRANSACTION in transaction that uses orioledb table": ["*"],
	r"\s+QUERY PLAN": ["*"],
	r"-+": ["*"],
	r"\(\d+ rows?\)": ["*"],
}

def is_allowed_line(testName, line):
	for regex, tests in allowedRegexes.items():
		if re.match(regex, line):
			for test in tests:
				if test == '*' or test == testName:
					return True
	return False


for patched_file in patch_set:
	scans = ["Bitmap Heap Scan", "Bitmap Index Scan", "Result", "Custom Scan"]
	index = 0
	lines = io.StringIO()
	testName = os.path.splitext(os.path.basename(patched_file.target_file))[0]
	for hunk in patched_file:
		plan_hunk=False
		for line in hunk:
			if any(scan in line.value for scan in scans):
				# import ipdb; ipdb.set_trace();
				print(f"{line}")
			if line.is_added:
				value = line.value
				if value == "\n" or re.match(r"[a-z0-9]+: (NOTICE|WARNING|DETAIL): ", value):
					pass
				elif not is_allowed_line(testName, value):
					lines.write(f"{value}")
		break
	value = lines.getvalue()
	if len(value) > 0:
		print(patched_file.target_file)
		print(value)
	lines.close()
