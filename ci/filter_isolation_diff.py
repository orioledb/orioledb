#!/usr/bin/env python3
# coding: utf-8

from unidiff import PatchSet
import argparse
import sys
import io
import re
import os

def get_permutation_lines(filepath):
	output = []
	f = open(filepath, 'r')
	i = 1
	for line in f.readlines():
		if line.startswith('starting permutation: '):
			output.append(i)
		i = i + 1
	return output

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
	r"ERROR:  Not implemented: orioledb_set_tidrange": ["*"],
	r"ERROR:  REINDEX CONCURRENTLY is not supported for orioledb tables yet": ["*"],
	r"ERROR:  orioledb tables does not support CLUSTER": ["*"],
	r"ERROR:  orioledb table \"[a-z0-9_]+\" does not support VACUUM FULL": ["*"],
	r"ERROR:  cannot use PREPARE TRANSACTION in transaction that uses orioledb table": ["*"],
	r"c1      |(0,1) |0|0|4": ['eval-plan-qual'],
	r"QUERY PLAN": ['eval-plan-qual', 'merge-join', 'drop-index-concurrently-1'],
	r"\s+\d+(|\s+\d+)*": ['vacuum-no-cleanup-lock', 'stats', 'horizons'],
	r"setup of session s1 failed: ERROR:  current transaction is aborted, commands ignored until end of transaction block": ['stats'],
	r"key  |data": ['eval-plan-qual-trigger'],
	r"ERROR:  tuple concurrently updated": ['intra-grant-inplace'],
	r"\s*\d+": ['inherit-temp'],
	r"setup failed: ERROR:  function \"[a-z0-9_]+\" cannot be used here": ['insert-conflict-specconflict'],
	r"  3|setup1 updated by merge1 source not matched by merge2a": ['merge-update'],
	r"  2|setup1 updated by merge1": ['merge-update'],
	r"ERROR:  concurrent index creation is not supported for orioledb tables yet": ['*'],
	r"ERROR:  REFRESH MATERIALIZED VIEW CONCURRENTLY is not supported for orioledb tables yet": ['*'],
	r"ERROR:  could not serialize access due to concurrent delete": ['partition-key-update-3'],
}

def is_allowed_line(testName, line):
	for regex, tests in allowedRegexes.items():
		if re.match(regex, line):
			for test in tests:
				if test == '*' or test == testName:
					return True
	return False


for patched_file in patch_set:
	permutationLines = get_permutation_lines(patched_file.target_file)
	index = 0
	clean = True
	lines = io.StringIO()
	testName = os.path.splitext(os.path.basename(patched_file.target_file))[0]
	for hunk in patched_file:
		for line in hunk:
			if line.is_added:
				while index < len(permutationLines) - 1 and line.target_line_no > permutationLines[index + 1]:
					index = index + 1
					clean = True
				if clean:
					value = line.value
					if value.startswith('step ') or value == "\n" or re.match(r"[a-z0-9]+: (NOTICE|WARNING|DETAIL): ", value):
						pass
					elif not is_allowed_line(testName, value):
						lines.write(f"{value}")
						clean = False
					else:
						clean = False
	value = lines.getvalue()
	if len(value) > 0:
		print(patched_file.target_file)
		print(value)
	lines.close()
