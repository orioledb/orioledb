#!/usr/bin/env python3
# coding: utf-8

# This script filters out for regression.diffs:
#   - all known ERROR messages
#   - all NOTICE|WARNING|DETAIL|INFO|HINT messages
#   - all tables differences that only differ in order
#   - all plans that basically equal for postgres and orioledb 
#     but have some different node name
#   - known plan differences which are processed in compare_trees
#   - also for now all differences for \d output are ignored, 
#     more smart filter should be added later

from unidiff import PatchSet
import argparse
import sys
import io
import re
import os
from enum import Enum

ap = argparse.ArgumentParser(
    description=
    "Filter out known expected error from pg_regress regression.diffs output")
ap.add_argument("--diff", "-d", help="path to regression.diffs")
args = ap.parse_args()

if args.diff:
	with open(args.diff, "r", encoding="utf-8", errors="replace") as f:
		diff_text = f.read()
else:
	diff_text = sys.stdin.read()

patch_set = PatchSet(diff_text)

knownErrors = {
    r"ERROR:  orioledb does not support SERIALIZABLE isolation level": ["*"],
    r"ERROR:  tuple to be locked has its primary key changed due to concurrent update":
    ["*"],
    r"ERROR:  Not implemented: orioledb_tuple_tid_valid": ["*"],
    r"ERROR:  REINDEX CONCURRENTLY is not supported for orioledb tables yet":
    ["*"],
    r"ERROR:  orioledb tables does not support CLUSTER": ["*"],
    r"ERROR:  orioledb table \"[a-z0-9_]+\" does not support VACUUM FULL":
    ["*"],
    r"ERROR:  cannot use PREPARE TRANSACTION in transaction that uses orioledb table":
    ["*"],
    r"ERROR:  replica identity type INDEX is not supported for OrioleDB tables yet":
    ["*"],

    # alter_table specific errors
    r"ERROR:  unsupported alter table subcommand": ["alter_table"],
    r"ERROR:  table \"[a-z0-9_]+\" has different type for column \"[a-z0-9_]+\"":
    ["alter_table"],
    r"ERROR:  typed tables cannot inherit": ["alter_table"],
    r"ERROR:  table is missing column \"[a-z0-9_]+\"": ["alter_table"],
    r"ERROR:  could not change table \"[a-z0-9_]+\" to logged because it references unlogged table \"[a-z0-9_]+\"":
    ["alter_table"],
    # this probably could be easily fixed
    r"ERROR:  check constraint \"atacc1_chk\" of relation \"atacc1\" is violated by some row":
    ["alter_table"],
    # some errors related to ALTER TABLE ... OF ...
    r"ERROR:  table has column \"y\" where type requires \"x\"":
    ["alter_table"],
    r"ERROR:  table has extra column \"z\"": ["alter_table"],
    r"ERROR:  could not change table \"logged1\" to unlogged because it references logged table \"logged2\"":
    ["alter_table"],

    # create_index specific errors
    # some errors related to not working CONCURRENT index build right now
    r"ERROR:  could not create unique index \"[a-z0-9_]+\"": ["create_index"],
    r"ERROR:  relation \"[a-z0-9_]+\" does not exist": ["create_index"],
    r"ERROR:  index \"[a-z0-9_]+\" does not exist": ["create_index"],
}


def is_known_error(testName, line):
	for regex, tests in knownErrors.items():
		if re.match(regex, line):
			for test in tests:
				if test == '*' or test == testName:
					return True
	return False


# TODO: explain every table diff
known_table_diffs = {
    "create_index": [
        [[],
         [[
             'materialized view concur_reindex_matview',
             'access method orioledb', 'n'
         ], ['table concur_reindex_tab', 'access method orioledb', 'n']]],
        [[['concur_replident_i_idx', 't']], [['concur_replident_i_idx', 'f']]],
        [[],
         [['table concur_reindex_part_0_1', 'access method orioledb', 'n'],
          ['table concur_reindex_part_0_2', 'access method orioledb', 'n']]],
        [[['table1', 'r', 'relfilenode is unchanged'],
          ['table2', 'r', 'relfilenode is unchanged']],
         [['pg_toast_TABLE', 't', 'relfilenode has changed'],
          ['pg_toast_TABLE', 't', 'relfilenode has changed'],
          ['pg_toast_TABLE_index', 'i', 'relfilenode has changed'],
          ['pg_toast_TABLE_index', 'i', 'relfilenode has changed'],
          ['table1', 'r', 'relfilenode has changed'],
          ['table2', 'r', 'relfilenode has changed']]],
    ],
    "alter_table": [
        [[['fkdd2', '"RI_FKey_noaction_upd"', '17', 't', 't'],
          ['fkdi2', '"RI_FKey_noaction_upd"', '17', 't', 'f'],
          ['fknd2', '"RI_FKey_noaction_upd"', '17', 'f', 'f']],
         [['fkdd2', '"RI_FKey_noaction_upd"', '17', 'f', 'f'],
          ['fkdi2', '"RI_FKey_noaction_upd"', '17', 'f', 'f'],
          ['fknd2', '"RI_FKey_noaction_upd"', '17', 't', 't']]],
        [[['fkdd2', '"RI_FKey_check_ins"', '5', 't', 't'],
          ['fkdd2', '"RI_FKey_check_upd"', '17', 't', 't'],
          ['fkdi2', '"RI_FKey_check_ins"', '5', 't', 'f'],
          ['fkdi2', '"RI_FKey_check_upd"', '17', 't', 'f'],
          ['fknd2', '"RI_FKey_check_ins"', '5', 'f', 'f'],
          ['fknd2', '"RI_FKey_check_upd"', '17', 'f', 'f']],
         [['fkdd2', '"RI_FKey_check_ins"', '5', 'f', 'f'],
          ['fkdd2', '"RI_FKey_check_upd"', '17', 'f', 'f'],
          ['fkdi2', '"RI_FKey_check_ins"', '5', 'f', 'f'],
          ['fkdi2', '"RI_FKey_check_upd"', '17', 'f', 'f'],
          ['fknd2', '"RI_FKey_check_ins"', '5', 't', 't'],
          ['fknd2', '"RI_FKey_check_upd"', '17', 't', 't']]],
        [[['f']], [['t']]],
        [[['alterlock', 'ShareUpdateExclusiveLock']],
         [['alterlock', 'ShareUpdateExclusiveLock'],
          ['alterlock_pkey', 'AccessShareLock']]],
        [[['alterlock', 'AccessExclusiveLock']],
         [['alterlock', 'AccessExclusiveLock'],
          ['alterlock_pkey', 'AccessShareLock']]],
        [[['unlogged1', 'r', 'p'], ['unlogged1 toast index', 'i', 'p'],
          ['unlogged1 toast table', 't', 'p'], ['unlogged1_f1_seq', 'S', 'p'],
          ['unlogged1_pkey', 'i', 'p']],
         [['unlogged1', 'r', 'u'], ['unlogged1 toast index', 'i', 'u'],
          ['unlogged1 toast table', 't', 'u'], ['unlogged1_f1_seq', 'S', 'u'],
          ['unlogged1_pkey', 'i', 'u']]],
        [[['logged1', 'r', 'u'], ['logged1 toast index', 'i', 'u'],
          ['logged1 toast table', 't', 'u'], ['logged1_f1_seq', 'S', 'u'],
          ['logged1_pkey', 'i', 'u']],
         [['logged1', 'r', 'p'], ['logged1 toast index', 'i', 'p'],
          ['logged1 toast table', 't', 'p'], ['logged1_f1_seq', 'S', 'p'],
          ['logged1_pkey', 'i', 'p']]],
    ]
}


def compare_trees(src_tree: list, target_tree: list, test_name: str):
	equal = True
	src_stack = [[src_tree, 0]]
	target_stack = [[target_tree, 0]]
	while equal and len(src_stack) > 0 and len(target_stack) > 0:
		src_down = False
		src_up = False
		target_down = False
		target_up = False
		src_cur = src_stack[0][0][src_stack[0][1]]
		target_cur = target_stack[0][0][target_stack[0][1]]
		if target_cur[1].startswith("Result"):
			target_down = True
		elif src_cur[1].startswith("Result"):
			src_down = True
		elif src_cur[1].startswith('Bitmap Heap Scan'):
			if target_cur[1].startswith('Custom Scan'):
				if target_cur[2][0] != 'Bitmap heap scan':
					equal = False
				else:
					src_down = True
					target_down = True
			else:
				equal = False
		elif src_cur[1] == target_cur[1]:
			src_down = True
			target_down = True
		else:
			# processing known real plan differences,
			# that are omitted for now but should be checked in the future
			# put checks for certain files at the end, to process all other
			# checks before

			if (src_cur[1].startswith('Index Only Scan')
			    and target_cur[1].startswith('Custom Scan')
			    and target_cur[2][0] == 'Bitmap heap scan'):
				# sometimes we have bitmap heap scan instead index scan
				src_up = True
				target_up = True
			elif (src_cur[1].startswith('Index Only Scan')
			      and target_cur[1].startswith('Seq Scan')):
				# sometimes we have seq scan instead of index scan
				src_up = True
				target_up = True
			elif (src_cur[1].startswith('Index Only Scan')
			      and target_cur[1].startswith('Sort')):
				# Sometimes we have sort, because we use bitmap heap scan
				# instead of index scan
				target_down = True
			elif test_name == 'create_index':
				# different processing of or clauses for our tables
				if (src_cur[1] == 'BitmapAnd'
				    and target_cur[1] == 'Bitmap Index Scan on tenk1_hundred'):
					src_up = True
					target_up = True

		if src_down:
			if len(src_cur[3]) > 0:
				src_stack.insert(0, [src_cur[3], 0])
			else:
				src_up = True
		if src_up:
			while (len(src_stack) > 0
			       and src_stack[0][1] == len(src_stack[0][0]) - 1):
				src_stack.pop(0)
			if len(src_stack) > 0:
				src_stack[0][1] += 1
		if target_down:
			if len(target_cur[3]) > 0:
				target_stack.insert(0, [target_cur[3], 0])
			else:
				target_up = True
		if target_up:
			while (len(target_stack) > 0
			       and target_stack[0][1] == len(target_stack[0][0]) - 1):
				target_stack.pop(0)
			if len(target_stack) > 0:
				target_stack[0][1] += 1
	return equal


def find_table_lines(line_no, lines):
	table_start = 0
	if re.match(r"^[-+]+$", lines[line_no]):
		table_start = line_no
	else:
		while table_start == 0 and line_no > 0:
			if re.match(r"^[-+]+$", lines[line_no]):
				table_start = line_no
			line_no -= 1

	table_end = 0
	while table_end == 0 and line_no < len(lines):
		if re.match(r"\(\d+ rows?\)", lines[line_no]):
			table_end = line_no + 1
		line_no += 1
	return table_start, table_end


def find_desc_lines(line_no, lines):
	desc_start = 0
	desc_end = 0
	while desc_start == 0 and line_no > 0:
		if re.match(r"^\\d", lines[line_no]):
			desc_start = line_no
		line_no -= 1

	in_column_table = False
	line_no += 1
	while desc_end == 0 and line_no < len(lines):
		line_type = type_of_line(lines[line_no])
		if in_column_table and line_type == LineType.description:
			in_column_table = False
		if (not in_column_table and line_type == LineType.table
		    and ([x.strip() for x in lines[line_no].split("|")]
		         == ["Column", "Type", "Collation", "Nullable", "Default"])):
			in_column_table = True
		if not in_column_table and line_type != LineType.description:
			desc_end = line_no - 1
		line_no += 1
	return (desc_start, desc_end)


class LineType(Enum):
	error = 1
	description = 2
	table = 3
	info = 4


def type_of_line(line: str):
	if re.match(r"ERROR: ", line):
		return LineType.error
	elif re.match(r"(NOTICE|WARNING|DETAIL|INFO|HINT): ", line):
		return LineType.info
	# TODO: Also add regexes for column types and other things
	elif (re.match(r"^\\d", line) or re.match(r"^\s+Table \".*\"", line)
	      or re.match(r"Indexes:", line)
	      or re.match(r".*[,\"] btree \(", line)):
		return LineType.description
	else:
		return LineType.table


def query_plan_to_tree(table_lines: str) -> list:
	tree = []
	stack = []
	for table_line in table_lines:
		level = 0
		char_num = 1
		while table_line[0][char_num] == ' ':
			if level == 0:
				char_num += 2
			else:
				char_num += 6
			level += 1

		if table_line[0].strip()[0:2] == '->':
			value = table_line[0].strip()[4:]
		else:
			value = table_line[0].strip()
		if len(stack) == 0:
			# level, value, children, properties
			tree += [[level, value, [], []]]
			# level_list, level_index
			stack.insert(0, [tree, len(tree) - 1])
		elif level == stack[0][0][stack[0][1]][0] + 1:
			if table_line[0].strip()[0:2] == '->':
				children = stack[0][0][stack[0][1]][3]
				children += [[level, value, [], []]]
				stack.insert(0, [children, len(children) - 1])
			else:
				properties = stack[0][0][stack[0][1]][2]
				properties += [value]
		elif level == stack[0][0][stack[0][1]][0]:
			if len(stack) == 1:
				siblings = tree
			else:
				siblings = stack[1][0][stack[1][1]][3]
			siblings += [[level, value, [], []]]
			stack.pop(0)
			stack.insert(0, [siblings, len(siblings) - 1])
		else:
			stack.clear()
			# level, value, properties, children
			tree += [[level, value, [], []]]
			# level_list, level_index
			stack.insert(0, [tree, len(tree) - 1])

	return tree


src_table_start = 0
src_table_end = 0
src_table_lines = []
target_table_start = 0
target_table_end = 0
target_table_lines = []
table_hunks = []
finish_table = False

src_desc_start = 0
src_desc_end = 0
target_desc_start = 0
target_desc_end = 0
desc_hunks = []
finish_desc = False

patched_files = list(patch_set)
# patched_files = patched_files[0:5]
# patched_files = patched_files[0:9]
# patched_files = patched_files[-1:]
# patched_files = patched_files[9:10]
patch_set.clear()
for patched_file in patched_files:
	index = 0
	testName = os.path.splitext(os.path.basename(patched_file.target_file))[0]
	hunks = list(patched_file)
	with open(patched_file.source_file) as sf:
		source = sf.readlines()
	with open(patched_file.target_file) as tf:
		target = tf.readlines()

	for hunk_num in range(0, len(hunks)):
		hunk = hunks[hunk_num]
		lines = list(hunk)
		# ignore all non significant log messages and know errors
		for line_num in range(0, len(lines)):
			line = lines[line_num]
			if (line.value == "\n"
			    or (line.line_type != ' '
			        and type_of_line(line.value) == LineType.info)
			    or is_known_error(testName, line.value)):
				hunk.remove(line)
		if hunk.added == 0 and hunk.removed == 0:
			patched_file.remove(hunk)
		else:
			# here, we are finding all tables, query plans, \d outputs
			# and then removing all tables that only with different row order,
			# query plans with known path differences and known removed/existing
			# columns or indices
			for line_num in range(0, len(lines)):
				line = lines[line_num]
				line_type = type_of_line(line.value)
				if line.is_removed:
					# print(f"{line.line_type}:{patched_file.source_file}:{line.source_line_no}:{line_type}: {source[line.source_line_no - 1]}", end="")
					if line_type == LineType.table:
						if src_table_start == 0:
							src_table_start, src_table_end = find_table_lines(
							    line.source_line_no, source)
							# print(f"SRC TABLE {(src_table_start, src_table_end)}")
						if (line.source_line_no <= src_table_end
						    and line.source_line_no > src_table_start + 1):
							if line.source_line_no < src_table_end:
								src_table_lines += [line.value.split("|")]
							if hunk not in table_hunks:
								table_hunks += [hunk]
					elif line_type == LineType.description:
						if src_desc_start == 0:
							src_desc_start, src_desc_end = find_desc_lines(
							    line.source_line_no, source)
							# print(f"SOURCE DESC: ({src_desc_start}, {src_desc_end})")
						if (line.source_line_no <= src_desc_end
						    and line.source_line_no > src_desc_start + 1):
							if hunk not in desc_hunks:
								desc_hunks += [hunk]

				elif line.is_added:
					# print(f"{line.line_type}:{patched_file.target_file}:{line.target_line_no}:{line_type}: {target[line.target_line_no - 1]}", end="")
					if line_type == LineType.table:
						if target_table_start == 0:
							target_table_start, target_table_end = find_table_lines(
							    line.target_line_no, target)
							# print(f"TARGET TABLE {(target_table_start, target_table_end)}")
						if (line.target_line_no <= target_table_end
						    and line.target_line_no > target_table_start + 1):
							if line.target_line_no < target_table_end:
								target_table_lines += [line.value.split("|")]
							if hunk not in table_hunks:
								table_hunks += [hunk]
					elif line_type == LineType.description:
						if target_desc_start == 0:
							target_desc_start, target_desc_end = find_desc_lines(
							    line.target_line_no, target)
						if (line.target_line_no <= target_desc_end
						    and line.target_line_no > target_desc_start + 1):
							if hunk not in desc_hunks:
								desc_hunks += [hunk]
				else:
					if src_table_end != 0 and target_table_end != 0 and line.source_line_no > src_table_end and line.target_line_no > target_table_end:
						finish_table = True
					elif src_desc_end != 0 and line.source_line_no > src_desc_end:
						finish_desc = True
				# import ipdb; ipdb.set_trace()
				if src_table_end != 0 and target_table_end != 0:
					if (not finish_table and line_num == len(lines) - 1
					    and hunk_num != len(hunks) - 1
					    and hunks[hunk_num + 1].source_start > src_table_end):
						finish_table = True
					elif (line_num == len(hunk) - 1
					      and hunk_num == len(patched_file) - 1):
						finish_table = True

				if finish_table:
					# print("FINISH TABLE?")
					finish_table = False
					table_remove = False
					table_columns = [
					    x.strip()
					    for x in source[src_table_start - 1].split("|")
					]
					if table_columns[0].strip() == 'QUERY PLAN':
						src_tree = query_plan_to_tree(src_table_lines)
						target_tree = query_plan_to_tree(target_table_lines)
						# compare plan trees
						equal = compare_trees(src_tree, target_tree, testName)
						# sys.exit(0)
						if equal:
							table_remove = True
					else:
						src_table_lines = sorted(
						    [cell.strip() for cell in line]
						    for line in src_table_lines)
						target_table_lines = sorted(
						    [cell.strip() for cell in line]
						    for line in target_table_lines)
						# print(src_table_lines == target_table_lines)

						# remove diffs of tables just with different order
						if src_table_lines == target_table_lines:
							table_remove = True
						else:
							# print(testName)
							# print(src_table_lines)
							# print(target_table_lines)
							if testName in known_table_diffs:
								test_table_diffs = known_table_diffs[testName]
								for test_table_diff in test_table_diffs:
									if (test_table_diff[0] == src_table_lines
									    and test_table_diff[1]
									    == target_table_lines):
										table_remove = True
										break

					if table_remove:
						for table_hunk in table_hunks:
							table_lines = list(table_hunk)
							for table_line in table_lines:
								line_type = type_of_line(line.value)
								if line_type == LineType.table:
									if (table_line.is_removed
									    and table_line.source_line_no
									    >= src_table_start
									    and table_line.source_line_no
									    <= src_table_end):
										table_hunk.remove(table_line)
									elif (table_line.is_added
									      and table_line.target_line_no
									      >= target_table_start
									      and table_line.target_line_no
									      <= target_table_end):
										table_hunk.remove(table_line)

							if table_hunk.added == 0 and table_hunk.removed == 0:
								patched_file.remove(table_hunk)
					src_table_start = 0
					src_table_end = 0
					src_table_lines = []
					target_table_start = 0
					target_table_end = 0
					target_table_lines = []
					table_hunks = []

				if finish_desc:
					# print("FINISH DESC?")

					in_column_table = False
					for desc_hunk in desc_hunks:
						desc_lines = list(desc_hunk)
						for desc_line in desc_lines:
							line_type = type_of_line(desc_line.value)
							if in_column_table and line_type == LineType.description:
								in_column_table = False
							if (not in_column_table
							    and line_type == LineType.table and ([
							        x.strip()
							        for x in desc_line.value.split("|")
							    ] == [
							        "Column", "Type", "Collation", "Nullable",
							        "Default"
							    ])):
								in_column_table = True
							if (line_type == LineType.description
							    or (line_type == LineType.table
							        and in_column_table)):
								if (desc_line.is_removed and
								    desc_line.source_line_no >= src_desc_start
								    and desc_line.source_line_no
								    <= src_desc_end + 1):
									desc_hunk.remove(desc_line)

						if desc_hunk.added == 0 and desc_hunk.removed == 0:
							patched_file.remove(desc_hunk)

					finish_desc = False
					src_desc_start = 0
					src_desc_end = 0
					target_desc_start = 0
					target_desc_end = 0
					desc_hunks = []
	if len(patched_file) != 0:
		patch_set.append(patched_file)
print(patch_set, end='')
