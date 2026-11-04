#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import platform
import subprocess


def is_objdump(execname):
	try:
		output = subprocess.run([execname, '-v'],
		                        stdout=subprocess.PIPE,
		                        stderr=subprocess.PIPE).stdout.decode('utf-8')
	except:
		return False
	return output.startswith('GNU objdump')


def find_objdump():
	execname = os.getenv('OBJDUMP')
	if execname and is_objdump(execname):
		return execname
	if is_objdump('objdump'):
		return 'objdump'
	if is_objdump('gobjdump'):
		return 'gobjdump'
	raise Exception('objdump not found')


def has_dwarfdump():
	try:
		result = subprocess.run(['dwarfdump', '--version'],
		                        stdout=subprocess.PIPE,
		                        stderr=subprocess.PIPE)
		return result.returncode == 0
	except (FileNotFoundError, OSError):
		return False


def extract_typedefs_dwarfdump():
	"""Extract typedefs using dwarfdump (macOS)"""
	result = subprocess.run(['dwarfdump', '--debug-info'] +
	                        glob.glob('src/*/*.o') + glob.glob('src/*.o'),
	                        stdout=subprocess.PIPE,
	                        stderr=subprocess.PIPE)
	if result.returncode != 0:
		raise Exception(
		    f"dwarfdump failed with code {result.returncode}: {result.stderr.decode('utf-8')}"
		)
	output = result.stdout.decode('utf-8')

	typenames = []
	in_typedef = False

	for line in output.splitlines():
		if 'DW_TAG_typedef' in line:
			# Start tracking a new typedef block
			in_typedef = True
			continue

		if in_typedef:
			if 'DW_AT_name' in line:
				# Extract the name from lines like:
				# DW_AT_name ("TypeName")
				if '"' in line:
					parts = line.split('"')
					if len(parts) >= 2:
						typenames.append(parts[1])
				in_typedef = False
			elif 'DW_TAG_' in line:
				# Reset if we encounter another tag without finding DW_AT_name
				in_typedef = False

	return typenames


def extract_typedefs_objdump():
	"""Extract typedefs using objdump (Linux/Ubuntu)"""
	result = subprocess.run([find_objdump(), '-W'] + glob.glob('src/*/*.o') +
	                        glob.glob('src/*.o'),
	                        stdout=subprocess.PIPE,
	                        stderr=subprocess.PIPE)
	if result.returncode != 0:
		raise Exception(
		    f"objdump failed with code {result.returncode}: {result.stderr.decode('utf-8')}"
		)
	output = result.stdout.decode('utf-8')

	typenames = []
	i = 3

	for line in output.splitlines():
		if line.find('DW_TAG_typedef') >= 0:
			i = 0
			continue
		i = i + 1
		if i > 3:
			continue
		fields = line.split()
		if len(fields) < 2:
			continue
		if fields[0] != 'DW_AT_name' and fields[1] != 'DW_AT_name':
			continue
		if fields[-1].startswith('DW_FORM_str'):
			continue
		typenames.append(fields[-1])

	return typenames


if platform.system() == 'Darwin' and has_dwarfdump():
	print("Using dwarfdump (macOS)")
	typenames = extract_typedefs_dwarfdump()
else:
	print("Using objdump")
	typenames = extract_typedefs_objdump()

file = open('orioledb.typedefs', 'w')
file.write("\n".join(sorted(set(typenames))) + "\n")
file.close()
