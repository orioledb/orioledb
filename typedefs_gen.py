#!/usr/bin/env python3
# coding: utf-8

import glob
import os
import re
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


output = subprocess.run([find_objdump(), '-W'] + glob.glob('src/*/*.o') +
                        glob.glob('src/*.o'),
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE).stdout.decode('utf-8')

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
	if fields[0] != 'DW_AT_name' and fields[1] != 'DW_AT_name':
		continue
	if fields[-1].startswith('DW_FORM_str'):
		continue
	typenames.append(fields[-1])

file = open('orioledb.typedefs', 'w')
file.write("\n".join(sorted(set(typenames))) + "\n")
file.close()
