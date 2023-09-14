#!/usr/bin/env python3
# coding: utf-8

event_names = []

f = open('stopevents.txt', 'r')
line = f.readline()
while line:
	event_names.append(line.strip())
	line = f.readline()
f.close()

f = open('include/utils/stopevents_defs.h', 'w')
f.write('/* Generated file, see stopevents_gen.py */\n\n')

for i, e in enumerate(event_names):
	f.write(f'#define STOPEVENT_{e.upper()} ({str(i)})\n')
	
f.write('#define STOPEVENTS_COUNT (' + str(i) + ')\n')
f.close()

f = open('include/utils/stopevents_data.h', 'w')
f.write('/* Generated file, see stopevents_gen.py */\n\n')

for e in event_names:
	f.write(f'"{e}",\n')

f.close()
