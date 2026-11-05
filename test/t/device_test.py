#!/usr/bin/env python3
# coding: utf-8

import os

from testgres.exceptions import StartNodeException

from .base_test import BaseTest


class DeviceTest(BaseTest):
	"""
	Block device mode settings must be validated at startup.
	"""

	def start_must_fail(self, node, expected):
		with self.assertRaises(StartNodeException):
			node.start()
		with open(node.pg_log_file) as f:
			log = f.read()
		self.assertIn(expected, log)

	def test_mmap_zero_length(self):
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.device_filename = '/dev/null'\n"
		    "orioledb.use_mmap = on\n")
		self.start_must_fail(node, "orioledb.device_length must be set")

	def test_mmap_failure(self):
		# /dev/null cannot be mapped (ENODEV)
		node = self.node
		node.append_conf(
		    'postgresql.conf', "orioledb.device_filename = '/dev/null'\n"
		    "orioledb.device_length = 1024\n"
		    "orioledb.use_mmap = on\n")
		self.start_must_fail(node, "could not map device file")

	def test_length_exceeds_file(self):
		node = self.node
		device = os.path.join(node.base_dir, 'device')
		with open(device, 'wb') as f:
			f.truncate(1024 * 1024)
		node.append_conf(
		    'postgresql.conf', "orioledb.device_filename = '%s'\n"
		    "orioledb.device_length = '2MB'\n"
		    "orioledb.use_mmap = on\n" % device)
		self.start_must_fail(node, "exceeds the size of device file")
