#!/usr/bin/env python3

import os
import re

from .base_test import BaseTest

from testgres.enums import NodeStatus

class FileOperationsTest(BaseTest):
	###
	#  Check create checkpoint file error and release of pages which belong to
	#  the tree.
	###
	def test_can_not_create_checkpoint_file(self):
		node = self.node
		node.start() # start PostgreSQL
		node.safe_psql('postgres',
			"CREATE EXTENSION IF NOT EXISTS orioledb;\n"
			"CREATE TABLE IF NOT EXISTS o_test (\n"
			"	id integer NOT NULL,\n"
			"	val text\n"
			") USING orioledb;\n"
		)

		was_busy = node.execute('postgres', "SELECT orioledb_page_stats();")[0][0]

		# have not access to create checkpoint file, exception
		try:
			self.unsetRWX([self.getOrioleDBDir()])
			node.safe_psql("SELECT * FROM o_test;")
			self.assertEqual("We should not be here", "")
		except Exception as e:
			message = re.sub('[0-9]+', 'x', e.message)
			self.assertTrue("FATAL:  could not open data file orioledb_data/x/x\n" in message)

		# checks that all pages has been released
		self.assertEqual(
			node.execute('postgres', "SELECT orioledb_page_stats();")[0][0],
			was_busy)

		# have access to create checkpoint file, no exception
		try:
			self.setRWX([self.getOrioleDBDir()])
			node.safe_psql("SELECT * FROM o_test;")
		except Exception:
			self.assertEqual("We should not be here", "")

	def unsetRWX(self, files):
		for fname in files:
			os.chmod(fname, 0o000)

	def setRWX(self, files):
		for fname in files:
			os.chmod(fname, 0o700)

	def getOrioleDBDir(self):
		return self.node.data_dir + "/orioledb_data"
