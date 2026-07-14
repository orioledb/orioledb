#!/usr/bin/env python3
# coding: utf-8
"""
Shared testgres node-scaffolding for backup-tool integration tests
(pgBackRest, wal-g, ...). Subclasses supply the tool-specific restore and
config logic as callback functions; this class only owns the node
creation boilerplate that's identical across tools.
"""

import glob
import inspect
import os
import shutil
import time

import testgres
from testgres.node import PostgresNode
from testgres.operations.os_ops import ConnectionParams

from test.t.base_test import BaseTest, TestPortManager


class BaseBackupTest(BaseTest):

	def createStandby(self):
		"""
		Scaffolds a fresh standby node directory and returns it.  The caller
		is responsible for restoring a backup into standby.data_dir and
		configuring replication before starting the node.
		"""
		(testPath,
		 _) = os.path.split(os.path.dirname(inspect.getfile(self.__class__)))
		baseDir = os.path.join(testPath, 'tmp_check_t', self.myName + '_tgsb')
		if os.path.exists(baseDir):
			shutil.rmtree(baseDir)

		port = self.getBasePort() + 1
		pm = TestPortManager(PostgresNode._get_os_ops(ConnectionParams()),
		                     port)
		standby = testgres.get_new_node('standby',
		                                base_dir=baseDir,
		                                port_manager=pm)
		os.makedirs(standby.data_dir, exist_ok=True)

		self.replica = standby
		return standby

	def restoreToNewNode(self, nodeName: str):
		"""
		Scaffolds a fresh scratch node directory and returns it.  The caller
		is responsible for restoring a backup into scratch.data_dir and
		finalizing postgresql.conf before starting the node.
		"""
		(testPath,
		 _) = os.path.split(os.path.dirname(inspect.getfile(self.__class__)))
		baseDir = os.path.join(testPath, 'tmp_check_t',
		                       self.myName + '_' + nodeName)
		if os.path.exists(baseDir):
			shutil.rmtree(baseDir)

		port = self._next_scratch_port()
		pm = TestPortManager(PostgresNode._get_os_ops(ConnectionParams()),
		                     port)
		scratch = testgres.get_new_node(nodeName,
		                                base_dir=baseDir,
		                                port_manager=pm)
		os.makedirs(scratch.data_dir, exist_ok=True)
		return scratch

	def waitForHistoryArchive(self,
	                          standby: PostgresNode,
	                          historyPattern,
	                          timeout=30):
		"""
		historyPattern(timeline) -> glob pattern for the archived
		.history file of that (int) timeline.
		"""
		walFilename = standby.execute(
		    'postgres', 'SELECT pg_walfile_name(pg_current_wal_lsn())')[0][0]
		timeline = int(walFilename[:8], 16)
		pattern = historyPattern(timeline)
		deadline = time.time() + timeout
		while time.time() < deadline:
			if glob.glob(pattern):
				return
			time.sleep(0.5)
		raise AssertionError(
		    f"timeline history file for timeline {timeline:08X} was not "
		    f"archived")

	def _next_scratch_port(self):
		"""
		Ports beyond the primary (basePort) and standby (basePort + 1),
		for one-off verification nodes.
		"""
		self._scratch_port_counter = getattr(self, '_scratch_port_counter',
		                                     1) + 1
		return self.getBasePort() + self._scratch_port_counter
