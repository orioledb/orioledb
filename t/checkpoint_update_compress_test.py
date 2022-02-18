#!/usr/bin/env python3
# coding: utf-8

from .checkpoint_update_base_test import CheckpointUpdateBaseTest

class CheckpointUpdateCompressTest(CheckpointUpdateBaseTest):
	def test_compress_concurrent_update_eviction_single_checkpoint(self):
		self.concurrent_update_eviction_base(True, False, False, 0)

	def test_compress_concurrent_update_eviction_first_checkpoint(self):
		self.concurrent_update_eviction_base(True, False, False, 3)

	def test_compress_concurrent_update_eviction_middle_checkpoint(self):
		self.concurrent_update_eviction_base(True, True, False, 1)

	def test_compress_concurrent_update_eviction_many_checkpoints(self):
		self.concurrent_update_eviction_base(True, True, True, 5, False)

	def test_compress_concurrent_update_eviction_many_update_checkpoints(self):
		self.concurrent_update_eviction_base(True, True, True, 5)
