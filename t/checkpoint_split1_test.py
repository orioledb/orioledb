#!/usr/bin/env python3
# coding: utf-8

from .checkpoint_split_base_test import CheckpointSplitBaseTest

class CheckpointSplit1Test(CheckpointSplitBaseTest):
	def test_checkpoint_concurrent_leaf_split_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,35,-1) id;",
			'downwards', None, 1,
			"SELECT '0034' || repeat('x', 2500);",
			17)

	def test_checkpoint_concurrent_node_split_var3_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,35,-1) id;",
			'upwards', None, 0,
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(3750, 3760) id;",
			27, with_eviction=True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_var4_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(5000,3500,-100) id;",
			'upwards', '0038', 0,
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(3801, 3826) id;",
			42, with_eviction=True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_var5_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(3500,5000,100) id;",
			'downwards', '0035', 1,
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(3501, 3526) id;",
			42, with_eviction=True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_leaf_split(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,35,-1) id;",
			'downwards', '0035', 1,
			"SELECT '0034' || repeat('x', 2500)",
			17, with_second_checkpoint=False)

	def test_checkpoint_concurrent_leaf_split_var2_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm00000') || repeat('x', 2500) FROM generate_series(50000,35000,-1000) id;",
			'downwards', '00035', 1,
			"SELECT to_char(id, 'fm00000') || repeat('x', 2500) FROM generate_series(38001,38011,1) id;",
			27)

	def test_checkpoint_concurrent_leaf_split_var2(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm00000') || repeat('x', 2500) FROM generate_series(50000,35000,-1000) id;",
			'downwards', '00035', 1,
			"SELECT to_char(id, 'fm00000') || repeat('x', 2500) FROM generate_series(38001,38011,1) id;",
			27, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,33,-1) id;",
			'downwards', None, 1,
			"SELECT '0032' || repeat('x', 2500)",
			19)

	def test_checkpoint_concurrent_node_split(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,33,-1) id;",
			'downwards', None, 1,
			"SELECT '0032' || repeat('x', 2500)",
			19, with_second_checkpoint=False)

	def test_checkpoint_concurrent_leaf_split_evict_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,35,-1) id;",
			'upwards', None, 0,
			"SELECT '0034' || repeat('x', 2500)",
			17,
			True)

	def test_checkpoint_concurrent_leaf_split_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,35,-1) id;",
			'upwards', None, 0,
			"SELECT '0034' || repeat('x', 2500)",
			17,
			True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_evict_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,33,-1) id;",
			'downwards', None, 1,
			"SELECT '0032' || repeat('x', 2500)",
			19,
			True)

	def test_checkpoint_concurrent_node_split_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(50,33,-1) id;",
			'downwards', None, 1,
			"SELECT '0032' || repeat('x', 2500)",
			19,
			True, with_second_checkpoint=False)
