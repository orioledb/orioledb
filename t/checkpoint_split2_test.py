#!/usr/bin/env python3
# coding: utf-8

from .checkpoint_split_base_test import CheckpointSplitBaseTest

class CheckpointSplit2Test(CheckpointSplitBaseTest):
	def test_checkpoint_concurrent_right_leaf_split_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10,18,1) id;",
			'downwards', '0015', 1,
			"SELECT '0019' || repeat('x', 2500)",
			10)

	def test_checkpoint_concurrent_right_leaf_split(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10,18,1) id;",
			'downwards', '0012', 1,
			"SELECT '0019' || repeat('x', 2500)",
			10, with_second_checkpoint=False)

	def test_checkpoint_concurrent_right_node_split_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(52,78,1) id;",
			'downwards', '0075', 1,
			"SELECT '0079' || repeat('x', 2500)",
			28)

	def test_checkpoint_concurrent_right_node_split(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(52,78,1) id;",
			'downwards', '0075', 1,
			"SELECT '0079' || repeat('x', 2500)",
			28, with_second_checkpoint=False)

	def test_checkpoint_concurrent_right_leaf_split_evict_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10,18,1) id;",
			'downwards', '0012', 1,
			"SELECT '0019' || repeat('x', 2500)",
			10,
			True)

	def test_checkpoint_concurrent_right_leaf_split_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(10,18,1) id;",
			'downwards', '0012', 1,
			"SELECT '0019' || repeat('x', 2500)",
			10,
			True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_right_node_split_evict_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(52,78,1) id;",
			'downwards', '0075', 1,
			"SELECT '0079' || repeat('x', 2500)",
			28,
			True)

	def test_checkpoint_concurrent_right_node_split_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(52,78,1) id;",
			'downwards', '0075', 1,
			"SELECT '0079' || repeat('x', 2500)",
			28,
			True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_load_node_split_evict_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(500, 330, -10) id;",
			'downwards', None, 1,
			"SELECT to_char(id, 'fm0000') || repeat('y', 2500) FROM generate_series(319, 370, 1) id;",
			70,
			True,
			True)

	def test_checkpoint_concurrent_node_split_evict_load(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(500, 330, -10) id;",
			'downwards', None, 1,
			"SELECT to_char(id, 'fm0000') || repeat('y', 2500) FROM generate_series(319, 370, 1) id;",
			70,
			True,
			True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_var2_evict_load_chkp(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(500, 330, -10) id;",
			'upwards', None, 0,
			"SELECT to_char(id, 'fm0000') || repeat('y', 2500) FROM generate_series(319, 370, 1) id;",
			70,
			True,
			True)

	def test_checkpoint_concurrent_node_split_var2_evict_load(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(500, 330, -10) id;",
			'upwards', None, 0,
			"SELECT to_char(id, 'fm0000') || repeat('y', 2500) FROM generate_series(319, 370, 1) id;",
			70,
			True,
			True, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_var3_evict_load(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(36,45,1) id;",
			'downwards', None, 1,
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(11,35,1) id;",
			35,
			True, True, False, with_second_checkpoint=False)

	def test_checkpoint_concurrent_node_split_var3_evict_load_evict(self):
		self.checkpoint_concurrent_split_base(
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(36,45,1) id;",
			'downwards', None, 1,
			"SELECT to_char(id, 'fm0000') || repeat('x', 2500) FROM generate_series(11,35,1) id;",
			35,
			True, True, True, with_second_checkpoint=False)
