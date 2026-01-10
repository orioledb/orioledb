#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import re

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from testgres.connection import DatabaseError

from testgres.enums import NodeStatus

# MAGIC NUMBERS RELATIVE TO TUPLES STRUCTURES
RELATION_SINGLE_FIELD_TUPLE_SIZE = 173
RELATION_EXTRA_FIELD_TUPLE_SIZE = 97
INDEX_EXTRA_TUPLE_SIZE = 891


class PageFitItemsTest(BaseTest):

	def assertSysTreePagesCount(self, expectedCount, dump=False):
		out = self.node.execute(
		    'postgres', "select orioledb_sys_tree_structure(2, 'ne');")[0][0]
		if dump:
			print("\n")
			print(out)
		givenCount = out.count('Page ')
		self.assertEqual(expectedCount, givenCount, out)

	def extractTupleSize(self):
		out = self.node.execute(
		    'postgres', "select orioledb_sys_tree_structure(2, 'ne');")[0][0]
		pattern = r"Item (\d+): offset = (\d+), tuple = \(\(\((\d+), (\d+), (\d+)\), (\d+), (\d+)\), (\d+)\)"

		match = re.findall(pattern, out)
		tuples = []
		for item in match:
			tuples.append(
			    (int(item[0]), int(item[1]), int(item[3]), int(item[-1])))
		return tuples

	def estimateTupleSize(self, relFields, indexCount):
		tupSize = RELATION_SINGLE_FIELD_TUPLE_SIZE + (
		    relFields - 1
		) * RELATION_EXTRA_FIELD_TUPLE_SIZE + indexCount * INDEX_EXTRA_TUPLE_SIZE
		tupSize = (tupSize + 7) // 8 * 8 + 40
		return tupSize

	def startup_page_filling(self, estimatedPageSize):
		fillingSize = 0
		i = 1
		relDesc = []

		while estimatedPageSize - fillingSize > RELATION_EXTRA_FIELD_TUPLE_SIZE:
			fillingSize += RELATION_SINGLE_FIELD_TUPLE_SIZE
			relFildsCount = 1
			indexCount = 0

			while estimatedPageSize - fillingSize > RELATION_EXTRA_FIELD_TUPLE_SIZE and relFildsCount < 4:
				fillingSize += RELATION_EXTRA_FIELD_TUPLE_SIZE
				relFildsCount += 1

			create_table_stmt = f"CREATE TABLE o_table{i}("
			for j in range(relFildsCount):
				sep = "," if j < relFildsCount - 1 else ")"
				create_table_stmt += f"t{j} text{sep} "
			create_table_stmt += f"USING orioledb;\n"
			self.node.safe_psql('postgres', create_table_stmt)

			newTup = self.extractTupleSize()[-1]
			fillingSize = newTup[1] + ((newTup[-1] + 7) // 8) * 8 + 40

			while estimatedPageSize - fillingSize > INDEX_EXTRA_TUPLE_SIZE and indexCount < relFildsCount:
				create_index_stmt = f"CREATE INDEX o_table{i}_idx_t{indexCount} ON o_table{i} USING BTREE(t{indexCount});\n"
				self.node.safe_psql('postgres', create_index_stmt)
				newTup = self.extractTupleSize()[-1]
				fillingSize = newTup[1] + ((newTup[-1] + 7) // 8) * 8 + 40
				indexCount += 1
			relDesc.append((relFildsCount, indexCount))
			i = i + 1
		return relDesc

	'''
		FAIL this check means just magic numbers became incorrect due to changes in tuples structures.
		To make test suite applicable to new versions tuples structures setup new magic numbers.
	'''

	def test_calibrate_sizes(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		create_tuples = self.extractTupleSize()
		self.assertEqual(len(create_tuples), 0)

		node.safe_psql(
		    'postgres', "CREATE TABLE o_table1(t1 text) USING orioledb;\n"
		    "CREATE TABLE o_table2(t1 text, t2 text) USING orioledb;\n"
		    "CREATE TABLE o_table3(t1 text, t2 text, t3 text) USING orioledb;\n"
		)

		create_tuples = self.extractTupleSize()
		self.assertEqual(len(create_tuples), 3)
		for tup in create_tuples:
			self.assertEqual(
			    tup[-1], RELATION_SINGLE_FIELD_TUPLE_SIZE +
			    tup[0] * RELATION_EXTRA_FIELD_TUPLE_SIZE)

		node.safe_psql(
		    'postgres',
		    "CREATE INDEX o_table1_t1 ON o_table1 USING BTREE(t1);\n"
		    "CREATE INDEX o_table2_t1 ON o_table2 USING BTREE(t1);\n"
		    "CREATE INDEX o_table3_t1 ON o_table3 USING BTREE(t1);\n")

		with_index_tuples = self.extractTupleSize()
		self.assertEqual(len(with_index_tuples), 3)
		for i, tup in enumerate(with_index_tuples):
			self.assertEqual(tup[-1],
			                 create_tuples[i][-1] + INDEX_EXTRA_TUPLE_SIZE)

		node.safe_psql(
		    'postgres',
		    "CREATE INDEX o_table2_t2 ON o_table2 USING BTREE(t2);")
		node.safe_psql(
		    'postgres',
		    "CREATE INDEX o_table3_t2 ON o_table3 USING BTREE(t2);")

		with_index_tuples = self.extractTupleSize()
		self.assertEqual(len(with_index_tuples), 3)
		for i, tup in enumerate(with_index_tuples):
			if i == 0:
				continue
			self.assertEqual(tup[-1],
			                 create_tuples[i][-1] + 2 * INDEX_EXTRA_TUPLE_SIZE)

		node.stop()

	'''
		TupleNew < FreeSpace
	'''

	def test_new_tuple_free_space_enought_fit_as_is(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")

		self.startup_page_filling(8192 - self.estimateTupleSize(4, 0) - 100)
		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    "CREATE TABLE o_table4(t1 text, t2 text, t3 text, t4 text) USING orioledb;\n"
		)

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleNew >  FreeSpace (No Vacated)
	'''

	def test_new_tuple_free_space_not_enought_fit_split(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		self.startup_page_filling(8192 - self.estimateTupleSize(4, 0) + 100)
		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    "CREATE TABLE o_table4(t1 text, t2 text, t3 text, t4 text) USING orioledb;\n"
		)

		self.assertSysTreePagesCount(3)
		node.stop()

	'''
		TupleNew <  FreeSpace + Vacated
	'''

	def test_new_tuple_sum_free_and_vacated_enought_fit_compact(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 -
		                                     self.estimateTupleSize(1, 0))
		self.assertSysTreePagesCount(1)

		smalestTupSize = self.estimateTupleSize(relDescr[0][0], relDescr[0][1])
		forDrop = 0
		for i, rel in enumerate(relDescr):
			tupSize = self.estimateTupleSize(rel[0], rel[1])
			if tupSize < smalestTupSize:
				forDrop = i
				smalestTupSize = tupSize

		node.safe_psql('postgres', f"DROP TABLE o_table{forDrop + 1};\n")

		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    f"CREATE TABLE o_test{len(relDescr) + 1}(t1 text, t2 text) USING orioledb;"
		)

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleNew >  FreeSpace + Vacated
	'''

	def test_new_tuple_free_and_vacated_not_enought_fit_split(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 -
		                                     self.estimateTupleSize(1, 0))
		self.assertSysTreePagesCount(1)

		smalestTupSize = self.estimateTupleSize(relDescr[0][0], relDescr[0][1])
		forDrop = 0
		for i, rel in enumerate(relDescr):
			tupSize = self.estimateTupleSize(rel[0], rel[1])
			if tupSize < smalestTupSize:
				forDrop = i
				smalestTupSize = tupSize

		node.safe_psql('postgres', f"DROP TABLE o_table{forDrop + 1};\n")

		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    f"CREATE TABLE o_test{len(relDescr) + 1}(t1 text, t2 text, t3 text, t4 text, t5 text, t6 text, t7 text) USING orioledb;"
		)

		self.assertSysTreePagesCount(3)
		node.stop()

	'''
		TupleReplace < TupleOld (No Free Space No Vacated)
	'''

	def test_replace_decrease_size_no_free_space_fit_as_is(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192)
		self.assertSysTreePagesCount(1)

		for i, rel in enumerate(relDescr):
			if rel[1] > 0:
				node.safe_psql('postgres',
				               f"DROP INDEX o_table{i + 1}_idx_t0;\n")
				break

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace < FreeSpace + TupleOld
	'''

	def test_replace_free_space_enought_fit_as_is(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 - INDEX_EXTRA_TUPLE_SIZE -
		                                     150)
		self.assertSysTreePagesCount(1)

		for i, rel in enumerate(relDescr):
			if rel[1] < rel[0]:
				node.safe_psql(
				    'postgres',
				    f"CREATE INDEX o_table{i+1}_idx_t{rel[0] - 1} ON o_table{i} USING BTREE(t{rel[0] - 1});\n"
				)
				break

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace > FreeSpace
		TupleReplace > Vacated
		TupleReplace < FreeeSpace + Vacated(another live tuple)
	'''

	def test_replace_use_vacated_from_live_tuple_for_another_fit_compact(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 - INDEX_EXTRA_TUPLE_SIZE)
		self.assertSysTreePagesCount(1)

		vacatedTuple = None
		for i, rel in enumerate(relDescr):
			if rel[1] > 0:
				node.safe_psql('postgres',
				               f"DROP INDEX o_table{i + 1}_idx_t0;\n")
				vacatedTuple = i
				break
		self.assertSysTreePagesCount(1)

		for i, rel in enumerate(relDescr):
			if i != vacatedTuple and rel[0] > rel[1]:
				node.safe_psql(
				    'postgres',
				    f"CREATE INDEX o_table{i + 1}_t{rel[0] - 1} ON o_table{i + 1} USING BTREE(t{rel[0] - 1})\n;"
				)
				break

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace > FreeSpace
		TupleReplace > Vacated
		TupleReplace < FreeeSpace + Vacated(another dead tuple)
	'''

	def test_replace_use_vacated_from_deleted_tuple_fit_compact(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 - INDEX_EXTRA_TUPLE_SIZE)
		self.assertSysTreePagesCount(1)

		vacatedTuple = None
		for i, rel in enumerate(relDescr):
			if rel[1] == 0:
				node.safe_psql('postgres', f"DROP TABLE o_table{i + 1};\n")
				vacatedTuple = i
				break
		self.assertSysTreePagesCount(1)

		for i, rel in enumerate(relDescr):
			if i != vacatedTuple and rel[0] > rel[1]:
				node.safe_psql(
				    'postgres',
				    f"CREATE INDEX o_table{i + 1}_t{rel[0] - 1} ON o_table{i + 1} USING BTREE(t{rel[0] - 1})\n;"
				)
				break

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace > FreeSpace
		TupleReplace > Vacated
		TupleReplace < FreeeSpace + Vacated(self tuple)
	'''

	def test_replace_encrease_self_vacated_enought_fit_compact(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 - INDEX_EXTRA_TUPLE_SIZE)
		self.assertSysTreePagesCount(1)

		vacatedTuple = None
		for i, rel in enumerate(relDescr):
			if rel[1] > 0:
				node.safe_psql('postgres',
				               f"DROP INDEX o_table{i + 1}_idx_t0;\n")
				vacatedTuple = i
				break
		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    f"CREATE INDEX o_table{vacatedTuple + 1}_idx_t0 ON o_table{vacatedTuple + 1} USING BTREE(t0) where t0 > 'abcd';"
		)

		self.assertSysTreePagesCount(1)

		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace > FreeSpace
		TupleReplace > Vacated
		TupleReplace > FreeeSpace + Vacated(self tuple)
	'''

	def test_replace_encrease_self_vacated_fit_split(self):
		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")
		relDescr = self.startup_page_filling(8192 - 100)
		self.assertSysTreePagesCount(1)

		vacatedTuple = None
		for i, rel in enumerate(relDescr):
			if rel[1] > 0:
				node.safe_psql('postgres',
				               f"DROP INDEX o_table{i + 1}_idx_t0;\n")
				vacatedTuple = i
				break
		self.assertSysTreePagesCount(1)

		node.safe_psql(
		    'postgres',
		    f"CREATE INDEX o_table{vacatedTuple + 1}_idx_t0 ON o_table{vacatedTuple + 1} USING BTREE(t0) where t0 > 'abcd';"
		)

		self.assertSysTreePagesCount(3)

		node.stop()

	def test_replace_drop_in_first_trasaction_commit(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		relDescr = self.startup_page_filling(8192 - 100)
		self.assertSysTreePagesCount(1)

		transactionRel = None
		for i, rel in enumerate(relDescr):
			if rel[1] == 0:
				transactionRel = i
				break

		con1 = node.connect()
		con2 = node.connect()
		t1 = ThreadQueryExecutor(con1,
		                         f"DROP TABLE o_table{transactionRel + 1};")
		t2 = ThreadQueryExecutor(
		    con2,
		    f"CREATE INDEX o_table{transactionRel + 1}_idx_t0 ON o_table{transactionRel + 1} USING BTREE(t0)\n;"
		)
		t1.start()
		t1.join()
		self.assertSysTreePagesCount(1)
		t2.start()
		self.assertSysTreePagesCount(1)
		con1.commit()
		with self.assertRaises(DatabaseError) as e:
			t2.join()
			con2.commit()
		self.assertErrorMessageEquals(
		    e, f'relation "o_table{transactionRel + 1}" does not exist')

		self.assertSysTreePagesCount(1)
		con1.close()
		con2.close()
		node.stop()

	def test_replace_drop_in_first_trasaction_rollback(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		relDescr = self.startup_page_filling(8192 - 100)
		self.assertSysTreePagesCount(1)

		transactionRel = None
		for i, rel in enumerate(relDescr):
			if rel[1] == 0:
				transactionRel = i
				break

		con1 = node.connect()
		con2 = node.connect()
		t1 = ThreadQueryExecutor(con1,
		                         f"DROP TABLE o_table{transactionRel + 1};")
		t2 = ThreadQueryExecutor(
		    con2,
		    f"CREATE INDEX o_table{transactionRel + 1}_idx_t0 ON o_table{transactionRel + 1} USING BTREE(t0)\n;"
		)
		t1.start()
		t1.join()
		self.assertSysTreePagesCount(1)
		t2.start()
		self.assertSysTreePagesCount(1)
		con1.rollback()

		t2.join()
		con2.commit()
		self.assertSysTreePagesCount(3)
		con1.close()
		con2.close()
		node.stop()

	def test_replace_drop_in_second_trasaction_rollback(self):
		node = self.node
		node.start()
		node.safe_psql('postgres', "CREATE EXTENSION IF NOT EXISTS orioledb;")
		relDescr = self.startup_page_filling(8192 - 100)
		self.assertSysTreePagesCount(1)

		transactionRel = None
		for i, rel in enumerate(relDescr):
			if rel[1] == 0:
				transactionRel = i
				break

		con1 = node.connect()
		con2 = node.connect()
		t1 = ThreadQueryExecutor(
		    con1,
		    f"CREATE INDEX o_table{transactionRel + 1}_idx_t0 ON o_table{transactionRel + 1} USING BTREE(t0)\n;"
		)
		t2 = ThreadQueryExecutor(con2,
		                         f"DROP TABLE o_table{transactionRel + 1};")
		t1.start()
		t1.join()
		self.assertSysTreePagesCount(3)

		t2.start()
		self.assertSysTreePagesCount(3)

		con1.rollback()

		t2.join()
		con2.commit()
		con1.close()
		con2.close()

		self.assertSysTreePagesCount(3)
		node.stop()
