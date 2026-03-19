#!/usr/bin/env python3
# coding: utf-8

import unittest
import testgres
import re
import shutil
import os

from enum import Enum

from .base_test import BaseTest
from .base_test import ThreadQueryExecutor
from testgres.connection import DatabaseError

from testgres.enums import NodeStatus


class FieldIndex(Enum):
	ITEM_ID = 0
	OFFSET = 1
	TUPLE_DATOID = 2
	TUPLE_RELNODE = 3  # look at o_table_chunk_tup_print
	TUPLE_RELOID = 4
	TUPLE_CHUNKNUM = 5
	TUPLE_VERSION = 6
	DATALEN = 7


TABLE_PREF = "calib_table"


class PageFitItemsTest(BaseTest):

	# should be in sync with ORIOLEDB_BLCKSZ, BTREE_PAGE_HIKEYS_END for leaf
	ORIOLEDB_BLCKSZ = 8192
	BTREE_PAGE_HIKEYS_END = 256
	SPACE_FOR_TUPLES = ORIOLEDB_BLCKSZ - BTREE_PAGE_HIKEYS_END
	# filled in calibrate()
	TABLE_WITH_NO_FIELDS_SIZE = 0
	TABLE_EXTRA_FIELD_SIZE = 0
	TABLE_EXTRA_INDEX_SIZE = 0

	def setUp(self):
		super().setUp()
		opts = self.calibrate()

	def assertSysTreePagesCount(self, expectedCount, dump=True):
		out = self.node.execute(
		    'postgres', "select orioledb_sys_tree_structure(2, 'n');")[0][0]
		if dump:
			print("\n", flush=True)
			print(out, flush=True)
		givenCount = out.count('Page ')
		self.assertEqual(expectedCount, givenCount, out)

	def extractTupleSize(self):
		out = self.node.execute(
		    'postgres', "select orioledb_sys_tree_structure(2, 'n');")[0][0]
		print(out, flush=True)
		pattern = r"Item (\d+): offset = (\d+), tuple = \(\(\((\d+), (\d+), (\d+)\), chunknum (\d+), version (\d+)\), dataLength (\d+)\)"

		match = re.findall(pattern, out)
		tuples = []
		for item in match:
			tuples.append(item)
		return tuples

	def generate_cascade_create_table(self, count: int):

		stmts = ""

		for table_idx in range(count):
			fields = ""
			for i in range(table_idx):
				empty = len(fields) == 0
				fields += f'{"" if empty else ", "}t{i} text'

			name = f'{TABLE_PREF}{table_idx}'
			stmt = f'CREATE TABLE {name}({fields}) USING orioledb;'

			stmts += f'\n{stmt}\n'

		print(stmts, flush=True)
		return stmts

	def generate_create_index(self, table_idx: int, i: int):

		name = f'{TABLE_PREF}{table_idx}'
		ixname = f'{name}_ix{i}'
		fname = f't{i}'
		stmt = f'CREATE INDEX {ixname} ON {name} USING BTREE({fname});'

		return stmt

	def generate_cascade_create_index(self, table_idx: int, count: int):

		stmts = ""

		for i in range(count):
			stmts += f'\n{self.generate_create_index(table_idx, i)}\n'

		return stmts

	def recreate_node(self):
		print("recreate_node", flush=True)
		try:
			self.node.stop()
			self.node.cleanup()
		except Exception:
			pass

		datadir = self.node.data_dir
		if datadir and os.path.exists(datadir):
			shutil.rmtree(datadir)

		self.node = self.initNode(self.getBasePort())
		self.node.start()
		self.node.safe_psql('postgres',
		                    "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

	def calibrate_tuple_sizes(self, tables_count: int):

		node = self.node
		node.start()
		node.safe_psql('postgres',
		               "CREATE EXTENSION IF NOT EXISTS orioledb;\n")

		created = []

		self.node.safe_psql('postgres',
		                    self.generate_cascade_create_table(tables_count))

		tuples = self.extractTupleSize()
		prevlen = 0
		table_no_fields_size = 0
		perfield = 0
		for item in tuples:
			relnode = int(item[FieldIndex.TUPLE_RELNODE.value])
			datalen = int(item[FieldIndex.DATALEN.value])
			print(f'Check item relnode {relnode} datalen {datalen}: prevlen {prevlen} delta {datalen - prevlen}', flush=True)
			created.append({"relnode": relnode, "datalen": datalen})

			if prevlen == 0:  # the first table with zero fields
				table_no_fields_size = datalen
			else:
				if perfield == 0:
					perfield = datalen - prevlen
				else:
					self.assertEqual(perfield, datalen - prevlen)
			prevlen = datalen

		self.assertSysTreePagesCount(1)
		perindex = 0
		for table_idx in range(tables_count - 1):

			self.node.safe_psql('postgres',
			                    self.generate_create_index(table_idx + 1, 0))

			tuples = self.extractTupleSize()
			table = created[table_idx]
			for item in tuples:
				relnode = int(item[FieldIndex.TUPLE_RELNODE.value])
				if table["relnode"] == relnode:
					datalen = int(item[FieldIndex.DATALEN.value])
					delta = datalen - table["datalen"]
					print(f'Check #{table_idx} item relnode {relnode} datalen {datalen}: delta {delta}', flush=True)
					if perindex == 0:
						perindex = delta
					else:
						self.assertEqual(perindex, delta)
					break

		# set magic numbers
		self.TABLE_WITH_NO_FIELDS_SIZE = table_no_fields_size
		self.TABLE_EXTRA_FIELD_SIZE = perfield
		self.TABLE_EXTRA_INDEX_SIZE = perindex

		self.recreate_node()

		print({
			    "table_no_fields_size": table_no_fields_size,
			    "perfield": perfield,
			    "perindex": perindex
			}, flush=True)

	def calibrate(self):
		calib_tables_count = 8
		self.calibrate_tuple_sizes(calib_tables_count)

	def estimateTupleSize(self, relFields, indexCount):
		print("estimateTupleSize", flush=True)
		print(f"TABLE_WITH_NO_FIELDS_SIZE: {self.TABLE_WITH_NO_FIELDS_SIZE}", flush=True)
		print(f"TABLE_EXTRA_FIELD_SIZE: {self.TABLE_EXTRA_FIELD_SIZE}", flush=True)
		print(f"TABLE_EXTRA_INDEX_SIZE: {self.TABLE_EXTRA_INDEX_SIZE}", flush=True)
		tupSize = (self.TABLE_WITH_NO_FIELDS_SIZE +
				   relFields * self.TABLE_EXTRA_FIELD_SIZE + 
				   indexCount * self.TABLE_EXTRA_INDEX_SIZE)
		tupSize = (tupSize + 7) // 8 * 8 + 40
		return tupSize

	def startup_page_filling(self, estimatedPageSize):
		fillingSize = 0
		i = 1
		relDesc = []

		print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_FIELD_SIZE(%d)" %
			  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_FIELD_SIZE), flush=True)
		while estimatedPageSize - fillingSize > self.TABLE_EXTRA_FIELD_SIZE:
			print(f"fillingSize -1: {fillingSize}", flush=True)
			fillingSize += self.TABLE_WITH_NO_FIELDS_SIZE
			relFieldsCount = 0
			indexCount = 0
			print(f"fillingSize 0: {fillingSize}", flush=True)

			print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_FIELD_SIZE(%d) and relFieldsCount(%d) < 4" %
				  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_FIELD_SIZE, relFieldsCount), flush=True)
			while estimatedPageSize - fillingSize > self.TABLE_EXTRA_FIELD_SIZE and relFieldsCount < 4:
				fillingSize += self.TABLE_EXTRA_FIELD_SIZE
				relFieldsCount += 1
				print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_FIELD_SIZE(%d) and relFieldsCount(%d) < 4" %
					  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_FIELD_SIZE, relFieldsCount), flush=True)

			print(f"fillingSize 1: {fillingSize}", flush=True)
			create_table_stmt = f"CREATE TABLE o_table{i}("
			for j in range(relFieldsCount + 1):
				sep = "," if j < relFieldsCount else ")"
				create_table_stmt += f"t{j} text{sep} "
			create_table_stmt += f"USING orioledb;\n"
			self.node.safe_psql('postgres', create_table_stmt)

			newTup = self.extractTupleSize()[-1]
			fillingSize = int(newTup[FieldIndex.OFFSET.value]) + (
			    (int(newTup[FieldIndex.DATALEN.value]) + 7) // 8) * 8 + 40

			print(f"fillingSize 2: {fillingSize}", flush=True)
			print("startup_page_filling 0", flush=True)
			print(f"TABLE_EXTRA_INDEX_SIZE: {self.TABLE_EXTRA_INDEX_SIZE}", flush=True)
			print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_INDEX_SIZE(%d) and indexCount(%d) < relFieldsCount(%d)" %
				  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_INDEX_SIZE, indexCount, relFieldsCount), flush=True)
			while estimatedPageSize - fillingSize > self.TABLE_EXTRA_INDEX_SIZE and indexCount < relFieldsCount:
				create_index_stmt = f"CREATE INDEX o_table{i}_idx_t{indexCount} ON o_table{i} USING BTREE(t{indexCount});\n"
				self.node.safe_psql('postgres', create_index_stmt)
				newTup = self.extractTupleSize()[-1]
				fillingSize = int(newTup[FieldIndex.OFFSET.value]) + (
				    (int(newTup[FieldIndex.DATALEN.value]) + 7) // 8) * 8 + 40
				indexCount += 1
				print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_INDEX_SIZE(%d) and indexCount(%d) < relFieldsCount(%d)" %
					  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_INDEX_SIZE, indexCount, relFieldsCount), flush=True)
			print(f"fillingSize 3: {fillingSize}", flush=True)
			relDesc.append((relFieldsCount, indexCount))
			i = i + 1
			print("\nrelDesc: \n", relDesc, flush=True)
			print("estimatedPageSize(%d) - fillingSize(%d) > self.TABLE_EXTRA_FIELD_SIZE(%d)" %
				  (estimatedPageSize, fillingSize, self.TABLE_EXTRA_FIELD_SIZE), flush=True)
		return relDesc

	'''
		FAIL this check means just magic numbers became incorrect due to changes in tuples structures.
		To make test suite applicable to new versions tuples structures setup new magic numbers.
	'''

	def test_calibrate_sizes(self):

		node = self.node

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
			item_id = int(tup[FieldIndex.ITEM_ID.value]) + 1
			datalen = int(tup[FieldIndex.DATALEN.value])
			self.assertEqual(
			    datalen, self.TABLE_WITH_NO_FIELDS_SIZE +
			    item_id * self.TABLE_EXTRA_FIELD_SIZE)

		node.safe_psql(
		    'postgres',
		    "CREATE INDEX o_table1_t1 ON o_table1 USING BTREE(t1);\n"
		    "CREATE INDEX o_table2_t1 ON o_table2 USING BTREE(t1);\n"
		    "CREATE INDEX o_table3_t1 ON o_table3 USING BTREE(t1);\n")

		with_index_tuples = self.extractTupleSize()
		self.assertEqual(len(with_index_tuples), 3)
		for i, tup in enumerate(with_index_tuples):
			self.assertEqual(
			    int(tup[FieldIndex.DATALEN.value]),
			    int(create_tuples[i][FieldIndex.DATALEN.value]) +
			    self.TABLE_EXTRA_INDEX_SIZE)

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
			datalen = int(tup[FieldIndex.DATALEN.value])
			self.assertEqual(
			    datalen,
			    int(create_tuples[i][FieldIndex.DATALEN.value]) +
			    2 * self.TABLE_EXTRA_INDEX_SIZE)

		node.stop()

	'''
		TupleNew < FreeSpace
	'''

	def test_new_tuple_free_space_enough_fit_as_is(self):

		node = self.node

		self.startup_page_filling(self.SPACE_FOR_TUPLES - self.estimateTupleSize(4, 0) + self.estimateTupleSize(0, 0))
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

	def test_new_tuple_free_space_not_enough_fit_split(self):

		node = self.node

		self.startup_page_filling(self.SPACE_FOR_TUPLES - self.estimateTupleSize(0, 0))
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

	def test_new_tuple_sum_free_and_vacated_enough_to_fit_compact(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.estimateTupleSize(2, 0))
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

	def test_new_tuple_free_and_vacated_not_enough_fit_split(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.estimateTupleSize(2, 0))
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

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES)
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

	def test_replace_free_space_enough_fit_as_is(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.TABLE_EXTRA_INDEX_SIZE)
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
		TupleReplace < FreeSpace + Vacated(another live tuple)
	'''

	def test_replace_use_vacated_from_live_tuple_for_another_fit_compact(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.TABLE_EXTRA_INDEX_SIZE)
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
		TupleReplace < FreeSpace + Vacated(another dead tuple)
	'''

	def test_replace_use_vacated_from_deleted_tuple_fit_compact(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.TABLE_EXTRA_INDEX_SIZE)
		self.assertSysTreePagesCount(1)

		vacatedTuple = None
		for i, rel in enumerate(relDescr):
			print(f"rel: {rel}", flush=True)
			if rel[1] == 2:
				node.safe_psql('postgres', f"DROP TABLE o_table{i + 1};\n")
				vacatedTuple = i
				break
		self.assertSysTreePagesCount(1)

		for i, rel in enumerate(relDescr):
			if i != vacatedTuple and rel[0] == rel[1]:
				node.safe_psql(
				    'postgres',
				    f"CREATE INDEX o_table{i + 1}_t{rel[0] - 1} ON o_table{i + 1} USING BTREE(t{rel[0] - 1})\n;"
				)
				node.safe_psql(
				    'postgres',
				    f"CREATE INDEX o_table{i + 1}_t{rel[0] - 2} ON o_table{i + 1} USING BTREE(t{rel[0] - 2})\n;"
				)
				break

		self.assertSysTreePagesCount(1)
		node.stop()

	'''
		TupleReplace > TupleOld
		TupleReplace > FreeSpace
		TupleReplace > Vacated
		TupleReplace < FreeSpace + Vacated(self tuple)
	'''

	def test_replace_increase_self_vacated_enough_fit_compact(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES -
		                                     self.TABLE_EXTRA_INDEX_SIZE)
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

	def test_replace_increase_self_vacated_fit_split(self):

		node = self.node

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES - 100)
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

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES)
		self.assertSysTreePagesCount(1)
		print(relDescr, flush=True)

		transactionRel = None
		for i, rel in enumerate(relDescr):
			print(rel, flush=True)
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

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES)
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

		relDescr = self.startup_page_filling(self.SPACE_FOR_TUPLES)
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
