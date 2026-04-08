#!/usr/bin/env python3
# coding: utf-8

from .base_test import BaseTest

class LogicalStreamingTest(BaseTest):

	o_relname = "o_data"

	setup_sql = f"""
		CREATE EXTENSION IF NOT EXISTS orioledb;
		CREATE TABLE {o_relname}(id serial primary key, data text) USING orioledb;
	"""

	def setUp(self):
		super().setUp()
		self.node.append_conf('postgresql.conf', "wal_level = logical\n")
		self.node.append_conf('postgresql.conf', "logical_decoding_work_mem = 64kB\n")

	def test_streaming_on_large_transaction(self):
		"""
		Tests PostgreSQL logical replication with streaming = on.
		Demonstrates that changes from a large transaction arrive to
		the subscriber.
		"""

		o_relname = self.o_relname
		setup_sql = self.setup_sql

		with self.node as publisher:
			publisher.start()

			subscriber = self.getSubscriber()
			with subscriber.start() as subscriber:

				publisher.safe_psql(setup_sql)
				subscriber.safe_psql(setup_sql)

				pub = publisher.publish('test_pub', tables=[f'{o_relname}'])
				sub = subscriber.subscribe(pub, 'test_sub', streaming='on')
				self.wait_ready(subscriber)

				with publisher.connect() as pub1:
					with publisher.connect() as pub2:
						# Insert 10k rows with each tuple ~672 bytes
						# This should easily put us over 64kB work_mem
						pub1.begin()
						pub1.execute(f"""
							insert into {o_relname}(data)
								select repeat(md5(a::text), 20)
									from generate_series(1, 10000) f(a);
						""")

						pub2.begin()
						pub2.execute(f"""
							insert into {o_relname}(data)
								select repeat(md5(a::text), 20)
									from generate_series(1, 10000) f(a);
						""")
						pub2.commit()

						# Make sure the two transactions are interlaced
						pub1.execute(f"""
							insert into {o_relname}(data)
								select repeat(md5(a::text), 20)
									from generate_series(1, 100) f(a);
						""")

						pub1.commit()


				# wait until changes apply on subscriber and check them
				sub.catchup()

				with subscriber.connect() as con:
					output = con.execute(f"""select count(*) from {o_relname};""")
					tup = output[0][0]
					self.assertEqual(tup, 20100)

