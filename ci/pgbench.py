#!/usr/bin/env python3
# coding: utf-8

import testgres
import re
import argparse
import socket
import subprocess
import tarfile
import telegram
import tempfile
import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import time
import psutil
import shutil
import json


from testgres.utils import \
	get_bin_path, \
	execute_utility

data_size_regex = r'\s*(\d+)\s*(kB|MB|GB|TB)\s*'

def engineGetSchema(engine):
	if engine == 'orioledb':
		return 'orioledb'
	else:
		return 'public'

class ReadOnlyTest():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		pass

	def prepareForRun(self, engine, node):
		pass

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid random(1, 100000 * :scale)\n" +
				"SELECT abalance FROM {0}.pgbench_accounts WHERE aid = :aid;\n").format(schema)

class ReadOnlyZipfTest():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		pass

	def prepareForRun(self, engine, node):
		pass

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid random_zipfian(1, 100000 * :scale, 1.5)\n" +
				"SELECT abalance FROM {0}.pgbench_accounts WHERE aid = :aid;\n").format(schema)

class ReadOnlyTest9():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		pass

	def prepareForRun(self, engine, node):
		pass

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid1 random(1, 100000 * :scale)\n" +
				"\\set aid2 random(1, 100000 * :scale)\n" +
				"\\set aid3 random(1, 100000 * :scale)\n" +
				"\\set aid4 random(1, 100000 * :scale)\n" +
				"\\set aid5 random(1, 100000 * :scale)\n" +
				"\\set aid6 random(1, 100000 * :scale)\n" +
				"\\set aid7 random(1, 100000 * :scale)\n" +
				"\\set aid8 random(1, 100000 * :scale)\n" +
				"\\set aid9 random(1, 100000 * :scale)\n" +
				"SELECT abalance FROM {0}.pgbench_accounts WHERE aid IN (:aid1,:aid2,:aid3,:aid4,:aid5,:aid6,:aid7,:aid8,:aid9);\n").format(schema)

class ReadWriteTest():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		pass

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')
		schema = engineGetSchema(engine)
		node.safe_psql('TRUNCATE {0}.pgbench_history;'.format(schema))

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid random(1, 100000 * :scale)\n" +
				"\\set bid random(1, 1 * :scale)\n" +
				"\\set tid random(1, 10 * :scale)\n" +
				"\\set delta random(-5000, 5000)\n" +
				"BEGIN;\n" +
				"UPDATE {0}.pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n" +
				"SELECT abalance FROM {0}.pgbench_accounts WHERE aid = :aid;\n" +
				"UPDATE {0}.pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;\n" +
				"UPDATE {0}.pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;\n" +
				"INSERT INTO {0}.pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);\n" +
				"END;").format(schema)

class ReadWriteZipfTest():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		pass

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')
		schema = engineGetSchema(engine)
		node.safe_psql('TRUNCATE {0}.pgbench_history;'.format(schema))

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid random_zipfian(1, 100000 * :scale, 1.5)\n" +
				"\\set bid random_zipfian(1, 1 * :scale, 1.5)\n" +
				"\\set tid random_zipfian(1, 10 * :scale, 1.5)\n" +
				"\\set delta random(-5000, 5000)\n" +
				"BEGIN;\n" +
				"UPDATE {0}.pgbench_accounts SET abalance = abalance + :delta WHERE aid = :aid;\n" +
				"SELECT abalance FROM {0}.pgbench_accounts WHERE aid = :aid;\n" +
				"UPDATE {0}.pgbench_tellers SET tbalance = tbalance + :delta WHERE tid = :tid;\n" +
				"UPDATE {0}.pgbench_branches SET bbalance = bbalance + :delta WHERE bid = :bid;\n" +
				"INSERT INTO {0}.pgbench_history (tid, bid, aid, delta, mtime) VALUES (:tid, :bid, :aid, :delta, CURRENT_TIMESTAMP);\n" +
				"END;").format(schema)

class ReadWriteProcTest():
	def needsStdTables(self):
		return True

	def prepare(self, engine, node):
		schema = engineGetSchema(engine)
		node.safe_psql((
			"CREATE OR REPLACE FUNCTION {0}.pgbench_transaction(_aid int, _bid int, _tid int, _delta int) RETURNS void AS $$\n" +
			"BEGIN\n" +
			"UPDATE {0}.pgbench_accounts SET abalance = abalance + _delta WHERE aid = _aid;\n" +
			"PERFORM abalance FROM {0}.pgbench_accounts WHERE aid = _aid;\n" +
			"UPDATE {0}.pgbench_tellers SET tbalance = tbalance + _delta WHERE tid = _tid;\n" +
			"UPDATE {0}.pgbench_branches SET bbalance = bbalance + _delta WHERE bid = _bid;\n" +
			"INSERT INTO {0}.pgbench_history (tid, bid, aid, delta, mtime) VALUES (_tid, _bid, _aid, _delta, CURRENT_TIMESTAMP);\n" +
			"END;"
			"$$ LANGUAGE plpgsql;").format(schema))

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')
		schema = engineGetSchema(engine)
		node.safe_psql('TRUNCATE {0}.pgbench_history;'.format(schema))

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set aid random(1, 100000 * :scale)\n" +
				"\\set bid random(1, 1 * :scale)\n" +
				"\\set tid random(1, 10 * :scale)\n" +
				"\\set delta random(-5000, 5000)\n" +
				"SELECT {0}.pgbench_transaction(:aid, :bid, :tid, :delta);\n").format(schema)

class OrderedInsertTest():
	def needsStdTables(self):
		return False

	def prepare(self, engine, node):
		if engine == 'orioledb':
			node.safe_psql(
				"CREATE TABLE orioledb.insert_test (\n" +
				"  ts timestamp NOT NULL,\n" +
				"  client_id int NOT NULL,\n" +
				"  PRIMARY KEY(ts, client_id)) USING orioledb;")
		else:
			node.safe_psql(
				"CREATE TABLE public.insert_test (\n" +
				"  ts timestamp NOT NULL,\n" +
				"  client_id int NOT NULL,\n" +
				"  PRIMARY KEY(ts, client_id));")

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')
		schema = engineGetSchema(engine)
		node.safe_psql('TRUNCATE {0}.insert_test;'.format(schema))

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("INSERT INTO {0}.insert_test VALUES (current_timestamp, :client_id);\n").format(schema)

class BloatTest():
	def needsStdTables(self):
		return False

	def prepare(self, engine, node):
		if engine == 'orioledb':
			node.safe_psql(
				"CREATE TABLE orioledb.bloat_test (\n" +
				"  id integer primary key,\n" +
				"  value1 float8 not null,\n" +
				"  value2 float8 not null,\n" +
				"  value3 float8 not null,\n" +
				"  value4 float8 not null,\n" +
				"  ts timestamp not null\n" +
				") USING orioledb;\n" +
				"CREATE INDEX bloat_test_value1_idx ON orioledb.bloat_test (value1);\n" +
				"CREATE INDEX bloat_test_value2_idx ON orioledb.bloat_test (value2);\n" +
				"CREATE INDEX bloat_test_value3_idx ON orioledb.bloat_test (value3);\n" +
				"CREATE INDEX bloat_test_value4_idx ON orioledb.bloat_test (value4);\n" +
				"CREATE INDEX bloat_test_ts_idx ON orioledb.bloat_test (ts);")
		else:
			node.safe_psql(
				"CREATE TABLE public.bloat_test (\n" +
				"  id integer primary key,\n" +
				"  value1 float8 not null,\n" +
				"  value2 float8 not null,\n" +
				"  value3 float8 not null,\n" +
				"  value4 float8 not null,\n" +
				"  ts timestamp not null\n" +
				");\n" +
				"CREATE INDEX bloat_test_value1_idx ON public.bloat_test (value1);\n" +
				"CREATE INDEX bloat_test_value2_idx ON public.bloat_test (value2);\n" +
				"CREATE INDEX bloat_test_value3_idx ON public.bloat_test (value3);\n" +
				"CREATE INDEX bloat_test_value4_idx ON public.bloat_test (value4);\n" +
				"CREATE INDEX bloat_test_ts_idx ON public.bloat_test (ts);")

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')
		schema = engineGetSchema(engine)
		node.safe_psql('TRUNCATE {0}.bloat_test;'.format(schema))

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return ("\\set id random(1,100000 * :scale)\n" +
				"INSERT INTO {0}.bloat_test VALUES(:id, random(), random(), random(), random(), now())\n" +
				"ON CONFLICT (id) DO UPDATE SET ts = now();").format(schema)

class WGTest():
	def needsStdTables(self):
		return False

	def prepare(self, engine, node):
		if engine == 'orioledb':
			schema = 'orioledb'
			tableAm = 'orioledb'
		else:
			schema = 'public'
			tableAm = 'heap'

		node.safe_psql(('''
CREATE TYPE {0}.trx_type AS ENUM ('void','normal');
CREATE TYPE {0}.trx_status AS ENUM ('finished','in_progress');
CREATE TYPE {0}.trx_hold AS ENUM ('begin','commit', 'rollback');
CREATE TYPE {0}.trx_origin AS ENUM ('receipt','invoice', 'game');
CREATE TYPE {0}.trx_reason AS ENUM ('game','purchase', 'other');
CREATE TYPE {0}.op_type AS ENUM ('grant','consume');

CREATE TABLE {0}.trx_pgbench_0 (
    id bigint NOT NULL,
    root_player_id bigint NOT NULL,
    emitter smallint NOT NULL,
    ns_id integer NOT NULL,
    idempotency_key uuid NOT NULL,
    type {0}.trx_type NOT NULL,
    origin {0}.trx_origin,
    meta_data jsonb,
    internal_meta_data jsonb,
    status {0}.trx_status DEFAULT 'finished'::{0}.trx_status NOT NULL,
    hold {0}.trx_hold,
    reason {0}.trx_reason,
    created timestamp without time zone DEFAULT timezone('UTC'::text, now()),
    updated timestamp without time zone DEFAULT timezone('UTC'::text, now())
) USING {1};

ALTER TABLE ONLY {0}.trx_pgbench_0
    ADD CONSTRAINT trx_pgbench_0_pkey PRIMARY KEY (id);
CREATE INDEX trx_pgbench_0_root_player_id_idx ON {0}.trx_pgbench_0
USING btree (root_player_id);
CREATE INDEX trx_pgbench_0_status_holds_idx ON {0}.trx_pgbench_0 USING btree (status, hold);


CREATE TABLE {0}.balance_pgbench_0 (
    ns_id integer NOT NULL,
    player_id bigint NOT NULL,
    currency_id integer NOT NULL,
    amount bigint,
    expires_after timestamp without time zone,
    priority_id integer,
    created timestamp without time zone DEFAULT timezone('UTC'::text, now()),
    updated timestamp without time zone DEFAULT timezone('UTC'::text, now()),
    is_single boolean,
    classifier_id smallint DEFAULT 0 NOT NULL,
    CONSTRAINT balance_pgbench_0_amount_check CHECK ((amount >= 0))
) USING {1};

ALTER TABLE ONLY {0}.balance_pgbench_0
    ADD CONSTRAINT balance_pgbench_0_pkey PRIMARY KEY (ns_id, player_id, currency_id);

CREATE TABLE {0}.op_pgbench_0 (
    id bigint NOT NULL,
    ns_id integer NOT NULL,
    player_id bigint NOT NULL,
    trx_id bigint NOT NULL,
    currency_id integer,
    amount bigint,
    balance_id bigint,
    created timestamp without time zone DEFAULT timezone('UTC'::text, now()),
    type {0}.op_type NOT NULL,
    "order" smallint,
    CONSTRAINT op_pgbench_0_amount_check CHECK ((amount >= 0))
) USING {1};

ALTER TABLE ONLY {0}.op_pgbench_0
    ADD CONSTRAINT op_pgbench_0_pkey PRIMARY KEY (id);
CREATE INDEX op_pgbench_0_player_id_idx ON {0}.op_pgbench_0 USING btree (player_id);
CREATE INDEX op_pgbench_0_ns_id_idx ON {0}.op_pgbench_0 USING btree (ns_id);
CREATE INDEX op_pgbench_0_balance_id_idx ON {0}.op_pgbench_0 USING btree (balance_id);
CREATE INDEX op_pgbench_0_trx_id_idx ON {0}.op_pgbench_0 USING btree (trx_id);

CREATE TABLE {0}.balance_version_pgbench_0 (
    root_player_id bigint NOT NULL,
    balance_version bigint NOT NULL
) USING {1};

ALTER TABLE ONLY {0}.balance_version_pgbench_0
    ADD CONSTRAINT balance_version_pgbench_0_pkey PRIMARY KEY (root_player_id);

CREATE SEQUENCE IF NOT EXISTS {0}.trx_pgbench_seq_0;
CREATE SEQUENCE IF NOT EXISTS {0}.op_pgbench_seq_0;
CREATE SEQUENCE IF NOT EXISTS {0}.balance_version_pgbench_seq_0;
''').format(schema, tableAm))

	def prepareForRun(self, engine, node):
		node.safe_psql('CHECKPOINT;')

	def getScript(self, engine):
		schema = engineGetSchema(engine)
		return (('''
\\set region_id 0
\\set ns_id random_zipfian(1, 10, 1.1)
\\set emitter random_zipfian(1000, 3000, 1.1)
\\set root_player_id random(1, 100000*:scale)
\\set currency_id random_zipfian(1, 50, 1.1)
\\set amount random(1, 10000)
BEGIN;
INSERT INTO {0}.trx_pgbench_0
    (id, ns_id, idempotency_key,
     origin, type, hold, status, meta_data, internal_meta_data, emitter,
     root_player_id, reason)
VALUES
    (nextval('{0}.trx_pgbench_seq_0'),
     :ns_id, gen_random_uuid(), NULL, 'normal', NULL, 'finished',
     '{{"reason": 0, "eventID": null}}', NULL, :emitter, :root_player_id, 'game');

INSERT INTO {0}.op_pgbench_0
    (id, ns_id, player_id, trx_id, currency_id, amount, balance_id, type)
VALUES
    (nextval('{0}.op_pgbench_seq_0'),
     :ns_id, :root_player_id, currval('{0}.trx_pgbench_seq_0'),
     :currency_id, :amount, NULL, 'grant');


INSERT INTO {0}.balance_version_pgbench_0
    (root_player_id, balance_version)
VALUES
    (:root_player_id, nextval('{0}.balance_version_pgbench_seq_0'))
ON CONFLICT (root_player_id) DO UPDATE
    SET balance_version = excluded.balance_version;

INSERT INTO {0}.balance_pgbench_0
    (ns_id, player_id, currency_id, amount, classifier_id)
VALUES
    (:ns_id, :root_player_id, :currency_id, :amount, 0)
ON CONFLICT (ns_id, player_id, currency_id) DO UPDATE
    SET amount = balance_pgbench_0.amount + :amount;
END;
''').format(schema))


test_classes = {
	'read-write' : ReadWriteTest,
	'read-write-proc' : ReadWriteProcTest,
	'read-write-zipf' : ReadWriteZipfTest,
	'read-only' : ReadOnlyTest,
	'read-only-9' : ReadOnlyTest9,
	'read-only-zipf' : ReadOnlyZipfTest,
	'ordered-insert' : OrderedInsertTest,
	'bloat' : BloatTest,
	'wg' : WGTest,
}

def parse_data_size(value):
	match = re.match(data_size_regex, value)
	if not match:
		raise argparse.ArgumentTypeError("%s is an invalid data size value" % value)
	grp = match.groups()
	return grp[0] + grp[1]

def parse_clinets(value):
	result = []
	for c in value.split(','):
		c = int(c)
		if c <= 0:
			raise argparse.ArgumentTypeError("%s is an invalid positive int value" % c)
		result.append(c)
	return result

def parse_engines(value):
	result = []
	for c in value.split(','):
		if c == 'builtin' or c == 'orioledb':
			result.append(c)
		else:
			raise argparse.ArgumentTypeError("%s is unknown engine" % c)
	return result

def parse_tests(value):
	result = []
	for c in value.split(','):
		if c in test_classes:
			result.append(c)
		else:
			raise argparse.ArgumentTypeError("%s is unknown test" % c)
	return result

def parse_on_off(value):
	if value in ['on', 'off']:
		return value
	raise argparse.ArgumentTypeError("%s is unknown on/off value" % value)

def parse_on_off_bool(value):
	if value == 'on':
		return True
	elif value == 'off':
		return False
	raise argparse.ArgumentTypeError("%s is unknown on/off value" % value)

def check_positive(value):
	ivalue = int(value)
	if ivalue <= 0:
		raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
	return ivalue

instance_type_regex = r'^instance-type: (.*)$'

def get_machine_name():
	name = socket.gethostname()
	try:
		result = subprocess.run(['ec2metadata'], stdout = subprocess.PIPE)
		for line in result.stdout.splitlines():
			match = re.search(instance_type_regex, line.decode('utf8'))
			if match:
				name = "%s (%s)" % (name, match.groups()[0])
	except:
		pass
	return name

tps_regex = r'^tps = (\d+\.\d+) '

# Read pgbench output and find TPS
def get_tps(fname):
	try:
		tps = None
		with open(fname, 'rt') as fh:
			for line in fh:
				match = re.search(tps_regex, line)
				if match:
					tps = match.groups()[0]
					tps = int(round(float(tps)))
	except:
		tps = None
	return tps

class PgBenchTest:
	def parse_args(self):
		default_output = 'results-%s-%s.tar.gz' % (
			socket.gethostname(), str(int(time.time())))
		parser = argparse.ArgumentParser()
		parser.add_argument('--shared_buffers', type=parse_data_size,
							dest='shared_buffers', default='1GB')
		parser.add_argument('--undo_buffers', type=parse_data_size,
							dest='undo_buffers', default='1GB')
		parser.add_argument('--checkpoint_flush_after', type=parse_data_size,
							dest='checkpoint_flush_after', default='1MB')
		parser.add_argument('--max_wal_size', type=parse_data_size,
							dest='max_wal_size', default='1GB')
		parser.add_argument('--max_connections', type=int, default=100)
		parser.add_argument('--clients', type=parse_clinets,
							default=[1, 5, 10, 20])
		parser.add_argument('--time', type=check_positive,
							default=5)
		parser.add_argument('--ntries', type=check_positive,
							default=1)
		parser.add_argument('--scale', type=check_positive,
							default=10)
		parser.add_argument('--output', default=default_output)
		parser.add_argument('--engines', type=parse_engines,
							default=['builtin', 'orioledb'])
		parser.add_argument('--tests', type=parse_tests,
							default=['read-only', 'read-only-9'])
		parser.add_argument('--base_dir', default=None)
		parser.add_argument('--wal_dir', default=None)
		parser.add_argument('--port', type=check_positive,
							default=None)
		parser.add_argument('--bot_token',
							default=os.getenv('TELEGRAM_BOT_TOKEN'))
		parser.add_argument('--chat_id',
							default=os.getenv('TELEGRAM_CHAT_ID'))
		parser.add_argument('--fsync',
							type=parse_on_off, default='on')
		parser.add_argument('--synchronous_commit',
							type=parse_on_off, default='off')
		parser.add_argument('--rate', type=check_positive,
							default=None)
		parser.add_argument('--checkpoint_timeout', type=check_positive,
							default=300)
		parser.add_argument('--max_io_concurrency', type=int, default=0)
		parser.add_argument('--initdb',
							type=parse_on_off_bool, default='on')
		parser.add_argument('--device_filename', default=None)
		parser.add_argument('--device_length', type=parse_data_size,
							dest='device_length', default='1GB')
		parser.add_argument('--use_mmap',
							type=parse_on_off, default='off')
		parser.add_argument('--results_dir', default=None)

		self.args = parser.parse_args()

	def report_progress(self, msg):
		args = self.args
		if  self.bot:
			try:
				self.bot.send_message(chat_id=args.chat_id, text=msg)
			except:
				pass

	def report_file(self, filename):
		if self.bot:
			for i in range(0, 10):
				try:
					self.bot.send_document(chat_id = self.args.chat_id,
										   document = open(filename, 'rb'))
					return
				except:
					time.sleep(1)

	def report_image(self, filename):
		if self.bot:
			for i in range(0, 10):
				try:
					self.bot.send_photo(chat_id = self.args.chat_id,
										photo = open(filename, 'rb'))
					return
				except:
					time.sleep(1)

	def prepare(self):
		args = self.args
		if args.bot_token and args.chat_id:
			self.bot = telegram.Bot(args.bot_token)
		else:
			self.bot = None
		node = testgres.get_new_node('test',
									 base_dir = args.base_dir,
									 port = args.port)
		self.node = node
		if args.results_dir:
			self.results_dir = args.results_dir
			os.makedirs(self.results_dir, exist_ok=True)
		else:
			self.results_dir = tempfile.mkdtemp(prefix='benchmark_')

		if args.initdb:
			node.init() # run initdb

			if args.wal_dir:
				shutil.move(os.path.join(node.data_dir, 'pg_wal'),
							args.wal_dir)
				os.symlink(os.path.join(args.wal_dir, 'pg_wal'),
						   os.path.join(node.data_dir, 'pg_wal'))

			node.append_conf('postgresql.conf',
							 "fsync = %s\n"
							 "log_statement = 'none'\n"
							 "max_connections = %s\n"
							 "synchronous_commit = %s\n"
							 "checkpoint_timeout = %s\n"
							 "checkpoint_flush_after = %s\n"
							 "max_wal_size = %s\n"
							 "checkpoint_timeout = %s\n"%
							 (args.fsync,
							  str(args.max_connections),
							  args.synchronous_commit,
							  str(args.checkpoint_timeout),
							  args.checkpoint_flush_after,
							  str(args.max_wal_size),
							  str(args.checkpoint_timeout)))

			if 'builtin' in args.engines:
				node.append_conf("shared_buffers = %s\n" %
								 (args.shared_buffers))

			if 'orioledb' in args.engines:
				node.append_conf("shared_preload_libraries = 'orioledb'\n"
								 "orioledb.main_buffers = %s\n"
								 "orioledb.undo_buffers = %s\n"
								 "orioledb.checkpoint_completion_ratio = 1.0\n"
								 "orioledb.max_io_concurrency = %s\n" %
								 (args.shared_buffers,
								  args.undo_buffers,
								  args.max_io_concurrency))

			if args.device_filename:
				node.append_conf("orioledb.use_mmap = %s\n"
								 "orioledb.device_filename = '%s'\n"
								 "orioledb.device_length = '%s'\n" %
								 (args.use_mmap,
								  args.device_filename,
								  args.device_length))

		stdTablesNeeded = False
		tests = {}
		for test_name in args.tests:
			testInstance = test_classes[test_name]()
			tests[test_name] = testInstance
			if testInstance.needsStdTables():
				stdTablesNeeded = True
		self.tests = tests

		node.start() # start PostgreSQL

		if args.initdb:
			if 'orioledb' in args.engines:
				node.safe_psql('postgres',
							   "CREATE EXTENSION orioledb;\n"
							   "CREATE SCHEMA orioledb;")

			if stdTablesNeeded:
				if 'builtin' in args.engines:
					node.safe_psql('postgres',
								   "CREATE TABLE public.pgbench_accounts (\n"
								   "	aid integer NOT NULL,\n"
								   "	bid integer,\n"
								   "	abalance integer,\n"
								   "	filler character(84)\n"
								   "	);\n"
								   "CREATE TABLE public.pgbench_branches (\n"
								   "	bid integer NOT NULL,\n"
								   "	bbalance integer,\n"
								   "	filler character(88)\n"
								   ");\n"
								   "CREATE TABLE public.pgbench_tellers (\n"
								   "	tid integer NOT NULL,\n"
								   "	bid integer,\n"
								   "	tbalance integer,\n"
								   "	filler character(84)\n"
								   ");\n"
								   "CREATE TABLE public.pgbench_history\n"
								   "(\n"
								   "	tid integer NOT NULL,\n"
								   "	bid integer NOT NULL,\n"
								   "	aid integer NOT NULL,\n"
								   "	delta integer NOT NULL,\n"
								   "	mtime timestamp NOT NULL,\n"
								   "	filler character(22)\n"
								   ");\n")

				if 'orioledb' in args.engines:
					node.safe_psql('postgres',
								   "CREATE TABLE orioledb.pgbench_accounts (\n"
								   "	aid integer NOT NULL PRIMARY KEY,\n"
								   "	bid integer,\n"
								   "	abalance integer,\n"
								   "	filler character(84)\n"
								   ") USING orioledb;\n"
								   "CREATE TABLE orioledb.pgbench_branches (\n"
								   "	bid integer NOT NULL PRIMARY KEY,\n"
								   "	bbalance integer,\n"
								   "	filler character(88)\n"
								   ") USING orioledb;\n"
								   "CREATE TABLE orioledb.pgbench_tellers (\n"
								   "	tid integer NOT NULL PRIMARY KEY,\n"
								   "	bid integer,\n"
								   "	tbalance integer,\n"
								   "	filler character(84)\n"
								   ") USING orioledb;\n"
								   "CREATE TABLE orioledb.pgbench_history\n"
								   "(\n"
								   "	tid integer NOT NULL,\n"
								   "	bid integer NOT NULL,\n"
								   "	aid integer NOT NULL,\n"
								   "	delta integer NOT NULL,\n"
								   "	mtime timestamp NOT NULL,\n"
								   "	filler character(22),\n"
								   "	PRIMARY KEY(bid, mtime, tid, aid, delta)\n"
								   ") USING orioledb;\n")

				for engine in args.engines:
					schema = engineGetSchema(engine)
					for i in range(0, args.scale):
						node.safe_psql('postgres',
									   "INSERT INTO %s.pgbench_branches (bid, bbalance)\n"
									   "	(SELECT i, 0\n"
									   "	 FROM generate_series(%s, %s) i);\n" %
									   (schema, i * 1 + 1, (i + 1) * 1))
						node.safe_psql('postgres',
									   "INSERT INTO %s.pgbench_tellers (tid, bid, tbalance)\n"
									   "	(SELECT i, (i - 1) / 10 + 1, 0\n"
									   "	 FROM generate_series(%s, %s) i);\n" %
									   (schema, i * 10 + 1, (i + 1) * 10))
						node.safe_psql('postgres',
									   "INSERT INTO %s.pgbench_accounts (aid, bid, abalance, filler)\n"
									   "	(SELECT i, (i - 1) / 100000 + 1, 0, ''\n"
									   "	 FROM generate_series(%s, %s) i);\n" %
									   (schema, i * 100000 + 1, (i + 1) * 100000))

				if stdTablesNeeded and 'builtin' in args.engines:
					node.safe_psql('postgres',
								   "ALTER TABLE public.pgbench_branches ADD PRIMARY KEY (bid);\n"
								   "ALTER TABLE public.pgbench_tellers ADD PRIMARY KEY (tid);\n"
								   "ALTER TABLE public.pgbench_accounts ADD PRIMARY KEY (aid);\n")

					node.safe_psql('postgres', 'VACUUM ANALYZE public.pgbench_accounts;')
					node.safe_psql('postgres', 'VACUUM ANALYZE public.pgbench_branches;')
					node.safe_psql('postgres', 'VACUUM ANALYZE public.pgbench_tellers;')
					node.safe_psql('postgres', 'VACUUM ANALYZE public.pgbench_history;')

			for engine in args.engines:
				for test_name in args.tests:
					self.tests[test_name].prepare(engine, node)

		node.safe_psql('postgres', 'CHECKPOINT;')

		self.report_progress('initilization completed')

	def run_pgbench(self, args, run_name):
		output_filename = '%s/%s.log' % (
				self.results_dir,
				run_name)
		resources_filename = '%s/%s-resources.log' % (
				self.results_dir,
				run_name)
		con = self.node.connect()
		output_file = open(output_filename, 'w')
		resources_file = open(resources_filename, 'w')
		process = subprocess.Popen(
			args,
			stdout=output_file,
			stderr=subprocess.STDOUT)

		prev_cpu_times = psutil.cpu_times()
		prev_disk_usage = psutil.disk_io_counters()
		cpu_count = psutil.cpu_count()
		t = time.time()
		i = 0
		mount_point = self.node.base_dir

		while process.poll() is None:
			i = i + 1
			time.sleep(max(t + i - time.time(), 0.0))
			cpu_times = psutil.cpu_times()
			disk_usage = psutil.disk_io_counters()
			try:
				disk_space_used = shutil.disk_usage(mount_point).used
			except:
				disk_space_used = None
			(waits, lsn) = con.execute("""
								SELECT jsonb_object_agg(k, v)::text waits,
								       pg_current_wal_lsn() lsn
								FROM (SELECT coalesce(wait_event, 'CPU') k, count(*) v
									  FROM pg_stat_activity
									  GROUP BY wait_event) x
								""")[0]
			con.commit()
			delta = {'time': i,
					 'disk_used': disk_space_used,
					 'system': (cpu_times.system - prev_cpu_times.system) / cpu_count * 100.0,
					 'user': (cpu_times.user - prev_cpu_times.user) / cpu_count * 100.0,
					 'idle': (cpu_times.idle - prev_cpu_times.idle) / cpu_count * 100.0,
					 'read_count': disk_usage.read_count - prev_disk_usage.read_count,
					 'write_count': disk_usage.write_count - prev_disk_usage.write_count,
					 'read_bytes': disk_usage.read_bytes - prev_disk_usage.read_bytes,
					 'write_bytes': disk_usage.write_bytes - prev_disk_usage.write_bytes,
					 'waits': json.loads(waits),
					 'lsn': lsn}
			prev_cpu_times = cpu_times
			prev_disk_usage = disk_usage
			resources_file.write(json.dumps(delta) + "\n")
			resources_file.flush()

		output_file.close()
		resources_file.close()
		con.close()

		tps = get_tps(output_filename)
		return tps

	def benchmark(self):
		args = self.args
		node = self.node
		self.results = {}
		for engine in args.engines:
			for test_name in args.tests:
				test_file = tempfile.mktemp(prefix='benchmark_')
				with open(test_file, 'wt') as f:
					f.write(self.tests[test_name].getScript(engine))
				serie = []
				for c in args.clients:
					measures = []
					for num in range(0, args.ntries):
						self.tests[test_name].prepareForRun(engine, node)
						params = [
							get_bin_path("pgbench"),
							'-s', str(args.scale),
							"-p", str(node.port),
							"-h", node.host,
							'-c', str(c),
							'-j', str(c),
							'-M', 'prepared',
							'-f', test_file,
							'-T', str(args.time),
							'-P', '1'
						]
						if args.rate:
							params.append('-R')
							params.append(str(args.rate))
						params.append('postgres')
						run_name = '%s-%s-scale-%s-%s-%s' % (
								engine,
								test_name,
								str(args.scale),
								str(c),
								str(num))
						tps = self.run_pgbench(params, run_name)
						measures.append(tps)
						self.report_progress('%s: %s' % (run_name, str(tps)))
					measures.sort()
					if len(measures) % 2 == 1:
						serie.append(measures[len(measures) // 2])
					else:
						serie.append((measures[len(measures) // 2] + measures[len(measures) // 2 - 1]) / 2.0)
				self.results[engine + '-' + test_name] = serie
		self.report_progress('benchmark completed')

	def draw_graph(self):
		args = self.args
		colors = ['#2200C6', '#039533', '#F60114', '#FC6A17', '#8030D4', '#ACAC16', '#02C0D7', '#E81091']
		markers = ['v', 'o', 'x', 's', 'x', 'o', 'v', 's']
		fig = plt.figure(figsize = (10, 6))
		ax = fig.add_subplot(1, 1, 1)
		i = 0
		for name in self.results:
			serie = self.results[name]
			line, = ax.plot(self.args.clients, serie, label = name, color = colors[i])
			plt.setp(line, linewidth = 4, marker = markers[i], markersize = 8, markeredgewidth = 2, markeredgecolor = colors[i])
			i = i + 1
		legend = ax.legend(loc = 0, fancybox = True)
		title = ("pgbench -s %s on %s\nmedian of %s %s-seconds runs "
				 "with shared_buffers = %s, max_connections = %s") % (
				 args.scale, get_machine_name(), args.ntries, args.time,
				 args.shared_buffers, args.max_connections)
		ax.set_title(title, y = 1.03)
		ax.ticklabel_format(axis = 'y', style = 'sci', scilimits = (-2, 10))
		plt.xlabel('# Clients')
		plt.ylabel('TPS')
		axes = plt.gca()
		ax.grid(True)
		plt.tight_layout()
		graph_filename = self.results_dir + '/graph.png'
		plt.savefig(graph_filename, format = 'png', dpi = 144, transparent = False)
		self.report_image(graph_filename)

	def tear_down(self):
		if hasattr(self, 'node'):
			self.node.stop() # stop PostgreSQL

	def run(self):
		try:
			self.parse_args()
			self.prepare()
			self.benchmark()
			self.draw_graph()
		finally:
			self.tear_down()

test = PgBenchTest()
test.run()
