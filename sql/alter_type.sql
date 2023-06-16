CREATE SCHEMA alter_type;
SET SESSION search_path = 'alter_type';
CREATE EXTENSION orioledb;

CREATE TABLE o_ddl_check
(
	f1 text NOT NULL COLLATE "C",
	f2 varchar NOT NULL,
	f3 integer,
	PRIMARY KEY (f1)
) USING orioledb;

INSERT INTO o_ddl_check VALUES ('ABC1', 'ABC2', NULL), ('ABC2', 'ABC4', NULL),
							   ('ABC3', 'ABC6', NULL);

SELECT * FROM o_ddl_check;
SELECT orioledb_table_description('o_ddl_check'::regclass);

ALTER TABLE o_ddl_check ALTER f2 TYPE text;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because 'f2' isn't indexed
ALTER TABLE o_ddl_check ALTER f2 TYPE varchar COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE varchar COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because same type and collation
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "C";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
-- OK, because binary compatible and collations match
ALTER TABLE o_ddl_check ALTER f1 TYPE text COLLATE "POSIX";
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- Fails, because no default conversion
ALTER TABLE o_ddl_check ALTER f2 TYPE timestamp;
-- Fails, because wrong date format
ALTER TABLE o_ddl_check ALTER f2 TYPE timestamp
  USING f2::timestamp without time zone;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

-- OK, because expression is valid conversion to char
ALTER TABLE o_ddl_check ALTER f2 TYPE char
	USING substr(f2, substr(f2,4,1)::int / 2, 1);
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

BEGIN;
ALTER TABLE o_ddl_check ALTER f2 TYPE int
	USING ('x' || lpad(f2, 8, '0'))::bit(32)::int;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;
ROLLBACK;
SELECT orioledb_table_description('o_ddl_check'::regclass);
SELECT orioledb_tbl_indices('o_ddl_check'::regclass);
SELECT * FROM o_ddl_check;

CREATE TABLE o_test_alter_change_byval (
    val_1 int
) USING orioledb;

INSERT INTO o_test_alter_change_byval VALUES (1);
ALTER TABLE o_test_alter_change_byval ADD COLUMN val_2 float8 DEFAULT 0.1;
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');
ALTER TABLE o_test_alter_change_byval ALTER COLUMN val_2 SET DEFAULT 0.2;
INSERT INTO o_test_alter_change_byval VALUES (2);
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');
ALTER TABLE o_test_alter_change_byval ALTER val_2 TYPE text USING val_2::text;
SELECT * FROM o_test_alter_change_byval;
SELECT orioledb_tbl_structure('o_test_alter_change_byval'::regclass, 'nue');

CREATE TABLE o_test_pkey_alter_type (
	val_1 int,
	val_2 int
) USING orioledb;

INSERT INTO o_test_pkey_alter_type
	SELECT v, v * 10 FROM generate_series(1, 5) v;

ALTER TABLE o_test_pkey_alter_type ADD PRIMARY KEY (val_1);
CREATE INDEX o_test_pkey_alter_type_ix1 ON o_test_pkey_alter_type (val_2);

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_1 TYPE int4;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_2 TYPE text USING val_2 || 'ROR';
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');

ALTER TABLE o_test_pkey_alter_type ALTER val_1 TYPE text USING val_1 || 'BOB';
SELECT * FROM o_test_pkey_alter_type ORDER BY val_1;
SELECT * FROM o_test_pkey_alter_type ORDER BY val_2;
SELECT orioledb_tbl_structure('o_test_pkey_alter_type'::regclass, 'nue');
RESET enable_seqscan;

BEGIN;
CREATE TABLE o_test_multiple_set_type_same_trx (
	val_1 int,
	val_3 int
) USING orioledb;

SELECT * FROM o_test_multiple_set_type_same_trx;

CREATE UNIQUE INDEX o_test_multiple_set_type_same_trx_ix1
	ON o_test_multiple_set_type_same_trx (val_1);

SELECT * FROM o_test_multiple_set_type_same_trx ORDER BY val_1;

INSERT INTO o_test_multiple_set_type_same_trx
	SELECT x, 3*x FROM generate_series(1,10) AS x;

ALTER TABLE o_test_multiple_set_type_same_trx ALTER val_1 TYPE bigint;

ALTER TABLE o_test_multiple_set_type_same_trx ALTER val_3 TYPE bigint;

COMMIT;

\d o_test_multiple_set_type_same_trx

CREATE TABLE IF NOT EXISTS o_test_alter_coercible (
	val_int4 int4 NOT NULL,
	val_oid oid NOT NULL,
	val_regconfig regconfig NOT NULL,
	val_regproc regproc NOT NULL,
	val_regoper regoper NOT NULL,
	val_regoperator regoperator NOT NULL,
	val_regrole regrole NOT NULL,
	val_regprocedure regprocedure NOT NULL,
	val_regcollation regcollation NOT NULL,
	val_regnamespace regnamespace  NOT NULL,
	val_regclass regclass NOT NULL,
	val_regdictionary regdictionary NOT NULL,
	val_regtype regtype NOT NULL
) USING orioledb;
INSERT INTO o_test_alter_coercible(val_int4, val_oid, val_regconfig,
								   val_regproc, val_regoper, val_regoperator,
								   val_regrole, val_regprocedure,
								   val_regcollation, val_regnamespace,
								   val_regclass, val_regdictionary,
								   val_regtype)
		VALUES(1, 2, 'simple', 'namein', '||/'::regoper, '=(integer,integer)',
			   'pg_stat_scan_tables', 'abs(numeric)', '"C"',
			   'information_schema', 'pg_type', 'simple', 'int2vector');

SELECT orioledb_table_description('o_test_alter_coercible'::regclass);
SELECT orioledb_tbl_indices('o_test_alter_coercible'::regclass);
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_int4 SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_oid SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regconfig SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regproc SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regoper SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regoperator SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regrole SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regprocedure SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regcollation SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regnamespace SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regclass SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regdictionary SET DATA TYPE int4;
ALTER TABLE o_test_alter_coercible
	ALTER COLUMN val_regtype SET DATA TYPE int4;
SELECT orioledb_table_description('o_test_alter_coercible'::regclass);
SELECT orioledb_tbl_indices('o_test_alter_coercible'::regclass);
SELECT pg_typeof(val_int4), pg_typeof(val_oid), pg_typeof(val_regconfig),
	   pg_typeof(val_regproc), pg_typeof(val_regoper),
	   pg_typeof(val_regoperator), pg_typeof(val_regrole),
	   pg_typeof(val_regprocedure), pg_typeof(val_regcollation),
	   pg_typeof(val_regnamespace), pg_typeof(val_regclass),
	   pg_typeof(val_regdictionary), pg_typeof(val_regtype)
	FROM o_test_alter_coercible;
SELECT val_int4, val_oid::oid, val_regconfig::regconfig,
	   val_regproc::regproc, val_regoper::regoper,
	   val_regoperator::regoperator, val_regrole::regrole,
	   val_regprocedure::regprocedure, val_regcollation::regcollation,
	   val_regnamespace::regnamespace, val_regclass::regclass,
	   val_regdictionary::regdictionary, val_regtype::regtype
	   FROM o_test_alter_coercible;

CREATE TABLE o_test_inherits_alter_type (
	aa TEXT
) USING orioledb;

CREATE TABLE o_test_inherits_alter_type_child (
	dd TEXT
) INHERITS (o_test_inherits_alter_type) USING orioledb;

insert into o_test_inherits_alter_type_child values('test','one');
select * from o_test_inherits_alter_type;
select * from o_test_inherits_alter_type_child;
alter table o_test_inherits_alter_type
	alter column aa type integer using bit_length(aa);
select * from o_test_inherits_alter_type;
select * from o_test_inherits_alter_type_child;

CREATE TABLE o_test_inherits_alter_type_int8 (
  val_1 int8,
  val_2 boolean
) USING orioledb;

CREATE TABLE o_test_inherits_alter_type_int8_child()
	inherits(o_test_inherits_alter_type_int8) USING orioledb;

INSERT INTO o_test_inherits_alter_type_int8(val_1, val_2)
	VALUES (1, false);
INSERT INTO o_test_inherits_alter_type_int8_child(val_1, val_2)
	VALUES (0, true);

SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8'::regclass,
							  'nue');
SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8_child'::regclass,
							  'nue');
ALTER TABLE o_test_inherits_alter_type_int8
	ALTER COLUMN val_2 TYPE text;
INSERT INTO o_test_inherits_alter_type_int8(val_1, val_2) VALUES (2, 'brr');
SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8'::regclass,
							  'nue');
SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8_child'::regclass,
							  'nue');
ALTER TABLE o_test_inherits_alter_type_int8
	ALTER COLUMN val_1 TYPE boolean USING val_1::int::boolean;
SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8'::regclass,
							  'nue');
SELECT orioledb_tbl_structure('o_test_inherits_alter_type_int8_child'::regclass,
							  'nue');

BEGIN;

CREATE TABLE o_test_alter_type_ix_included_rollback (
	val_1 int,
	val_2 text,
	val_3 int
) USING orioledb;

CREATE INDEX o_test_alter_type_ix_included_rollback_ix1
	ON o_test_alter_type_ix_included_rollback(val_3) INCLUDE (val_2);

INSERT INTO o_test_alter_type_ix_included_rollback
	SELECT v, 'XXX' || v, v * 10
		FROM generate_series(1, 5) v;

EXPLAIN (COSTS OFF)
	SELECT val_2, val_3 FROM o_test_alter_type_ix_included_rollback
		ORDER BY val_3;
SELECT val_2, val_3 FROM o_test_alter_type_ix_included_rollback ORDER BY val_3;

ALTER TABLE o_test_alter_type_ix_included_rollback
	ALTER val_2 TYPE text COLLATE "C";

EXPLAIN (COSTS OFF)
	SELECT val_2, val_3 FROM o_test_alter_type_ix_included_rollback
		ORDER BY val_3;
SELECT val_2, val_3 FROM o_test_alter_type_ix_included_rollback ORDER BY val_3;

ROLLBACK;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA alter_type CASCADE;
RESET search_path;
