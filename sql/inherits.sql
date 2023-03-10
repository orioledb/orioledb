CREATE SCHEMA inherits;
SET SESSION search_path = 'inherits';
CREATE EXTENSION orioledb;

CREATE TABLE p_stock (
	item_id int
) USING orioledb;

CREATE TABLE c_stock(
	ex int
) INHERITS (p_stock) USING orioledb;

SELECT orioledb_table_description('p_stock'::regclass);
SELECT orioledb_tbl_indices('p_stock'::regclass);
SELECT orioledb_table_description('c_stock'::regclass);
SELECT orioledb_tbl_indices('c_stock'::regclass);

ALTER TABLE p_stock ADD COLUMN balance int;

SELECT orioledb_table_description('p_stock'::regclass);
SELECT orioledb_tbl_indices('p_stock'::regclass);
SELECT orioledb_table_description('c_stock'::regclass);
SELECT orioledb_tbl_indices('c_stock'::regclass);

INSERT INTO p_stock (item_id, balance) VALUES (1, 2);
INSERT INTO p_stock (item_id, balance) VALUES (3, 4);

INSERT INTO c_stock (item_id, ex, balance) VALUES (5, 6, 7);

SELECT * FROM p_stock;
SELECT * FROM c_stock;

EXPLAIN (COSTS OFF) UPDATE p_stock SET balance = balance + 1;
UPDATE p_stock SET balance = balance + 1;

SELECT * FROM p_stock;
SELECT * FROM c_stock;

DELETE FROM p_stock WHERE item_id = 5;

SELECT * FROM p_stock;
SELECT * FROM c_stock;

--
-- Test inheritance features
--
CREATE TABLE a (aa TEXT) USING orioledb;
CREATE TABLE b (bb TEXT) INHERITS (a) USING orioledb;
CREATE TABLE c (cc TEXT) INHERITS (a) USING orioledb;
CREATE TABLE d (dd TEXT) INHERITS (b,c,a) USING orioledb;

INSERT INTO a(aa) VALUES('aaa');
INSERT INTO a(aa) VALUES('aaaa');
INSERT INTO a(aa) VALUES('aaaaa');
INSERT INTO a(aa) VALUES('aaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaa');
INSERT INTO a(aa) VALUES('aaaaaaaa');

INSERT INTO b(aa) VALUES('bbb');
INSERT INTO b(aa) VALUES('bbbb');
INSERT INTO b(aa) VALUES('bbbbb');
INSERT INTO b(aa) VALUES('bbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbb');
INSERT INTO b(aa) VALUES('bbbbbbbb');

INSERT INTO c(aa) VALUES('ccc');
INSERT INTO c(aa) VALUES('cccc');
INSERT INTO c(aa) VALUES('ccccc');
INSERT INTO c(aa) VALUES('cccccc');
INSERT INTO c(aa) VALUES('ccccccc');
INSERT INTO c(aa) VALUES('cccccccc');

INSERT INTO d(aa) VALUES('ddd');
INSERT INTO d(aa) VALUES('dddd');
INSERT INTO d(aa) VALUES('ddddd');
INSERT INTO d(aa) VALUES('dddddd');
INSERT INTO d(aa) VALUES('ddddddd');
INSERT INTO d(aa) VALUES('dddddddd');

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE a SET aa='zzzz' WHERE aa='aaaa';
UPDATE ONLY a SET aa='zzzzz' WHERE aa='aaaaa';
UPDATE b SET aa='zzz' WHERE aa='aaa';
UPDATE ONLY b SET aa='zzz' WHERE aa='aaa';
UPDATE a SET aa='zzzzzz' WHERE aa LIKE 'aaa%';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE b SET aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

UPDATE a SET aa='new';

DELETE FROM ONLY c WHERE aa='new';

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

DELETE FROM a;

SELECT relname, a.* FROM a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM d, pg_class where d.tableoid = pg_class.oid;
SELECT relname, a.* FROM ONLY a, pg_class where a.tableoid = pg_class.oid;
SELECT relname, b.* FROM ONLY b, pg_class where b.tableoid = pg_class.oid;
SELECT relname, c.* FROM ONLY c, pg_class where c.tableoid = pg_class.oid;
SELECT relname, d.* FROM ONLY d, pg_class where d.tableoid = pg_class.oid;

-- Confirm PRIMARY KEY adds NOT NULL constraint to child table
CREATE TABLE z (b TEXT, PRIMARY KEY(aa, b)) inherits (a) USING orioledb;
INSERT INTO z VALUES (NULL, 'text'); -- should fail

-- Check inherited UPDATE with all children excluded
create table some_tab (a int, b int) USING orioledb;
create table some_tab_child () inherits (some_tab) USING orioledb;
insert into some_tab_child values(1,2);

explain (verbose, costs off)
update some_tab set a = a + 1 where false;
update some_tab set a = a + 1 where false;
explain (verbose, costs off)
update some_tab set a = a + 1 where false returning b, a;
update some_tab set a = a + 1 where false returning b, a;
table some_tab;

drop table some_tab cascade;

-- Check UPDATE with inherited target and an inherited source table
CREATE TABLE foo(f1 int, f2 int);
CREATE TABLE foo2(f3 int) inherits (foo);
CREATE TABLE bar(f1 int, f2 int);
CREATE TABLE bar2(f3 int) inherits (bar);

insert into foo values(1,1);
insert into foo values(3,3);
insert into foo2 values(2,2,2);
insert into foo2 values(3,3,3);
insert into bar values(1,1);
insert into bar values(2,2);
insert into bar values(3,3);
insert into bar values(4,4);
insert into bar2 values(1,1,1);
insert into bar2 values(2,2,2);
insert into bar2 values(3,3,3);
insert into bar2 values(4,4,4);

update bar set f2 = f2 + 100 where f1 in (select f1 from foo);

select tableoid::regclass::text as relname, bar.* from bar order by 1,2;

-- Check UPDATE with inherited target and an appendrel subquery
update bar set f2 = f2 + 100
from
  ( select f1 from foo union all select f1+3 from foo ) ss
where bar.f1 = ss.f1;

select tableoid::regclass::text as relname, bar.* from bar order by 1,2;

-- Check UPDATE with *partitioned* inherited target and an appendrel subquery
create table some_tab (a int) USING orioledb;
insert into some_tab values (0);
create table some_tab_child () inherits (some_tab) USING orioledb;
insert into some_tab_child values (1);
drop table some_tab cascade;

/* Test multiple inheritance of column defaults */

CREATE TABLE firstparent (tomorrow date default now()::date + 1) USING orioledb;
CREATE TABLE secondparent (tomorrow date default  now() :: date  +  1) USING orioledb;
CREATE TABLE jointchild () INHERITS (firstparent, secondparent) USING orioledb;  -- ok
CREATE TABLE thirdparent (tomorrow date default now()::date - 1) USING orioledb;
CREATE TABLE otherchild () INHERITS (firstparent, thirdparent) USING orioledb;  -- not ok
CREATE TABLE otherchild (tomorrow date default now())
  INHERITS (firstparent, thirdparent) USING orioledb;  -- ok, child resolves ambiguous default

DROP TABLE firstparent, secondparent, jointchild, thirdparent, otherchild;

-- The above verified that we can change the type of a multiply-inherited
-- column; but we should reject that if any definition was inherited from
-- an unrelated parent.
CREATE TABLE parent1(f1 int, f2 int);
CREATE TABLE parent2(f1 int, f3 bigint);
CREATE TABLE childtab(f4 int) inherits(parent1, parent2);
alter table parent1 alter column f1 type bigint;  -- fail, conflict w/parent2
alter table parent1 alter column f2 type bigint;  -- ok

-- Test non-inheritable parent constraints
create table p1(ff1 int) USING orioledb;
alter table p1 add constraint p1chk check (ff1 > 0) no inherit;
alter table p1 add constraint p2chk check (ff1 > 10);
-- connoinherit should be true for NO INHERIT constraint
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pgc.connoinherit from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname = 'p1' order by 1,2;

-- Test that child does not inherit NO INHERIT constraints
create table c1 () inherits (p1) USING orioledb;
\d p1
\d c1

-- Test that child does not override inheritable constraints of the parent
create table c2 (constraint p2chk check (ff1 > 10) no inherit) inherits (p1) USING orioledb;	--fails

drop table p1 cascade;

-- Tests for casting between the rowtypes of parent and child
-- tables. See the pgsql-hackers thread beginning Dec. 4/04
create table base (i integer) USING orioledb;
create table derived () inherits (base) USING orioledb;
create table more_derived (like derived, b int) inherits (derived) USING orioledb;
insert into derived (i) values (0);
select derived::base from derived;
select NULL::derived::base;
-- remove redundant conversions.
explain (verbose on, costs off) select row(i, b)::more_derived::derived::base from more_derived;
explain (verbose on, costs off) select (1, 2)::more_derived::derived::base;
drop table more_derived;
drop table derived;
drop table base;

create table p1(ff1 int) USING orioledb;
create table p2(f1 text) USING orioledb;
create function p2text(p2) returns text as 'select $1.f1' language sql;
create table c1(f3 int) inherits(p1,p2) USING orioledb;
insert into c1 values(123456789, 'hi', 42);
select p2text(c1.*) from c1;
drop function p2text(p2);
drop table c1;
drop table p2;
drop table p1;

CREATE TABLE ac (aa TEXT) USING orioledb;
alter table ac add constraint ac_check check (aa is not null);
CREATE TABLE bc (bb TEXT) INHERITS (ac) USING orioledb;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);

alter table bc drop constraint ac_check;  -- fail, disallowed
alter table ac drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

-- try the unnamed-constraint case
alter table ac add check (aa is not null);
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

insert into ac (aa) values (NULL);
insert into bc (aa) values (NULL);

alter table bc drop constraint ac_aa_check;  -- fail, disallowed
alter table ac drop constraint ac_aa_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

alter table ac add constraint ac_check check (aa is not null);
alter table bc no inherit ac;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
alter table bc drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;
alter table ac drop constraint ac_check;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

drop table bc;
drop table ac;

create table ac (a int constraint check_a check (a <> 0)) USING orioledb;
create table bc (a int constraint check_a check (a <> 0), b int constraint check_b check (b <> 0)) inherits (ac) USING orioledb;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc') order by 1,2;

drop table bc;
drop table ac;

create table ac (a int constraint check_a check (a <> 0)) USING orioledb;
create table bc (b int constraint check_b check (b <> 0)) USING orioledb;
create table cc (c int constraint check_c check (c <> 0)) inherits (ac, bc) USING orioledb;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;

alter table cc no inherit bc;
select pc.relname, pgc.conname, pgc.contype, pgc.conislocal, pgc.coninhcount, pg_get_expr(pgc.conbin, pc.oid) as consrc from pg_class as pc inner join pg_constraint as pgc on (pgc.conrelid = pc.oid) where pc.relname in ('ac', 'bc', 'cc') order by 1,2;

drop table cc;
drop table bc;
drop table ac;

create table p1(f1 int) USING orioledb;
create table p2(f2 int) USING orioledb;
create table c1(f3 int) inherits(p1,p2) USING orioledb;
insert into c1 values(1,-1,2);
alter table p2 add constraint cc check (f2>0);  -- fail
alter table p2 add check (f2>0);  -- check it without a name, too
delete from c1;
insert into c1 values(1,1,2);
alter table p2 add check (f2>0);
insert into c1 values(1,-1,2);  -- fail
create table c2(f3 int) inherits(p1,p2) USING orioledb;
\d c2
create table c3 (f4 int) inherits(c1,c2) USING orioledb;
\d c3
drop table p1 cascade;
drop table p2 cascade;

create table pp1 (f1 int) USING orioledb;
create table cc1 (f2 text, f3 int) inherits (pp1) USING orioledb;
alter table pp1 add column a1 int check (a1 > 0);
\d cc1
create table cc2(f4 float) inherits(pp1,cc1) USING orioledb;
\d cc2
alter table pp1 add column a2 int check (a2 > 0);
\d cc2
drop table pp1 cascade;

-- Test for renaming in simple multiple inheritance
CREATE TABLE inht1 (a int, b int) USING orioledb;
CREATE TABLE inhs1 (b int, c int) USING orioledb;
CREATE TABLE inhts (d int) INHERITS (inht1, inhs1) USING orioledb;

ALTER TABLE inht1 RENAME a TO aa;
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
ALTER TABLE inhts RENAME aa TO aaa;      -- to be failed
ALTER TABLE inhts RENAME d TO dd;
\d+ inhts

DROP TABLE inhts;

-- Test for renaming in diamond inheritance
CREATE TABLE inht2 (x int) INHERITS (inht1) USING orioledb;
CREATE TABLE inht3 (y int) INHERITS (inht1) USING orioledb;
CREATE TABLE inht4 (z int) INHERITS (inht2, inht3) USING orioledb;

ALTER TABLE inht1 RENAME aa TO aaa;
\d+ inht4

CREATE TABLE inhts (d int) INHERITS (inht2, inhs1) USING orioledb;
ALTER TABLE inht1 RENAME aaa TO aaaa;
ALTER TABLE inht1 RENAME b TO bb;                -- to be failed
\d+ inhts

WITH RECURSIVE r AS (
  SELECT 'inht1'::regclass AS inhrelid
UNION ALL
  SELECT c.inhrelid FROM pg_inherits c, r WHERE r.inhrelid = c.inhparent
)
SELECT a.attrelid::regclass, a.attname, a.attinhcount, e.expected
  FROM (SELECT inhrelid, count(*) AS expected FROM pg_inherits
        WHERE inhparent IN (SELECT inhrelid FROM r) GROUP BY inhrelid) e
  JOIN pg_attribute a ON e.inhrelid = a.attrelid WHERE NOT attislocal
  ORDER BY a.attrelid::regclass::name, a.attnum;

DROP TABLE inht1, inhs1 CASCADE;


-- Test non-inheritable indices [UNIQUE, EXCLUDE] constraints
CREATE TABLE test_constraints (id int, val1 varchar, val2 int, UNIQUE(val1, val2)) USING orioledb;
CREATE TABLE test_constraints_inh () INHERITS (test_constraints) USING orioledb;
\d+ test_constraints
ALTER TABLE ONLY test_constraints DROP CONSTRAINT test_constraints_val1_val2_key;
\d+ test_constraints
\d+ test_constraints_inh
DROP TABLE test_constraints_inh;
DROP TABLE test_constraints;

-- Test non-inheritable foreign key constraints
CREATE TABLE test_primary_constraints(id int PRIMARY KEY) USING orioledb;
CREATE TABLE test_foreign_constraints(id1 int REFERENCES test_primary_constraints(id)) USING orioledb;
CREATE TABLE test_foreign_constraints_inh () INHERITS (test_foreign_constraints) USING orioledb;
\d+ test_primary_constraints
\d+ test_foreign_constraints
ALTER TABLE test_foreign_constraints DROP CONSTRAINT test_foreign_constraints_id1_fkey;
\d+ test_foreign_constraints
\d+ test_foreign_constraints_inh
DROP TABLE test_foreign_constraints_inh;
DROP TABLE test_foreign_constraints;
DROP TABLE test_primary_constraints;

-- Test foreign key behavior
create table inh_fk_1 (a int primary key) USING orioledb;
insert into inh_fk_1 values (1), (2), (3);
create table inh_fk_2 (x int primary key, y int references inh_fk_1 on delete cascade) USING orioledb;
insert into inh_fk_2 values (11, 1), (22, 2), (33, 3);
create table inh_fk_2_child () inherits (inh_fk_2) USING orioledb;
insert into inh_fk_2_child values (111, 1), (222, 2);
delete from inh_fk_1 where a = 1;
select * from inh_fk_1 order by 1;
select * from inh_fk_2 order by 1, 2;
drop table inh_fk_1, inh_fk_2, inh_fk_2_child;

-- Test that parent and child CHECK constraints can be created in either order
create table p1(f1 int) USING orioledb;
create table p1_c1() inherits(p1) USING orioledb;

alter table p1 add constraint inh_check_constraint1 check (f1 > 0);
alter table p1_c1 add constraint inh_check_constraint1 check (f1 > 0);

alter table p1_c1 add constraint inh_check_constraint2 check (f1 < 10);
alter table p1 add constraint inh_check_constraint2 check (f1 < 10);

select conrelid::regclass::text as relname, conname, conislocal, coninhcount
from pg_constraint where conname like 'inh\_check\_constraint%'
order by 1, 2;

drop table p1 cascade;

-- Test that a valid child can have not-valid parent, but not vice versa
create table invalid_check_con(f1 int) USING orioledb;
create table invalid_check_con_child() inherits(invalid_check_con) USING orioledb;

alter table invalid_check_con_child add constraint inh_check_constraint check(f1 > 0) not valid;
alter table invalid_check_con add constraint inh_check_constraint check(f1 > 0); -- fail
alter table invalid_check_con_child drop constraint inh_check_constraint;

insert into invalid_check_con values(0);

alter table invalid_check_con_child add constraint inh_check_constraint check(f1 > 0);
alter table invalid_check_con add constraint inh_check_constraint check(f1 > 0) not valid;

insert into invalid_check_con values(0); -- fail
insert into invalid_check_con_child values(0); -- fail

select conrelid::regclass::text as relname, conname,
       convalidated, conislocal, coninhcount, connoinherit
from pg_constraint where conname like 'inh\_check\_constraint%'
order by 1, 2;

-- We don't drop the invalid_check_con* tables, to test dump/reload with

--
-- Test parameterized append plans for inheritance trees
--

CREATE TABLE patest0 (id, x) as
  select x, x from generate_series(0,1000) x;
CREATE TABLE patest1() inherits (patest0);
insert into patest1
  select x, x from generate_series(0,1000) x;
CREATE TABLE patest2() inherits (patest0);
insert into patest2
  select x, x from generate_series(0,1000) x;
create index patest0i on patest0(id);
create index patest1i on patest1(id);
create index patest2i on patest2(id);
analyze patest0;
analyze patest1;
analyze patest2;

explain (costs off)
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;

drop index patest2i;

explain (costs off)
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;

drop table patest0 cascade;

--
-- Test merge-append plans for inheritance trees
--

create table matest0 (id serial primary key, name text) USING orioledb;
create table matest1 (id integer primary key) inherits (matest0) USING orioledb;
create table matest2 (id integer primary key) inherits (matest0) USING orioledb;
create table matest3 (id integer primary key) inherits (matest0) USING orioledb;

create index matest0i on matest0 ((1-id));
create index matest1i on matest1 ((1-id));
-- create index matest2i on matest2 ((1-id));  -- intentionally missing
create index matest3i on matest3 ((1-id));

insert into matest1 (name) values ('Test 1');
insert into matest1 (name) values ('Test 2');
insert into matest2 (name) values ('Test 3');
insert into matest2 (name) values ('Test 4');
insert into matest3 (name) values ('Test 5');
insert into matest3 (name) values ('Test 6');

set enable_indexscan = off;  -- force use of seqscan/sort, so no merge
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
explain (verbose, costs off) select min(1-id) from matest0;
select min(1-id) from matest0;
reset enable_indexscan;

set enable_seqscan = off;  -- plan with fewest seqscans should be merge
set enable_parallel_append = off; -- Don't let parallel-append interfere
explain (verbose, costs off) select * from matest0 order by 1-id;
select * from matest0 order by 1-id;
explain (verbose, costs off) select min(1-id) from matest0;
select min(1-id) from matest0;
reset enable_seqscan;
reset enable_parallel_append;

drop table matest0 cascade;

--
-- Check that use of an index with an extraneous column doesn't produce
-- a plan with extraneous sorting
--

create table matest0 (a int, b int, c int, d int) USING orioledb;
create table matest1 () inherits(matest0) USING orioledb;
create index matest0i on matest0 (b, c);
create index matest1i on matest1 (b, c);

set enable_nestloop = off;  -- we want a plan with two MergeAppends

explain (costs off)
select t1.* from matest0 t1, matest0 t2
where t1.b = t2.b and t2.c = t2.d
order by t1.b limit 10;

reset enable_nestloop;

drop table matest0 cascade;

--
-- Test merge-append for UNION ALL append relations
--

set enable_seqscan = off;
set enable_indexscan = on;
set enable_bitmapscan = off;

-- Check handling of duplicated, constant, or volatile targetlist items
explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, thousand FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous, thousand+tenthous AS x FROM tenk1
UNION ALL
SELECT 42, 42, hundred FROM tenk1
ORDER BY thousand, tenthous;

explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, random()::integer FROM tenk1
ORDER BY thousand, tenthous;

-- Check min/max aggregate optimization
explain (costs off)
SELECT min(x) FROM
  (SELECT unique1 AS x FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x FROM tenk1 b) s;

explain (costs off)
SELECT min(y) FROM
  (SELECT unique1 AS x, unique1 AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s;

-- XXX planner doesn't recognize that index on unique2 is sufficiently sorted
explain (costs off)
SELECT x, y FROM
  (SELECT thousand AS x, tenthous AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s
ORDER BY x, y;

-- exercise rescan code path via a repeatedly-evaluated subquery
explain (costs off)
SELECT
    ARRAY(SELECT f.i FROM (
        (SELECT d + g.i FROM generate_series(4, 30, 3) d ORDER BY 1)
        UNION ALL
        (SELECT d + g.i FROM generate_series(0, 30, 5) d ORDER BY 1)
    ) f(i)
    ORDER BY f.i LIMIT 10)
FROM generate_series(1, 3) g(i);

SELECT
    ARRAY(SELECT f.i FROM (
        (SELECT d + g.i FROM generate_series(4, 30, 3) d ORDER BY 1)
        UNION ALL
        (SELECT d + g.i FROM generate_series(0, 30, 5) d ORDER BY 1)
    ) f(i)
    ORDER BY f.i LIMIT 10)
FROM generate_series(1, 3) g(i);

reset enable_seqscan;
reset enable_indexscan;
reset enable_bitmapscan;

--
-- Check handling of a constant-null CHECK constraint
--
create table cnullparent (f1 int) USING orioledb;
create table cnullchild (check (f1 = 1 or f1 = null)) inherits(cnullparent) USING orioledb;
insert into cnullchild values(1);
insert into cnullchild values(2);
insert into cnullchild values(null);
select * from cnullparent;
select * from cnullparent where f1 = 2;
drop table cnullparent cascade;

--
-- Check use of temporary tables with inheritance trees
--
create table inh_perm_parent (a1 int) USING orioledb;
CREATE TABLE inh_temp_parent (a1 int);
CREATE TABLE inh_temp_child () inherits (inh_perm_parent); -- ok
create table inh_perm_child () inherits (inh_temp_parent) USING orioledb; -- error
CREATE TABLE inh_temp_child_2 () inherits (inh_temp_parent); -- ok
insert into inh_perm_parent values (1);
insert into inh_temp_parent values (2);
insert into inh_temp_child values (3);
insert into inh_temp_child_2 values (4);
select tableoid::regclass, a1 from inh_perm_parent;
select tableoid::regclass, a1 from inh_temp_parent;
drop table inh_perm_parent cascade;
drop table inh_temp_parent cascade;

CREATE TABLE o_test_inh_circular_parent (
	key int
) USING orioledb;

CREATE TABLE o_test_inh_circular_child (
	val int
) INHERITS (o_test_inh_circular_parent) USING orioledb;

ALTER TABLE o_test_inh_circular_parent INHERIT o_test_inh_circular_child;


--
-- test the "star" operators a bit more thoroughly -- this time,
-- throw in lots of NULL fields...
--
-- a is the type root
-- b and c inherit from a (one-level single inheritance)
-- d inherits from b and c (two-level multiple inheritance)
-- e inherits from c (two-level single inheritance)
-- f inherits from e (three-level single inheritance)
--

CREATE TABLE a_star (
	class		char,
	a 			int4
) USING orioledb;

CREATE TABLE b_star (
	b 			text
) INHERITS (a_star) USING orioledb;

CREATE TABLE c_star (
	c 			name
) INHERITS (a_star) USING orioledb;

CREATE TABLE d_star (
	d 			float8
) INHERITS (b_star, c_star) USING orioledb;

CREATE TABLE e_star (
	e 			int2
) INHERITS (c_star) USING orioledb;

CREATE TABLE f_star (
	f 			polygon
) INHERITS (e_star) USING orioledb;

INSERT INTO a_star (class, a) VALUES ('a', 1);

INSERT INTO a_star (class, a) VALUES ('a', 2);

INSERT INTO a_star (class) VALUES ('a');

INSERT INTO b_star (class, a, b) VALUES ('b', 3, 'mumble'::text);

INSERT INTO b_star (class, a) VALUES ('b', 4);

INSERT INTO b_star (class, b) VALUES ('b', 'bumble'::text);

INSERT INTO b_star (class) VALUES ('b');

INSERT INTO c_star (class, a, c) VALUES ('c', 5, 'hi mom'::name);

INSERT INTO c_star (class, a) VALUES ('c', 6);

INSERT INTO c_star (class, c) VALUES ('c', 'hi paul'::name);

INSERT INTO c_star (class) VALUES ('c');

INSERT INTO d_star (class, a, b, c, d)
   VALUES ('d', 7, 'grumble'::text, 'hi sunita'::name, '0.0'::float8);

INSERT INTO d_star (class, a, b, c)
   VALUES ('d', 8, 'stumble'::text, 'hi koko'::name);

INSERT INTO d_star (class, a, b, d)
   VALUES ('d', 9, 'rumble'::text, '1.1'::float8);

INSERT INTO d_star (class, a, c, d)
   VALUES ('d', 10, 'hi kristin'::name, '10.01'::float8);

INSERT INTO d_star (class, b, c, d)
   VALUES ('d', 'crumble'::text, 'hi boris'::name, '100.001'::float8);

INSERT INTO d_star (class, a, b)
   VALUES ('d', 11, 'fumble'::text);

INSERT INTO d_star (class, a, c)
   VALUES ('d', 12, 'hi avi'::name);

INSERT INTO d_star (class, a, d)
   VALUES ('d', 13, '1000.0001'::float8);

INSERT INTO d_star (class, b, c)
   VALUES ('d', 'tumble'::text, 'hi andrew'::name);

INSERT INTO d_star (class, b, d)
   VALUES ('d', 'humble'::text, '10000.00001'::float8);

INSERT INTO d_star (class, c, d)
   VALUES ('d', 'hi ginger'::name, '100000.000001'::float8);

INSERT INTO d_star (class, a) VALUES ('d', 14);

INSERT INTO d_star (class, b) VALUES ('d', 'jumble'::text);

INSERT INTO d_star (class, c) VALUES ('d', 'hi jolly'::name);

INSERT INTO d_star (class, d) VALUES ('d', '1000000.0000001'::float8);

INSERT INTO d_star (class) VALUES ('d');

INSERT INTO e_star (class, a, c, e)
   VALUES ('e', 15, 'hi carol'::name, '-1'::int2);

INSERT INTO e_star (class, a, c)
   VALUES ('e', 16, 'hi bob'::name);

INSERT INTO e_star (class, a, e)
   VALUES ('e', 17, '-2'::int2);

INSERT INTO e_star (class, c, e)
   VALUES ('e', 'hi michelle'::name, '-3'::int2);

INSERT INTO e_star (class, a)
   VALUES ('e', 18);

INSERT INTO e_star (class, c)
   VALUES ('e', 'hi elisa'::name);

INSERT INTO e_star (class, e)
   VALUES ('e', '-4'::int2);

INSERT INTO f_star (class, a, c, e, f)
   VALUES ('f', 19, 'hi claire'::name, '-5'::int2, '(1,3),(2,4)'::polygon);

INSERT INTO f_star (class, a, c, e)
   VALUES ('f', 20, 'hi mike'::name, '-6'::int2);

INSERT INTO f_star (class, a, c, f)
   VALUES ('f', 21, 'hi marcel'::name, '(11,44),(22,55),(33,66)'::polygon);

INSERT INTO f_star (class, a, e, f)
   VALUES ('f', 22, '-7'::int2, '(111,555),(222,666),(333,777),(444,888)'::polygon);

INSERT INTO f_star (class, c, e, f)
   VALUES ('f', 'hi keith'::name, '-8'::int2,
	   '(1111,3333),(2222,4444)'::polygon);

INSERT INTO f_star (class, a, c)
   VALUES ('f', 24, 'hi marc'::name);

INSERT INTO f_star (class, a, e)
   VALUES ('f', 25, '-9'::int2);

INSERT INTO f_star (class, a, f)
   VALUES ('f', 26, '(11111,33333),(22222,44444)'::polygon);

INSERT INTO f_star (class, c, e)
   VALUES ('f', 'hi allison'::name, '-10'::int2);

INSERT INTO f_star (class, c, f)
   VALUES ('f', 'hi jeff'::name,
           '(111111,333333),(222222,444444)'::polygon);

INSERT INTO f_star (class, e, f)
   VALUES ('f', '-11'::int2, '(1111111,3333333),(2222222,4444444)'::polygon);

INSERT INTO f_star (class, a) VALUES ('f', 27);

INSERT INTO f_star (class, c) VALUES ('f', 'hi carl'::name);

INSERT INTO f_star (class, e) VALUES ('f', '-12'::int2);

INSERT INTO f_star (class, f)
   VALUES ('f', '(11111111,33333333),(22222222,44444444)'::polygon);

INSERT INTO f_star (class) VALUES ('f');

-- Analyze the X_star tables for better plan stability in later tests
ANALYZE a_star;
ANALYZE b_star;
ANALYZE c_star;
ANALYZE d_star;
ANALYZE e_star;
ANALYZE f_star;

--
-- inheritance stress test
--
SELECT * FROM a_star*;

SELECT *
   FROM b_star* x
   WHERE x.b = text 'bumble' or x.a < 3;

SELECT class, a
   FROM c_star* x
   WHERE x.c ~ text 'hi';

SELECT class, b, c
   FROM d_star* x
   WHERE x.a < 100;

SELECT class, c FROM e_star* x WHERE x.c NOTNULL;

SELECT * FROM f_star* x WHERE x.c ISNULL;

-- grouping and aggregation on inherited sets have been busted in the past...

SELECT sum(a) FROM a_star*;

SELECT class, sum(a) FROM a_star* GROUP BY class ORDER BY class;


ALTER TABLE f_star RENAME COLUMN f TO ff;

ALTER TABLE e_star* RENAME COLUMN e TO ee;

ALTER TABLE d_star* RENAME COLUMN d TO dd;

ALTER TABLE c_star* RENAME COLUMN c TO cc;

ALTER TABLE b_star* RENAME COLUMN b TO bb;

ALTER TABLE a_star* RENAME COLUMN a TO aa;

SELECT class, aa
   FROM a_star* x
   WHERE aa ISNULL;

-- As of Postgres 7.1, ALTER implicitly recurses,
-- so this should be same as ALTER a_star*

ALTER TABLE a_star RENAME COLUMN aa TO foo;

SELECT class, foo
   FROM a_star* x
   WHERE x.foo >= 2;

ALTER TABLE a_star RENAME COLUMN foo TO aa;

SELECT *
   from a_star*
   WHERE aa < 1000;

ALTER TABLE f_star ADD COLUMN f int4;

UPDATE f_star SET f = 10;

ALTER TABLE e_star* ADD COLUMN e int4;

--UPDATE e_star* SET e = 42;

SELECT * FROM e_star*;

ALTER TABLE a_star* ADD COLUMN a text;

-- That ALTER TABLE should have added TOAST tables.
SELECT relname, reltoastrelid <> 0 AS has_toast_table
   FROM pg_class
   WHERE oid::regclass IN ('a_star', 'c_star')
   ORDER BY 1;

SELECT class, aa, a FROM a_star*;

CREATE TABLE o_test_1 (
  val_1 int PRIMARY KEY
) USING orioledb;

CREATE TABLE o_test_2 (
  CHECK (val_1 >= 0 AND val_1 < 10)
) INHERITS (o_test_1);

CREATE RULE rule_test_1 AS ON INSERT TO o_test_1
	WHERE new.val_1 >= 0 AND new.val_1 < 10
	  DO INSTEAD INSERT INTO o_test_2 VALUES (new.val_1);

CREATE RULE rule_test_2  AS ON UPDATE TO o_test_1
	WHERE old.val_1 >= 0 AND old.val_1 < 10
	DO INSTEAD UPDATE o_test_2 SET val_1 = new.val_1
	WHERE val_1 = old.val_1;

INSERT INTO o_test_1
  SELECT * FROM generate_series(5,19,1) a;

UPDATE o_test_1 SET val_1 = 4 WHERE val_1 = 5;

CREATE TABLE o_test_inherits_tableoid (
   val_1 int,
   val_2 int,
   val_3 int
) USING orioledb;

CREATE TABLE o_test_inherits_tableoid_child1 ()
   INHERITS (o_test_inherits_tableoid) USING orioledb;
CREATE TABLE o_test_inherits_tableoid_child2 ()
   INHERITS (o_test_inherits_tableoid) USING orioledb;

INSERT INTO o_test_inherits_tableoid_child1 VALUES (1, 2, 3);
INSERT INTO o_test_inherits_tableoid_child2 VALUES (1, 5, 6);

CREATE INDEX o_test_inherits_tableoid_child1_ix1
   ON o_test_inherits_tableoid_child1 (val_2);
CREATE INDEX o_test_inherits_tableoid_child2_ix1
   ON o_test_inherits_tableoid_child2 (val_2);

BEGIN;
SET LOCAL enable_seqscan = off;
EXPLAIN (COSTS OFF)
   SELECT tableoid::regclass, * FROM o_test_inherits_tableoid ORDER BY val_2;
SELECT tableoid::regclass, * FROM o_test_inherits_tableoid ORDER BY val_2;
COMMIT;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA inherits CASCADE;
RESET search_path;
