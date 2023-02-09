CREATE SCHEMA opclass;
SET SESSION search_path = 'opclass';
CREATE EXTENSION orioledb;

CREATE TABLE o_test_custom_opclass (
	int_val int
) USING orioledb;

CREATE FUNCTION cmp_is_valid_prepare(test_proc regproc)
RETURNS TABLE (a int, b int, ab int, ba int, abn int, ban int, valid bool)
AS $$
BEGIN
	CREATE TEMP TABLE s1 ON COMMIT DROP AS
		WITH s0 AS (
			SELECT (vs - 1) / 6 - 5 v,
				   COALESCE(NULLIF((vs - 1) / 6 - 5, 0), 1) vn,
				   (vs - 1) % 6 r
			FROM generate_series(1, 6 * 11) vs
		)
		SELECT v va, CASE WHEN (r = 0) THEN v - 4 * abs(vn)
						  WHEN (r = 1) THEN v - 2 * abs(vn)
						  WHEN (r = 2) THEN CASE WHEN (v > 0) THEN 0 ELSE v END
						  WHEN (r = 3) THEN CASE WHEN (v < 0) THEN 0 ELSE v END
						  WHEN (r = 4) THEN v + 7 * abs(vn)
						  WHEN (r = 5) THEN v + 9 * abs(vn)
						  ELSE 0
					 END vb
			FROM s0;

	EXECUTE 'CREATE TEMP TABLE s2 ON COMMIT DROP AS SELECT DISTINCT va, vb, ' ||
			test_proc || '(va, vb) vab, ' ||
			test_proc || '(vb, va) vba FROM s1';

	RETURN QUERY
	WITH
		s3 AS (
			SELECT va, vb, vab, vba,
				(CASE WHEN (vab = 0) THEN 0
					  WHEN (vab > 0) THEN 1 ELSE -1 END) vabn,
				(CASE WHEN (vba = 0) THEN 0
					  WHEN (vba > 0) THEN 1 ELSE -1 END) vban
			FROM s2
		),
		s4 AS (
			SELECT va, vb, vab, vba, vabn, vban,
				   ((vabn = -vban) or (vabn = vban and vabn = 0)) AS valid
				   FROM s3
		)
		SELECT * FROM s4;
	RETURN;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION cmp_is_valid(test_proc regproc)
RETURNS bool
AS $$
DECLARE
	result BOOL;
BEGIN
	SELECT bool_and(valid) into result
		FROM cmp_is_valid_prepare(test_proc);
	RETURN result;
END
$$ LANGUAGE plpgsql;

-- Letter in function names means:
-- s - sql
-- p - plpgsql
-- i - internal
-- v - volatile

-- if several letters go one next to the other in name
-- that means it calls that kind of function

CREATE FUNCTION my_int_cmp_p_i(a int, b int) RETURNS int AS $$
BEGIN
	RETURN btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
					 (b::bit(5) & X'A8'::bit(5))::int);
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_i(a int, b int) RETURNS int AS $$
	SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
					 (b::bit(5) & X'A8'::bit(5))::int);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_v_i(a int, b int) RETURNS int AS $$
	SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
					 (b::bit(5) & X'A8'::bit(5))::int);
$$ LANGUAGE SQL VOLATILE;

CREATE FUNCTION my_int_cmp_s_p_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_p_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_p_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_p_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_s_p_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_s_p_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION my_int_cmp_s_s_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_s_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_s_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_s_s_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_s_s_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;


CREATE FUNCTION my_int_cmp_s_v_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_v_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_v_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_v_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_int_cmp_s_s_s_v_i(a int, b int) RETURNS int AS $$
	SELECT my_int_cmp_s_s_v_i(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE TABLE test_ints AS (SELECT generate_series(1, 5) val);
CREATE PROCEDURE my_test_proc(a int, b int) AS $$
	SELECT a + b + 3;
$$ LANGUAGE SQL;

CREATE OPERATOR FAMILY my_op_family USING btree;

-- INVALID OPCLASSES

SELECT cmp_is_valid('my_int_cmp_p_i');
CREATE OPERATOR CLASS my_op_class_p_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_p_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_p_i);
DROP OPERATOR CLASS my_op_class_p_i USING btree CASCADE;

SELECT cmp_is_valid('my_int_cmp_s_p_i');
CREATE OPERATOR CLASS my_op_class_s_p_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_s_p_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_s_p_i);
DROP OPERATOR CLASS my_op_class_s_p_i USING btree CASCADE;

SELECT cmp_is_valid('my_int_cmp_s_s_s_p_i');
CREATE OPERATOR CLASS my_op_class_s_s_s_p_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_s_s_s_p_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_s_s_s_p_i);
DROP OPERATOR CLASS my_op_class_s_s_s_p_i USING btree CASCADE;

SELECT cmp_is_valid('my_int_cmp_v_i');
CREATE OPERATOR CLASS my_op_class_v_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_v_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_v_i);
DROP OPERATOR CLASS my_op_class_v_i USING btree CASCADE;

SELECT cmp_is_valid('my_int_cmp_s_v_i');
CREATE OPERATOR CLASS my_op_class_s_v_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_s_v_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_s_v_i);
DROP OPERATOR CLASS my_op_class_s_v_i USING btree CASCADE;

SELECT cmp_is_valid('my_int_cmp_s_s_s_v_i');
CREATE OPERATOR CLASS my_op_class_s_s_s_v_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_s_s_s_v_i(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_s_s_s_v_i);
DROP OPERATOR CLASS my_op_class_s_s_s_v_i USING btree CASCADE;

CREATE FUNCTION my_int_cmp_call(a int, b int) RETURNS int AS $$
	CALL my_test_proc(1, 5);
	SELECT (VALUES (a*a%11-b*b%11));
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_call');
CREATE OPERATOR CLASS my_op_class_call FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_call(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_call);
DROP OPERATOR CLASS my_op_class_call USING btree CASCADE;

CREATE FUNCTION my_int_cmp_delete(a int, b int) RETURNS int AS $$
	DELETE FROM test_ints WHERE val = a - b RETURNING val;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_delete');
CREATE OPERATOR CLASS my_op_class_delete FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_delete(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_delete);
DROP OPERATOR CLASS my_op_class_delete USING btree CASCADE;

CREATE FUNCTION my_int_cmp_update(a int, b int) RETURNS int AS $$
	UPDATE test_ints SET val = val WHERE val = a - b RETURNING val;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_update');
CREATE OPERATOR CLASS my_op_class_update FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_update(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_update);
DROP OPERATOR CLASS my_op_class_update USING btree CASCADE;

CREATE FUNCTION my_int_cmp_insert(a int, b int) RETURNS int AS $$
	INSERT INTO test_ints VALUES (-1) RETURNING val;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_insert');
CREATE OPERATOR CLASS my_op_class_insert FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_insert(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_insert);
DROP OPERATOR CLASS my_op_class_insert USING btree CASCADE;

CREATE FUNCTION my_int_cmp_select_from_table(a int, b int) RETURNS int AS $$
	SELECT a%(val+6)-b%(val+6) FROM test_ints LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_select_from_table');
CREATE OPERATOR CLASS my_op_class_select_from_table FOR TYPE int
	USING btree FAMILY my_op_family
	AS FUNCTION 1 my_int_cmp_select_from_table(int, int);
CREATE INDEX int_val_ix ON
	o_test_custom_opclass(int_val my_op_class_select_from_table);
DROP OPERATOR CLASS my_op_class_select_from_table USING btree CASCADE;

-- VALID OPCLASSES

CREATE OPERATOR <^ (
	LEFTARG = int4,
	RIGHTARG = int4,
	PROCEDURE = int4lt
);
CREATE OPERATOR =^ (
	LEFTARG = int4,
	RIGHTARG = int4,
	PROCEDURE = int4eq,
	COMMUTATOR = OPERATOR(=^)
);

SELECT cmp_is_valid('my_int_cmp_s_i');
CREATE OPERATOR CLASS my_op_class_s_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_s_i(int, int);
CREATE INDEX int_val_ix_s_i ON
	o_test_custom_opclass(int_val my_op_class_s_i);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_s_i USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

SELECT cmp_is_valid('my_int_cmp_s_s_s_s_i');
CREATE OPERATOR CLASS my_op_class_s_s_s_s_i FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_s_s_s_s_i(int, int);
CREATE INDEX int_val_ix_s_s_s_s_i ON
	o_test_custom_opclass(int_val my_op_class_s_s_s_s_i);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_s_s_s_s_i USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_simple_expression(a int, b int) RETURNS int
AS $$
	SELECT (3 - a % 2) - (3 - b % 2);
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_simple_expression');
CREATE OPERATOR CLASS my_op_class_simple_expression FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_simple_expression(int, int);
CREATE INDEX int_val_ix_simple_expression ON
	o_test_custom_opclass(int_val my_op_class_simple_expression);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_simple_expression USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_values(a int, b int) RETURNS int
AS $$
	SELECT (VALUES (a*a%11-b*b%11));
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_values');
CREATE OPERATOR CLASS my_op_class_values FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_values(int, int);
CREATE INDEX int_val_ix_values ON
	o_test_custom_opclass(int_val my_op_class_values);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_values USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_generate_series(a int, b int) RETURNS int
AS $$
	SELECT a % COUNT(*) - b % COUNT(*) FROM generate_series(1, 7);
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_generate_series');
CREATE OPERATOR CLASS my_op_class_generate_series FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_generate_series(int, int);
CREATE INDEX int_val_ix_generate_series ON
	o_test_custom_opclass(int_val my_op_class_generate_series);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_generate_series USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_subselect(a int, b int) RETURNS int
AS $$
	SELECT a % COUNT(*) - b % COUNT(*) FROM
		(SELECT v % 5, COUNT(v) FROM generate_series(1, 30) v GROUP BY 1) s1;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_subselect');
CREATE OPERATOR CLASS my_op_class_subselect FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_subselect(int, int);
CREATE INDEX int_val_ix_subselect ON
	o_test_custom_opclass(int_val my_op_class_subselect);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_subselect USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_cte(a int, b int) RETURNS int
AS $$
	WITH a1 (v) AS (
	    SELECT COUNT(*)+1 FROM generate_series(1, 5)
	)
	SELECT a%v-b%v FROM a1 LIMIT 1;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_cte');
CREATE OPERATOR CLASS my_op_class_cte FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_cte(int, int);
CREATE INDEX int_val_ix_cte ON
	o_test_custom_opclass(int_val my_op_class_cte);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_cte USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_test_func(a int, b int) RETURNS int AS $$
	SELECT LEAST(a, a)%8 - GREATEST(b, b)%8;
$$ LANGUAGE SQL IMMUTABLE;
CREATE FUNCTION my_int_cmp_rows_func(a int, b int) RETURNS int
AS $$
	SELECT * FROM ROWS FROM(my_test_func(a, b));
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_rows_func');
CREATE OPERATOR CLASS my_op_class_rows_func FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_rows_func(int, int);
CREATE INDEX int_val_ix_rows_func ON
	o_test_custom_opclass(int_val my_op_class_rows_func);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_rows_func USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_join(a int, b int) RETURNS int
AS $$
	SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
		LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_join');
CREATE OPERATOR CLASS my_op_class_join FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_join(int, int);
CREATE INDEX int_val_ix_join ON
	o_test_custom_opclass(int_val my_op_class_join);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_join USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_complex(a int, b int) RETURNS int
AS $$
	SELECT pow(a, s1.r)::int%5 - pow(b, s1.r)::int%5 FROM (SELECT 2) s1(r);
	WITH a1 (v) AS (
	    SELECT COUNT(*)+1 FROM generate_series(1, 5)
	)
	SELECT a%v-b%v FROM a1 LIMIT 1;
	SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
		LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
	SELECT (VALUES (a*a%11-b*b%11));
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_complex');
CREATE OPERATOR CLASS my_op_class_complex FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_complex(int, int);
CREATE INDEX int_val_ix_complex ON
	o_test_custom_opclass(int_val my_op_class_complex);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_complex USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_sqlbody(a int, b int) RETURNS int
LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT * FROM (VALUES (a * 7 % 3 - b * 7 % 3)) s1
		LEFT JOIN (VALUES (3), (5)) s2 USING(column1);
END;
SELECT cmp_is_valid('my_int_cmp_sqlbody');
CREATE OPERATOR CLASS my_op_class_sqlbody FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_sqlbody(int, int);
CREATE INDEX int_val_ix_sqlbody ON
	o_test_custom_opclass(int_val my_op_class_sqlbody);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_sqlbody USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_init_plan(a int, b int) RETURNS int
AS $$
	SELECT a % (select COUNT(*) from generate_series(1, 4)) -
		   b % (select COUNT(*) from generate_series(1, 4));
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_init_plan');
CREATE OPERATOR CLASS my_op_class_init_plan FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_init_plan(int, int);
CREATE INDEX int_val_ix_init_plan ON
	o_test_custom_opclass(int_val my_op_class_init_plan);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_init_plan USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_cast(a int, b int) RETURNS int
AS $$
	SELECT substring(CAST(ABS(a) as text) from 1 for 1)::int -
		   substring(CAST(ABS(b) as text) from 1 for 1)::int;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_cast');
CREATE OPERATOR CLASS my_op_class_cast FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_cast(int, int);
CREATE INDEX int_val_ix_cast ON
	o_test_custom_opclass(int_val my_op_class_cast);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_cast USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE FUNCTION my_int_cmp_windowagg(a int, b int) RETURNS int
AS $$
	WITH s1 AS (
		select exists(select COUNT(*) from generate_series(1, 4)) vv, sum(v) OVER () vvv
			from generate_series(1, 4) v
	)
	SELECT a % COUNT(vv) - b % COUNT(vvv) FROM s1;
$$ LANGUAGE SQL IMMUTABLE;
SELECT cmp_is_valid('my_int_cmp_windowagg');
CREATE OPERATOR CLASS my_op_class_windowagg FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_windowagg(int, int);
CREATE INDEX int_val_ix_windowagg ON
	o_test_custom_opclass(int_val my_op_class_windowagg);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,10);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^);
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^);
DROP OPERATOR CLASS my_op_class_windowagg USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

SELECT cmp_is_valid('my_int_cmp_s_i');
CREATE OPERATOR CLASS my_op_class_build_ix FOR TYPE int
	USING btree FAMILY my_op_family
	AS OPERATOR 1 <^, OPERATOR 3 =^,
	   FUNCTION 1 my_int_cmp_s_i(int, int);
INSERT INTO o_test_custom_opclass SELECT generate_series(1,300);
CREATE INDEX int_val_ix_build_ix ON
	o_test_custom_opclass(int_val my_op_class_build_ix);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
						ORDER BY int_val USING OPERATOR(<^) LIMIT 10;
SELECT * FROM o_test_custom_opclass
	ORDER BY int_val USING OPERATOR(<^) LIMIT 10;
DROP OPERATOR CLASS my_op_class_build_ix USING btree CASCADE;
TRUNCATE o_test_custom_opclass;

CREATE TYPE my_rec_arr AS (arr char[]);
CREATE FUNCTION my_rec_arr_to_int(my_rec_arr) RETURNS int
AS $$
	SELECT ASCII($1.arr[1])-ASCII('A')+1;
$$ LANGUAGE SQL IMMUTABLE;
CREATE FUNCTION int_to_my_rec_arr(int) RETURNS my_rec_arr
AS $$
	SELECT ROW(ARRAY[CHR(ASCII('A')+$1-1)])::my_rec_arr;
$$ LANGUAGE SQL IMMUTABLE;
CREATE CAST (my_rec_arr AS int)
	WITH FUNCTION my_rec_arr_to_int;
CREATE CAST (int AS my_rec_arr)
	WITH FUNCTION int_to_my_rec_arr;

CREATE TYPE my_record AS (
	val_1 integer,
	val_2 my_rec_arr
);

ALTER TABLE o_test_custom_opclass ADD COLUMN rec_val my_record;

INSERT INTO o_test_custom_opclass
	SELECT v, (v % 4 + 1,
			   ((v-1) % 3 + 1)::my_rec_arr)::my_record
		FROM generate_series(1, 10) v;

CREATE FUNCTION my_rec_cmp(a my_record, b my_record)
RETURNS integer
AS $$
	SELECT COALESCE(NULLIF(((a).val_2::int - (b).val_2::int),0),
						   a.val_1 - b.val_1);
$$ LANGUAGE SQL STRICT IMMUTABLE;

CREATE FUNCTION my_rec_lt(a my_record, b my_record)
RETURNS boolean
AS $$
	BEGIN
		RETURN my_rec_cmp(a,b) < 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_rec_eq(a my_record, b my_record)
RETURNS boolean
AS $$
	BEGIN
		RETURN my_rec_cmp(a,b) = 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE FUNCTION my_rec_gt(a my_record, b my_record)
RETURNS boolean
AS $$
	BEGIN
		RETURN my_rec_cmp(a,b) > 0;
	END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

CREATE OPERATOR #<# (
	LEFTARG = my_record,
	RIGHTARG = my_record,
	FUNCTION = my_rec_lt
);

CREATE OPERATOR #=# (
	LEFTARG = my_record,
	RIGHTARG = my_record,
	FUNCTION = my_rec_eq,
	MERGES
);

CREATE OPERATOR #># (
	LEFTARG = my_record,
	RIGHTARG = my_record,
	FUNCTION = my_rec_gt
);

CREATE INDEX rec_val_ix ON o_test_custom_opclass(rec_val);

SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
	ORDER BY rec_val;
SELECT * FROM o_test_custom_opclass ORDER BY rec_val;

CREATE OPERATOR CLASS my_op_class_rec
	DEFAULT FOR TYPE my_record
	USING btree FAMILY my_op_family
	AS OPERATOR 1 #<#, OPERATOR 3 #=#,
	   FUNCTION 1 my_rec_cmp(my_record, my_record);

CREATE INDEX rec_val_ix2 ON o_test_custom_opclass(rec_val);

EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
	ORDER BY rec_val;
SELECT * FROM o_test_custom_opclass ORDER BY rec_val;

DROP OPERATOR CLASS my_op_class_rec USING btree CASCADE;

EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(#<#);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
SELECT * FROM o_test_custom_opclass ORDER BY rec_val
	USING OPERATOR(<);
EXPLAIN (COSTS OFF) SELECT * FROM o_test_custom_opclass
	ORDER BY rec_val;
SELECT * FROM o_test_custom_opclass ORDER BY rec_val;
RESET enable_seqscan;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA opclass CASCADE;
RESET search_path;
