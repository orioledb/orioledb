CREATE SCHEMA partial;
SET SESSION search_path = 'partial';
CREATE EXTENSION orioledb;

CREATE FUNCTION my_cmp_plpgsql(a int, b int) RETURNS int AS $$
DECLARE
	a_tr int := (a::bit(5) & X'A8'::bit(5))::int;
	b_tr int := (b::bit(5) & X'A8'::bit(5))::int;
BEGIN
	RETURN btint4cmp(a_tr, b_tr);
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_cmp_sql(a int, b int) RETURNS int AS $$
	SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
					(b::bit(5) & X'A8'::bit(5))::int);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_cmp_sql_sql(a int, b int) RETURNS int AS $$
	SELECT extract(epoch from current_timestamp)::integer +
		   my_cmp_sql(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_cmp_sql_plpgsql(a int, b int) RETURNS int AS $$
	SELECT my_cmp_plpgsql(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_cmp_sql_sql_sql(a int, b int) RETURNS int AS $$
	SELECT my_cmp_sql_sql(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_cmp_sql_sql_plpgsql(a int, b int) RETURNS int AS $$
	SELECT my_cmp_sql_plpgsql(a, b);
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_cmp_sql_body(a int, b int) RETURNS int
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT btint4cmp((a::bit(5) & X'A8'::bit(5))::int,
					(b::bit(5) & X'A8'::bit(5))::int);
END;

CREATE FUNCTION my_cmp_inter(a int, b int) RETURNS int
	AS 'btint4cmp'
	LANGUAGE internal;

CREATE FUNCTION my_c_func(a int, b int)
	RETURNS int8
	AS 'orioledb', 'orioledb_compression_max_level'
	LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION my_eq_p_p(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN my_cmp_plpgsql(a, b) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_p_s(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN my_cmp_sql(a, b) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_p_sb(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN my_cmp_sql_body(a, b) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_p_i(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN my_cmp_inter(a, b) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_p_b(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN btint4cmp(a, b) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_p_c(a int, b int) RETURNS bool AS $$
BEGIN
	RETURN ((a % (my_c_func(a, b) - 20)) -
			(b % (my_c_func(a, b) - 20))) = 0;
END
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE FUNCTION my_eq_s_p(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_plpgsql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_s(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_sql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_s_s_s(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_sql_sql_sql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_s_s_p(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_sql_sql_plpgsql(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_sb(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_sql_body(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_i(a int, b int) RETURNS bool AS $$
	SELECT my_cmp_inter(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_b(a int, b int) RETURNS bool AS $$
	SELECT btint4cmp(a, b) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_s_c(a int, b int) RETURNS bool AS $$
	SELECT ((a % (my_c_func(a, b) - 20)) -
			(b % (my_c_func(a, b) - 20))) = 0;
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION my_eq_sb_p(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT my_cmp_plpgsql(a, b) = 0;
END;

CREATE FUNCTION my_eq_sb_s(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT my_cmp_sql(a, b) = 0;
END;

CREATE FUNCTION my_eq_sb_sb(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT my_cmp_sql_body(a, b) = 0;
END;

CREATE FUNCTION my_eq_sb_i(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT my_cmp_inter(a, b) = 0;
END;

CREATE FUNCTION my_eq_sb_b(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT btint4cmp(a, b) = 0;
END;

CREATE FUNCTION my_eq_sb_c(a int, b int) RETURNS bool
	LANGUAGE SQL IMMUTABLE
BEGIN ATOMIC
	SELECT ((a % (my_c_func(a, b) - 20)) -
			(b % (my_c_func(a, b) - 20))) = 0;
END;

CREATE FUNCTION my_eq_i(a int, b int) RETURNS bool
	AS 'int4eq'
	LANGUAGE internal IMMUTABLE;

CREATE TABLE o_test_sql_func (
	val int
) USING orioledb;

CREATE INDEX o_test_sql_func_ix_p_p ON
	o_test_sql_func(val) WHERE (my_eq_p_p(val, val + 10));
CREATE INDEX o_test_sql_func_ix_p_s ON
	o_test_sql_func(val) WHERE (my_eq_p_s(val, val + 10));
CREATE INDEX o_test_sql_func_ix_p_sb ON
	o_test_sql_func(val) WHERE (my_eq_p_sb(val, val + 10));
CREATE INDEX o_test_sql_func_ix_p_i ON
	o_test_sql_func(val) WHERE (my_eq_p_i(val % 2, 0));
CREATE INDEX o_test_sql_func_ix_p_b ON
	o_test_sql_func(val) WHERE (my_eq_p_b(val % 3, 0));
CREATE INDEX o_test_sql_func_ix_p_c ON
	o_test_sql_func(val) WHERE (my_eq_p_c(val % 4, 0));

CREATE INDEX o_test_sql_func_ix_s_p ON
	o_test_sql_func(val) WHERE (my_eq_s_p(val, val + 10));
CREATE INDEX o_test_sql_func_ix_s_s ON
	o_test_sql_func(val) WHERE (my_eq_s_s(val, val * 11));
CREATE INDEX o_test_sql_func_ix_s_s_s_s ON
	o_test_sql_func(val) WHERE (my_eq_s_s_s_s(val, val * 11));
CREATE INDEX o_test_sql_func_ix_s_s_s_p ON
	o_test_sql_func(val) WHERE (my_eq_s_s_s_p(val, val * 11));
CREATE INDEX o_test_sql_func_ix_s_sb ON
	o_test_sql_func(val) WHERE (my_eq_s_sb(val, val * 11));
CREATE INDEX o_test_sql_func_ix_s_i ON
	o_test_sql_func(val) WHERE (my_eq_s_i(val % 2, 0));
CREATE INDEX o_test_sql_func_ix_s_b ON
	o_test_sql_func(val) WHERE (my_eq_s_b(val % 3, 0));
CREATE INDEX o_test_sql_func_ix_s_c ON
	o_test_sql_func(val) WHERE (my_eq_s_c(val % 2, 1));
CREATE INDEX o_test_sql_func_ix_sb_p ON
	o_test_sql_func(val) WHERE (my_eq_sb_p(val, val + 10));
CREATE INDEX o_test_sql_func_ix_sb_s ON
	o_test_sql_func(val) WHERE (my_eq_sb_s(val, val * 11));
CREATE INDEX o_test_sql_func_ix_sb_sb ON
	o_test_sql_func(val) WHERE (my_eq_sb_sb(val, val * 11));
CREATE INDEX o_test_sql_func_ix_sb_i ON
	o_test_sql_func(val) WHERE (my_eq_sb_i(val % 2, 0));
CREATE INDEX o_test_sql_func_ix_sb_b ON
	o_test_sql_func(val) WHERE (my_eq_sb_b(val % 3, 0));
CREATE INDEX o_test_sql_func_ix_sb_c ON
	o_test_sql_func(val) WHERE (my_eq_sb_c(val % 2, 1));
CREATE INDEX o_test_sql_func_ix_i ON
	o_test_sql_func(val) WHERE (my_eq_i(val % 4, 2));
CREATE INDEX o_test_sql_func_ix_b ON
	o_test_sql_func(val) WHERE (int4eq(val % 4, 3));
CREATE INDEX o_test_sql_func_ix_c ON
	o_test_sql_func(val) WHERE ((val % (my_c_func(val,
														   val + 1) - 18)) =
								0);

CREATE SEQUENCE o_test_sql_func_seq;
CREATE INDEX o_test_sql_func_ix_nv ON
	o_test_sql_func(val) WHERE (nextval('o_test_sql_func_seq') % 2 = 0);

CREATE INDEX o_test_sql_func_ix_ss ON
	o_test_sql_func(((SELECT * FROM generate_series(1, 10) LIMIT 1) = val));

INSERT INTO o_test_sql_func SELECT generate_series(1, 10);

SELECT * FROM o_test_sql_func;

SELECT orioledb_tbl_structure('o_test_sql_func'::regclass, 'nue');

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA partial CASCADE;
RESET search_path;
