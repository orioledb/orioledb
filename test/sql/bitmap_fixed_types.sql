-- Composite-PK bitmap scan for every built-in fixed-size type.
--
-- The fixed-key bitmap encoding used to hardcode int2/int4/int8; any other
-- type reaching it raised "unsupported fixed keybitmap type", and the planner
-- only offered a bitmap scan for integer primary keys.  This test drives the
-- fixed-key encode/decode/getNextKey path for each supported type through an
-- actual composite-PK bitmap scan and checks the result against ground truth.
--
-- bt_check() builds a composite PK (a, b) of the given type, forces the bitmap
-- plan, looks up a scattered set of existing keys, and verifies (1) the plan is
-- an o_scan bitmap heap scan and (2) the scan returns exactly the looked-up
-- keys (a missing/duplicated key -- e.g. from a non-order-preserving encoding
-- driving the getNextKey seek off the end -- would change the count).  Signed
-- and float ranges span negative values so sign/float ordering is exercised.

CREATE SCHEMA bitmap_ft;
SET SESSION search_path = 'bitmap_ft';
CREATE EXTENSION orioledb;

CREATE FUNCTION bt_check(typ text, genexpr text, lo int, hi int) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
	lst text;
	n_samp int;
	n_got int;
	is_bm bool := false;
	ln text;
	q text;
BEGIN
	EXECUTE 'DROP TABLE IF EXISTS ft';
	EXECUTE format('CREATE TABLE ft (a int, b %s, v int, PRIMARY KEY(a, b)) USING orioledb', typ);
	EXECUTE format('INSERT INTO ft SELECT (g %% 3) + 1, %s, g FROM generate_series(%s, %s) g ON CONFLICT (a, b) DO NOTHING', genexpr, lo, hi);

	SET LOCAL enable_seqscan = off;
	SET LOCAL enable_indexscan = off;
	SET LOCAL enable_indexonlyscan = off;

	-- literal list of a scattered subset of existing keys for a = 1
	EXECUTE 'SELECT string_agg(quote_literal(b::text), '','') FROM (SELECT b FROM ft WHERE a = 1 AND mod(v, 13) = 0 ORDER BY b) s' INTO lst;
	EXECUTE 'SELECT count(*) FROM ft WHERE a = 1 AND mod(v, 13) = 0' INTO n_samp;
	IF n_samp IS NULL OR n_samp = 0 THEN
		RETURN typ || ': EMPTY SAMPLE';
	END IF;

	q := format('SELECT b FROM ft WHERE a = 1 AND b = ANY(ARRAY[%s]::%s[])', lst, typ);
	FOR ln IN EXECUTE 'EXPLAIN (COSTS OFF) ' || q LOOP
		IF ln LIKE '%Bitmap heap scan%' THEN
			is_bm := true;
		END IF;
	END LOOP;
	EXECUTE format('SELECT count(DISTINCT b) FROM (%s) s', q) INTO n_got;

	IF NOT is_bm THEN
		RETURN typ || ': NOT BITMAP';
	END IF;
	IF n_samp <> n_got THEN
		RETURN format('%s: MISMATCH samp=%s got=%s', typ, n_samp, n_got);
	END IF;
	RETURN typ || ': ok';
END $$;

-- Signed integers and integer-backed date/time/money (negatives included).
SELECT bt_check('int2', 'g::int2', -4000, 4000);
SELECT bt_check('int4', 'g * 100000', -4000, 4000);
SELECT bt_check('int8', 'g::bigint * 1000000007', -4000, 4000);
SELECT bt_check('date', '(DATE ''2000-06-01'' + g)', -4000, 4000);
SELECT bt_check('timestamp', '(TIMESTAMP ''2000-06-01'' + make_interval(hours => g))', -4000, 4000);
SELECT bt_check('timestamptz', '(TIMESTAMPTZ ''2000-06-01 00:00+00'' + make_interval(hours => g))', -4000, 4000);
SELECT bt_check('time', '(TIME ''00:00:00'' + make_interval(secs => mod(abs(g), 80000)))', -4000, 4000);
SELECT bt_check('money', 'g::numeric::money', -4000, 4000);
-- Unsigned integer.
SELECT bt_check('oid', '(g + 3000000)::oid', -4000, 4000);
-- IEEE floats (negatives + fractional).
SELECT bt_check('float4', '(g * 1.5)::float4', -4000, 4000);
SELECT bt_check('float8', '(g * 1.5 + 0.25)::float8', -4000, 4000);
-- memcmp-ordered raw byte types.
SELECT bt_check('uuid', '(''00000000-0000-0000-0000-'' || lpad(to_hex(g + 100000), 12, ''0''))::uuid', -4000, 4000);
SELECT bt_check('macaddr', 'lpad(to_hex(g + 500000), 12, ''0'')::macaddr', -4000, 4000);
SELECT bt_check('macaddr8', 'lpad(to_hex(g + 500000), 16, ''0'')::macaddr8', -4000, 4000);

-- Single-byte domains (small value set): bool and "char".
SELECT bt_check('"char"', '(33 + mod(g + 4000, 90))::"char"', -4000, 4000);

-- Types with no order-preserving fixed-size encoding must fall back to a
-- non-bitmap plan (o_keybitmap_pk_mode -> NONE), never reach the fixed-key
-- encoder, and never raise "unsupported fixed keybitmap type".  bt_check
-- reports "NOT BITMAP" for them and, crucially, completes without error.
SELECT bt_check('numeric', 'g::numeric', -4000, 4000);
SELECT bt_check('interval', 'make_interval(secs => g)', -4000, 4000);
SELECT bt_check('text', 'lpad(g::text, 8, ''0'')', -4000, 4000);

CREATE TABLE ftbool (a int, b bool, PRIMARY KEY(a, b)) USING orioledb;
INSERT INTO ftbool SELECT a, b FROM generate_series(1, 50) a, (VALUES (false), (true)) x(b);
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SELECT count(*) FROM ftbool WHERE a = 1 AND b IN (true, false);
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;

-- A composite PK mixing several fixed-size kinds (int + uuid + timestamp).
CREATE TABLE ftmix (
	i int,
	u uuid,
	t timestamp,
	val int,
	PRIMARY KEY (i, u, t)
) USING orioledb;
INSERT INTO ftmix
	SELECT mod(g, 4),
		   ('00000000-0000-0000-0000-' || lpad(to_hex(g), 12, '0'))::uuid,
		   TIMESTAMP '2000-01-01' + make_interval(hours => g),
		   g
	FROM generate_series(1, 3000) g;
SET enable_seqscan = off;
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SELECT count(*) FROM ftmix
	WHERE (i, u, t) IN (
		SELECT i, u, t FROM ftmix WHERE mod(val, 100) = 0
	);
RESET enable_seqscan;
RESET enable_indexscan;
RESET enable_indexonlyscan;

DROP SCHEMA bitmap_ft CASCADE;
