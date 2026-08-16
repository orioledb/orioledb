-- Schema for the SQLSmith fuzzing job.
--
-- SQLSmith builds its grammar from the catalog, so whatever is not here is
-- never generated.  The goal is to reach as many OrioleDB code paths as
-- possible: every primary-key shape it supports (int, composite, text, and
-- none at all), TOASTed values, the bridge index machinery, partitioning and
-- the type combinations that show up in real schemas.

CREATE EXTENSION IF NOT EXISTS orioledb;

SET default_table_access_method = orioledb;

CREATE TYPE ss_mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN ss_positive AS int CHECK (VALUE > 0);

-- 1. Plain int primary key, the most common shape.
CREATE TABLE ss_int_pk (
	id       int PRIMARY KEY,
	sk       int NOT NULL,
	val      text,
	amount   numeric(12, 2),
	flag     bool,
	ts       timestamptz,
	mood     ss_mood
);
CREATE INDEX ss_int_pk_sk ON ss_int_pk (sk);
CREATE INDEX ss_int_pk_partial ON ss_int_pk (amount) WHERE flag;
CREATE INDEX ss_int_pk_expr ON ss_int_pk (lower(val));

-- 2. Composite primary key: exercises multi-column key comparison, which is
--    where several past bitmap-scan bugs lived.
CREATE TABLE ss_composite_pk (
	a        int NOT NULL,
	b        text NOT NULL,
	c        bigint,
	payload  jsonb,
	PRIMARY KEY (a, b)
);
CREATE UNIQUE INDEX ss_composite_uniq ON ss_composite_pk (c) WHERE c IS NOT NULL;

-- 3. No primary key at all -- OrioleDB synthesises a ctid-based one.
CREATE TABLE ss_no_pk (
	x        int,
	y        text,
	arr      int[],
	ts       timestamp
);
CREATE INDEX ss_no_pk_x ON ss_no_pk (x);

-- 4. Text primary key plus deliberately TOASTable values.
CREATE TABLE ss_toast (
	k        text PRIMARY KEY,
	body     text,
	blob     bytea,
	tags     text[]
);

-- 5. Bridge-index table: GIN and GiST are not native to OrioleDB, so these go
--    through the bridge, a code path worth as much fuzzing as we can give it.
CREATE TABLE ss_bridged (
	id       bigserial PRIMARY KEY,
	doc      jsonb,
	txt      tsvector,
	rng      int4range
);
CREATE INDEX ss_bridged_gin  ON ss_bridged USING gin (doc);
CREATE INDEX ss_bridged_gist ON ss_bridged USING gist (rng);
CREATE INDEX ss_bridged_txt  ON ss_bridged USING gin (txt);

-- 6. Partitioned table with OrioleDB partitions.
CREATE TABLE ss_part (
	id       int NOT NULL,
	part_key int NOT NULL,
	val      text,
	PRIMARY KEY (id, part_key)
) PARTITION BY RANGE (part_key);
CREATE TABLE ss_part_0 PARTITION OF ss_part FOR VALUES FROM (0) TO (100);
CREATE TABLE ss_part_1 PARTITION OF ss_part FOR VALUES FROM (100) TO (200);
CREATE TABLE ss_part_2 PARTITION OF ss_part FOR VALUES FROM (200) TO (1000);

-- 7. Generated column, check constraint, and a self-referencing foreign key.
CREATE TABLE ss_constrained (
	id       int PRIMARY KEY,
	base     ss_positive NOT NULL,
	doubled  int GENERATED ALWAYS AS (base * 2) STORED,
	parent   int REFERENCES ss_constrained (id),
	CONSTRAINT ss_base_bound CHECK (base < 1000000)
);

-- A view and a set-returning function give SQLSmith more to compose with.
CREATE VIEW ss_view AS
	SELECT i.id, i.sk, i.val, c.a, c.payload
	  FROM ss_int_pk i LEFT JOIN ss_composite_pk c ON i.sk = c.a;

CREATE FUNCTION ss_rows(n int)
RETURNS TABLE (id int, val text) LANGUAGE sql STABLE AS $$
	SELECT g, 'row' || g FROM generate_series(1, n) g
$$;

-- Seed data.  Enough rows to build multi-level trees, and long values so the
-- TOAST paths are populated rather than merely reachable.
INSERT INTO ss_int_pk
	SELECT g, g % 97, 'value ' || g, (g % 1000)::numeric / 7, g % 3 = 0,
	       now() - (g || ' minutes')::interval,
	       (ARRAY['sad', 'ok', 'happy'])[1 + g % 3]::ss_mood
	  FROM generate_series(1, 20000) g;

INSERT INTO ss_composite_pk
	SELECT g % 500, 'key' || g, g * 3, jsonb_build_object('g', g, 'mod', g % 13)
	  FROM generate_series(1, 10000) g;

INSERT INTO ss_no_pk
	SELECT g, 'n' || g, ARRAY[g, g + 1, g + 2], now() - (g || ' hours')::interval
	  FROM generate_series(1, 10000) g;

INSERT INTO ss_toast
	SELECT 'k' || g, repeat('toasted body ' || g || ' ', 400),
	       decode(md5(g::text), 'hex'), ARRAY['t' || g, 't' || (g % 17)]
	  FROM generate_series(1, 2000) g;

INSERT INTO ss_bridged (doc, txt, rng)
	SELECT jsonb_build_object('id', g, 'name', 'n' || g, 'tags', ARRAY[g % 7, g % 11]),
	       to_tsvector('simple', 'document number ' || g || ' body text'),
	       int4range(g, g + 10)
	  FROM generate_series(1, 10000) g;

INSERT INTO ss_part
	SELECT g, g % 1000, 'p' || g FROM generate_series(1, 10000) g;

INSERT INTO ss_constrained (id, base, parent)
	SELECT g, 1 + g % 5000, CASE WHEN g > 1 THEN 1 + (g - 1) % 100 END
	  FROM generate_series(1, 5000) g;

ANALYZE;
