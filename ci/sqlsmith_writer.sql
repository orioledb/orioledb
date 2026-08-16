-- One round of concurrent write traffic for the SQLSmith job.
--
-- SQLSmith itself generates essentially only SELECTs, so on its own it walks a
-- frozen tree.  OrioleDB's crashes cluster in the concurrent paths -- undo
-- chains, page splits and merges, checkpoints, the bridge index -- so the
-- fuzzer needs someone mutating the data underneath it.  This file is run in a
-- loop by several psql sessions in parallel.
--
-- Deliberately no DDL: dropped or recreated objects would make SQLSmith emit a
-- flood of "relation does not exist" errors and drown the signal.

-- NB: `\set r random(1, 1000000)` is pgbench syntax, not psql's.  psql would
-- substitute the text verbatim, and since PG17 has a two-argument random(),
-- every occurrence of :r would then be re-evaluated to a *different* value --
-- which, among other things, produced int4range() bounds in the wrong order.
-- \gset evaluates once and binds literals.
SELECT (random() * 999999)::int + 1 AS r,
       (random() * 19999)::int + 1 AS k,
       (random() * 999)::int        AS p \gset

BEGIN;
INSERT INTO ss_int_pk (id, sk, val, amount, flag, ts, mood)
	VALUES (:r, :r % 97, 'w' || :r, (:r % 1000)::numeric / 3, :r % 2 = 0,
	        now(), (ARRAY['sad', 'ok', 'happy'])[1 + :r % 3]::ss_mood)
	ON CONFLICT (id) DO UPDATE
		SET val = ss_int_pk.val || ',' || :r, amount = EXCLUDED.amount;

UPDATE ss_int_pk SET sk = sk + 1, ts = now() WHERE id = :k;
DELETE FROM ss_int_pk WHERE id = :r AND :r % 7 = 0;
COMMIT;

BEGIN;
INSERT INTO ss_composite_pk (a, b, c, payload)
	VALUES (:r % 500, 'key' || :r, :r, jsonb_build_object('w', :r))
	ON CONFLICT (a, b) DO UPDATE SET c = EXCLUDED.c;

SAVEPOINT sp;
UPDATE ss_composite_pk SET c = c + 1 WHERE a = :r % 500;
ROLLBACK TO SAVEPOINT sp;

UPDATE ss_no_pk SET y = 'u' || :r WHERE x = :k % 10000;
INSERT INTO ss_no_pk (x, y, arr, ts) VALUES (:r, 'i' || :r, ARRAY[:r], now());
COMMIT;

-- TOAST churn: rewriting long values exercises the toast tree and its undo.
BEGIN;
INSERT INTO ss_toast (k, body, blob, tags)
	VALUES ('k' || :r, repeat('body ' || :r || ' ', 300),
	        decode(md5(:r::text), 'hex'), ARRAY['t' || :r])
	ON CONFLICT (k) DO UPDATE SET body = repeat('upd ' || :r || ' ', 350);
DELETE FROM ss_toast WHERE k = 'k' || (:r % 2000) AND :r % 5 = 0;
COMMIT;

-- Bridge-index churn.
BEGIN;
INSERT INTO ss_bridged (doc, txt, rng)
	VALUES (jsonb_build_object('id', :r, 'tags', ARRAY[:r % 7]),
	        to_tsvector('simple', 'doc ' || :r), int4range(:r % 1000, :r % 1000 + 5));
UPDATE ss_bridged SET doc = doc || jsonb_build_object('upd', :r)
	WHERE id = (:k % 10000) + 1;
DELETE FROM ss_bridged WHERE id = (:r % 10000) + 1 AND :r % 11 = 0;
COMMIT;

BEGIN;
INSERT INTO ss_part (id, part_key, val) VALUES (:r, :p, 'w' || :r)
	ON CONFLICT (id, part_key) DO UPDATE SET val = EXCLUDED.val;
UPDATE ss_part SET val = val || '!' WHERE id = :k AND part_key = :p;
COMMIT;

-- Occasionally push the storage layer: vacuum reclaims undo, checkpoint walks
-- the dirty pages.  Both are frequent participants in past crash reports.
SELECT CASE WHEN :r % 40 = 0 THEN 1 ELSE 0 END AS do_maintenance \gset
\if :do_maintenance
	VACUUM ANALYZE ss_int_pk;
	VACUUM ss_toast;
	CHECKPOINT;
\endif
