setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;

	DROP TABLE IF EXISTS public.o_btree_scan;
	CREATE TABLE public.o_btree_scan (
		id bytea NOT NULL,
		val varchar NOT NULL
	) USING orioledb
	WITH (autovacuum_enabled='true', toast.autovacuum_enabled='true');
	CREATE UNIQUE INDEX _inforg175_bydims_b ON public.o_btree_scan USING btree (id);

	ALTER TABLE public.o_btree_scan CLUSTER ON _inforg175_bydims_b;

	CREATE OR REPLACE FUNCTION generate_random_varchar(length INTEGER) RETURNS TEXT AS $$
		DECLARE
			chars CHARACTER(1)[];
			result TEXT := '';
			i INTEGER := 0;
		BEGIN
			chars := ARRAY['0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'];
			IF length < 0 THEN
				RAISE EXCEPTION 'Given length cannot be less than 0';
			END IF;

			FOR i IN 1..length LOOP
				result := result || chars[1 + floor(random() * array_length(chars, 1))];
			END LOOP;

			RETURN result;
		END;
		$$ LANGUAGE plpgsql;

	DELETE FROM o_btree_scan;

	INSERT INTO o_btree_scan (id,val)
		SELECT decode(to_char(id, 'fm0000') || repeat('a', 500), 'hex'), v
		FROM generate_series(1, 4096, 1) id, generate_random_varchar(1024) AS v
		ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val;
}

teardown
{
	DROP TABLE o_btree_scan;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; }
step "s1_delete_hung" { DELETE FROM o_btree_scan; }

session "s2"

step "s2_setup" { SELECT pg_stopevent_set('seq_scan_load_internal_page', 'true'); }
step "s2_delete1" { DELETE FROM o_btree_scan WHERE id < '3900'; }
step "s2_delete2" { DELETE FROM o_btree_scan WHERE id < '4095'; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_reset" { SELECT pg_stopevent_reset('seq_scan_load_internal_page'); }


permutation "s1_setup" "s2_setup" "s2_delete1" "s2_checkpoint" "s2_delete2" "s1_delete_hung" "s2_checkpoint" "s2_reset"
