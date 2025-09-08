setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	DROP TABLE IF EXISTS public._inforg175;
	CREATE TABLE public._inforg175 (
		_fld176 bytea NOT NULL,
		_fld177 varchar NOT NULL
	)
	WITH (autovacuum_enabled='true', toast.autovacuum_enabled='true');
	ALTER TABLE public._inforg175 OWNER TO usr1cv8;
	CREATE UNIQUE INDEX _inforg175_bydims_b ON public._inforg175 USING btree (_fld176);

	ALTER TABLE public._inforg175 CLUSTER ON _inforg175_bydims_b;

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

	DELETE FROM _inforg175;

	INSERT INTO _inforg175 (_fld176,_fld177)
		SELECT decode(to_char(id, 'fm0000') || repeat('a', 500), 'hex'), v
		FROM generate_series(1, 4096, 1) id, generate_random_varchar(1024) AS v
		ON CONFLICT (_fld176) DO UPDATE SET _fld177 = EXCLUDED._fld177;
}

teardown
{
	DROP TABLE _inforg175;
}

session "s1"

step "s1_setup"  { SET orioledb.enable_stopevents = true; }
step "s1_delete_hung" { DELETE FROM _inforg175; }

session "s2"

step "s2_setup" { SELECT pg_stopevent_set('seq_scan_load_internal_nonleftmost_page', 'true'); }
step "s2_delete1" { DELETE FROM _inforg175 WHERE _fld176 < '3900'; }
step "s2_delete2" { DELETE FROM _inforg175 WHERE _fld176 < '4095'; }
step "s2_checkpoint" { CHECKPOINT; }
step "s2_sleep" { SELECT pg_sleep(10); }
step "s2_reset" { SELECT pg_stopevent_reset('seq_scan_load_internal_nonleftmost_page'); }


permutation "s1_setup" "s2_setup" "s2_delete1" "s2_checkpoint" "s2_sleep" "s2_delete2" "s1_delete_hung" "s2_checkpoint" "s2_sleep" "s2_reset"
