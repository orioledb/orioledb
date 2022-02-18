setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE IF NOT EXISTS o_iso0 (
		key int8 NOT NULL,
		value int8 NOT NULL,
		text_val text NOT NULL,
		PRIMARY KEY(text_val),
		UNIQUE(key, text_val)
	) USING orioledb;
	TRUNCATE o_iso0 CASCADE;
	CREATE TABLE IF NOT EXISTS o_iso1(
		key int8 NOT NULL,
		fkey int8 NOT NULL,
		ftext_val text NOT NULL,
		FOREIGN KEY(fkey, ftext_val) REFERENCES o_iso0 (key, text_val) ON UPDATE CASCADE ON DELETE CASCADE,
		PRIMARY KEY(key)
	) USING orioledb;
}

teardown
{
	DROP TABLE o_iso0 CASCADE;
	DROP TABLE o_iso1;
}

session "s1"

step "s1_begin" { BEGIN; }
step "s1_select0" { SELECT * FROM o_iso0 WHERE key > 0 FOR UPDATE; }
step "s1_select1" { SELECT * FROM o_iso1 WHERE key > 0 FOR UPDATE; }
step "s1_insert0" { INSERT INTO o_iso0 SELECT i, i + 1, CAST(i as text) FROM generate_series(1, 6) AS i; }
step "s1_insert1" { INSERT INTO o_iso1 VALUES (1,1,'1'); }
step "s1_update0" { UPDATE o_iso0 SET key = 11, text_val = 'hi bro'  WHERE key = 5 RETURNING *; }
step "s1_update1" { UPDATE o_iso1 SET fkey = 5, ftext_val = '5' WHERE key = 1 RETURNING *; }
step "s1_delete0" { DELETE FROM o_iso0 where key = 1; }
step "s1_commit" { COMMIT; }

session "s2"

step "s2_begin" { BEGIN; }
step "s2_select0" { SELECT * FROM o_iso0 WHERE key > 0 FOR UPDATE; }
step "s2_select1" { SELECT * FROM o_iso1 WHERE key > 0 FOR UPDATE; }
step "s2_insert1" { INSERT INTO o_iso1 VALUES (2,1,'1'); }
step "s2_update0" { UPDATE o_iso0 SET key = 12, text_val = 'hi man' WHERE key = 6 RETURNING *; }
step "s2_update1" { UPDATE o_iso1 SET fkey = 6, ftext_val = '6'  WHERE key = 1 RETURNING *; }
step "s2_delete0" { DELETE FROM o_iso0 where key = 1; }
step "s2_commit" { COMMIT; }

permutation "s1_insert0" "s1_insert1" "s1_update1" "s2_update1" "s1_update0" "s2_update0" "s2_select0" "s2_select1"
permutation "s1_insert0" "s1_insert1" "s1_delete0" "s2_delete0" "s1_select0" "s2_select0"
permutation "s1_insert0" "s1_insert1" "s2_insert1" "s2_select0" "s2_select1" "s1_delete0" "s2_delete0" 
permutation "s1_insert0" "s1_insert1" "s1_select1" "s2_select1" "s1_begin" "s1_update1" "s1_commit" "s2_begin" "s2_update1" "s2_commit"
permutation "s1_insert0" "s1_insert1" "s1_select0" "s2_select0" "s1_begin" "s1_select1" "s1_update1" "s1_commit" "s2_begin" "s2_update1" "s2_commit"



