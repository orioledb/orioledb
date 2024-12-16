setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	
	CREATE TABLE o_test_1(
		val_1 int, 
		val_2 text, 
		PRIMARY KEY (val_1) INCLUDE (val_2)
	)USING orioledb;

	CREATE UNIQUE INDEX ON o_test_1(lower(val_2)) 
			INCLUDE (val_1);
}

teardown
{
	DROP TABLE o_test_1;
}

session s1

step "begin_1" { BEGIN; }

step "insert_1" { INSERT INTO o_test_1(val_1, val_2) 
				  VALUES(1, 'a')
				  ON CONFLICT DO NOTHING; }

step "insert_1_uniq" { INSERT INTO o_test_1(val_1, val_2) 
					   VALUES(1, 'Ab') 
					   ON CONFLICT (lower(val_2)) DO UPDATE 
					   SET val_2 = EXCLUDED.val_2; }

step "abort_1"  { ABORT; }

step "commit_1" { COMMIT; }

step "select_1" { SELECT * FROM o_test_1; }

session s2

step "begin_2" { BEGIN; }

step "insert_2" { INSERT INTO o_test_1(val_1, val_2) 
				  VALUES(1, 'b'), (1, 'c') 
				  ON CONFLICT DO NOTHING; }

step "insert_2_uniq" { INSERT INTO o_test_1(val_1, val_2) 
					   VALUES(2, 'aB') 
					   ON CONFLICT (lower(val_2)) DO UPDATE 
					   SET val_2 = EXCLUDED.val_2; }

step "select_2" { SELECT * FROM o_test_2; }

step "commit_2" { COMMIT; }

permutation "begin_1" "begin_2" "insert_1" "commit_1" "insert_2" "commit_2" "select_1"
permutation "begin_1" "begin_2" "insert_1_uniq" "insert_2_uniq" "abort_1" "select_2" "commit_2"
