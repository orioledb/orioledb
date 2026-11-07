# A row lock whose undo no snapshot retains any more -- here a finished
# foreign key check's FOR KEY SHARE, moved past by a checkpoint -- ends the
# row lock conflict walk.  If the walk saw no version of the row by then, the
# version under the lock is older than any snapshot.  An upsert, which locks
# the row and then updates it in one command, must update it rather than
# take its own lock for a version it wrote.  And a writer that is still in
# progress under such a lock must still be waited for.

setup
{
	CREATE EXTENSION IF NOT EXISTS orioledb;
	CREATE TABLE rlpr (
		id int PRIMARY KEY,
		name text UNIQUE NOT NULL,
		v int NOT NULL
	) USING orioledb;
	CREATE TABLE rlpr_child (
		id int PRIMARY KEY,
		rid int REFERENCES rlpr (id)
	) USING orioledb;
	INSERT INTO rlpr VALUES (1, 'r1', 0);
}

teardown
{
	DROP TABLE rlpr_child, rlpr;
}

session "w"
step "w_begin" { BEGIN; }
step "w_update" { UPDATE rlpr SET v = v + 1 WHERE id = 1; }
step "w_commit" { COMMIT; }

session "l"
step "l_fk" { INSERT INTO rlpr_child VALUES (1, 1); }

session "c"
step "c_checkpoint" { CHECKPOINT; }

session "t"
step "t_upsert" {
	INSERT INTO rlpr VALUES (2, 'r1', 0)
		ON CONFLICT (name) DO UPDATE SET v = rlpr.v + 100
		RETURNING id, v;
}
step "t_update" { UPDATE rlpr SET v = v + 100 WHERE id = 1 RETURNING v; }
step "t_select" { SELECT * FROM rlpr ORDER BY id; }

# the upsert updates the row
permutation "l_fk" "c_checkpoint" "t_upsert" "t_select"
# an in-progress writer under the lock is waited for
permutation "w_begin" "w_update" "l_fk" "c_checkpoint" "t_update" "w_commit" "t_select"
permutation "w_begin" "w_update" "l_fk" "c_checkpoint" "t_upsert" "w_commit" "t_select"
