# A row lock whose undo no snapshot retains any more -- here a finished
# foreign key check's FOR KEY SHARE, moved past by a checkpoint -- ends the
# row lock conflict walk.  If the walk saw no version of the row by then, the
# version under the lock is older than any snapshot.  An upsert, which locks
# the row and then updates it in one command, must update it rather than
# take its own lock for a version it wrote.  And a writer that is still in
# progress under such a lock must still be waited for.
#
# UPDATE and DELETE lock the row first too when the table has a BEFORE
# UPDATE or DELETE row trigger.  They must still change the row past such
# a lock, and so must MERGE and foreign key cascades into a table that has
# one.

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

	CREATE FUNCTION rlpr_pass() RETURNS trigger AS
	$$BEGIN RETURN COALESCE(NEW, OLD); END;$$ LANGUAGE plpgsql;
	CREATE TABLE rlpr_trg (
		id int PRIMARY KEY,
		v int NOT NULL
	) USING orioledb;
	CREATE TABLE rlpr_trg_child (
		id int PRIMARY KEY,
		rid int REFERENCES rlpr_trg (id) ON DELETE CASCADE ON UPDATE CASCADE
	) USING orioledb;
	CREATE TRIGGER rlpr_trg_pass BEFORE UPDATE OR DELETE ON rlpr_trg
		FOR EACH ROW EXECUTE FUNCTION rlpr_pass();
	CREATE TRIGGER rlpr_trg_child_pass BEFORE UPDATE OR DELETE ON rlpr_trg_child
		FOR EACH ROW EXECUTE FUNCTION rlpr_pass();
	INSERT INTO rlpr_trg VALUES (1, 0), (2, 0);
	INSERT INTO rlpr_trg_child VALUES (10, 2);
}

teardown
{
	DROP TABLE rlpr_child, rlpr, rlpr_trg_child, rlpr_trg;
	DROP FUNCTION rlpr_pass();
}

session "w"
step "w_begin" { BEGIN; }
step "w_update" { UPDATE rlpr SET v = v + 1 WHERE id = 1; }
step "w_commit" { COMMIT; }

session "l"
step "l_fk" { INSERT INTO rlpr_child VALUES (1, 1); }
step "l_lock_trg" { SELECT id FROM rlpr_trg WHERE id = 1 FOR KEY SHARE; }
step "l_lock_child" { SELECT id FROM rlpr_trg_child WHERE id = 10 FOR KEY SHARE; }

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
step "t_trg_update" { UPDATE rlpr_trg SET v = v + 100 WHERE id = 1; }
step "t_trg_delete" { DELETE FROM rlpr_trg WHERE id = 1; }
step "t_merge_update" {
	MERGE INTO rlpr_trg t USING (VALUES (1)) s (id) ON t.id = s.id
		WHEN MATCHED THEN UPDATE SET v = t.v + 100;
}
step "t_merge_delete" {
	MERGE INTO rlpr_trg t USING (VALUES (1)) s (id) ON t.id = s.id
		WHEN MATCHED THEN DELETE;
}
step "t_trg_select" { SELECT * FROM rlpr_trg ORDER BY id; }
step "t_cascade_delete" { DELETE FROM rlpr_trg WHERE id = 2; }
step "t_cascade_update" { UPDATE rlpr_trg SET id = 3 WHERE id = 2; }
step "t_child_select" { SELECT * FROM rlpr_trg_child ORDER BY id; }

# the upsert updates the row
permutation "l_fk" "c_checkpoint" "t_upsert" "t_select"
# an in-progress writer under the lock is waited for
permutation "w_begin" "w_update" "l_fk" "c_checkpoint" "t_update" "w_commit" "t_select"
permutation "w_begin" "w_update" "l_fk" "c_checkpoint" "t_upsert" "w_commit" "t_select"
# statements through a BEFORE UPDATE or DELETE row trigger change the row
permutation "l_lock_trg" "c_checkpoint" "t_trg_update" "t_trg_select"
permutation "l_lock_trg" "c_checkpoint" "t_trg_delete" "t_trg_select"
permutation "l_lock_trg" "c_checkpoint" "t_merge_update" "t_trg_select"
permutation "l_lock_trg" "c_checkpoint" "t_merge_delete" "t_trg_select"
# the cascades change the child row
permutation "l_lock_child" "c_checkpoint" "t_cascade_delete" "t_child_select"
permutation "l_lock_child" "c_checkpoint" "t_cascade_update" "t_child_select"
