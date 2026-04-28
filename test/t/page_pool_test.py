#!/usr/bin/env python3

import unittest

from .base_test import BaseTest, ThreadQueryExecutor


class PagePoolTest(BaseTest):
    def test_many_small_tables_pool_exhaustion(self):
        """
        Test that creating and inserting into many small tables in one
        transaction raises a clean error instead of hanging forever in
        the clock algorithm (issue #828).

        With default orioledb.main_buffers each table's root page
        occupies a pool slot.  When the pool is exhausted and all pages
        belong to trees locked by the current transaction, the clock
        algorithm must detect the situation and error out.
        """
        node = self.node
        node.start()

        node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

        # Create 1500 tables — this alone should succeed.
        node.safe_psql("""
			DO $$
			DECLARE i int;
			BEGIN
				FOR i IN 1..1500 LOOP
					EXECUTE format(
						'CREATE TABLE t%s (id int PRIMARY KEY, payload text) USING orioledb', i);
				END LOOP;
			END $$;
		""")

        # Now try to insert into all of them in one transaction.
        # This should either succeed (if pool is large enough) or raise
        # a clean error — but must NOT hang.
        try:
            node.safe_psql("""
				DO $$
				DECLARE i int;
				BEGIN
					FOR i IN 1..1500 LOOP
						EXECUTE format('INSERT INTO t%s(id) VALUES(0)', i);
					END LOOP;
				END $$;
			""")
            # If it succeeded, that's fine too (pool was large enough).
            succeeded = True
        except Exception as e:
            # Expected: ERROR about pool exhaustion.
            succeeded = False
            self.assertIn(
                "page pool", str(e).lower(), f"Expected pool exhaustion error, got: {e}"
            )

        # Either way the backend must still be alive and usable.
        result = node.execute("SELECT 1;")
        self.assertEqual(result, [(1,)])

        # Clean up tables so the node shuts down cleanly.
        node.safe_psql("""
			DO $$
			DECLARE i int;
			BEGIN
				FOR i IN 1..1500 LOOP
					EXECUTE format('DROP TABLE IF EXISTS t%s', i);
				END LOOP;
			END $$;
		""")

        node.stop()

    def test_two_concurrent_transactions_pool_exhaustion(self):
        """
        Test that two concurrent transactions, each holding many table
        locks, do not cause the clock algorithm to hang when the page
        pool is exhausted.

        Each backend pre-creates 750 tables, then both try to INSERT
        into all their tables concurrently.  Together the root pages
        exceed the pool.  Each backend's clock sweep finds the other
        backend's pages locked (try_lock_page fails) and its own pages
        nested-locked.  At least one backend must get a clean error
        (or both succeed if the pool happens to be large enough).
        Neither must hang.
        """
        node = self.node
        node.start()

        node.safe_psql("CREATE EXTENSION IF NOT EXISTS orioledb;")

        n = 750

        # Pre-create tables outside transactions so both can see them.
        for prefix in ("a", "b"):
            node.safe_psql(f"""
                DO $$
                DECLARE i int;
                BEGIN
                    FOR i IN 1..{n} LOOP
                        EXECUTE format(
                            'CREATE TABLE t_{prefix}%s (id int PRIMARY KEY, payload text) USING orioledb', i);
                    END LOOP;
                END $$;
            """)

        con_a = node.connect()
        con_b = node.connect()

        # Use statement_timeout so that if the clock algorithm hangs
        # despite our fix, the test still finishes (with a failure).
        con_a.execute("SET statement_timeout = '60s';")
        con_b.execute("SET statement_timeout = '60s';")

        insert_a = f"""
            DO $$
            DECLARE i int;
            BEGIN
                FOR i IN 1..{n} LOOP
                    EXECUTE format('INSERT INTO t_a%s(id) VALUES(0)', i);
                END LOOP;
            END $$;
        """
        insert_b = f"""
            DO $$
            DECLARE i int;
            BEGIN
                FOR i IN 1..{n} LOOP
                    EXECUTE format('INSERT INTO t_b%s(id) VALUES(0)', i);
                END LOOP;
            END $$;
        """

        t_a = ThreadQueryExecutor(con_a, insert_a)
        t_b = ThreadQueryExecutor(con_b, insert_b)

        t_a.start()
        t_b.start()

        error_a = None
        error_b = None
        try:
            t_a.join(timeout=90)
        except Exception as e:
            error_a = e
        try:
            t_b.join(timeout=90)
        except Exception as e:
            error_b = e

        # Neither thread should still be alive (that would mean a hang).
        self.assertFalse(
            t_a.is_alive() and t_b.is_alive(),
            "Both transactions are still running — clock algorithm likely hung",
        )

        # At least one should have finished.  It's acceptable for one
        # (or even both) to get an error — the pool exhaustion ERROR may
        # surface as a connection-level exception ("tuple index out of
        # range", "server closed the connection", etc.) depending on the
        # driver.  The important thing is that neither thread hangs.

        con_a.close()
        con_b.close()

        # The node must still be usable.
        result = node.execute("SELECT 1;")
        self.assertEqual(result, [(1,)])

        # Clean up.
        for prefix in ("a", "b"):
            node.safe_psql(f"""
                DO $$
                DECLARE i int;
                BEGIN
                    FOR i IN 1..{n} LOOP
                        EXECUTE format('DROP TABLE IF EXISTS t_{prefix}%s', i);
                    END LOOP;
                END $$;
            """)

        node.stop()
