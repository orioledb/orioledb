-- pg_net's upstream suite is pytest behind nix-provisioned nginx/pathod
-- servers.  This checks the part that touches storage: the background worker
-- dequeues from net.http_request_queue and inserts into net._http_response,
-- both created with the default access method.  The target port is closed, so
-- no network is needed and the response row is an error row.
CREATE EXTENSION pg_net;
SELECT c.relname, a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam
 WHERE c.relnamespace = 'net'::regnamespace AND c.relkind = 'r' ORDER BY 1;
SELECT net.http_get('http://127.0.0.1:9/') AS request_id;
DO $$
DECLARE n int := 0;
BEGIN
  LOOP
    EXIT WHEN EXISTS (SELECT 1 FROM net._http_response WHERE id = 1);
    n := n + 1;
    IF n > 300 THEN RAISE EXCEPTION 'pg_net worker did not write a response within 30s'; END IF;
    PERFORM pg_sleep(0.1);
  END LOOP;
END $$;
SELECT id, status_code, timed_out, error_msg IS NOT NULL AS has_error FROM net._http_response WHERE id = 1;
SELECT count(*) AS queued FROM net.http_request_queue;
