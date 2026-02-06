CREATE SCHEMA row_security_schema;
SET SESSION search_path = 'row_security_schema';
CREATE EXTENSION orioledb;

CREATE USER regress_rls_alice NOLOGIN;
CREATE USER regress_rls_bob NOLOGIN;
CREATE USER regress_rls_carol NOLOGIN;
CREATE USER regress_rls_dave NOLOGIN;
CREATE USER regress_rls_exempt_user BYPASSRLS NOLOGIN;
CREATE ROLE regress_rls_group1 NOLOGIN;
CREATE ROLE regress_rls_group2 NOLOGIN;

GRANT regress_rls_group1 TO regress_rls_bob;
GRANT regress_rls_group2 TO regress_rls_carol;

CREATE SCHEMA regress_rls_schema;
GRANT ALL ON SCHEMA regress_rls_schema to public;
SET search_path = regress_rls_schema;

CREATE OR REPLACE FUNCTION f_leak(text) RETURNS bool
    COST 0.0000001 LANGUAGE plpgsql
    AS 'BEGIN RAISE NOTICE ''f_leak => %'', $1; RETURN true; END';
GRANT EXECUTE ON FUNCTION f_leak(text) TO public;

SET SESSION AUTHORIZATION regress_rls_alice;
CREATE TABLE uaccount (
    pguser      name primary key,
    seclv       int
) USING orioledb;
GRANT SELECT ON uaccount TO public;
INSERT INTO uaccount VALUES
    ('regress_rls_alice', 99),
    ('regress_rls_bob', 1),
    ('regress_rls_carol', 2),
    ('regress_rls_dave', 3);

CREATE TABLE category (
    cid        int primary key,
    cname      text
) USING orioledb;
GRANT ALL ON category TO public;
INSERT INTO category VALUES
    (11, 'novel'),
    (22, 'science fiction'),
    (33, 'technology'),
    (44, 'manga');

CREATE TABLE document (
    did         int primary key,
    cid         int references category(cid),
    dlevel      int not null,
    dauthor     name,
    dtitle      text
) USING orioledb;
GRANT ALL ON document TO public;
INSERT INTO document VALUES
    ( 1, 11, 1, 'regress_rls_bob', 'my first novel'),
    ( 2, 11, 2, 'regress_rls_bob', 'my second novel'),
    ( 3, 22, 2, 'regress_rls_bob', 'my science fiction'),
    ( 4, 44, 1, 'regress_rls_bob', 'my first manga'),
    ( 5, 44, 2, 'regress_rls_bob', 'my second manga'),
    ( 6, 22, 1, 'regress_rls_carol', 'great science fiction'),
    ( 7, 33, 2, 'regress_rls_carol', 'great technology book'),
    ( 8, 44, 1, 'regress_rls_carol', 'great manga'),
    ( 9, 22, 1, 'regress_rls_dave', 'awesome science fiction'),
    (10, 33, 2, 'regress_rls_dave', 'awesome technology book');

TABLE document;
ALTER TABLE document ENABLE ROW LEVEL SECURITY;

CREATE POLICY p1 ON document AS PERMISSIVE
    USING (dlevel <= (SELECT seclv FROM uaccount WHERE pguser = current_user));

CREATE POLICY p2r ON document AS RESTRICTIVE TO regress_rls_dave
    USING (cid <> 44 AND cid < 50);

CREATE POLICY p1r ON document AS RESTRICTIVE TO regress_rls_dave
    USING (cid <> 44);

SET SESSION AUTHORIZATION regress_rls_bob;
SET row_security TO ON;

SET SESSION AUTHORIZATION regress_rls_carol;

SET SESSION AUTHORIZATION regress_rls_dave;
INSERT INTO document VALUES (100, 44, 1, 'regress_rls_dave', 'testing sorting of policies'); -- fail
INSERT INTO document VALUES (100, 11, 1, 'regress_rls_dave', 'testing sorting of policies'); -- ok

TABLE document;

SET SESSION AUTHORIZATION DEFAULT;
ALTER TABLE document DISABLE ROW LEVEL SECURITY;

SET SESSION AUTHORIZATION regress_rls_dave;
INSERT INTO document VALUES (101, 44, 1, 'regress_rls_dave', 'testing sorting of policies'); -- ok

TABLE document;

SET SESSION AUTHORIZATION DEFAULT;
ALTER TABLE document ENABLE ROW LEVEL SECURITY;

TABLE document;

SET SESSION AUTHORIZATION regress_rls_dave;
INSERT INTO document VALUES (100, 44, 1, 'regress_rls_dave', 'testing sorting of policies'); -- fail

TABLE document;
SET SESSION AUTHORIZATION DEFAULT;
TABLE document;

-- Test AT_ForceRowSecurity and AT_NoForceRowSecurity
-- These commands control whether table owners are subject to RLS policies

-- Create a new table owned by carol with mixed security levels
SET SESSION AUTHORIZATION regress_rls_carol;

CREATE TABLE classified_docs (
    did         int primary key,
    dlevel      int not null,
    dauthor     name,
    dtitle      text
) USING orioledb;

GRANT ALL ON classified_docs TO public;

-- Insert documents with varying security levels
-- carol has seclv=2, so she can normally see dlevel <= 2
INSERT INTO classified_docs VALUES
    (1, 1, 'regress_rls_bob', 'public document'),
    (2, 2, 'regress_rls_carol', 'internal document'),
    (3, 3, 'regress_rls_alice', 'confidential document'),
    (4, 4, 'regress_rls_alice', 'secret document'),
    (5, 5, 'regress_rls_alice', 'top secret document');

-- Enable RLS and create policy
ALTER TABLE classified_docs ENABLE ROW LEVEL SECURITY;

CREATE POLICY classified_policy ON classified_docs AS PERMISSIVE
    USING (dlevel <= (SELECT seclv FROM uaccount WHERE pguser = current_user));

-- Check current settings (RLS enabled, not forced)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'classified_docs';

-- As owner, carol can see ALL rows (bypassing RLS) even though her seclv=2
SELECT COUNT(*) as carol_sees_without_force FROM classified_docs;
SELECT did, dlevel, dtitle FROM classified_docs ORDER BY did;

-- Test AT_ForceRowSecurity - force RLS policies to apply even to table owner
ALTER TABLE classified_docs FORCE ROW LEVEL SECURITY;

-- Verify setting changed (relforcerowsecurity = true)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'classified_docs';

-- Now carol is subject to the policy and can only see dlevel <= 2
SELECT COUNT(*) as carol_sees_with_force FROM classified_docs;
SELECT did, dlevel, dtitle FROM classified_docs ORDER BY did;

-- Verify bob (seclv=1) sees even fewer documents
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT COUNT(*) as bob_sees_with_force FROM classified_docs;
SELECT did, dlevel, dtitle FROM classified_docs ORDER BY did;

-- Test AT_NoForceRowSecurity - allow owner to bypass RLS again
SET SESSION AUTHORIZATION regress_rls_carol;
ALTER TABLE classified_docs NO FORCE ROW LEVEL SECURITY;

-- Verify setting changed back (relforcerowsecurity = false)
SELECT relname, relrowsecurity, relforcerowsecurity
FROM pg_class
WHERE relname = 'classified_docs';

-- Carol should now see ALL rows again (bypassing RLS as owner)
SELECT COUNT(*) as carol_sees_after_unforce FROM classified_docs;
SELECT did, dlevel, dtitle FROM classified_docs ORDER BY did;

-- Regular users still subject to policies
SET SESSION AUTHORIZATION regress_rls_bob;
SELECT COUNT(*) as bob_sees_after_unforce FROM classified_docs;

-- Cleanup
SET SESSION AUTHORIZATION regress_rls_carol;
DROP TABLE classified_docs CASCADE;

SET SESSION AUTHORIZATION DEFAULT;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA row_security_schema CASCADE;
RESET search_path;
