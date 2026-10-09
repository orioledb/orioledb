-- pg_jsonschema has no SQL suite upstream (its tests are pgrx #[pg_test]
-- functions that start their own server).  Exercise each documented function
-- and a CHECK constraint on an orioledb table.
CREATE EXTENSION pg_jsonschema;
CREATE TABLE docs (
    id int PRIMARY KEY,
    doc jsonb,
    CHECK (jsonb_matches_schema('{"type":"object","properties":{"a":{"type":"integer"}},"required":["a"]}', doc))
);
SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = 'docs';
INSERT INTO docs VALUES (1, '{"a": 1}');
INSERT INTO docs VALUES (2, '{"a": "x"}');
INSERT INTO docs VALUES (3, '{}');
SELECT * FROM docs ORDER BY id;
SELECT json_matches_schema('{"type":"string"}', '"x"');
SELECT json_matches_schema('{"type":"string"}', '1');
SELECT jsonschema_is_valid('{"type":"object"}');
SELECT jsonschema_is_valid('{"type":"nope"}');
SELECT jsonschema_validation_errors('{"type":"object","required":["a"]}', '{}');
