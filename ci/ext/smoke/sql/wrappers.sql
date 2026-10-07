-- wrappers has no SQL suite upstream; this follows the helloworld_fdw README
-- (the only FDW built here) and lands the rows in an orioledb table.
CREATE EXTENSION wrappers;
CREATE FOREIGN DATA WRAPPER helloworld_wrapper
  HANDLER hello_world_fdw_handler
  VALIDATOR hello_world_fdw_validator;
CREATE SERVER my_helloworld_server
  FOREIGN DATA WRAPPER helloworld_wrapper
  OPTIONS (foo 'bar');
CREATE FOREIGN TABLE hello (id bigint, col text)
  SERVER my_helloworld_server
  OPTIONS (foo 'bar');
SELECT * FROM hello;
CREATE TABLE landed AS SELECT * FROM hello;
SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = 'landed';
SELECT * FROM landed;
