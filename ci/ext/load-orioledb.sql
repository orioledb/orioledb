-- Run by regress/run_test.pl right after it creates its database (its
-- --after-create-db-script hook): the same thing --load-extension=orioledb does
-- for pg_regress-driven suites, since createdb --template=template0 cannot
-- inherit the access method.
CREATE EXTENSION IF NOT EXISTS orioledb;
