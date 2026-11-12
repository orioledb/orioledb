-- Page checksums are part of the on-disk format
CREATE SCHEMA checksum;
SET SESSION search_path = 'checksum';
CREATE EXTENSION orioledb;

SELECT orioledb_page_checksum(decode(repeat('00', 8192), 'hex'));
SELECT orioledb_page_checksum(decode(repeat('ff', 8192), 'hex'));
-- Words a b b a read the same in either byte order
SELECT orioledb_page_checksum(decode(string_agg(a || b || b || a, '' ORDER BY i), 'hex'))
	FROM generate_series(1, 2048) i, md5(i::text) m, substr(m, 1, 2) a, substr(m, 3, 2) b;
SELECT orioledb_page_checksum(decode(string_agg(a || b || b || a, '' ORDER BY i), 'hex'))
	FROM generate_series(1, 128) i, md5(i::text) m, substr(m, 1, 2) a, substr(m, 3, 2) b;

DROP EXTENSION orioledb CASCADE;
DROP SCHEMA checksum CASCADE;
RESET search_path;
