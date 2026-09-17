CREATE EXTENSION IF NOT EXISTS orioledb;

CREATE TABLE o_sample_diagnostic (
    id int PRIMARY KEY,
    info text
) USING orioledb;

INSERT INTO o_sample_diagnostic VALUES (1, 'veri1'), (2, 'veri2');
SELECT orioledb_custom_page_stats('o_sample_diagnostic');

DROP TABLE o_sample_diagnostic;