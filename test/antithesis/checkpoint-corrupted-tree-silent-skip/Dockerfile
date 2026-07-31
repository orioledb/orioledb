# syntax=docker/dockerfile:1

FROM python:3.13-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.10 antithesis==0.2.0

ENV PGHOST=orioledb \
    PGPORT=5432 \
    PGDATABASE=checkpointcorrupt \
    PGUSER=checkpointcorrupt \
    PGPASSWORD=pw \
    PGDATA_DIR=/var/lib/postgresql/data

COPY --chmod=755 \
	helper_common.py \
	first_checkpoint-corrupted-tree-silent-skip-setup \
	singleton_driver_checkpoint-corrupted-tree-silent-skip-corrupt \
	serial_driver_checkpoint-corrupted-tree-silent-skip-probe \
	parallel_driver_checkpoint-corrupted-tree-silent-skip-healthy-dml \
	anytime_checkpoint-corrupted-tree-silent-skip-check \
	finally_checkpoint-corrupted-tree-silent-skip-check \
	/opt/antithesis/test/v1/checkpoint-corrupted-tree-silent-skip/
COPY --chmod=755 entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint"]
