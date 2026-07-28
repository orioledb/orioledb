# syntax=docker/dockerfile:1

FROM python:3.13-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.10 antithesis==0.2.0

ENV PGHOST=orioledb \
    PGPORT=5432 \
    PGDATABASE=skrebuilddesync \
    PGUSER=skrebuilddesync \
    PGPASSWORD=pw

COPY --chmod=755 \
	helper_common.py \
	first_sk-rebuild-desync-setup \
	parallel_driver_sk-rebuild-desync-dml \
	anytime_sk-rebuild-desync-check \
	finally_sk-rebuild-desync-check \
	/opt/antithesis/test/v1/sk-rebuild-desync/
COPY --chmod=755 entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint"]
