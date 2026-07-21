# syntax=docker/dockerfile:1

FROM python:3.13-slim

RUN pip install --no-cache-dir psycopg2-binary==2.9.10 antithesis==0.2.0

WORKDIR /opt/sk-recovery-race-chaos
COPY driver.py ./

ENV PGHOST=orioledb \
    PGPORT=5432 \
    PGDATABASE=skrecoveryracechaos \
    PGUSER=skrecoveryracechaos \
    PGPASSWORD=pw \
    BURST_SECONDS=5 \
    BURST_WORKERS=8

COPY --chmod=755 singleton_driver_sk-recovery-race-chaos /opt/antithesis/test/v1/sk-recovery-race-chaos/
COPY --chmod=755 entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint"]
