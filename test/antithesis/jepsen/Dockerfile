# syntax=docker/dockerfile:1

# BEGIN build image
FROM --platform=${BUILDPLATFORM} clojure:temurin-21-lein-2.11.2-bookworm-slim AS build

ARG JEPSEN_POSTGRES_REF=main

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

# 2026-07-17: temporary workaround until https://github.com/jepsen-io/sql/pull/2 is merged upsgream
WORKDIR /src
RUN git clone https://github.com/pmbauer/sql.git \
    && cd sql \
    && lein install

WORKDIR /src/jepsen-postgres
RUN git init \
    && git remote add origin https://github.com/jepsen-io/postgres.git \
    && git fetch --depth=1 origin "${JEPSEN_POSTGRES_REF}" \
    && git checkout --detach FETCH_HEAD \
    && cd postgres \
    && lein uberjar \
    && cp target/*-standalone.jar /jepsen-postgres.jar

RUN jlink \
    --add-modules java.base,java.desktop,java.instrument,java.management,java.naming,java.scripting,java.security.jgss,java.sql,jdk.crypto.ec,jdk.random,jdk.unsupported \
    --no-header-files \
    --no-man-pages \
    --output /opt/java-runtime

# END build image

# BEGIN runtime

FROM --platform=${BUILDPLATFORM} debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      git-core \
      gnuplot-nox \
      graphviz \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:${PATH}"

WORKDIR /opt/jepsen
COPY --from=build /opt/java-runtime /opt/java
COPY --from=build /jepsen-postgres.jar ./jepsen-postgres.jar

ENV JEPSEN_MAX_WRITES_PER_KEY=4 \
    JEPSEN_CONCURRENCY=50 \
    JEPSEN_RATE=500 \
    JEPSEN_WORKLOAD=append \
    JEPSEN_ISOLATION=serializable \
    JEPSEN_EXPECTED_CONSISTENCY_MODEL=serializable \
    JEPSEN_TIME_LIMIT=60 \
    JEPSEN_NEMESIS=none \
    JEPSEN_NODE=localhost \
    JEPSEN_POSTGRES_USER=jepsen \
    JEPSEN_POSTGRES_PASSWORD=pw \
    JEPSEN_POSTGRES_PORT=5432

COPY --chmod=755 singleton_driver_jepsen-postgres /opt/antithesis/test/v1/jepsen-postgres/
COPY --chmod=755 entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint"]

# END runtime
