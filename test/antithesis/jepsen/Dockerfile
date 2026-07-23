# syntax=docker/dockerfile:1

# BEGIN build image
FROM clojure:temurin-21-lein-2.11.2-bookworm-slim AS build

ARG JEPSEN_POSTGRES_REF=main

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# 2026-07-23: temporary workaround until https://github.com/jepsen-io/sql/pull/2 is
# pushed to clojars
RUN git clone --depth=1 https://github.com/jepsen-io/sql.git \
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
    --add-modules java.base,java.desktop,java.instrument,java.management,java.naming,java.scripting,java.security.jgss,java.sql,jdk.attach,jdk.crypto.ec,jdk.hotspot.agent,jdk.jcmd,jdk.jdi,jdk.jdwp.agent,jdk.jfr,jdk.random,jdk.unsupported \
    --no-header-files \
    --no-man-pages \
    --output /opt/java-runtime \
    && for tool in java jcmd jdb jfr jhsdb jinfo jmap jps jstack; do \
         test -x "/opt/java-runtime/bin/${tool}"; \
       done

# Retain the exact workload sources without build output so JDB can resolve
# source locations recorded in the compiled classes.
FROM build AS debug-source
RUN rm -rf \
    /src/sql/.git \
    /src/sql/target \
    /src/jepsen-postgres/.git \
    /src/jepsen-postgres/postgres/target

# END build image

# BEGIN runtime

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
      binutils \
      file \
      gdb \
      git-core \
      gnuplot-nox \
      graphviz \
      lsof \
      procps \
      strace \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java \
    PATH="/opt/java/bin:${PATH}"

WORKDIR /opt/jepsen
COPY --from=build /opt/java-runtime /opt/java
COPY --from=build /jepsen-postgres.jar ./jepsen-postgres.jar
COPY --from=debug-source /src/sql /src/sql
COPY --from=debug-source /src/jepsen-postgres /src/jepsen-postgres

RUN set -eux; \
    for tool in gdb jcmd jdb jfr jhsdb jinfo jmap jps jstack lsof pgrep strace; do \
      command -v "${tool}"; \
    done

ENV JEPSEN_WORKLOAD=append \
    JEPSEN_CONCURRENCY=50 \
    JEPSEN_RATE=500 \
    JEPSEN_MAX_WRITES_PER_KEY=4 \
    JEPSEN_TIME_LIMIT=60 \
    JEPSEN_ISOLATION=serializable \
    JEPSEN_EXPECTED_CONSISTENCY_MODEL=serializable \
    JEPSEN_NEMESIS=none \
    JEPSEN_NODE=localhost \
    JEPSEN_POSTGRES_USER=jepsen \
    JEPSEN_POSTGRES_PASSWORD=pw \
    JEPSEN_POSTGRES_PORT=5432 \
    JEPSEN_JDWP_PORT=

COPY ./test /opt/antithesis/test
COPY --chmod=755 entrypoint /usr/local/bin/

ENTRYPOINT ["/usr/local/bin/entrypoint"]

# END runtime
