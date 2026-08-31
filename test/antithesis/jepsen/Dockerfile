# syntax=docker/dockerfile:1

# BEGIN build image
FROM clojure:temurin-21-lein-2.11.2-bookworm-slim AS build

ARG JEPSEN_POSTGRES_REF=main

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# 2026-07-29: use antithesis-instrumented sql workload
RUN git clone --depth=1 https://github.com/pmbauer/jepsen-sql.git sql \
    && cd sql \
    # pin to v0.1.1sb-SNAPSHOT
    && git checkout fc117a6e6c10b7408dfe49dad6bd6cadbcb0541a \
    && lein install

# 2026-07-29: use antithesis-instrumented postgres driver
WORKDIR /src/jepsen-postgres
RUN git init \
    && git remote add origin https://github.com/pmbauer/jepsen-postgres.git \
    && git fetch --depth=1 origin "${JEPSEN_POSTGRES_REF}" \
    && git checkout 1b99cd9dd11df56a2e45ec7c5638dae82c54446f \
    && cd postgres \
    && lein jar \
    && mkdir -p /opt/jepsen \
    && cp target/postgres-*.jar /opt/jepsen/jepsen-postgres.jar \
    && lein classpath \
       | tr ':' '\n' \
       | while IFS= read -r dependency; do \
           case "${dependency}" in \
             *.jar) cp "${dependency}" /opt/jepsen/ ;; \
           esac; \
         done

# Extract elle's anomaly set so finally_jepsen-results can emit one Antithesis assertion per anomaly type.
RUN java -cp '/opt/jepsen/*' -Djava.awt.headless=true clojure.main -e "\
(require '[elle.consistency-model :as cm]) \
(spit \"/opt/jepsen/elle-anomalies.edn\" \
      (pr-str (into (sorted-set) cm/all-anomalies)))" \
    && cat /opt/jepsen/elle-anomalies.edn

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

# BEGIN babashka

# Babashka runs the finally_jepsen-results check (parses results.edn, emits
# Antithesis fallback-SDK assertions) without dragging a JVM start-up or a
# Python stack into the runtime image.
FROM debian:bookworm-slim AS babashka

ARG TARGETARCH
ARG BABASHKA_VERSION=1.13.219

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH}" in \
         amd64) bb_arch=amd64; bb_sha256=9ac1fe988d7001625b30ef3e3307e67f8545505a7cb49a4aa179f578115a3e09 ;; \
         arm64) bb_arch=aarch64; bb_sha256=e8d7a9c66c364b80627a43cb6ba5c14fb6ac7e4af114e8e5d80f97551ccdfe11 ;; \
         *) echo "unsupported TARGETARCH=${TARGETARCH}" >&2; exit 1 ;; \
       esac \
    && curl -fsSL -o /tmp/babashka.tar.gz \
       "https://github.com/babashka/babashka/releases/download/v${BABASHKA_VERSION}/babashka-${BABASHKA_VERSION}-linux-${bb_arch}-static.tar.gz" \
    && printf '%s  %s\n' "${bb_sha256}" /tmp/babashka.tar.gz | sha256sum -c - \
    && tar -xzf /tmp/babashka.tar.gz -C /usr/local/bin bb \
    && rm /tmp/babashka.tar.gz

# END babashka

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
COPY --from=build /opt/jepsen /opt/jepsen
COPY --from=babashka /usr/local/bin/bb /usr/local/bin/bb
COPY --from=debug-source /src/sql /src/sql
COPY --from=debug-source /src/jepsen-postgres /src/jepsen-postgres

RUN set -eux; \
    for tool in bb gdb jcmd jdb jfr jhsdb jinfo jmap jps jstack lsof pgrep strace; do \
      command -v "${tool}"; \
    done

# Register only the application JAR for Antithesis coverage instrumentation and assertions.
#RUN mkdir -p /opt/antithesis/catalog \
#    && ln -s /opt/jepsen/jepsen-postgres.jar /opt/antithesis/catalog/jepsen-postgres.jar

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
