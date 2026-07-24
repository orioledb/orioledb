# Antithesis-instrumented OrioleDB image.
# PostgreSQL is built with assertions; only orioledb.so receives coverage instrumentation.

# NB this is modified from docker/Dockerfie.ubuntu in the following ways
# - uses build container, separate build and runtime dependencies
# - removes: --with-krb5 --with-gssapi --with-ldap --with-tcl --with-perl --with-python --with-llvm
# - adds: --enable-debug --enable-cassert
# - instruments orioledb.so for simulation testing in Antithesis
#
# At some point we may want to unify our Antithesis, release ubuntu, release alpine builds to avoid drift

ARG UBUNTU_VERSION=noble

# BEGIN build image

FROM ubuntu:${UBUNTU_VERSION} AS builder

ARG UBUNTU_VERSION
ARG PG_MAJOR=17
ARG ORIOLEDB_REPO=https://github.com/orioledb/orioledb.git
ARG ORIOLEDB_REF=main
ARG ANTITHESIS_SDK_REF=0.4.8
ARG PGTAG=

ENV LANG=en_US.utf8

RUN set -eux; \
	apt-get update; \
	DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		bison \
		build-essential \
		ca-certificates \
		clang \
		curl \
		flex \
		gdb \
		git \
		libcurl4-openssl-dev \
		libicu-dev \
		libipc-run-perl \
		liblz4-dev \
		libreadline-dev \
		libssl-dev \
		libxml2-dev \
		libxslt1-dev \
		libzstd-dev \
		lld \
		locales \
		make \
		pkg-config \
		python3 \
		tzdata \
		uuid-dev \
		zlib1g-dev \
	; \
	echo 'en_US.UTF-8 UTF-8' >> /etc/locale.gen; \
	locale-gen; \
	rm -rf /var/lib/apt/lists/*; \
    apt-get clean

# shallow clone OrioleDB sources at specified coordinates
RUN set -eux; \
	git init /usr/src/postgresql/contrib/orioledb; \
	cd /usr/src/postgresql/contrib/orioledb; \
	git remote add origin "${ORIOLEDB_REPO}"; \
	git fetch --depth 1 origin "${ORIOLEDB_REF}"; \
	git checkout --detach FETCH_HEAD

# NB we are not coverage-instrumenting the main postgres service.
# Only instrumenting orioledb.so will help guide the simulation search and focus on orioledb.so
# However, we may eventually run a build with the patched postgres instrumented as well
# as this current build will not focus on those changes.

RUN set -eux; \
	if [ -z "${PGTAG}" ]; then \
		PGTAG="$(awk -v major="${PG_MAJOR}" '$1 == major ":" { print $2; exit }' /usr/src/postgresql/contrib/orioledb/.pgtags)"; \
	fi; \
	test -n "${PGTAG}"; \
	ORIOLEDB_VERSION=$(grep "^#define ORIOLEDB_VERSION" /usr/src/postgresql/contrib/orioledb/include/orioledb.h | cut -d'"' -f2); \
	ORIOLEDB_BUILDTIME=$(date -Iseconds); \
    \
	echo "PG_MAJOR=${PG_MAJOR}"; \
	echo "PGTAG=${PGTAG}"; \
	echo "ORIOLEDB_VERSION=${ORIOLEDB_VERSION}"; \
	echo "ORIOLEDB_BUILDTIME=${ORIOLEDB_BUILDTIME}"; \
	mkdir -p /opt/orioledb; \
	printf '%s\n' \
		"PGTAG=${PGTAG}" \
		"PG_MAJOR=${PG_MAJOR}" \
		"ORIOLEDB_REF=${ORIOLEDB_REF}" \
		"ORIOLEDB_VERSION=${ORIOLEDB_VERSION}" \
		"ORIOLEDB_BUILDTIME=${ORIOLEDB_BUILDTIME}" \
		> /opt/orioledb/manifest.out; \
    \
	curl -fsSL "https://github.com/orioledb/postgres/tarball/${PGTAG}" -o /tmp/postgresql.tar.gz; \
	tar \
		--extract \
		--file /tmp/postgresql.tar.gz \
		--directory /usr/src/postgresql \
		--strip-components 1; \
	rm /tmp/postgresql.tar.gz; \
	cd /usr/src/postgresql; \
	\
	POSTGRESQL_VERSION=$(grep "PACKAGE_VERSION=" ./configure | cut -d"'" -f2); \
	echo "POSTGRESQL_VERSION=${POSTGRESQL_VERSION}"; \
	\
	gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
# explicitly update autoconf config.guess and config.sub so they support more arches/libcs
	cp /usr/src/postgresql/contrib/orioledb/docker/config.guess config/config.guess; \
	cp /usr/src/postgresql/contrib/orioledb/docker/config.sub config/config.sub; \
	( CC=clang ./configure \
		--build="${gnuArch}" \
		--enable-debug \
		--enable-cassert \
		--enable-integer-datetimes \
		--enable-thread-safety \
		--enable-tap-tests \
		--disable-rpath \
		--with-uuid=e2fs \
		--with-gnu-ld \
		--with-pgport=5432 \
		--with-system-tzdata=/usr/share/zoneinfo \
		--prefix=/usr/local \
		--with-includes=/usr/local/include \
		--with-libraries=/usr/local/lib \
		--with-openssl \
		--with-libxml \
		--with-libxslt \
		--with-icu \
		--with-lz4 \
		--with-zstd \
		# The "testgres" package expects the PostgreSQL version as the last word.
		# Therefore, the extra ${POSTGRESQL_VERSION} is added as a workaround.
		--with-extra-version=" ${ORIOLEDB_VERSION} PGTAG=${PGTAG} antithesis ubuntu:${UBUNTU_VERSION}+clang build:${ORIOLEDB_BUILDTIME} ${POSTGRESQL_VERSION}" \
		|| { cat config.log; exit 1; } \
	); \
	if printf "%s\n" "$PGTAG" | grep -Fqe "patches${PG_MAJOR}_"; then \
		echo "ORIOLEDB_PATCHSET_VERSION = `echo $PGTAG | cut -d'_' -f2`" >> src/Makefile.global; \
	else \
		echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global; \
	fi

RUN set -eux; \
	cd /usr/src/postgresql; \
	make -j "$(nproc)"; \
	make -C contrib -j "$(nproc)"; \
	make install; \
	make -C contrib install

# Build orioledb.so with Antithesis coverage instrumentation
# see https://antithesis.com/docs/reference/sdk/cpp/instrumentation/

# Antithesis SDK header plus the single TU that must be linked into the
# instrumented artifact: it defines __sanitizer_cov_trace_pc_guard(_init)
# and the runtime loader antithesis_load_libvoidstar.  Deliberately compiled
# WITHOUT -fsanitize-coverage/-flto: the callbacks must not trace themselves,
# and a regular object mixes fine into a ThinLTO link.
# (Kept after the postgres layer so an SDK bump only rebuilds orioledb.)
RUN set -eux; \
	mkdir -p /usr/src/antithesis; \
	curl -fsSL \
		"https://raw.githubusercontent.com/antithesishq/antithesis-sdk-cpp/${ANTITHESIS_SDK_REF}/antithesis_instrumentation.h" \
		-o /usr/src/antithesis/antithesis_instrumentation.h; \
	printf '#include "antithesis_instrumentation.h"\n' > /usr/src/antithesis/antithesis_instrumentation.c; \
	clang -c -O2 -fPIC -I/usr/src/antithesis \
		-o /usr/src/antithesis/antithesis_instrumentation.o \
		/usr/src/antithesis/antithesis_instrumentation.c

RUN set -eux; \
	cd /usr/src/postgresql/contrib/orioledb; \
	make USE_PGXS=1 clean; \
# COPT reaches every object's CFLAGS (and LDFLAGS) via Makefile.global;
# the LDFLAGS_SL override reaches the .so link line
	make -j "$(nproc)" USE_PGXS=1 IS_DEV=1 \
		COPT="-fsanitize-coverage=trace-pc-guard -flto=thin -g" \
		LDFLAGS_SL="-flto=thin -fsanitize-coverage=trace-pc-guard -fuse-ld=lld -Wl,--build-id /usr/src/antithesis/antithesis_instrumentation.o"; \
	make USE_PGXS=1 IS_DEV=1 \
		COPT="-fsanitize-coverage=trace-pc-guard -flto=thin -g" \
		LDFLAGS_SL="-flto=thin -fsanitize-coverage=trace-pc-guard -fuse-ld=lld -Wl,--build-id /usr/src/antithesis/antithesis_instrumentation.o" \
		install; \
	PKGLIB="$(pg_config --pkglibdir)"; \
# sanity assertions that orioledb.so is instrumented
	nm "${PKGLIB}/orioledb.so" | grep -q 'T antithesis_load_libvoidstar'; \
	nm "${PKGLIB}/orioledb.so" | grep -q 'sanitizer_cov_trace_pc_guard'; \
	readelf -S "${PKGLIB}/orioledb.so" | grep -q 'sancov_guards'; \
	readelf -n "${PKGLIB}/orioledb.so" | grep -q 'Build ID'; \
	ldconfig; \
	postgres --version; \
	initdb --version

# Keep an exact, source-only copy at the paths recorded in the binaries' DWARF
# data.  This stage can be cleaned without affecting the installed artifacts
# copied from builder below.
FROM builder AS debug-source

RUN set -eux; \
	cd /usr/src/postgresql; \
	make distclean; \
	rm -rf /usr/src/postgresql/contrib/orioledb/.git

# END Build Image

# BEGIN sim image

FROM ubuntu:${UBUNTU_VERSION} AS runtime

ARG UBUNTU_VERSION

ENV LANG=en_US.utf8

# Explicitly set the postgres user and group IDs.
RUN set -eux; \
	groupadd -r postgres --gid=999; \
	useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres; \
	mkdir -p /var/lib/postgresql; \
	chown -R postgres:postgres /var/lib/postgresql

RUN set -eux; \
	apt-get update; \
	DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		binutils \
		ca-certificates \
		file \
		gdb \
		gnupg \
		libc6-dbg \
		locales \
		lsof \
		procps \
		strace \
		tzdata \
		\
		# Ensures compatibility with the official PostgreSQL Docker image
		libnss-wrapper \
		xz-utils \
		zstd \
		\
		libcurl4t64 \
		libicu74 \
		liblz4-1 \
		libreadline8t64 \
		libssl3t64 \
		libuuid1 \
		libxml2 \
		libxslt1.1 \
		libzstd1 \
	; \
	echo 'en_US.UTF-8 UTF-8' >> /etc/locale.gen; \
	locale-gen; \
	locale -a | grep 'en_US.utf8'; \
    \
	rm -rf /var/lib/apt/lists/*; \
	apt-get clean

# grab gosu for easy step-down from root
# https://github.com/tianon/gosu/releases
ENV GOSU_VERSION=1.19
RUN set -eux; \
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get update; \
	apt-get install -y --no-install-recommends ca-certificates wget; \
	rm -rf /var/lib/apt/lists/*; \
	dpkgArch="$(dpkg --print-architecture | awk -F- '{ print $NF }')"; \
	wget -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch"; \
	wget -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$dpkgArch.asc"; \
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --batch --keyserver hkps://keys.openpgp.org --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4; \
	gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
	gpgconf --kill all; \
	rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
	apt-mark auto '.*' > /dev/null; \
	[ -z "$savedAptMark" ] || apt-mark manual $savedAptMark > /dev/null; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false; \
	chmod +x /usr/local/bin/gosu; \
	gosu --version; \
	gosu nobody true

COPY --from=builder /usr/local /usr/local
COPY --from=builder /opt/orioledb/manifest.out /opt/orioledb/manifest.out
COPY --from=debug-source /usr/src/postgresql /usr/src/postgresql

RUN set -eux; \
	ldconfig; \
	command -v gdb; \
	command -v lsof; \
	command -v pgrep; \
	command -v strace; \
	readelf --sections --wide /usr/local/bin/postgres | grep -q '[.]debug_info'; \
	readelf --sections --wide /usr/local/lib/postgresql/orioledb.so | grep -q '[.]debug_info'; \
	mkdir -p \
		/etc/postgresql \
		/var/run/postgresql \
		/docker-entrypoint-initdb.d \
		/docker-default-initdb.d; \
	chown -R postgres:postgres \
		/etc/postgresql \
		/var/run/postgresql \
		/docker-entrypoint-initdb.d \
		/docker-default-initdb.d; \
	chmod 700 /etc/postgresql; \
	chmod 2777 /var/run/postgresql

COPY --from=builder /usr/src/postgresql/contrib/orioledb/docker/init/postgresql.docker.conf /etc/postgresql/postgresql.conf
COPY --from=builder /usr/src/postgresql/contrib/orioledb/docker/init/docker-entrypoint.sh /usr/local/bin/
COPY --from=builder /usr/src/postgresql/contrib/orioledb/docker/init/default-orioledb.sh /docker-default-initdb.d/

RUN set -eux; \
	sed -i -e 's/su-exec/gosu/g' /usr/local/bin/docker-entrypoint.sh; \
	chown postgres:postgres /etc/postgresql/postgresql.conf; \
	mkdir /etc/postgresql/conf.d; \
	echo "include_dir 'conf.d'" >> /etc/postgresql/postgresql.conf; \
	chmod +x /usr/local/bin/docker-entrypoint.sh; \
	mkdir /symbols; \
	ln -s /usr/local/lib/postgresql/orioledb.so /symbols/orioledb.so

ENV PGDATA=/var/lib/postgresql/data
# NB this 777 will be replaced by 700 at runtime (allows semi-arbitrary "--user" values)
RUN mkdir -p "$PGDATA" && chown -R postgres:postgres "$PGDATA" && chmod 777 "$PGDATA"
VOLUME /var/lib/postgresql/data

ENV POSTGRES_INITDB_ARGS="--locale-provider=icu --icu-locale=en"

ENTRYPOINT ["docker-entrypoint.sh"]
STOPSIGNAL SIGINT
EXPOSE 5432
CMD ["postgres", "-D", "/etc/postgresql"]

# END sim image
