# This is slightly adjusted Dockerfile from
# https://github.com/docker-library/postgres

# Base image configuration
# BASE_IMAGE=alpine
# BASE_VERSION= [ edge 3.20 3.19 3.18 3.17 3.16 3.15 3.14 3.13]
# New recommended way:
#   docker build --build-arg BASE_IMAGE=alpine --build-arg BASE_VERSION=3.20 -t orioletest:alpine-3.20 .
ARG BASE_IMAGE=alpine
ARG BASE_VERSION=3.20

# Legacy support (deprecated, will be removed):
#   docker build --build-arg ALPINE_VERSION=3.20 -t orioletest:ubuntu-noble .
ARG ALPINE_VERSION
# If ALPINE_VERSION is set, override BASE_IMAGE/VERSION for backwards compatibility
ARG BASE_IMAGE=${ALPINE_VERSION:+alpine}
ARG BASE_VERSION=${ALPINE_VERSION:+${ALPINE_VERSION}}

FROM ${BASE_IMAGE}:${BASE_VERSION}

# Re-declare ARGs after FROM for use in subsequent instructions
ARG BASE_IMAGE
ARG BASE_VERSION

# Record build information
ENV BUILD_OS="${BASE_IMAGE}:${BASE_VERSION}"

# Set PG_MAJOR = [ 17 16 ]
ARG PG_MAJOR=17
ENV PG_MAJOR=${PG_MAJOR}

# set compiler: [ clang gcc ]
ARG BUILD_CC_COMPILER=clang
ENV BUILD_CC_COMPILER=${BUILD_CC_COMPILER}

# Enable debug mode and preserve the build environments for debugging.
# In this case, each image size exceeds 1GB
ARG DEBUG_MODE=false
ENV DEBUG_MODE=${DEBUG_MODE}

# Define build dependencies for LLVM [ llvm-dev clang ]
# These include the specific versions of 'llvm-dev' and 'clang' suitable for the current version of PostgreSQL.
# They are useful for building downstream extensions using the same LLVM, like PostGIS alpine https://github.com/postgis/docker-postgis
# Note: Some older PostgreSQL version does not support LLVM 16. Therefore, for Alpine >=3.18, please use "llvm15-dev clang15".
# Reference: https://github.com/docker-library/postgres/pull/1077
ARG DOCKER_PG_LLVM_DEPS="llvm-dev clang"
ENV DOCKER_PG_LLVM_DEPS=${DOCKER_PG_LLVM_DEPS}

# 70 is the standard uid/gid for "postgres" in Alpine
# https://git.alpinelinux.org/aports/tree/main/postgresql/postgresql.pre-install?h=3.12-stable
RUN set -eux; \
	addgroup -g 70 -S postgres; \
	adduser -u 70 -S -D -G postgres -H -h /var/lib/postgresql -s /bin/sh postgres; \
	mkdir -p /var/lib/postgresql; \
	chown -R postgres:postgres /var/lib/postgresql

# su-exec (gosu-compatible) is installed further down

# make the "en_US.UTF-8" locale so postgres will be utf-8 enabled by default
# alpine doesn't require explicit locale-file generation
ENV LANG=en_US.utf8

RUN mkdir -p /usr/src/postgresql/contrib/orioledb

COPY . /usr/src/postgresql/contrib/orioledb

RUN mkdir /docker-entrypoint-initdb.d

RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 2777 /var/run/postgresql

RUN set -eux; \
	\
	PGTAG=$(grep "^$PG_MAJOR: " /usr/src/postgresql/contrib/orioledb/.pgtags | cut -d' ' -f2-) ; \
	ORIOLEDB_VERSION=$(grep "^#define ORIOLEDB_VERSION" /usr/src/postgresql/contrib/orioledb/include/orioledb.h | cut -d'"' -f2) ; \
	ORIOLEDB_BUILDTIME=$(date -Iseconds) ; \
	ALPINE_VERSION=$(cat /etc/os-release | grep VERSION_ID | cut -d = -f 2 | cut -d . -f 1,2 | cut -d _ -f 1) ; \
	\
	# To get support for all locales: IF >=Alpine3.16 THEN install icu-data-full
	# https://wiki.alpinelinux.org/wiki/Release_Notes_for_Alpine_3.16.0#ICU_data_split
	# https://github.com/docker-library/postgres/issues/327#issuecomment-1201582069
	case "$ALPINE_VERSION" in 3.13 | 3.14 | 3.15 )  EXTRA_ICU_PACKAGES='' ;; \
		3.16 | 3.17 | 3.18 | 3.19 | 3.20 | 3.21* )  EXTRA_ICU_PACKAGES=icu-data-full ;; \
		*) echo "Unsupported Alpine version: $ALPINE_VERSION" && exit 1 ;; \
	esac ; \
	\
	echo "PG_MAJOR=$PG_MAJOR" ; \
	echo "PGTAG=$PGTAG" ; \
	echo "BUILD_CC_COMPILER=$BUILD_CC_COMPILER" ; \
	echo "ORIOLEDB_VERSION=$ORIOLEDB_VERSION" ; \
	echo "ORIOLEDB_BUILDTIME=$ORIOLEDB_BUILDTIME" ; \
	echo "ALPINE_VERSION=$ALPINE_VERSION" ; \
	echo "EXTRA_ICU_PACKAGES=$EXTRA_ICU_PACKAGES" ; \
	echo "DOCKER_PG_LLVM_DEPS=$DOCKER_PG_LLVM_DEPS" ; \
	echo "DEBUG_MODE=$DEBUG_MODE" ; \
	\
	# check if the custom llvm version is set, and if so, set the LLVM_CONFIG and CLANG variables
	CUSTOM_LLVM_VERSION=$(echo "$DOCKER_PG_LLVM_DEPS" | sed -n 's/.*llvm\([0-9]*\).*/\1/p') ; \
	if [ ! -z "${CUSTOM_LLVM_VERSION}" ];  then \
		echo "CUSTOM_LLVM_VERSION=$CUSTOM_LLVM_VERSION" ; \
		export LLVM_CONFIG="/usr/lib/llvm${CUSTOM_LLVM_VERSION}/bin/llvm-config" ; \
		export CLANG=clang-${CUSTOM_LLVM_VERSION} ; \
		if [[ "${BUILD_CC_COMPILER}" == "clang" ]]; then \
			export BUILD_CC_COMPILER=clang-${CUSTOM_LLVM_VERSION} ; \
			echo "fix: BUILD_CC_COMPILER=clang-${CUSTOM_LLVM_VERSION}" ; \
		fi ; \
	fi ; \
	\
	apk add --no-cache --virtual .build-deps \
		${DOCKER_PG_LLVM_DEPS} \
		bison \
		coreutils \
		curl \
		dpkg-dev dpkg \
		flex \
		g++ \
		gcc \
		krb5-dev \
		libc-dev \
		libedit-dev \
		libxml2-dev \
		libxslt-dev \
		linux-headers \
# needed for s3 support
		curl-dev \
		make \
		openldap-dev \
		openssl-dev \
# configure: error: prove not found
		perl-utils \
# configure: error: Perl module IPC::Run is required to run TAP tests
		perl-ipc-run \
		perl-dev \
		python3 \
		python3-dev \
		tcl-dev \
		util-linux-dev \
		zlib-dev \
		zstd-dev \
# https://www.postgresql.org/docs/10/static/release-10.html#id-1.11.6.9.5.13
		icu-dev \
# https://www.postgresql.org/docs/14/release-14.html#id-1.11.6.5.5.3.7
		lz4-dev \
	; \
	\
	curl -o postgresql.tar.gz \
			--header "Accept: application/vnd.github.v3.raw" \
			--remote-name \
			--location https://github.com/orioledb/postgres/tarball/$PGTAG; \
	mkdir -p /usr/src/postgresql; \
	tar \
		--extract \
		--file postgresql.tar.gz \
		--directory /usr/src/postgresql \
		--strip-components 1 \
	; \
	rm postgresql.tar.gz; \
	\
	cd /usr/src/postgresql; \
	\
	POSTGRESQL_VERSION=$(grep "PACKAGE_VERSION=" ./configure | cut -d"'" -f2) ; \
	echo "POSTGRESQL_VERSION=$POSTGRESQL_VERSION" ; \
	\
# update "DEFAULT_PGSOCKET_DIR" to "/var/run/postgresql" (matching Debian)
# see https://anonscm.debian.org/git/pkg-postgresql/postgresql.git/tree/debian/patches/51-default-sockets-in-var.patch?id=8b539fcb3e093a521c095e70bdfa76887217b89f
	awk '$1 == "#define" && $2 == "DEFAULT_PGSOCKET_DIR" && $3 == "\"/tmp\"" { $3 = "\"/var/run/postgresql\""; print; next } { print }' src/include/pg_config_manual.h > src/include/pg_config_manual.h.new; \
	grep '/var/run/postgresql' src/include/pg_config_manual.h.new; \
	mv src/include/pg_config_manual.h.new src/include/pg_config_manual.h; \
	gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
# explicitly update autoconf config.guess and config.sub so they support more arches/libcs
	wget --tries=10 --timeout=60 -O config/config.guess 'https://git.savannah.gnu.org/cgit/config.git/plain/config.guess?id=7d3d27baf8107b630586c962c057e22149653deb'; \
	wget --tries=10 --timeout=60 -O config/config.sub 'https://git.savannah.gnu.org/cgit/config.git/plain/config.sub?id=7d3d27baf8107b630586c962c057e22149653deb'; \
# configure options taken from:
# https://anonscm.debian.org/cgit/pkg-postgresql/postgresql.git/tree/debian/rules?h=9.5
	( CC=${BUILD_CC_COMPILER} ./configure \
		--build="$gnuArch" \
# "/usr/src/postgresql/src/backend/access/common/tupconvert.c:105: undefined reference to `libintl_gettext'"
#		--enable-nls \
		--enable-integer-datetimes \
		--enable-thread-safety \
		--enable-tap-tests \
# skip debugging info -- we want tiny size instead
#		--enable-debug \
		--disable-rpath \
		--with-uuid=e2fs \
		--with-gnu-ld \
		--with-pgport=5432 \
		--with-system-tzdata=/usr/share/zoneinfo \
		--prefix=/usr/local \
		--with-includes=/usr/local/include \
		--with-libraries=/usr/local/lib \
		--with-gssapi \
		--with-ldap \
		--with-tcl \
		--with-perl \
		--with-python \
#		--with-pam \
		--with-openssl \
		--with-libxml \
		--with-libxslt \
		--with-icu \
		--with-llvm \
		--with-lz4 \
		--with-zstd \
		# The "testgres" package expects the PostgreSQL version as the last word.
		# Therefore, the extra ${POSTGRESQL_VERSION} is added as a workaround.
		--with-extra-version=" ${ORIOLEDB_VERSION} PGTAG=${PGTAG} ${BUILD_OS}+${BUILD_CC_COMPILER} build:${ORIOLEDB_BUILDTIME} ${POSTGRESQL_VERSION}" \
	|| cat config.log ); \
	echo "ORIOLEDB_PATCHSET_VERSION = `echo $PGTAG | cut -d'_' -f2`" >> src/Makefile.global; \
	# install postgresql
	make -j "$(nproc)"; \
	make -C contrib -j "$(nproc)"; \
	make install; \
	make -C contrib install; \
	# install orioledb extension
	cd /usr/src/postgresql/contrib/orioledb; \
	make USE_PGXS=1 -j "$(nproc)"; \
	make USE_PGXS=1 install; \
	\
	# Clean up only if not in debug mode
	if [ "$DEBUG_MODE" != "true" ]; then \
		runDeps="$( \
			scanelf --needed --nobanner --format '%n#p' --recursive /usr/local \
				| tr ',' '\n' \
				| sort -u \
				| awk 'system("[ -e /usr/local/lib/" $1 " ]") == 0 { next } { print "so:" $1 }' \
				# Remove plperl, plpython and pltcl dependencies by default to save image size
				# To use the pl extensions, those have to be installed in a derived image
				| grep -v -e perl -e python -e tcl \
		)"; \
		apk add --no-cache --virtual .postgresql-rundeps \
			$runDeps \
		; \
		apk del --no-network .build-deps; \
		rm -rf \
			/usr/src/postgresql \
			/usr/local/share/doc \
			/usr/local/share/man \
			/tmp/* \
		; \
	fi ; \
	\
	apk add --no-cache \
		bash \
		su-exec \
		# tzdata is optional, but only adds around 1Mb to image size and is recommended by Django documentation:
		# https://docs.djangoproject.com/en/1.10/ref/databases/#optimizing-postgresql-s-configuration
		tzdata \
		# install extra icu packages ( >=Alpine3.16 )
		$EXTRA_ICU_PACKAGES \
	; \
	cd / ; \
	postgres --version ; \
	initdb --version

ENV PGDATA=/var/lib/postgresql/data
# this 777 will be replaced by 700 at runtime (allows semi-arbitrary "--user" values)
RUN mkdir -p "$PGDATA" && chown -R postgres:postgres "$PGDATA" && chmod 777 "$PGDATA"
VOLUME /var/lib/postgresql/data

RUN mkdir -p /etc/postgresql && chown -R postgres:postgres /etc/postgresql && chmod 700 /etc/postgresql
COPY --chown=postgres:postgres postgresql.docker.conf /etc/postgresql/postgresql.conf
ENV PG_CONF=/etc/postgresql/postgresql.conf

COPY docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]

# We set the default STOPSIGNAL to SIGINT, which corresponds to what PostgreSQL
# calls "Fast Shutdown mode" wherein new connections are disallowed and any
# in-progress transactions are aborted, allowing PostgreSQL to stop cleanly and
# flush tables to disk, which is the best compromise available to avoid data
# corruption.
#
# Users who know their applications do not keep open long-lived idle connections
# may way to use a value of SIGTERM instead, which corresponds to "Smart
# Shutdown mode" in which any existing sessions are allowed to finish and the
# server stops when all sessions are terminated.
#
# See https://www.postgresql.org/docs/12/server-shutdown.html for more details
# about available PostgreSQL server shutdown signals.
#
# See also https://www.postgresql.org/docs/12/server-start.html for further
# justification of this as the default value, namely that the example (and
# shipped) systemd service files use the "Fast Shutdown mode" for service
# termination.
#
STOPSIGNAL SIGINT
#
# An additional setting that is recommended for all users regardless of this
# value is the runtime "--stop-timeout" (or your orchestrator/runtime's
# equivalent) for controlling how long to wait between sending the defined
# STOPSIGNAL and sending SIGKILL (which is likely to cause data corruption).
#
# The default in most runtimes (such as Docker) is 10 seconds, and the
# documentation at https://www.postgresql.org/docs/12/server-start.html notes
# that even 90 seconds may not be long enough in many instances.

EXPOSE 5432
CMD ["postgres", "-D", "/etc/postgresql"]
