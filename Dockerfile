# This is slightly adjusted Dockerfile from
# https://github.com/docker-library/postgres

# set ALPINE_VERSION= [ edge 3.18 3.17 3.16 3.15 3.14 3.13 ]
ARG ALPINE_VERSION=3.17
FROM alpine:${ALPINE_VERSION}

ARG ALPINE_VERSION
# Set PG_MAJOR = [16 15 14 13 ]
ARG PG_MAJOR=14
ENV PG_MAJOR ${PG_MAJOR}

# set compiler: [ clang gcc ]
ARG BUILD_CC_COMPILER=clang
ENV BUILD_CC_COMPILER ${BUILD_CC_COMPILER}

# Define build dependencies for LLVM [ llvm-dev clang ]
# These include the specific versions of 'llvm-dev' and 'clang' suitable for the current version of PostgreSQL.
# They are useful for building downstream extensions using the same LLVM, like PostGIS alpine https://github.com/postgis/docker-postgis
# Note: PostgreSQL does not support LLVM 16. Therefore, for Alpine >=3.18, please use "llvm15-dev clang15".
# Reference: https://github.com/docker-library/postgres/pull/1077
ARG DOCKER_PG_LLVM_DEPS="llvm-dev clang"
ENV DOCKER_PG_LLVM_DEPS ${DOCKER_PG_LLVM_DEPS}

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
ENV LANG en_US.utf8

RUN mkdir -p /usr/src/postgresql/contrib/orioledb

COPY . /usr/src/postgresql/contrib/orioledb

RUN mkdir /docker-entrypoint-initdb.d

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
		3.16 | 3.17 | 3.18 | 3.19* ) EXTRA_ICU_PACKAGES=icu-data-full ;; \
		*) : ;; \
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
# temporary not allowing LLVM 16 to be used
# reason: we can't overwrite and fix the DOCKER_PG_LLVM_DEPS
# and the future downstream extensions like PostGIS need a correct build information (DOCKER_PG_LLVM_DEPS)
	if \
	# if the custom llvm version is set ( via DOCKER_PG_LLVM_DEPS ), and it is 16, then halt operation
		( [ ! -z "${CUSTOM_LLVM_VERSION}" ] && [ "$CUSTOM_LLVM_VERSION" == "16" ] ) \
	# or - if the custom llvm version is not set, and the Alpine version is >=3.18, then halt operation
	  ||  ( [ -z "${CUSTOM_LLVM_VERSION}" ] && ( [ "$ALPINE_VERSION" == "3.18" ] || [ "$ALPINE_VERSION" == "3.19" ]) ) \
	  ;  then \
			set +x ; \	  
			echo "------------------------------" ; \
			echo "Error: The LLVM 16 is not compatible with the current PostgreSQL! Halting operation." ; \
			echo "Suggested workarounds: use --build-arg DOCKER_PG_LLVM_DEPS='llvm15-dev clang15' " ; \
			exit 1; \
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
# update "DEFAULT_PGSOCKET_DIR" to "/var/run/postgresql" (matching Debian)
# see https://anonscm.debian.org/git/pkg-postgresql/postgresql.git/tree/debian/patches/51-default-sockets-in-var.patch?id=8b539fcb3e093a521c095e70bdfa76887217b89f
	awk '$1 == "#define" && $2 == "DEFAULT_PGSOCKET_DIR" && $3 == "\"/tmp\"" { $3 = "\"/var/run/postgresql\""; print; next } { print }' src/include/pg_config_manual.h > src/include/pg_config_manual.h.new; \
	grep '/var/run/postgresql' src/include/pg_config_manual.h.new; \
	mv src/include/pg_config_manual.h.new src/include/pg_config_manual.h; \
	gnuArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"; \
# explicitly update autoconf config.guess and config.sub so they support more arches/libcs
	wget -O config/config.guess 'https://git.savannah.gnu.org/cgit/config.git/plain/config.guess?id=7d3d27baf8107b630586c962c057e22149653deb'; \
	wget -O config/config.sub 'https://git.savannah.gnu.org/cgit/config.git/plain/config.sub?id=7d3d27baf8107b630586c962c057e22149653deb'; \
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
		--with-extra-version=" ${ORIOLEDB_VERSION} PGTAG=${PGTAG} alpine:${ALPINE_VERSION}+${BUILD_CC_COMPILER} build:${ORIOLEDB_BUILDTIME}" \
	|| cat config.log ); \
	echo "ORIOLEDB_PATCHSET_VERSION = `echo $PGTAG | cut -d'_' -f2`" >> src/Makefile.global; \
	make -j "$(nproc)"; \
	make -C contrib -j "$(nproc)"; \
	make -C contrib/orioledb -j "$(nproc)"; \
	make install; \
	make -C contrib install; \
	make -C contrib/orioledb install; \
	\
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
		bash \
		su-exec \
# tzdata is optional, but only adds around 1Mb to image size and is recommended by Django documentation:
# https://docs.djangoproject.com/en/1.10/ref/databases/#optimizing-postgresql-s-configuration
		tzdata \
# install extra icu packages ( >=Alpine3.16 )
		$EXTRA_ICU_PACKAGES \
	; \
	apk del --no-network .build-deps; \
	cd /; \
	rm -rf \
		/usr/src/postgresql \
		/usr/local/share/doc \
		/usr/local/share/man \
	; \
	\
	postgres --version

# make the sample config easier to munge (and "correct by default")
RUN set -eux; \
	cp -v /usr/local/share/postgresql/postgresql.conf.sample /usr/local/share/postgresql/postgresql.conf.sample.orig; \
	sed -ri "s!^#?(listen_addresses)\s*=\s*\S+.*!\1 = '*'!" /usr/local/share/postgresql/postgresql.conf.sample; \
	echo "shared_preload_libraries = 'orioledb'" >>  /usr/local/share/postgresql/postgresql.conf.sample; \
	echo "orioledb.main_buffers = 512MB" >>  /usr/local/share/postgresql/postgresql.conf.sample; \
	echo "orioledb.undo_buffers = 256MB" >>  /usr/local/share/postgresql/postgresql.conf.sample; \
	echo "max_wal_size=8GB" >>  /usr/local/share/postgresql/postgresql.conf.sample; \
	grep -F "listen_addresses = '*'" /usr/local/share/postgresql/postgresql.conf.sample

RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 2777 /var/run/postgresql

ENV PGDATA /var/lib/postgresql/data
# this 777 will be replaced by 700 at runtime (allows semi-arbitrary "--user" values)
RUN mkdir -p "$PGDATA" && chown -R postgres:postgres "$PGDATA" && chmod 777 "$PGDATA"
VOLUME /var/lib/postgresql/data

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
CMD ["postgres"]
