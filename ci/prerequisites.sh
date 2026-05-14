#!/bin/bash

set -eu

# print the hostname to be able to identify runner by logs
echo "HOSTNAME=`hostname`"
TIMESTAMP=$(date +%s)
echo "TIMESTAMP=$TIMESTAMP" >> $GITHUB_ENV
echo "TIMESTAMP=$TIMESTAMP"

# Disable background apt tasks
sudo systemctl stop --now apt-daily{,-upgrade}.service apt-daily{,-upgrade}.timer || true
sudo systemctl disable apt-daily{,-upgrade}.timer || true
sudo systemctl mask apt-daily{,-upgrade}.service || true
sudo systemctl stop --now unattended-upgrades || true

# Wait for locks to be released
while sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; do
	echo "apt is busy, waiting..."
	sleep 3
done
while sudo fuser /var/lib/dpkg/lock >/dev/null 2>&1; do
	echo "dpkg is busy, waiting..."
	sleep 3
done

sudo apt-get -y install -qq wget ca-certificates

sudo apt-get update -qq

apt_packages="build-essential flex bison pkg-config libreadline-dev make gdb libipc-run-perl libicu-dev python3-full python3-pip python3-setuptools python3-testresources libzstd1 libzstd-dev libcurl4-openssl-dev libssl-dev lcov"

if [ $COMPILER = "clang" ]; then
	apt_packages="$apt_packages llvm-$LLVM_VER clang-$LLVM_VER clang-tools-$LLVM_VER"
fi

if [ $CHECK_TYPE = "static" ] || [ $COMPILER = "gcc" ]; then
	apt_packages="$apt_packages cppcheck"
fi

if [ $CHECK_TYPE = "valgrind_1" ] || [ $CHECK_TYPE = "valgrind_2" ]; then
	apt_packages="$apt_packages valgrind"
fi

if [ $CHECK_TYPE = "dm_log_writes" ]; then
	apt_packages="$apt_packages e2fsprogs"
fi

# install required packages
sudo apt-get -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" -y install -qq $apt_packages

if [ $CHECK_TYPE = "dm_log_writes" ]; then
	# dm-log-writes is built into the CI runner kernels; abort early if a
	# future kernel update drops it instead of silently skipping.
	if ! sudo dmsetup targets 2>/dev/null | grep -q "log-writes"; then
		echo "ERROR: dm-log-writes DM target is not available in kernel $(uname -r)."
		exit 1
	fi

	# Fetch and build replay-log from josefbacik/log-writes pinned to a
	# known-good revision.  The repo is tiny (3 files) so we wget them
	# directly rather than cloning.
	LOG_WRITES_REV=7b70d8a6863c5de30933d42a7672d35d01d2dc6c
	LOG_WRITES_URL="https://raw.githubusercontent.com/josefbacik/log-writes/$LOG_WRITES_REV"
	TMPLW=$(mktemp -d)
	wget -q -O "$TMPLW/log-writes.h" "$LOG_WRITES_URL/log-writes.h"
	wget -q -O "$TMPLW/log-writes.c" "$LOG_WRITES_URL/log-writes.c"
	wget -q -O "$TMPLW/replay-log.c" "$LOG_WRITES_URL/replay-log.c"
	sudo gcc -O2 -I "$TMPLW" -o /usr/local/bin/replay-log "$TMPLW/replay-log.c" "$TMPLW/log-writes.c"
	rm -rf "$TMPLW"
fi
