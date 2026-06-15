{ pkgs, config, ... }:

{
  packages = with pkgs; [
    # C toolchain (set CC=gcc or CC=clang to choose)
    gcc
    clang

    # Build system
    gnumake
    pkg-config
    flex
    bison

    # postgres build dependencies
    readline
    icu
    icu.dev
    zstd
    zstd.dev
    openssl
    openssl.dev
    curl
    curl.dev

    # TAP test runtime
    perl
    perlPackages.IPCRun

    # Development tools
    gdb
    lcov
    git
  ];

  languages.python = {
    enable = true;
    venv = {
      enable = true;
      requirements = ./requirements.txt;
    };
  };

  env = {
    PG_INSTALL_ROOT = "${config.devenv.root}/.pg-install";
    PG_SRC_ROOT = "${config.devenv.root}/.pg-src";
    PG_DATA_ROOT = "${config.devenv.root}/.pg-data";
    PGTAGS_FILE = "${config.devenv.root}/.pgtags";
  };

  # Add the selected postgres bin to PATH when it is already built.
  enterShell = ''
    _pgmajor=''${PG_MAJOR:-18}
    _pgbin="$PG_INSTALL_ROOT/$_pgmajor/bin"
    if [ -d "$_pgbin" ]; then
      export PATH="$_pgbin:$PATH"
    else
      echo "postgres $_pgmajor not built — run: devenv tasks run orioledb:build-pg"
    fi
    unset _pgmajor _pgbin
  '';

  tasks = {
    # -------------------------------------------------------------------- #
    # orioledb:build-pg                                                    #
    # Fetches the orioledb postgres fork at the revision from .pgtags and  #
    # builds it into .pg-install/<PG_MAJOR>/.                              #
    # Re-running is a no-op when the revision has not changed.             #
    # -------------------------------------------------------------------- #
    "orioledb:build-pg" = {
      description = "Fetch and build the orioledb postgres fork (PG_MAJOR=18 default)";
      exec = /* bash */ ''
        set -euo pipefail

        # Set variables

        PG_MAJOR=''${PG_MAJOR:-18}

        PGTAG=$(awk -v maj="''${PG_MAJOR}" -F': ' '$1 == maj {print $2}' "$PGTAGS_FILE")
        if [ -z "$PGTAG" ]; then
          echo "ERROR: PG_MAJOR=$PG_MAJOR not found in $PGTAGS_FILE"
          exit 1
        fi

        PG_SRC="$PG_SRC_ROOT/$PG_MAJOR"
        PG_INSTALL="$PG_INSTALL_ROOT/$PG_MAJOR"
        REV_FILE="$PG_SRC/.built-rev"

        if [ -f "$PG_INSTALL/bin/postgres" ] \
           && [ -f "$REV_FILE" ] \
           && [ "$(cat "$REV_FILE")" = "$PGTAG" ]; then
          echo "postgres $PG_MAJOR ($PGTAG) already up to date"
          exit 0
        fi

        # Read the conventional CC variable, defaulting to gcc
        PG_CC="''${PG_CC:-gcc}"
        if [ "$PG_CC" = "clang" ]; then
          PG_CC="${pkgs.clang}/bin/clang"
        else
          PG_CC="${pkgs.gcc}/bin/gcc"
        fi

        echo "CC $PG_CC"

        # Download PostgreSQL

        echo "fetching orioledb/postgres @ $PGTAG ..."
        rm -rf "$PG_SRC" "$PG_INSTALL"
        mkdir -p "$PG_SRC"
        curl -fsSL "https://github.com/orioledb/postgres/archive/$PGTAG.tar.gz" \
          | tar -xz --strip-components=1 -C "$PG_SRC"

        # Build PostgreSQL

        cd "$PG_SRC"

        ./configure \
          CC="$PG_CC" \
          --enable-debug \
          --enable-cassert \
          --enable-tap-tests \
          --with-icu \
          --prefix="$PG_INSTALL"

        # Tarball builds have no .git metadata, so configure can't derive this reliably.
        if printf "%s\n" "$PGTAG" | grep -Fq "patches''${PG_MAJOR}_"; then
          echo "ORIOLEDB_PATCHSET_VERSION = ''${PGTAG##*_}" >> src/Makefile.global
        else
          echo "ORIOLEDB_PATCHSET_VERSION = $PGTAG" >> src/Makefile.global
        fi

        make -j"$(nproc)" -s
        make install -s
        make -C contrib -j"$(nproc)" install -s

        if [ "$PG_MAJOR" = "17" ]; then
          make -C src/test/modules/injection_points install
        fi

        echo "$PGTAG" > "$REV_FILE"
        echo "postgres $PG_MAJOR built → $PG_INSTALL"
      '';
    };

    # -------------------------------------------------------------------- #
    # orioledb:build                                                       #
    # Builds and installs the orioledb extension into the postgres prefix. #
    # Requires orioledb:build-pg to have run first.                        #
    # -------------------------------------------------------------------- #
    "orioledb:build" = {
      description = "Build and install the orioledb extension (PG_MAJOR=18 default)";
      after = [ "orioledb:build-pg" ];
      exec = /* bash */ ''
        set -euo pipefail
        PG_MAJOR=''${PG_MAJOR:-18}
        PG_BIN="$PG_INSTALL_ROOT/$PG_MAJOR/bin"

        export PATH="$PG_BIN:$PATH"
        make -j"$(nproc)" USE_PGXS=1 IS_DEV=1
        make USE_PGXS=1 IS_DEV=1 install
      '';
    };

    # -------------------------------------------------------------------- #
    # orioledb:initdb                                                      #
    # Creates a fresh data directory at .pg-data/<PG_MAJOR>/ with the       #
    # settings required to use orioledb as the default table AM.           #
    # -------------------------------------------------------------------- #
    "orioledb:initdb" = {
      description = "Initialise PGDATA with orioledb settings (PG_MAJOR=18 default)";
      after = [ "orioledb:build" ];
      before = [ "devenv:enterShell" ];
      exec = /* bash */ ''
        set -euo pipefail
        PG_MAJOR=''${PG_MAJOR:-18}
        PG_BIN="$PG_INSTALL_ROOT/$PG_MAJOR/bin"
        PGDATA="$PG_DATA_ROOT/$PG_MAJOR"

        if [ -d "$PGDATA" ]; then
          echo "data directory $PGDATA already exists, skipping"
          exit 0
        fi

        export PATH="$PG_BIN:$PATH"
        initdb -N --encoding=UTF-8 --locale-provider=icu --icu-locale=en-US -D "$PGDATA"

        {
          echo "shared_preload_libraries = 'orioledb'"
          echo "default_table_access_method = 'orioledb'"
          echo "wal_level = logical"
        } >> "$PGDATA/postgresql.conf"

        echo "initialised $PGDATA"
      '';
    };
  };

  # Started by 'devenv up'. Run 'devenv tasks run orioledb:initdb' first to
  # build postgres, build the extension, and initialise PGDATA.
  processes.postgres.exec = ''
    set -euo pipefail
    PG_MAJOR=''${PG_MAJOR:-18}
    PG_BIN="$PG_INSTALL_ROOT/$PG_MAJOR/bin"
    PGDATA="$PG_DATA_ROOT/$PG_MAJOR"

    exec "$PG_BIN/postgres" -D "$PGDATA"
  '';
}
