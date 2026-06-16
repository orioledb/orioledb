{
  description = "OrioleDB PostgreSQL extension — Nix development shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

  outputs = { self, nixpkgs }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
      forEachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      devShells = forEachSystem (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          lib = pkgs.lib;

          # ---------------------------------------------------------------- #
          # orioledb-build-pg                                                #
          # Fetches the orioledb postgres fork at the revision from .pgtags  #
          # and builds it into .pg-install/<PG_MAJOR>/.                      #
          # Re-running is a no-op when the revision has not changed.         #
          # ---------------------------------------------------------------- #
          orioledbBuildPg = pkgs.writeShellScriptBin "orioledb-build-pg" ''
            set -euo pipefail

            PG_MAJOR=''${PG_MAJOR:-18}

            PGTAG=$(awk -v maj="$PG_MAJOR" -F': ' '$1 == maj {print $2}' "$PGTAGS_FILE")
            if [ -z "$PGTAG" ]; then
              echo "ERROR: PG_MAJOR=$PG_MAJOR not found in $PGTAGS_FILE" >&2
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

            PG_CC="''${PG_CC:-gcc}"
            if [ "$PG_CC" = "clang" ]; then
              PG_CC="${pkgs.clang}/bin/clang"
            else
              PG_CC="${pkgs.gcc}/bin/gcc"
            fi

            echo "fetching orioledb/postgres @ $PGTAG ..."
            rm -rf "$PG_SRC" "$PG_INSTALL"
            mkdir -p "$PG_SRC"
            curl -fsSL "https://github.com/orioledb/postgres/archive/$PGTAG.tar.gz" \
              | tar -xz --strip-components=1 -C "$PG_SRC"

            cd "$PG_SRC"

            ./configure \
              CC="$PG_CC" \
              --enable-debug \
              --enable-cassert \
              --enable-tap-tests \
              --with-icu \
              --prefix="$PG_INSTALL"

            # Tarball builds have no .git metadata; set the patchset version manually.
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

          # ---------------------------------------------------------------- #
          # orioledb-build                                                   #
          # Builds and installs the orioledb extension into the PG prefix.  #
          # Requires orioledb-build-pg to have run first.                   #
          # ---------------------------------------------------------------- #
          orioledbBuild = pkgs.writeShellScriptBin "orioledb-build" ''
            set -euo pipefail
            PG_MAJOR=''${PG_MAJOR:-18}
            export PATH="$PG_INSTALL_ROOT/$PG_MAJOR/bin:$PATH"
            make -j"$(nproc)" USE_PGXS=1 IS_DEV=1
            make USE_PGXS=1 IS_DEV=1 install
          '';

          # ---------------------------------------------------------------- #
          # orioledb-initdb                                                  #
          # Creates a fresh data directory at .pg-data/<PG_MAJOR>/ with the  #
          # settings required to use orioledb as the default table AM.       #
          # ---------------------------------------------------------------- #
          orioledbInitdb = pkgs.writeShellScriptBin "orioledb-initdb" ''
            set -euo pipefail
            PG_MAJOR=''${PG_MAJOR:-18}
            PGDATA="$PG_DATA_ROOT/$PG_MAJOR"

            if [ -d "$PGDATA" ]; then
              echo "data directory $PGDATA already exists, skipping"
              exit 0
            fi

            export PATH="$PG_INSTALL_ROOT/$PG_MAJOR/bin:$PATH"
            initdb -N --encoding=UTF-8 --locale-provider=icu --icu-locale=en-US -D "$PGDATA"

            {
              echo "shared_preload_libraries = 'orioledb'"
              echo "default_table_access_method = 'orioledb'"
              echo "wal_level = logical"
            } >> "$PGDATA/postgresql.conf"

            echo "initialised $PGDATA"
          '';

          # ---------------------------------------------------------------- #
          # orioledb-start                                                   #
          # Starts the postgres server for the selected PG_MAJOR.           #
          # ---------------------------------------------------------------- #
          orioledbStart = pkgs.writeShellScriptBin "orioledb-start" ''
            set -euo pipefail
            PG_MAJOR=''${PG_MAJOR:-18}
            exec "$PG_INSTALL_ROOT/$PG_MAJOR/bin/postgres" -D "$PG_DATA_ROOT/$PG_MAJOR"
          '';

        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              # C toolchain (set PG_CC=clang to use clang instead of gcc)
              gcc
              clang

              # Build system
              gnumake
              pkg-config
              flex
              bison
              coreutils

              # PostgreSQL build dependencies
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
              python3

              # OrioleDB helper scripts
              orioledbBuildPg
              orioledbBuild
              orioledbInitdb
              orioledbStart
            ];

            shellHook = ''
              export ORIOLEDB_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
              export PG_INSTALL_ROOT="$ORIOLEDB_ROOT/.pg-install"
              export PG_SRC_ROOT="$ORIOLEDB_ROOT/.pg-src"
              export PG_DATA_ROOT="$ORIOLEDB_ROOT/.pg-data"
              export PGTAGS_FILE="$ORIOLEDB_ROOT/.pgtags"

              # Build postgres, the extension, and initialise PGDATA on first entry.
              # Each step is idempotent — subsequent entries are fast no-ops.
              orioledb-build-pg
              orioledb-build
              orioledb-initdb

              export PATH="$PG_INSTALL_ROOT/''${PG_MAJOR:-18}/bin:$PATH"

              if [ ! -d "$ORIOLEDB_ROOT/.venv" ]; then
                python3 -m venv "$ORIOLEDB_ROOT/.venv"
                "$ORIOLEDB_ROOT/.venv/bin/pip" install --quiet -r "$ORIOLEDB_ROOT/requirements.txt"
              fi
              export VIRTUAL_ENV="$ORIOLEDB_ROOT/.venv"
              export PATH="$VIRTUAL_ENV/bin:$PATH"
            '';
          };
        }
      );
    };
}
