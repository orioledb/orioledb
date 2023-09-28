# OrioleDB development quickstart

## Linux

### Install prerequisites

```bash
sudo apt-get update
sudo apt install git build-essential flex bison pkg-config libreadline-dev make gdb libipc-run-perl libicu-dev python3 python3-dev python3-pip python3-setuptools python3-testresources libzstd1 libzstd-dev valgrind
```

### Download and install PostgreSQL 15 with patches
```bash
git clone https://github.com/orioledb/postgres.git --branch patches15 --single-branch postgres-patches15
cd postgres-patches15/
```

### Checkout to required patch tag:
Check required postgres patch version in [.pgtags](../.pgtags) or [README.md](../README.md#build-from-source) files. Because documentation can become outdated.
```bash
git checkout patches15_18
```

### Enable Valgrind support in PostgreSQL code (optional)
```bash
sed -i.bak "s/\/\* #define USE_VALGRIND \*\//#define USE_VALGRIND/g" src/include/pg_config_manual.h
```

### Configure and build
```bash
PG_PREFIX=$HOME/pg15
./configure --enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=$PG_PREFIX
make -j$(nproc)
make -j$(nproc) install
echo "export PATH=\"$PG_PREFIX/bin:\$PATH\"" >> ~/.bashrc
source ~/.bashrc
```

### Install python requirements
```bash
pip3 install psycopg2 six testgres
sudo pip3 install compiledb
```

### Download and build the OrioleDB extension
```bash
cd ..
git clone https://github.com/orioledb/orioledb.git
cd orioledb
# Build with compiledb, because it creates compile_commands.json needed for VSCode C/C++ extension
compiledb make USE_PGXS=1
# Exclude compile_commands.json from the Git tracking
echo "compile_commands.json" >> .git/info/exclude
```

### Download and install Visual Studio Code
```bash
cd ..
wget --content-disposition "https://code.visualstudio.com/sha/download?build=stable&os=linux-deb-x64"
sudo apt install ./code_*.deb
# Install Python and C++ extension
code --install-extension ms-python.python
code --install-extension ms-vscode.cpptools
code orioledb
```

### Check installation

#### Run automated tests
```bash
cd orioledb
make USE_PGXS=1 installcheck
```

#### Manual installation and running
```bash
cd orioledb
make USE_PGXS=1 install
initdb --no-locale -D $HOME/pgdata
sed -i 's/#shared_preload_libraries = '\'''\''/shared_preload_libraries = '\''orioledb'\''/' $HOME/pgdata/postgresql.conf
pg_ctl -D $HOME/pgdata/ start -l $HOME/log
psql -c "CREATE EXTENSION IF NOT EXISTS orioledb; SELECT orioledb_commit_hash();" -d postgres
```

# MacOS

### Disable System Integrity Protection
Follow [the instruction to disable System Integrity Protection](
http://osxdaily.com/2015/10/05/disable-rootless-system-integrity-protection-mac-os-x/).

### Install Homebrew
```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Install prerequisites
```bash
brew install python zstd pkg-config icu4c openssl wget gnu-sed
sudo cpan IPC::Run
echo "export PKG_CONFIG_PATH=\"\$PKG_CONFIG_PATH:/usr/local/opt/icu4c/lib/pkgconfig\"" >> ~/.zshrc
echo "export CFLAGS=\"$CFLAGS -I/usr/local/include\"" >> ~/.zshrc
echo "export LDFLAGS=\"$LDFLAGS -L/usr/local/lib\"" >> ~/.zshrc

exec zsh -l
```

### Download and install PostgreSQL 15 with patches
```bash
git clone https://github.com/orioledb/postgres.git --branch patches15 --single-branch postgres-patches15
cd postgres-patches15/
```

### Checkout to required patch tag:
Check required postgres patch version in [.pgtags](../.pgtags) or [README.md](../README.md#build-from-source) files. Because documentation can become outdated.
```bash
git checkout patches15_18
```

### Configure and build
```bash
PG_PREFIX=$HOME/pg15
./configure --enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=$PG_PREFIX
make -j$(nproc)
make -j$(nproc) install
echo "export PATH=\"$PG_PREFIX/bin:\$PATH\"" >> ~/.zshrc
exec zsh -l
```

### Install python requirements
```bash
pip3 install psycopg2 six testgres
sudo pip3 install compiledb
```

### Download and build the OrioleDB extension
```bash
cd ..
git clone https://github.com/orioledb/orioledb.git
cd orioledb
# Build with compiledb, because it creates compile_commands.json needed for VSCode C/C++ extension
compiledb make USE_PGXS=1
# Exclude compile_commands.json from the Git tracking
echo "compile_commands.json" >> .git/info/exclude
```

### Download and install Visual Studio Code
```bash
cd ..
brew install --cask visual-studio-code
exec zsh -l
# Install Python and C++ extension
code --install-extension ms-python.python
code --install-extension ms-vscode.cpptools
code orioledb
```

### Check installation

#### Run automated tests
```bash
cd orioledb
make USE_PGXS=1 installcheck
```

#### Manual installation and running
```bash
cd orioledb
make USE_PGXS=1 install
initdb --no-locale -D $HOME/pgdata
gsed -i 's/#shared_preload_libraries = '\'''\''/shared_preload_libraries = '\''orioledb'\''/' $HOME/pgdata/postgresql.conf
pg_ctl -D $HOME/pgdata/ start -l $HOME/log
psql -c "CREATE EXTENSION IF NOT EXISTS orioledb; SELECT orioledb_commit_hash();" -d postgres
```

## Windows

### Install ubuntu in WSL

```bat
wsl --install -d Ubuntu
```

Then reboot, start Ubuntu from start menu, and choose login/password.

```bat
wsl --shutdown
```

Start Ubuntu from start menu again.

### Install prerequisites

```bash
sudo hwclock --hctosys
sudo apt-get update
sudo apt install git build-essential flex bison pkg-config libreadline-dev make gdb libipc-run-perl libicu-dev python3 python3-dev python3-pip python3-setuptools python3-testresources libzstd1 libzstd-dev valgrind
```

### Download and install PostgreSQL 15 with patches
```bash
git clone https://github.com/orioledb/postgres.git --branch patches15 --single-branch postgres-patches15
cd postgres-patches15/
```

### Checkout to required patch tag:
Check required postgres patch version in [.pgtags](../.pgtags) or [README.md](../README.md#build-from-source) files. Because documentation can become outdated.
```bash
git checkout patches15_18
```

### Enable Valgrind support in PostgreSQL code (optional)
```bash
sed -i.bak "s/\/\* #define USE_VALGRIND \*\//#define USE_VALGRIND/g" src/include/pg_config_manual.h
```

### Configure and build
```bash
PG_PREFIX=$HOME/pg15
./configure --enable-debug --enable-cassert --enable-tap-tests --with-icu --prefix=$PG_PREFIX
make -j$(nproc)
make -j$(nproc) install
echo "export PATH=\"$PG_PREFIX/bin:\$PATH\"" >> ~/.bashrc
source ~/.bashrc
```

### Install python requirements
```bash
pip3 install psycopg2 six testgres
sudo pip3 install compiledb
```

### Download and build the OrioleDB extension
```bash
cd ..
git clone https://github.com/orioledb/orioledb.git
cd orioledb
# Build with compiledb, because it creates compile_commands.json needed for VSCode C/C++ extension
compiledb make USE_PGXS=1
# Exclude compile_commands.json from the Git tracking
echo "compile_commands.json" >> .git/info/exclude
```

### Download and install Visual Studio Code

https://code.visualstudio.com/sha/download?build=stable&os=win32-x64-user

### Install Python and C++ VSCode extensions
```bat
code --install-extension ms-vscode-remote.remote-wsl
code --remote wsl+ubuntu /home/USERNAME/orioledb
```

In VSCode terminal:
```bash
code --install-extension ms-python.python
code --install-extension ms-vscode.cpptools
```

### Check installation

#### Run automated tests
```bash
make USE_PGXS=1 installcheck
```

#### Manual installation and running
```bash
make USE_PGXS=1 install
initdb --no-locale -D $HOME/pgdata
sed -i 's/#shared_preload_libraries = '\'''\''/shared_preload_libraries = '\''orioledb'\''/' $HOME/pgdata/postgresql.conf
pg_ctl -D $HOME/pgdata/ start -l $HOME/log
psql -c "CREATE EXTENSION IF NOT EXISTS orioledb; SELECT orioledb_commit_hash();" -d postgres
```




