#!/usr/bin/env bash
# Copyright 2026 Xeondb. All rights reserved.

INSTALL_PREFIX="/usr/local/xeondb"
CONFIG_PATH="/etc/xeondb/settings.yml"
DATA_DIR="/var/lib/xeondb/data"
HOST="0.0.0.0"
PORT="9876"
FULL_BUILD=0
FORCE=0
ASSUME_YES=0
NO_START=0
DB_USER=""
DB_PASS=""
DB_PASS_ENV=""

REPO_URL="https://github.com/xeondb/Xeondb.git"
REPO_REF="main"
USE_LOCAL=0
KEEP_SOURCE=0

PROMPT_FD=""
PROMPT_AVAILABLE=0

UNIT_PATH="/etc/systemd/system/xeondb.service"

set -euo pipefail

onError() {
	local exitCode=$?
	echo "install.sh failed (exit=$exitCode) at line $LINENO: $BASH_COMMAND" >&2
	exit "$exitCode"
}
trap onError ERR

setupPrompts() {
	PROMPT_AVAILABLE=0
	PROMPT_FD=""
	if [ -r /dev/tty ] && [ -w /dev/tty ]; then
		exec 3</dev/tty
		PROMPT_FD=3
		PROMPT_AVAILABLE=1
	fi
}

promptRead() {
	local __var="$1"
	local __prompt="$2"
	local __val=""
	if [ "$PROMPT_AVAILABLE" -eq 1 ]; then
		printf "%s" "$__prompt" >/dev/tty
		read -r -u "$PROMPT_FD" __val
	else
		die "Non-interactive install requires --yes"
	fi
	printf -v "$__var" "%s" "$__val"
}

promptReadSecret() {
	local __var="$1"
	local __prompt="$2"
	local __val=""
	if [ "$PROMPT_AVAILABLE" -eq 1 ]; then
		printf "%s" "$__prompt" >/dev/tty
		read -r -s -u "$PROMPT_FD" __val
		printf "\n" >/dev/tty
	else
		die "Non-interactive install requires --yes"
	fi
	printf -v "$__var" "%s" "$__val"
}

promptConfirm() {
	local __q="$1"
	local __default_yes="$2"
	local __reply=""
	local __suffix="[y/N] "
	if [ "$__default_yes" -eq 1 ]; then
		__suffix="[Y/n] "
	fi
	if [ "$ASSUME_YES" -eq 1 ]; then
		return 0
	fi
	promptRead __reply "$__q $__suffix"
	case "$__reply" in
		y|Y|yes|YES)
			return 0
			;;
		n|N|no|NO)
			return 1
			;;
		"")
			[ "$__default_yes" -eq 1 ] && return 0 || return 1
			;;
		*)
			[ "$__default_yes" -eq 1 ] && return 0 || return 1
			;;
	esac
}

checkTimeSync() {
	echo "Checking system time synchronization..."
	ntp_active=0
	if command -v timedatectl >/dev/null 2>&1; then
		sync_status=$(timedatectl show --property=NTPSynchronized --value)
		if [ "$sync_status" = "yes" ]; then
			ntp_active=1
		fi
	fi
	if [ $ntp_active -eq 0 ]; then
		echo "Warning: System time synchronization (NTP/systemd-timesyncd) is not enabled."
		if [ "$ASSUME_YES" -eq 1 ] || [ "$PROMPT_AVAILABLE" -ne 1 ]; then
			echo "Proceeding without enabling time sync (non-interactive)."
			return 0
		fi
		if promptConfirm "Enable time synchronization now?" 1; then
			if [ "${EUID:-$(id -u)}" -ne 0 ]; then
				echo "Cannot enable time sync: not running as root. Please run with sudo or enable NTP manually."
			elif command -v timedatectl >/dev/null 2>&1; then
				echo "Enabling systemd-timesyncd..."
				timedatectl set-ntp true || echo "Failed to enable NTP. Please enable manually."
			else
				echo "Please install and enable NTP or systemd-timesyncd for reliable operation."
			fi
		else
			echo "Proceeding without time sync. This may cause issues in multi-node setups."
		fi
	else
		echo "System time synchronization is enabled."
	fi
}

die() {
	echo "Error: $*" >&2
	exit 1
}

installDep() {
	dep="$1"
	pkgname="$dep"
	if command -v apt-get >/dev/null 2>&1; then
		pkgmgr="apt-get"
		case "$dep" in
			ninja)
				pkgname="ninja-build" ;;
		esac
		installcmd="apt-get install -y $pkgname"
	elif command -v dnf >/dev/null 2>&1; then
		pkgmgr="dnf"
		installcmd="dnf install -y $dep"
	elif command -v yum >/dev/null 2>&1; then
		pkgmgr="yum"
		installcmd="yum install -y $dep"
	elif command -v pacman >/dev/null 2>&1; then
		pkgmgr="pacman"
		installcmd="pacman -Sy --noconfirm $dep"
	else
		die "Missing dependency: $dep (no supported package manager found)"
	fi
	echo "$dep is required but not installed."
	if [ "$ASSUME_YES" -eq 1 ]; then
		echo "Installing $pkgname..."
		$installcmd || die "Failed to install $pkgname"
		return 0
	fi
	if promptConfirm "Install $dep using $pkgmgr?" 1; then
		echo "Installing $pkgname..."
		$installcmd || die "Failed to install $pkgname"
	else
		die "Missing dependency: $dep"
	fi
}

needCmd() {
	if ! command -v "$1" >/dev/null 2>&1; then
		installDep "$1"
		command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1 (install failed)"
	fi
}

if [ -z "${BASH_VERSION:-}" ]; then
    die "Unsupported shell; please run with bash"
fi

printUsage() {
	cat <<'EOF'
Usage: install.sh [options]

Options:
  -h, --help              Show this help message and exit
  -d, --directory DIR     Install prefix (default: /usr/local/xeondb)
  --config PATH       Config path (default: /etc/xeondb/settings.yml)
  --data-dir PATH     Data directory (default: /var/lib/xeondb/data)
  --host HOST         Bind host (default: 0.0.0.0)
  --port PORT         Bind port (default: 9876)
  --full              Run full build (lint + tests) via Ninja target `build`
  --force             Overwrite existing config and unit files
  --yes               Skip confirmation prompt
  --no-start          Install unit but do not enable/start
  --repo URL          Git repo to build from (default: https://github.com/xeondb/Xeondb.git)
  --ref REF           Git ref to build (default: main)
  --use-local         Build from the local directory containing install.sh
  --keep-source       Do not delete the temporary source checkout
  --db-user USER      Enable auth (non-interactive) and set username
  --db-pass PASS      Enable auth (non-interactive) and set password (less safe; shows in process args)
  --db-pass-env NAME  Enable auth (non-interactive) and read password from env var NAME

Run as root:
  sudo ./install.sh
EOF
}

getScriptDir() {
	local src="${BASH_SOURCE[0]}"
	if [ -n "$src" ] && [ -f "$src" ]; then
		(cd "$(dirname "$src")" && pwd)
		return 0
	fi
	return 1
}

while [ $# -gt 0 ]; do
	case "$1" in
		-h|--help)
			printUsage
			exit 0
			;;
		-d|--directory)
			shift
			[ $# -gt 0 ] || die "Missing argument for --directory"
			INSTALL_PREFIX="$1"
			shift
			;;
		--config)
			shift
			[ $# -gt 0 ] || die "Missing argument for --config"
			CONFIG_PATH="$1"
			shift
			;;
		--data-dir)
			shift
			[ $# -gt 0 ] || die "Missing argument for --data-dir"
			DATA_DIR="$1"
			shift
			;;
		--host)
			shift
			[ $# -gt 0 ] || die "Missing argument for --host"
			HOST="$1"
			shift
			;;
		--port)
			shift
			[ $# -gt 0 ] || die "Missing argument for --port"
			PORT="$1"
			shift
			;;
		--full)
			FULL_BUILD=1
			shift
			;;
		--force)
			FORCE=1
			shift
			;;
		--yes)
			ASSUME_YES=1
			shift
			;;
		--no-start)
			NO_START=1
			shift
			;;
		--repo)
			shift
			[ $# -gt 0 ] || die "Missing argument for --repo"
			REPO_URL="$1"
			shift
			;;
		--ref)
			shift
			[ $# -gt 0 ] || die "Missing argument for --ref"
			REPO_REF="$1"
			shift
			;;
		--use-local)
			USE_LOCAL=1
			shift
			;;
		--keep-source)
			KEEP_SOURCE=1
			shift
			;;
		--db-user)
			shift
			[ $# -gt 0 ] || die "Missing argument for --db-user"
			DB_USER="$1"
			shift
			;;
		--db-pass)
			shift
			[ $# -gt 0 ] || die "Missing argument for --db-pass"
			DB_PASS="$1"
			shift
			;;
		--db-pass-env)
			shift
			[ $# -gt 0 ] || die "Missing argument for --db-pass-env"
			DB_PASS_ENV="$1"
			shift
			;;
		*)
			die "Unknown option: $1 (try --help)"
			;;
	esac
done

setupPrompts

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
	die "This installer must run as root. Try: curl -fsSL https://xeondb.com/install | sudo bash"
fi

if [ "$PROMPT_AVAILABLE" -ne 1 ] && [ "$ASSUME_YES" -ne 1 ]; then
	die "Non-interactive install requires --yes"
fi

checkTimeSync

needCmd cmake
needCmd ninja
needCmd g++
needCmd python3
needCmd git

if command -v systemctl >/dev/null 2>&1; then
	:
else
	echo "Warning: systemctl not found; will install files but not enable/start service." >&2
	NO_START=1
fi

confirmInstall() {
	echo
	echo "About to install Xeondb system-wide with these settings:"
	echo "  Install prefix: $INSTALL_PREFIX"
	echo "  Binary dest:    $INSTALL_PREFIX/bin/xeondb"
	echo "  Config path:    $CONFIG_PATH"
	echo "  Data dir:       $DATA_DIR"
	echo "  Host:           $HOST"
	echo "  Port:           $PORT"
	if [ "$USE_LOCAL" -eq 1 ]; then
		echo "  Source:         local"
	else
		echo "  Source:         $REPO_URL ($REPO_REF)"
	fi
	if [ "$FULL_BUILD" -eq 1 ]; then
		echo "  Build:          cmake -S <src> -B <src>/build -G Ninja && ninja -C <src>/build build (full)"
	else
		echo "  Build:          cmake -S <src> -B <src>/build -G Ninja && ninja -C <src>/build Xeondb"
	fi
	if [ "$NO_START" -eq 1 ]; then
		echo "  Service:        will NOT auto-start"
	else
		echo "  Service:        systemd enable --now xeondb"
	fi
	if [ "$FORCE" -eq 1 ]; then
		echo "  Overwrite:      yes (--force)"
	else
		echo "  Overwrite:      no"
	fi
	echo
	if promptConfirm "Continue?" 0; then
		return 0
	fi
	return 1
}

handleExistingInstall() {
	if { [ -e "$CONFIG_PATH" ] || [ -e "$UNIT_PATH" ]; } && [ "$FORCE" -ne 1 ]; then
		[ -e "$CONFIG_PATH" ] && echo "Existing Xeondb config detected: $CONFIG_PATH"
		[ -e "$UNIT_PATH" ] && echo "Existing Xeondb systemd unit detected: $UNIT_PATH"
		if [ "$ASSUME_YES" -eq 1 ]; then
			die "Refusing to overwrite existing install without --force"
		fi
		if promptConfirm "Overwrite existing config/unit and reinstall?" 0; then
			FORCE=1
		else
			echo "Leaving existing install as-is. (Use --force to overwrite)"
			exit 0
		fi
	fi
}

maybePromptAuth() {
	local enable=0
	if [ -n "$DB_PASS_ENV" ]; then
		DB_PASS="${!DB_PASS_ENV:-}"
	fi

	if [ -n "$DB_USER" ] || [ -n "$DB_PASS" ]; then
		enable=1
	fi

	if [ "$ASSUME_YES" -eq 1 ] || [ "$PROMPT_AVAILABLE" -ne 1 ]; then
		if [ "$enable" -eq 1 ]; then
			[ -n "$DB_USER" ] || die "--db-user is required when enabling auth non-interactively"
			[ -n "$DB_PASS" ] || die "Database password not provided (use --db-pass or --db-pass-env)"
			return 0
		fi
		echo "Auth: disabled (recommended for real deployments; re-run with --db-user + --db-pass-env to enable)"
		DB_USER=""
		DB_PASS=""
		return 0
	fi

	if promptConfirm "Enable authentication? (recommended)" 1; then
		enable=1
	else
		enable=0
	fi

	if [ "$enable" -eq 1 ]; then
		promptRead DB_USER "Enter database username: "
		promptReadSecret DB_PASS "Enter database password: "
		[ -n "$DB_USER" ] || die "Username cannot be empty when auth is enabled"
		[ -n "$DB_PASS" ] || die "Password cannot be empty when auth is enabled"
	else
		DB_USER=""
		DB_PASS=""
	fi
}

handleExistingInstall

maybePromptAuth

if [ "$ASSUME_YES" -ne 1 ]; then
	confirmInstall || die "Aborted by user"
fi

echo "Building Xeondb server..."
SRC_DIR=""
SCRIPT_DIR=""

cleanupSource() {
	if [ "$KEEP_SOURCE" -eq 1 ]; then
		echo "Keeping source checkout: $SRC_DIR"
		return 0
	fi
	if [ -n "$SRC_DIR" ] && [ "$USE_LOCAL" -ne 1 ] && [ -d "$SRC_DIR" ]; then
		rm -rf "$SRC_DIR" || true
	fi
}

if [ "$USE_LOCAL" -eq 1 ]; then
	SCRIPT_DIR="$(getScriptDir)" || die "--use-local requires running install.sh from a file (not via curl | bash)"
	SRC_DIR="$SCRIPT_DIR"
else
	SRC_DIR="$(mktemp -d)"
	trap cleanupSource EXIT
	echo "Fetching latest source: $REPO_URL ($REPO_REF)"
	git clone --depth 1 --branch "$REPO_REF" "$REPO_URL" "$SRC_DIR"
	if command -v git >/dev/null 2>&1; then
		commit="$(git -C "$SRC_DIR" rev-parse --short HEAD 2>/dev/null || true)"
		[ -n "$commit" ] && echo "Source commit: $commit"
	fi
fi

BUILD_DIR="$SRC_DIR/build"

if [ "$FULL_BUILD" -eq 1 ]; then
	needCmd clang-tidy
	python3 -c "import pytest" >/dev/null 2>&1 || die "pytest not installed. recommended: python3 -m pip install -r $SRC_DIR/tests/requirementsDev.txt"
fi

cmake -S "$SRC_DIR" -B "$BUILD_DIR" -G Ninja

if [ "$FULL_BUILD" -eq 1 ]; then
	ninja -C "$BUILD_DIR" build
else
	ninja -C "$BUILD_DIR" Xeondb
fi

BIN_SRC="$BUILD_DIR/Xeondb"
[ -x "$BIN_SRC" ] || die "Build did not produce executable: $BIN_SRC"

BIN_DST="$INSTALL_PREFIX/bin/xeondb"

echo "Installing xeondb to $BIN_DST"
install -Dm755 "$BIN_SRC" "$BIN_DST"

echo "Ensuring data directory exists: $DATA_DIR"
mkdir -p "$DATA_DIR"

echo "Writing config: $CONFIG_PATH"
mkdir -p "$(dirname "$CONFIG_PATH")"
cat >"$CONFIG_PATH" <<EOF
# XeonDB server configuration

# Network configuration
network:
	host: $HOST
	port: $PORT

# Storage configuration
storage:
	dataDir: $DATA_DIR

# Limits / safety valves
limits:
	maxLineBytes: 1048576
	maxConnections: 1024
	# Optional quota enforcement (auth-enabled deployments only)
	# - quotaEnforcementEnabled: when true, enforce per-keyspace quota rows in SYSTEM.KEYSPACE_QUOTAS
	# - quotaBytesUsedCacheTtlMs: Limit the size that a keyspace can be.
	quotaEnforcementEnabled: false
	quotaBytesUsedCacheTtlMs: 2000

# Write-ahead log (WAL)
wal:
	walFsync: periodic
	walFsyncIntervalMs: 50
	walFsyncBytes: 1048576

# In-memory write buffer
memtable:
	memtableMaxBytes: 33554432

# SSTable configuration
sstable:
	sstableIndexStride: 16

# Optional authentication.
# If both username and password are set, clients must authenticate first.
# If either is missing/empty, auth is disabled.
# Client must send: AUTH "<username>" "<password>";
auth:
EOF
		if [ -n "$DB_USER" ] && [ -n "$DB_PASS" ]; then
				echo "  username: $DB_USER" >> "$CONFIG_PATH"
				echo "  password: $DB_PASS" >> "$CONFIG_PATH"
		else
				echo "  # username: admin" >> "$CONFIG_PATH"
				echo "  # password: change-me" >> "$CONFIG_PATH"
		fi
echo "Installing systemd unit: $UNIT_PATH"
if [ -e "$UNIT_PATH" ] && [ "$FORCE" -ne 1 ]; then
	echo "Unit exists; leaving as-is (use --force to overwrite): $UNIT_PATH"
else
	cat >"$UNIT_PATH" <<EOF
[Unit]
Description=Xeondb Server
After=network.target

[Service]
Type=simple
ExecStart=$BIN_DST --config $CONFIG_PATH
Restart=on-failure
TimeoutSec=30

[Install]
WantedBy=multi-user.target
EOF
	chmod 0644 "$UNIT_PATH"
fi

if [ "$NO_START" -eq 1 ]; then
	echo "Install complete."
	echo "To start manually: $BIN_DST --config $CONFIG_PATH"
	if command -v systemctl >/dev/null 2>&1; then
		echo "To enable/start: systemctl daemon-reload && systemctl enable --now xeondb"
	fi
	exit 0
fi

echo "Reloading systemd..."
systemctl daemon-reload

echo "Enabling and starting xeondb..."
systemctl enable --now xeondb

echo "Done. Service status:"
systemctl --no-pager --full status xeondb || true
