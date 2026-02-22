#!/usr/bin/env bash
# Copyright 2026 Xeondb. All rights reserved.

set -euo pipefail

onError() {
	local exitCode=$?
	echo "install.sh failed (exit=$exitCode) at line $LINENO: $BASH_COMMAND" >&2
	exit "$exitCode"
}
trap onError ERR

die() {
	echo "Error: $*" >&2
	exit 1
}

needCmd() {
	command -v "$1" >/dev/null 2>&1 || die "Missing dependency: $1"
}

if [ -z "${BASH_VERSION:-}" ]; then
	die "Unsupported shell; please run with bash"
fi

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
	die "This installer must run as root. Try: sudo ./install.sh"
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

Run as root:
  sudo ./install.sh
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

INSTALL_PREFIX="/usr/local/xeondb"
CONFIG_PATH="/etc/xeondb/settings.yml"
DATA_DIR="/var/lib/xeondb/data"
HOST="0.0.0.0"
PORT="9876"
FULL_BUILD=0
FORCE=0
ASSUME_YES=0
NO_START=0

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
		*)
			die "Unknown option: $1 (try --help)"
			;;
	esac
done

needCmd cmake
needCmd ninja
needCmd g++
needCmd python3

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
	if [ "$FULL_BUILD" -eq 1 ]; then
		echo "  Build:          cmake -S $SCRIPT_DIR -B $SCRIPT_DIR/build -G Ninja && ninja -C $SCRIPT_DIR/build build (full)"
	else
		echo "  Build:          cmake -S $SCRIPT_DIR -B $SCRIPT_DIR/build -G Ninja && ninja -C $SCRIPT_DIR/build Xeondb"
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
	printf "Continue? [y/N] "
	read -r reply
	case "${reply}" in
		y|Y|yes|YES) return 0 ;;
		*) return 1 ;;
	esac
}

if [ "$ASSUME_YES" -ne 1 ]; then
	if [ -t 0 ]; then
		confirmInstall || die "Aborted by user"
	else
		die "Non-interactive install requires --yes"
	fi
fi

echo "Building Xeondb server..."
BUILD_DIR="$SCRIPT_DIR/build"

if [ "$FULL_BUILD" -eq 1 ]; then
	needCmd clang-tidy
	py=python3; if [ -x "$SCRIPT_DIR/.venv/bin/python3" ]; then py="$SCRIPT_DIR/.venv/bin/python3"; fi
	"$py" -c "import pytest" >/dev/null 2>&1 || die "pytest not installed. recommended: python3 -m venv $SCRIPT_DIR/.venv && $SCRIPT_DIR/.venv/bin/pip install -r $SCRIPT_DIR/tests/requirementsDev.txt"
fi

cmake -S "$SCRIPT_DIR" -B "$BUILD_DIR" -G Ninja

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
if [ -e "$CONFIG_PATH" ] && [ "$FORCE" -ne 1 ]; then
	echo "Config exists; leaving as-is (use --force to overwrite): $CONFIG_PATH"
else
	cat >"$CONFIG_PATH" <<EOF
host: $HOST
port: $PORT
dataDir: $DATA_DIR
maxLineBytes: 1048576
maxConnections: 1024
walFsync: periodic
walFsyncIntervalMs: 50
walFsyncBytes: 1048576
memtableMaxBytes: 33554432
sstableIndexStride: 16
EOF
fi

UNIT_PATH="/etc/systemd/system/xeondb.service"
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
