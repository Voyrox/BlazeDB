#!/usr/bin/env bash
# Copyright 2026 Xeondb. All rights reserved.

set -euo pipefail

PROMPT_FD=""
PROMPT_AVAILABLE=0

onError() {
	local exitCode=$?
	echo "update.sh failed (exit=$exitCode) at line $LINENO: $BASH_COMMAND" >&2
	exit "$exitCode"
}

trap onError ERR

die() {
	echo "Error: $*" >&2
	exit 1
}

warn() {
	echo "Warning: $*" >&2
}

needCmd() {
	command -v "$1" >/dev/null 2>&1
}

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
		die "Non-interactive update requires --yes"
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
		y|Y|yes|YES) return 0 ;;
		n|N|no|NO) return 1 ;;
		"") [ "$__default_yes" -eq 1 ] && return 0 || return 1 ;;
		*) [ "$__default_yes" -eq 1 ] && return 0 || return 1 ;;
	esac
}

installDep() {
	dep="$1"
	pkgname="$dep"
	if needCmd apt-get; then
		pkgmgr="apt-get"
		case "$dep" in
			ninja)
				pkgname="ninja-build" ;;
		esac
		installcmd="apt-get install -y $pkgname"
	elif needCmd dnf; then
		pkgmgr="dnf"
		installcmd="dnf install -y $dep"
	elif needCmd yum; then
		pkgmgr="yum"
		installcmd="yum install -y $dep"
	elif needCmd pacman; then
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

ensureCmd() {
	if ! needCmd "$1"; then
		installDep "$1"
		needCmd "$1" || die "Missing dependency: $1 (install failed)"
	fi
}

installGitIfMissing() {
	if needCmd git; then
		return 0
	fi

	echo "git is required to update Xeondb."
	if [ "${ASSUME_YES}" -eq 1 ]; then
		ensureCmd git
		return 0
	fi
	if promptConfirm "Install git now?" 1; then
		ensureCmd git
	else
		die "git is not installed"
	fi
}

printUsage() {
	cat <<'EOF'
Usage: update.sh [options]

Safe-by-default updater for Xeondb:
  - Default: fast-forward update only; refuses if repo is dirty.
  - --wipe: discards all local changes/untracked files in repo dir.

Options:
  -h, --help                 Show this help message and exit
  --remote NAME              Git remote (default: origin)
  --branch NAME              Git branch (default: main)
  --repo-url URL             Repo URL used with --clone-dir
  --clone-dir PATH           Use/manage a dedicated clone directory
  --prefix PATH              Install prefix (default: /usr/local/xeondb)
  --full                     Build Ninja target `build` (lint + tests)
  --wipe                     HARD reset + clean repo dir (destructive)
  --allow-repo-data-dir      Allow --wipe even if dataDir appears repo-local
  --no-restart               Do not restart systemd service
  --yes                      Non-interactive; assume yes to prompts

Examples:
  ./update.sh
  ./update.sh --wipe
  ./update.sh --clone-dir /opt/xeondb-src --repo-url https://github.com/xeondb/Xeondb.git

  # Curl installer-style update (recommended)
  curl -fsSL https://xeondb.com/update | sudo bash
EOF
}

REMOTE="origin"
BRANCH="main"
REPO_URL="https://github.com/xeondb/Xeondb.git"
CLONE_DIR=""
PREFIX="/usr/local/xeondb"
FULL_BUILD=0
WIPE=0
ASSUME_YES=0
NO_RESTART=0
ALLOW_REPO_DATA_DIR=0

while [ $# -gt 0 ]; do
	case "$1" in
		-h|--help)
			printUsage
			exit 0
			;;
		--remote)
			shift
			[ $# -gt 0 ] || die "Missing argument for --remote"
			REMOTE="$1"
			shift
			;;
		--branch)
			shift
			[ $# -gt 0 ] || die "Missing argument for --branch"
			BRANCH="$1"
			shift
			;;
		--repo-url)
			shift
			[ $# -gt 0 ] || die "Missing argument for --repo-url"
			REPO_URL="$1"
			shift
			;;
		--clone-dir)
			shift
			[ $# -gt 0 ] || die "Missing argument for --clone-dir"
			CLONE_DIR="$1"
			shift
			;;
		--prefix)
			shift
			[ $# -gt 0 ] || die "Missing argument for --prefix"
			PREFIX="$1"
			shift
			;;
		--full)
			FULL_BUILD=1
			shift
			;;
		--wipe)
			WIPE=1
			shift
			;;
		--allow-repo-data-dir)
			ALLOW_REPO_DATA_DIR=1
			shift
			;;
		--no-restart)
			NO_RESTART=1
			shift
			;;
		--yes)
			ASSUME_YES=1
			shift
			;;
		*)
			die "Unknown option: $1 (try --help)"
			;;
	esac
done

setupPrompts

if [ "${EUID:-$(id -u)}" -ne 0 ]; then
	die "This updater must run as root. Try: curl -fsSL https://xeondb.com/update | sudo bash"
fi

if [ "$PROMPT_AVAILABLE" -ne 1 ] && [ "$ASSUME_YES" -ne 1 ]; then
	die "Non-interactive update requires --yes"
fi

installGitIfMissing

repoDir=""
if [ -n "$CLONE_DIR" ]; then
	repoDir="$CLONE_DIR"
else
	if git -C "$(pwd)" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
		repoDir="$(git -C "$(pwd)" rev-parse --show-toplevel)"
	else
		repoDir="/opt/xeondb-src"
		echo "No repo detected in current directory; using managed clone dir: $repoDir"
	fi
fi

if [ ! -e "$repoDir" ]; then
	echo "Cloning Xeondb into: $repoDir"
	git clone "$REPO_URL" "$repoDir"
fi

[ -d "$repoDir" ] || die "Not a directory: $repoDir"
if [ ! -d "$repoDir/.git" ]; then
	die "Not a git repo: $repoDir (use --clone-dir to choose a different directory)"
fi

echo "Using repo: $repoDir"

if [ "$WIPE" -eq 1 ]; then
	echo
	echo "DESTRUCTIVE UPDATE (--wipe)"
	echo "This will permanently discard ALL local changes and untracked files in:"
	echo "  $repoDir"
	echo "It will NOT touch your database data unless your dataDir points inside this directory."
	echo
	if [ "$ASSUME_YES" -ne 1 ]; then
		if ! promptConfirm "Continue?" 0; then
			die "Aborted by user"
		fi
	fi
	if [ "$ALLOW_REPO_DATA_DIR" -ne 1 ] && [ -r "/etc/xeondb/settings.yml" ]; then
		dataDirLine="$(grep -E '^[[:space:]]*dataDir:[[:space:]]*' /etc/xeondb/settings.yml | head -n 1 || true)"
		if [ -n "$dataDirLine" ]; then
			dataDir="${dataDirLine#*:}"
			dataDir="${dataDir# }"
			dataDir="${dataDir#\t}"
			dataDir="${dataDir%\"}"
			dataDir="${dataDir#\"}"
			if [ -n "$dataDir" ]; then
				repoReal="$(realpath -m "$repoDir")"
				dataReal="$(realpath -m "$dataDir" 2>/dev/null || true)"
				if [ -z "$dataReal" ] || [[ "$dataDir" != /* ]]; then
					warn "settings.yml dataDir looks relative or unreadable: '$dataDir'"
					warn "Refusing --wipe because it could delete database data under the repo dir."
					warn "If you're sure, re-run with --allow-repo-data-dir."
					exit 1
				fi
				case "$dataReal" in
					"$repoReal"/*)
						warn "settings.yml dataDir appears inside repo: $dataReal"
						warn "Refusing --wipe to avoid deleting database data."
						warn "If you're sure, re-run with --allow-repo-data-dir."
						exit 1
						;;
				esac
			fi
		fi
	fi
fi

git -C "$repoDir" remote get-url "$REMOTE" >/dev/null 2>&1 || die "Unknown remote: $REMOTE"

if ! git -C "$repoDir" show-ref --verify --quiet "refs/heads/$BRANCH"; then
	if ! git -C "$repoDir" show-ref --verify --quiet "refs/remotes/$REMOTE/$BRANCH"; then
		git -C "$repoDir" fetch "$REMOTE" "$BRANCH" >/dev/null 2>&1 || true
	fi
fi

dirty="$(git -C "$repoDir" status --porcelain)"
if [ -n "$dirty" ] && [ "$WIPE" -ne 1 ]; then
	die "Repo has local changes/untracked files. Commit/stash them or re-run with --wipe."
fi

if [ "$WIPE" -eq 1 ]; then
	git -C "$repoDir" fetch --prune "$REMOTE"
	git -C "$repoDir" checkout -f "$BRANCH"
	git -C "$repoDir" reset --hard "$REMOTE/$BRANCH"
	git -C "$repoDir" clean -fdx
else
	git -C "$repoDir" checkout "$BRANCH" >/dev/null 2>&1 || true
	git -C "$repoDir" fetch "$REMOTE" "$BRANCH"
	git -C "$repoDir" pull --ff-only "$REMOTE" "$BRANCH"
fi

ensureCmd cmake
ensureCmd ninja
ensureCmd g++

echo "Building Xeondb..."
buildDir="$repoDir/build"
cmake -S "$repoDir" -B "$buildDir" -G Ninja

if [ "$FULL_BUILD" -eq 1 ]; then
	ninja -C "$buildDir" build
else
	ninja -C "$buildDir" Xeondb
fi

binSrc="$buildDir/Xeondb"
[ -x "$binSrc" ] || die "Build did not produce executable: $binSrc"

binDst="$PREFIX/bin/xeondb"

echo "Installing binary to: $binDst"
install -Dm755 "$binSrc" "$binDst"

if [ "$NO_RESTART" -eq 0 ] && needCmd systemctl && systemctl list-unit-files 2>/dev/null | grep -q '^xeondb\.service'; then
	echo "Restarting systemd service: xeondb"
	systemctl daemon-reload || true
	systemctl restart xeondb || true
fi

echo "Update complete."
