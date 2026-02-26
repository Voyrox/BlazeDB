#!/usr/bin/env bash
# Copyright 2026 Xeondb. All rights reserved.

set -euo pipefail

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

installGitIfMissing() {
	if needCmd git; then
		return 0
	fi

	echo "git is required to update Xeondb."
	if [ "${ASSUME_YES}" -eq 1 ]; then
		reply="y"
	else
		printf "Install git now? [Y/n] "
		read -r reply
	fi

	case "$reply" in
		n|N|no|NO)
			die "git is not installed"
			;;
		*)
			if needCmd apt-get; then
				if [ "${EUID:-$(id -u)}" -ne 0 ]; then
					die "Installing git requires root. Re-run with sudo."
				fi
				apt-get update -y
				apt-get install -y git
			elif needCmd dnf; then
				if [ "${EUID:-$(id -u)}" -ne 0 ]; then
					die "Installing git requires root. Re-run with sudo."
				fi
				dnf install -y git
			elif needCmd yum; then
				if [ "${EUID:-$(id -u)}" -ne 0 ]; then
					die "Installing git requires root. Re-run with sudo."
				fi
				yum install -y git
			elif needCmd pacman; then
				if [ "${EUID:-$(id -u)}" -ne 0 ]; then
					die "Installing git requires root. Re-run with sudo."
				fi
				pacman -Sy --noconfirm git
			else
				die "git missing and no supported package manager found (apt-get/dnf/yum/pacman)"
			fi
			needCmd git || die "git install failed"
			;;
	esac
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

installGitIfMissing

repoDir=""
if [ -n "$CLONE_DIR" ]; then
	repoDir="$CLONE_DIR"
	if [ ! -e "$repoDir" ]; then
		echo "Cloning Xeondb into: $repoDir"
		git clone "$REPO_URL" "$repoDir"
	fi
	[ -d "$repoDir" ] || die "Not a directory: $repoDir"
	if [ ! -d "$repoDir/.git" ]; then
		die "clone-dir is not a git repo: $repoDir"
	fi
else
	repoDir="$(pwd)"
	if ! git -C "$repoDir" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
		die "Not a git repo. Run from the Xeondb checkout or use --clone-dir."
	fi
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
		printf "Continue? [y/N] "
		read -r reply
		case "$reply" in
			y|Y|yes|YES) : ;;
			*) die "Aborted by user" ;;
		esac
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

needCmd cmake || die "cmake is required (install it first)"
needCmd ninja || die "ninja is required (install it first)"

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
installCmd=(install -Dm755 "$binSrc" "$binDst")

if [ "${EUID:-$(id -u)}" -eq 0 ]; then
	"${installCmd[@]}"
else
	if needCmd sudo; then
		if [ "$ASSUME_YES" -eq 1 ]; then
			sudo "${installCmd[@]}"
		else
			printf "Root required to install to %s. Use sudo? [Y/n] " "$PREFIX"
			read -r reply
			case "$reply" in
				n|N|no|NO) die "Not installed. Re-run with sudo or choose a writable --prefix." ;;
				*) sudo "${installCmd[@]}" ;;
			esac
		fi
	else
		die "Not root and sudo not found. Re-run with sudo or choose a writable --prefix."
	fi
fi

if [ "$NO_RESTART" -eq 0 ] && needCmd systemctl && systemctl list-unit-files 2>/dev/null | grep -q '^xeondb\.service'; then
	echo "Restarting systemd service: xeondb"
	restartCmd=(systemctl daemon-reload)
	restartCmd2=(systemctl restart xeondb)
	if [ "${EUID:-$(id -u)}" -eq 0 ]; then
		"${restartCmd[@]}" || true
		"${restartCmd2[@]}" || true
	else
		if needCmd sudo; then
			sudo "${restartCmd[@]}" || true
			sudo "${restartCmd2[@]}" || true
		else
			warn "systemd detected but not root and sudo missing; not restarting service"
		fi
	fi
fi

echo "Update complete."
