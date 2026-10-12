#!/usr/bin/env bash
# Cross compile rust-mqtt-adapter.
#
# Usage: scripts/cross-build.sh [TARGET] [--debug]
#   TARGET   Rust target triple (default: aarch64-unknown-linux-gnu)
#   --debug  Build a debug binary instead of release
set -euo pipefail

TARGET="aarch64-unknown-linux-gnu"
PROFILE="release"
for arg in "$@"; do
    case "$arg" in
        --debug) PROFILE="debug" ;;
        -h|--help) sed -n '2,7p' "$0"; exit 0 ;;
        -*) echo "error: unknown option $arg" >&2; exit 1 ;;
        *) TARGET="$arg" ;;
    esac
done

cd "$(dirname "$0")/.."

for tool in cargo rustup; do
    command -v "$tool" >/dev/null || { echo "error: $tool not found in PATH" >&2; exit 1; }
done

# Linker for the target: honour .cargo/config.toml, falling back to the known default.
case "$TARGET" in
    aarch64-unknown-linux-gnu) LINKER="aarch64-linux-gnu-gcc"; PKG="gcc-aarch64-linux-gnu" ;;
    armv7-unknown-linux-gnueabihf) LINKER="arm-linux-gnueabihf-gcc"; PKG="gcc-arm-linux-gnueabihf" ;;
    *) LINKER=""; PKG="" ;;
esac
if [[ -n "$LINKER" ]] && ! command -v "$LINKER" >/dev/null; then
    echo "error: linker '$LINKER' not found. Install it with: sudo apt install $PKG" >&2
    exit 1
fi

rustup target add "$TARGET"

FLAGS=()
[[ "$PROFILE" == "release" ]] && FLAGS+=(--release)
cargo build "${FLAGS[@]}" --target "$TARGET"

BIN="target/$TARGET/$PROFILE/rust-mqtt-adapter"
echo
echo "Built: $BIN"
file "$BIN" 2>/dev/null || true
