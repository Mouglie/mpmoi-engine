#!/usr/bin/env bash
# Build the pure-Go engine binary the desktop app runs in supervisor mode.
#
# Output: engine/mpmoi-engine (gitignored) — where config.ts (ENGINE_BIN) looks for it in dev, and
# what the packaging step copies into the app resources (RESOURCE_DIR/bin/mpmoi-engine).
#
# Signal links libsignal (Rust) via cgo, and the prebuilt src-signal/libsignal_ffi.a is arm64. The
# Homebrew Go toolchain on this Mac is amd64, so we cross-build arm64 and point the linker at the .a.
# (WhatsApp/Signal/Meta are all one binary; Meta is pure Go and adds no native link.)
set -euo pipefail

cd "$(dirname "$0")"
OUT="${1:-$PWD/mpmoi-engine}"
SIGDIR="$(cd ../infra/matrix-native/src-signal && pwd)"

if [ ! -f "$SIGDIR/libsignal_ffi.a" ]; then
  echo "!! $SIGDIR/libsignal_ffi.a missing — run infra/matrix-native/src-signal/build.sh first" >&2
  exit 1
fi

echo "building engine (arm64 + libsignal) → $OUT"
CGO_ENABLED=1 GOARCH=arm64 \
  CC="clang -arch arm64" CXX="clang++ -arch arm64" \
  CGO_LDFLAGS="-L$SIGDIR" GOFLAGS=-mod=mod \
  go build -o "$OUT" .

echo "done: $OUT"
