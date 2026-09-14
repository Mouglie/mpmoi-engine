#!/usr/bin/env bash
# Cross-compile the pure-Go engine for windows/amd64 (the Phase-3 Windows port).
#
# Output: engine/mpmoi-engine.exe (gitignored) — the Windows counterpart of build.sh's
# arm64 mac binary. The desktop packaging copies this into the app resources; config.ts
# (ENGINE_BIN) resolves `bin/mpmoi-engine.exe` when packaged, `engine/mpmoi-engine.exe` in dev.
#
# Native links on Windows:
#   - libsignal (Signal, cgo) — libsignal_ffi.a must be the GNU/mingw ABI (built for
#     x86_64-pc-windows-gnu; see infra/matrix-native/src-signal/build-win.sh). Go's cgo on
#     Windows links via mingw gcc, NOT MSVC, so the .a must match.
#   - libwebp — NO LONGER a native link on Windows: msgconv's webp encode is pure-Go
#     (nativewebp) under the `windows` build tag (see src-whatsapp/pkg/msgconv/webp_purego.go).
#
# Toolchain (mac cross): brew mingw-w64 (x86_64-w64-mingw32-gcc) + the GNU-ABI libsignal_ffi.a.
set -euo pipefail

cd "$(dirname "$0")"
OUT="${1:-$PWD/mpmoi-engine.exe}"

# GNU-ABI libsignal for win-x64 lives in its own dir so it doesn't collide with the arm64
# src-signal/libsignal_ffi.a. build-win.sh in src-signal drops it here.
SIGDIR_WIN="$(cd ../infra/matrix-native/src-signal && pwd)/win-x64"

if [ ! -f "$SIGDIR_WIN/libsignal_ffi.a" ]; then
  echo "!! $SIGDIR_WIN/libsignal_ffi.a missing — run infra/matrix-native/src-signal/build-win.sh first" >&2
  exit 1
fi

CC="${CC:-x86_64-w64-mingw32-gcc}"
CXX="${CXX:-x86_64-w64-mingw32-g++}"

echo "cross-building engine (windows/amd64 + libsignal) → $OUT"
CGO_ENABLED=1 GOOS=windows GOARCH=amd64 \
  CC="$CC" CXX="$CXX" \
  CGO_LDFLAGS="-L$SIGDIR_WIN" GOFLAGS=-mod=mod \
  go build -o "$OUT" .

echo "done: $OUT"
