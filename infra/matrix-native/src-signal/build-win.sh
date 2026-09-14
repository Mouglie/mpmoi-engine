#!/bin/sh
# Build libsignal_ffi.a for win-x64 (GNU/mingw ABI) so the engine can cross-compile to
# windows/amd64 with cgo. Go's cgo on Windows links via mingw gcc, NOT MSVC, so the lib
# MUST target x86_64-pc-windows-gnu (an MSVC-built .lib is not Go-cgo-linkable).
#
# Toolchain (mac cross): rustup nightly (libsignal pins one via rust-toolchain) + the
# x86_64-pc-windows-gnu target + brew mingw-w64 (x86_64-w64-mingw32-gcc) as the C/linker.
#   brew install mingw-w64 rustup
#   rustup toolchain install <pinned-nightly> --target x86_64-pc-windows-gnu
#
# Output: infra/matrix-native/src-signal/win-x64/libsignal_ffi.a (where engine/build-win.sh looks).
set -e

cd "$(dirname "$0")"
ROOT="$(pwd)"
TARGET=x86_64-pc-windows-gnu

git submodule update --init

cd pkg/libsignalgo/libsignal

# boring-sys only sets OPENSSL_NO_ASM when HOST is windows; cross-compiling to windows-gnu
# from macOS otherwise takes BoringSSL's nasm asm path, but the hand-written fiat p256 ADX
# routines exist only as GAS .S (no nasm build) → bcm.cc references fiat_p256_adx_mul/sqr
# with no provider → the engine link fails. Force pure-C for the windows target. boring-sys
# is a git dep in ~/.cargo cache (fingerprinted by commit, so an edit alone won't rebuild it
# — the caller must `cargo clean -p boring-sys --target x86_64-pc-windows-gnu` after a fresh
# fetch). We patch idempotently here; crypto perf is a non-issue for a desktop bridge.
# cargo fetch first so the boring-sys checkout exists before we glob for it (fresh machine).
cargo fetch --target "$TARGET" >/dev/null 2>&1 || true
BORING_MAIN="$(ls "$HOME"/.cargo/git/checkouts/boring-*/*/boring-sys/build/main.rs 2>/dev/null | head -1 || true)"
if [ -n "$BORING_MAIN" ] && ! grep -q "mpmoi win-x64 patch" "$BORING_MAIN"; then
  # Drop the `if config.host.contains("windows")` guard so NO_ASM applies to the target.
  perl -0pi -e 's/if config\.host\.contains\("windows"\) \{\s*\n(\s*\/\/[^\n]*\n)*\s*boringssl_cmake\.define\("OPENSSL_NO_ASM", "YES"\);\s*\n\s*\}/\/\/ mpmoi win-x64 patch: force pure-C for all windows targets (see build-win.sh).\n            boringssl_cmake.define("OPENSSL_NO_ASM", "YES");/s' "$BORING_MAIN"
  echo "patched boring-sys OPENSSL_NO_ASM: $BORING_MAIN"
  cargo clean -p boring-sys --target "$TARGET" --release 2>/dev/null || true
fi

# boring-sys runs bindgen (libclang) over BoringSSL's headers for the target; libclang
# needs the mingw sysroot on its include path or it can't find stdlib.h/stddef.h. Derive
# both dirs from the cross-gcc itself so this isn't version-pinned to a mingw release.
GCC="${CC_x86_64_pc_windows_gnu:-x86_64-w64-mingw32-gcc}"
SYSROOT="$("$GCC" -print-sysroot)"
GCC_INC="$("$GCC" -print-search-dirs | sed -n 's/^install: //p')include"
MINGW_INC="$SYSROOT/x86_64-w64-mingw32/include"
BINDGEN_ARGS="--target=x86_64-w64-mingw32 -I$MINGW_INC -I$GCC_INC"

# -crt-static off: link against mingw's CRT (Go's mingw provides it), matching the unix build.
# cc-rs compiles libsignal's C (boring asm etc.) with the mingw cross-gcc for the gnu target;
# nasm assembles BoringSSL's win64 asm (brew install nasm).
CC_x86_64_pc_windows_gnu="$GCC" \
CXX_x86_64_pc_windows_gnu="${CXX_x86_64_pc_windows_gnu:-x86_64-w64-mingw32-g++}" \
AR_x86_64_pc_windows_gnu="${AR_x86_64_pc_windows_gnu:-x86_64-w64-mingw32-ar}" \
BINDGEN_EXTRA_CLANG_ARGS_x86_64_pc_windows_gnu="$BINDGEN_ARGS" \
BINDGEN_EXTRA_CLANG_ARGS="$BINDGEN_ARGS" \
RUSTFLAGS="-Ctarget-feature=-crt-static" RUSTC_WRAPPER="" \
  cargo build -p libsignal-ffi --profile=release --target "$TARGET"

mkdir -p "$ROOT/win-x64"
cp -f "target/$TARGET/release/libsignal_ffi.a" "$ROOT/win-x64/libsignal_ffi.a"
echo "done: $ROOT/win-x64/libsignal_ffi.a"
