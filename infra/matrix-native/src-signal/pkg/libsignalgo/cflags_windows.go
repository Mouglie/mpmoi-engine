//go:build windows

package libsignalgo

/*
// Windows (mingw/gnu ABI) cgo link flags for libsignal_ffi. The unix flags
// (-ldl -lz, see cflags.go) don't exist on mingw; instead the Rust staticlib
// pulls in the Win32 system libs that Rust std + BoringSSL depend on. When
// linking a Rust *staticlib* into a non-Rust program (the Go engine via cgo),
// these transitive native libs are NOT added automatically, so we list them.
// -static links libgcc/libstdc++/libwinpthread INTO the exe (system DLLs like kernel32
// stay dynamic) so the engine ships as a single .exe with no mingw-runtime DLL deps.
#cgo LDFLAGS: -lsignal_ffi -lws2_32 -lbcrypt -lntdll -luserenv -ladvapi32 -lcrypt32 -lsecur32 -lncrypt -lkernel32 -luser32 -liphlpapi -lpsapi -ldbghelp -lole32 -loleaut32 -lstdc++ -lm -static
*/
import "C"
