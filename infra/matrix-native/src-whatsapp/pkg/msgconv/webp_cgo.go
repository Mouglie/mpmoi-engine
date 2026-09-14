//go:build !windows

package msgconv

import (
	"image"
	"io"

	cwebp "go.mau.fi/webp"
)

// encodeWebP encodes an image to WebP via the cgo libwebp binding (go.mau.fi/webp)
// on platforms that ship a C toolchain + libwebp (macOS/Linux). Used for outbound
// WhatsApp sticker conversion. The Windows build uses a pure-Go encoder instead
// (webp_purego.go) so the engine cross-compiles without a native libwebp.
func encodeWebP(w io.Writer, img image.Image) error {
	return cwebp.Encode(w, img, nil)
}
