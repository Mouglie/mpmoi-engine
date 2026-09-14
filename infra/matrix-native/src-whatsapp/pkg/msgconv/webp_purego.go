//go:build windows

package msgconv

import (
	"image"
	"io"

	"github.com/HugoSmits86/nativewebp"
)

// encodeWebP encodes an image to WebP on Windows using a pure-Go VP8L (lossless)
// encoder, avoiding the cgo libwebp dependency (go.mau.fi/webp) that would require
// a native libwebp on win-x64. Used for outbound WhatsApp sticker conversion.
// VP8L output is valid WebP that WhatsApp accepts for stickers.
func encodeWebP(w io.Writer, img image.Image) error {
	return nativewebp.Encode(w, img, nil)
}
