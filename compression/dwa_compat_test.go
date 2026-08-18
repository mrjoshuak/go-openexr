package compression

import (
	"bytes"
	"testing"
)

func TestDeprecatedDWAWrappersRoundTrip(t *testing.T) {
	const w, h = 32, 16
	src := make([]byte, w*h*2)
	for i := 0; i < len(src); i += 2 {
		v := uint16(0x3400 + (i/2)%64)
		src[i] = byte(v)
		src[i+1] = byte(v >> 8)
	}
	enc, err := CompressDWAA(src, w, h, 45)
	if err != nil {
		t.Fatalf("CompressDWAA: %v", err)
	}
	dst := make([]byte, len(src))
	if err := DecompressDWAA(enc, dst, w, h); err != nil {
		t.Fatalf("DecompressDWAA: %v", err)
	}
	if bytes.Equal(dst, make([]byte, len(dst))) {
		t.Fatal("decoded to all zeros")
	}
}
