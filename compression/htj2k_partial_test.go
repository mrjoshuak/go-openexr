package compression

import (
	"encoding/binary"
	"image"
	"math"
	"testing"
)

// buildHTJ2KChunk packs a float image into one HTJ2K chunk and returns the
// chunk together with the channel description it was written from.
func buildHTJ2KChunk(t *testing.T, w, h int) ([]byte, []HTJ2KChannelInfo, []float32) {
	t.Helper()
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: w, Height: h, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, w*h*4)
	want := make([]float32, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			// A smooth ramp: a reduced-resolution decode of noise has no
			// relationship to the full one, so the comparison below would have
			// nothing to say.
			v := float32(x)/float32(w) + float32(y)/float32(h)
			want[y*w+x] = v
			binary.LittleEndian.PutUint32(src[(y*w+x)*4:], math.Float32bits(v))
		}
	}
	chunk, err := HTJ2KCompress(src, h, channels, 128)
	if err != nil {
		t.Fatalf("HTJ2KCompress: %v", err)
	}
	return chunk, channels, want
}

// TestHTJ2KPartialFullDecodeMatches is the control: with no options, the
// partial decoder must produce exactly what the ordinary one does. Without it
// every check below could be satisfied by a decoder that returns anything.
func TestHTJ2KPartialFullDecodeMatches(t *testing.T) {
	const w, h = 64, 32
	chunk, channels, _ := buildHTJ2KChunk(t, w, h)

	whole, err := HTJ2KDecompress(chunk, 0, channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress: %v", err)
	}
	part, err := HTJ2KDecompressPartial(chunk, channels, nil)
	if err != nil {
		t.Fatalf("HTJ2KDecompressPartial(nil): %v", err)
	}
	if part.Width != w || part.Height != h {
		t.Fatalf("full partial decode produced %dx%d, want %dx%d", part.Width, part.Height, w, h)
	}
	if len(part.Data) != len(whole) {
		t.Fatalf("full partial decode produced %d bytes, the ordinary decode %d", len(part.Data), len(whole))
	}
	for i := range whole {
		if whole[i] != part.Data[i] {
			t.Fatalf("full partial decode differs from the ordinary decode at byte %d", i)
		}
	}
}

// TestHTJ2KPartialOptionsAreRefusedNotIgnored records what the underlying
// codec can and cannot do today, so this package does not appear to offer more
// than it delivers.
//
// Both options are refused by go-jpeg2000 rather than honoured:
//
//   - ReduceResolution stops the inverse wavelet at an LL subband, and an EXR
//     HTJ2K chunk always carries an NLT point transform (half and float alike),
//     so the surviving values are what NLT maps back from rather than samples.
//     Measured before the refusal landed: dimensions correct, samples off by
//     175 on a ramp spanning 0 to 2.
//   - DecodeArea was declared and read by nothing, so a region request returned
//     the whole chunk.
//
// What matters here is that the request fails rather than returning wrong
// pixels quietly. A viewport is still resolvable to byte ranges — that is
// File.ChunkRange and the packet index, neither of which needs this — but
// turning those bytes into fewer pixels than the chunk holds is not yet
// possible.
func TestHTJ2KPartialOptionsAreRefusedNotIgnored(t *testing.T) {
	const w, h = 64, 32
	chunk, channels, _ := buildHTJ2KChunk(t, w, h)

	if _, err := HTJ2KDecompressPartial(chunk, channels, &HTJ2KDecodeOptions{ReduceResolution: 1}); err == nil {
		t.Error("ReduceResolution was accepted; it must fail while the codec cannot honour it")
	}
	region := image.Rect(16, 8, 48, 24)
	if _, err := HTJ2KDecompressPartial(chunk, channels, &HTJ2KDecodeOptions{Region: &region}); err == nil {
		t.Error("Region was accepted; it must fail while the codec cannot honour it")
	}

	// The control: the whole-chunk path must keep working, or the checks above
	// are satisfied by a function that fails at everything.
	res, err := HTJ2KDecompressPartial(chunk, channels, nil)
	if err != nil {
		t.Fatalf("whole-chunk decode must still work: %v", err)
	}
	if res.Width != w || res.Height != h {
		t.Errorf("whole-chunk decode produced %dx%d, want %dx%d", res.Width, res.Height, w, h)
	}
}
