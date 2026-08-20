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

// TestHTJ2KPartialRegionIsASubset is what the roadmap asked for: a caller can
// decode a region of an HTJ2K chunk without decompressing the whole of it, and
// the result matches the same region of a full decode.
//
// Exactness is the right bar and not a lucky one: every coefficient that can
// reach the region is decoded and synthesised as usual, so the samples are the
// same arithmetic, not an approximation.
func TestHTJ2KPartialRegionIsASubset(t *testing.T) {
	const w, h = 64, 32
	chunk, channels, _ := buildHTJ2KChunk(t, w, h)

	whole, err := HTJ2KDecompress(chunk, 0, channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress: %v", err)
	}
	_, fullLine := htj2kLineLayoutAt(channels, w)
	full := make([]float32, 0, w*h)
	for y := 0; y < h; y++ {
		line := whole[y*fullLine:]
		for x := 0; x < w; x++ {
			full = append(full, math.Float32frombits(binary.LittleEndian.Uint32(line[x*4:])))
		}
	}

	region := image.Rect(16, 8, 48, 24)
	res, err := HTJ2KDecompressPartial(chunk, channels, &HTJ2KDecodeOptions{Region: &region})
	if err != nil {
		t.Fatalf("Region: %v", err)
	}
	if res.Width != region.Dx() || res.Height != region.Dy() {
		t.Fatalf("region %v produced %dx%d", region, res.Width, res.Height)
	}
	if res.Width*res.Height >= w*h {
		t.Fatalf("a %dx%d region of a %dx%d chunk produced %d samples; it must produce fewer",
			region.Dx(), region.Dy(), w, h, res.Width*res.Height)
	}

	for y := 0; y < res.Height; y++ {
		line := res.Data[y*res.BytesPerLine:]
		for x := 0; x < res.Width; x++ {
			got := math.Float32frombits(binary.LittleEndian.Uint32(line[x*4:]))
			want := full[(region.Min.Y+y)*w+(region.Min.X+x)]
			if got != want {
				t.Fatalf("region sample (%d,%d) = %v, the full decode has %v at (%d,%d)",
					x, y, got, want, region.Min.X+x, region.Min.Y+y)
			}
		}
	}
	t.Logf("region %v of %dx%d: %dx%d samples, exact", region, w, h, res.Width, res.Height)
}

// TestHTJ2KPartialRegionCostsLess is the other half of the bar. A region decode
// that produced the right samples by decompressing the whole chunk and cropping
// would pass the test above and buy nothing.
//
// The chunk is 256x256 because that is the smallest size at which the saving
// exists at all with the reference codec's parameters. Those are fixed —
// 128x32 code-blocks and five decompositions, matching internal_ht.cpp, because
// a chunk this library writes has to be the chunk libOpenEXR would have
// written. A code-block's influence is its band rectangle grown by the
// synthesis margin, which at the lowest resolution of a five-level decode is 64
// samples in every direction, so on a small chunk every block reaches every
// pixel and nothing can be skipped.
func TestHTJ2KPartialRegionCostsLess(t *testing.T) {
	const w, h = 256, 256
	chunk, channels, _ := buildHTJ2KChunk(t, w, h)

	whole, err := HTJ2KDecompressPartial(chunk, channels, nil)
	if err != nil {
		t.Fatalf("whole chunk: %v", err)
	}
	if whole.DecodedBytes == 0 {
		t.Fatal("a whole-chunk decode reports no code-block data; the measurement is not wired up")
	}
	if whole.SkippedBytes != 0 {
		t.Errorf("a whole-chunk decode skipped %d bytes; it should skip nothing", whole.SkippedBytes)
	}

	region := image.Rect(0, 0, 64, 64)
	part, err := HTJ2KDecompressPartial(chunk, channels, &HTJ2KDecodeOptions{Region: &region})
	if err != nil {
		t.Fatalf("region: %v", err)
	}
	if part.SkippedBytes == 0 {
		t.Fatal("a 64x64 region of a 256x256 chunk skipped no code-blocks; it decoded the chunk and cropped")
	}
	if part.DecodedBytes >= whole.DecodedBytes {
		t.Fatalf("the region decoded %d code-block bytes and the whole chunk %d; a region must cost less",
			part.DecodedBytes, whole.DecodedBytes)
	}
	// The two must account for the same total, or blocks are being lost rather
	// than skipped.
	if part.DecodedBytes+part.SkippedBytes != whole.DecodedBytes {
		t.Errorf("the region decoded %d and skipped %d, totalling %d; the whole chunk is %d",
			part.DecodedBytes, part.SkippedBytes,
			part.DecodedBytes+part.SkippedBytes, whole.DecodedBytes)
	}
	t.Logf("region %v of a %dx%d chunk: decoded %d of %d code-block bytes (%.0f%%), skipped %d",
		region, w, h, part.DecodedBytes, whole.DecodedBytes,
		100*float64(part.DecodedBytes)/float64(whole.DecodedBytes), part.SkippedBytes)
}

// TestHTJ2KPartialReducedResolution is what a sequence player actually wants:
// a chunk decoded at a fraction of its resolution, for a fraction of the cost.
//
// This replaces a test that pinned a refusal. The refusal was withdrawn in
// go-jpeg2000 v1.5.6, where it turned out to rest on the wrong comparison — a
// reduced decode was measured against a downsample of the full decode, which is
// not what it produces, and against the reference implementation's own reduced
// decode this library was already exact.
//
// What is checked here is what this package can check without an oracle: the
// dimensions and the cost. The values are the gate's business, and deliberately
// so — see TestHTJ2KReducedResolutionIsNotADownsample for why an obvious-looking
// range assertion here would be wrong.
func TestHTJ2KPartialReducedResolution(t *testing.T) {
	const w, h = 256, 256
	chunk, channels, _ := buildHTJ2KChunk(t, w, h)

	whole, err := HTJ2KDecompressPartial(chunk, channels, nil)
	if err != nil {
		t.Fatalf("whole chunk: %v", err)
	}

	for _, reduce := range []int{1, 2, 3} {
		res, err := HTJ2KDecompressPartial(chunk, channels,
			&HTJ2KDecodeOptions{ReduceResolution: reduce})
		if err != nil {
			t.Fatalf("reduce %d: %v", reduce, err)
		}
		wantW, wantH := w>>uint(reduce), h>>uint(reduce)
		if res.Width != wantW || res.Height != wantH {
			t.Fatalf("reduce %d produced %dx%d, want %dx%d",
				reduce, res.Width, res.Height, wantW, wantH)
		}
		if res.SkippedBytes == 0 {
			t.Errorf("reduce %d skipped no code-blocks; it decoded every resolution "+
				"and discarded the ones it had just spent the time on", reduce)
		}
		if res.DecodedBytes >= whole.DecodedBytes {
			t.Errorf("reduce %d decoded %d code-block bytes and the whole chunk %d; "+
				"a reduced decode must cost less", reduce, res.DecodedBytes, whole.DecodedBytes)
		}

		t.Logf("reduce %d: %dx%d, decoded %d of %d code-block bytes (%.0f%%), skipped %d",
			reduce, res.Width, res.Height, res.DecodedBytes, whole.DecodedBytes,
			100*float64(res.DecodedBytes)/float64(whole.DecodedBytes), res.SkippedBytes)
	}

	// The control: the whole-chunk path must still work, or this is satisfied
	// by a function that returns something smaller for every request.
	if whole.Width != w || whole.Height != h {
		t.Errorf("whole-chunk decode produced %dx%d, want %dx%d", whole.Width, whole.Height, w, h)
	}
}

// TestHTJ2KReducedResolutionIsNotADownsample records what a reduced decode of a
// float chunk actually produces, because the obvious assumption is wrong and
// acting on it has already cost this project a year of a working capability.
//
// An EXR HTJ2K chunk carries float samples as reinterpreted bit patterns under
// an NLT Type 3 point transform. The wavelet therefore runs over bit patterns,
// not values. That is exact and reversible for a full decode, which is all the
// format asks of it. But the LL band of a reduced decode is a lowpass of bit
// patterns, and a float's bit pattern is roughly logarithmic in its value, so
// the result is a log-domain average — and near zero, whose bit pattern is an
// outlier among its neighbours, it is not even that.
//
// Measured on a ramp over [0, 2): one level of reduction produces values from
// 2.2e-23 to 17.75. The reference implementation produces the same values, bit
// for bit, at every level — this is the format's behaviour and not a defect.
//
// The consequence is worth stating where someone will find it: a reduced
// decode is a correct JPEG 2000 operation and is NOT a proxy image. Anything
// wanting a viewable half-resolution frame has to downsample the samples, not
// the codestream.
func TestHTJ2KReducedResolutionIsNotADownsample(t *testing.T) {
	const w, h = 256, 256
	chunk, channels, want := buildHTJ2KChunk(t, w, h)

	lo, hi := want[0], want[0]
	for _, v := range want {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}

	res, err := HTJ2KDecompressPartial(chunk, channels, &HTJ2KDecodeOptions{ReduceResolution: 1})
	if err != nil {
		t.Fatalf("reduce 1: %v", err)
	}
	outside := 0
	rlo, rhi := float32(math.Inf(1)), float32(math.Inf(-1))
	for y := 0; y < res.Height; y++ {
		line := res.Data[y*res.BytesPerLine:]
		for x := 0; x < res.Width; x++ {
			v := math.Float32frombits(binary.LittleEndian.Uint32(line[x*4:]))
			if v < rlo {
				rlo = v
			}
			if v > rhi {
				rhi = v
			}
			if v < lo || v > hi {
				outside++
			}
		}
	}
	if outside == 0 {
		t.Fatalf("every reduced sample fell inside the chunk's range [%v, %v]; this test "+
			"exists because they do not, and if that has changed the documentation "+
			"above and in the roadmap is now wrong", lo, hi)
	}
	t.Logf("chunk range [%v, %v]; reduced by 1 the range is [%v, %v], with %d of %d "+
		"samples outside the chunk's own range — a reduced decode is not a downsample",
		lo, hi, rlo, rhi, outside, res.Width*res.Height)
}

// TestHTJ2KReducedResolutionOfAConstantChunk is the oracle-free correctness
// check: every resolution of a constant image is that constant, whatever the
// wavelet is running over, so this catches a decode that stopped in the wavelet
// domain without assuming anything about what a reduced decode of real content
// should look like.
func TestHTJ2KReducedResolutionOfAConstantChunk(t *testing.T) {
	const w, h = 128, 128
	const constant = float32(0.75)

	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: w, Height: h, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, w*h*4)
	for i := 0; i < w*h; i++ {
		binary.LittleEndian.PutUint32(src[i*4:], math.Float32bits(constant))
	}
	chunk, err := HTJ2KCompress(src, h, channels, 128)
	if err != nil {
		t.Fatalf("HTJ2KCompress: %v", err)
	}

	for _, reduce := range []int{0, 1, 2, 3} {
		var opts *HTJ2KDecodeOptions
		if reduce > 0 {
			opts = &HTJ2KDecodeOptions{ReduceResolution: reduce}
		}
		res, err := HTJ2KDecompressPartial(chunk, channels, opts)
		if err != nil {
			t.Fatalf("reduce %d: %v", reduce, err)
		}
		for y := 0; y < res.Height; y++ {
			line := res.Data[y*res.BytesPerLine:]
			for x := 0; x < res.Width; x++ {
				v := math.Float32frombits(binary.LittleEndian.Uint32(line[x*4:]))
				if v != constant {
					t.Fatalf("reduce %d sample (%d,%d) = %v, want %v; every level of a "+
						"constant chunk is that constant", reduce, x, y, v, constant)
				}
			}
		}
	}
}
