package exr

import (
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// PXR24 had no write-side coverage at all: nothing here asked whether a PXR24
// file this library produces is one the OpenEXR reference implementation can
// read, let alone whether it holds the image the writer was given. A round trip
// through this library's own decoder cannot answer that — it passes just as
// well when the encoder and the decoder share a mistake.
//
// The tests below start from files the reference implementation wrote
// (testdata/conformance/grad_*_none.exr), re-encode those exact samples as
// PXR24, and hand the result back to the reference.
//
// PXR24 is not one codec but three, one per pixel type, and they are held to
// different standards here because the format sets different standards:
//
//	HALF   stored verbatim, two bytes per sample. Lossless: the file this
//	       library writes must come back bit-identical to the reference's own
//	       uncompressed file.
//	UINT   stored verbatim, four bytes per sample, byte-planed and differenced.
//	       Lossless, and checked the same way.
//	FLOAT  reduced to 24 bits — sign, all 8 exponent bits, the top 15 mantissa
//	       bits — and therefore lossy, to the bound derived at
//	       pxr24FloatRelativeError.
//
// Skipped when oiiotool is not installed, like the other write-side tests.

// pxr24FloatRelativeError is the relative error PXR24 may introduce in a FLOAT
// sample.
//
// The 24-bit form keeps 15 explicit mantissa bits, so within a binade the
// representable values are 2^-15 apart relative to the value — that is the
// format's guarantee, and an encoder that truncated the low 8 bits would need
// all of it. This library's encoder rounds to nearest instead (see
// floatToFloat24 in compression/pxr24.go: it adds bit 7 before shifting), which
// halves the worst case to half an interval, 2^-16 = 1.52587890625e-05.
//
// The bound holds for normal floats. It does not hold for denormals, where the
// dropped bits are not relative to an implicit leading one, nor within one ulp
// of FLT_MAX, where the encoder falls back to truncation to avoid rounding up
// into infinity. The gradient fixtures contain neither.
const pxr24FloatRelativeError = 1.0 / 65536.0

// pxr24PrintSlack absorbs oiiotool's own rounding. --dumpdata prints nine
// decimal places, so a transcript value can be half of 1e-9 away from what the
// reference actually decoded, in either direction. That is far below PXR24's
// error at every sample in these fixtures — the smallest non-zero sample is
// around 1e-4, where the bound above is 1.5e-09 — but it is not zero, so it is
// allowed on top of the bound rather than pretended away.
const pxr24PrintSlack = 1e-9

var pxr24Cases = []struct {
	name    string
	fixture string
	typ     PixelType
	// exact records whether PXR24 is lossless for this pixel type by
	// specification, never by measurement.
	exact bool
}{
	{"half", "grad_half_none", PixelTypeHalf, true},
	{"uint", "grad_uint_none", PixelTypeUint, true},
	{"float", "grad_float_none", PixelTypeFloat, false},
}

// TestConformancePxr24WriteIsExactForHalfAndUint requires the reference
// implementation to read a PXR24 file this library wrote as bit-identical to
// the reference's own uncompressed file holding the same samples.
//
// Nothing about the comparison comes from this library: both operands go to
// oiiotool, one of them written by oiiotool in the first place, and every
// difference threshold is pinned to zero so a single altered sample fails.
func TestConformancePxr24WriteIsExactForHalfAndUint(t *testing.T) {
	oiiotool := lookOiiotool(t)

	for _, c := range pxr24Cases {
		if !c.exact {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			ref := filepath.Join(conformanceDir, c.fixture+".exr")
			src, w, h := readAllChannels(t, ref)
			ours := filepath.Join(t.TempDir(), "pxr24.exr")
			writePxr24(t, ours, src, c.typ, w, h)

			out, err := exec.Command(oiiotool,
				"--fail", "0", "--failpercent", "0", "--hardfail", "0",
				"--warn", "0", "--warnpercent", "0", "--hardwarn", "0",
				"--diff", ref, ours).CombinedOutput()
			text := string(out)
			if !strings.Contains(text, "PASS") {
				t.Fatalf("the reference implementation does not read our PXR24 %s file as identical to its own uncompressed file (%v):\n%s",
					c.name, err, text)
			}
		})
	}
}

// TestConformancePxr24WriteFloatIsWithinItsBound holds the FLOAT case to the
// precision the format specifies rather than to exactness: every sample the
// reference reads out of our PXR24 file must be within pxr24FloatRelativeError
// of the sample we handed the writer.
//
// It also requires the codec to be lossy somewhere in the image. A "PXR24"
// encoder that quietly stored 32-bit floats would satisfy the bound and be
// unreadable as PXR24 by anything that trusted the chunk size; here the
// reference reads the file, so the file really is PXR24, and at least one
// sample really did lose its low mantissa bits.
func TestConformancePxr24WriteFloatIsWithinItsBound(t *testing.T) {
	oiiotool := lookOiiotool(t)

	ref := filepath.Join(conformanceDir, "grad_float_none.exr")
	src, w, h := readAllChannels(t, ref)
	dir := t.TempDir()
	ours := filepath.Join(dir, "pxr24.exr")
	writePxr24(t, ours, src, PixelTypeFloat, w, h)

	// The reference implementation's own view of the file we wrote.
	golden := dumpGolden(t, oiiotool, ours, filepath.Join(dir, "pxr24.golden"))
	if len(golden.pixels) != w*h {
		t.Fatalf("the reference read %d pixels from our file, want %d", len(golden.pixels), w*h)
	}

	worstRel, worstAbs, changed := 0.0, 0.0, 0
	for ci, name := range golden.channels {
		want, ok := src[name]
		if !ok {
			t.Fatalf("the reference reports a channel %q we did not write", name)
		}
		for i, px := range golden.pixels {
			have := px[ci]
			diff := math.Abs(have - want[i])
			bound := pxr24FloatRelativeError*math.Abs(want[i]) + pxr24PrintSlack
			if diff > bound {
				t.Fatalf("channel %s sample %d: we wrote %v, the reference reads %v; error %g exceeds PXR24's bound %g (relative %g)",
					name, i, want[i], have, diff, bound, pxr24FloatRelativeError)
			}
			if diff > pxr24PrintSlack {
				changed++
			}
			if diff > worstAbs {
				worstAbs = diff
			}
			if want[i] != 0 && diff/math.Abs(want[i]) > worstRel {
				worstRel = diff / math.Abs(want[i])
			}
		}
	}
	if changed == 0 {
		t.Fatalf("no sample lost any precision; this file cannot be PXR24-encoded FLOAT data")
	}
	t.Logf("PXR24 FLOAT: %d of %d samples quantised, worst absolute error %g, worst relative error %g (bound %g)",
		changed, len(golden.pixels)*len(golden.channels), worstAbs, worstRel, pxr24FloatRelativeError)
}

// writePxr24 writes a single-pixel-type PXR24 scanline file holding src.
func writePxr24(t *testing.T, path string, src map[string][]float64, typ PixelType, w, h int) {
	t.Helper()

	names := []string{"A", "B", "G", "R"}
	header := NewScanlineHeader(w, h)
	header.SetCompression(CompressionPXR24)
	cl := NewChannelList()
	for _, name := range names {
		if _, ok := src[name]; !ok {
			t.Fatalf("source image has no channel %s (has %v)", name, keysOf(src))
		}
		cl.Add(Channel{Name: name, Type: typ, XSampling: 1, YSampling: 1})
	}
	header.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	wr, err := NewScanlineWriter(f, header)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}
	fb := NewFrameBuffer()
	for _, name := range names {
		vals := src[name]
		switch typ {
		case PixelTypeHalf:
			buf := make([]half.Half, w*h)
			for i, v := range vals {
				buf[i] = half.FromFloat32(float32(v))
			}
			fb.Set(name, NewSliceFromHalf(buf, w, h))
		case PixelTypeFloat:
			buf := make([]float32, w*h)
			for i, v := range vals {
				buf[i] = float32(v)
			}
			fb.Set(name, NewSliceFromFloat32(buf, w, h))
		case PixelTypeUint:
			buf := make([]uint32, w*h)
			for i, v := range vals {
				buf[i] = uint32(v)
			}
			fb.Set(name, NewSliceFromUint32(buf, w, h))
		default:
			t.Fatalf("unexpected pixel type %v", typ)
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	// Close flushes the chunk offset table; without it the file is truncated.
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
