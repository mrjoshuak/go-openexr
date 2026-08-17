package exr

import (
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// B44 stores HALF channels as quantised 4x4 blocks and copies UINT and FLOAT
// channels through uncompressed, interleaved in the same per-scanline channel
// order as every other codec. This library used to write a run of zero bytes for
// each non-HALF channel and to skip over those bytes on read, so a mixed-type
// B44 file lost its FLOAT and UINT channels entirely, in both directions,
// without a single test noticing.
//
// The fixtures here are the reference implementation's answer to that: the
// mixed_* and flat_* files in testdata/conformance are written by oiiotool, and
// each carries its own golden — the values oiiotool itself reads back from that
// exact file. Because B44 is lossy for HALF, a lossy fixture cannot share a
// golden with its uncompressed twin the way the ZIP and PIZ groups do; but the
// FLOAT and UINT channels are untouched by the codec, so those must match the
// uncompressed twin exactly, which is what TestConformanceB44PassthroughIsExact
// asserts.

// b44MixedFixtures are the reference-written fixtures with mixed pixel types,
// each with the transcript of the reference implementation reading it. A golden
// is shared only where the generator verified the reference reads both files
// identically: mixed_b44a matches mixed_b44 because this data has no flat 4x4
// blocks, and flat_b44a matches flat_none because B44A encodes a flat block
// exactly.
//
// flat_b44a is the fixture that moves the passthrough: its HALF channels are
// constant, so B44A stores each 4x4 block in 3 bytes instead of 14, and the
// FLOAT and UINT runs that follow the HALF runs shift accordingly.
var b44MixedFixtures = []struct{ fixture, golden string }{
	{"mixed_none", "mixed_none"},
	{"mixed_b44", "mixed_b44"},
	{"mixed_b44a", "mixed_b44"},
	{"flat_none", "flat"},
	{"flat_b44a", "flat"},
}

// TestConformanceB44MixedPixelTypes decodes each mixed-type fixture and requires
// the exact values the reference implementation reads from the same file.
func TestConformanceB44MixedPixelTypes(t *testing.T) {
	for _, f := range b44MixedFixtures {
		t.Run(f.fixture, func(t *testing.T) {
			path := filepath.Join(conformanceDir, f.fixture+".exr")
			goldenPath := filepath.Join(conformanceDir, f.golden+".golden")
			for _, p := range []string{path, goldenPath} {
				if _, err := os.Stat(p); err != nil {
					t.Fatalf("missing %s; run scripts/gen-conformance-testdata.sh", p)
				}
			}
			golden := parseGolden(t, goldenPath)
			got, _, _ := readAllChannels(t, path)
			compareMixedToGolden(t, path, got, golden)
		})
	}
}

// compareMixedToGolden is compareToGolden for a transcript whose channels are
// not all of the same pixel type. oiiotool prints UINT samples normalised to
// 0..1 when it prints them next to FLOAT ones, so those are scaled back before
// comparing; everything else is compared as printed.
func compareMixedToGolden(t *testing.T, path string, got map[string][]float64, g *goldenImage) {
	t.Helper()

	const uintScale = float64(1 << 32)
	const maxReport = 5
	reported := 0

	for ci, name := range g.channels {
		vals, ok := got[name]
		if !ok {
			t.Errorf("%s: decoded image has no channel %q (has %v)", path, name, keysOf(got))
			continue
		}
		if len(vals) != len(g.pixels) {
			t.Errorf("%s: channel %s has %d samples, golden has %d", path, name, len(vals), len(g.pixels))
			continue
		}
		mismatches := 0
		for i, px := range g.pixels {
			have := vals[i]
			if g.types[ci] == "uint" {
				have /= uintScale
			}
			if equalPrinted(have, px[ci]) {
				continue
			}
			mismatches++
			if reported < maxReport {
				reported++
				t.Errorf("%s: channel %s (%s) pixel %d = %v, reference says %v",
					path, name, g.types[ci], i, have, px[ci])
			}
		}
		if mismatches > maxReport {
			t.Errorf("%s: channel %s: %d of %d samples differ from the reference implementation",
				path, name, mismatches, len(vals))
		}
	}
}

// equalPrinted compares a decoded sample against a transcript value. oiiotool
// prints nine decimal places, so a sample near 1e-5 — which HALF quantisation
// produces readily — reaches the transcript with only four significant digits.
// This allows the transcript's own rounding (half of 1e-9) on top of the
// relative slack equalSample applies; that is far below the spacing of
// neighbouring half values anywhere in this range, so it cannot hide a wrong
// sample.
func equalPrinted(have, want float64) bool {
	return equalSample(have, want) || math.Abs(have-want) <= 1e-9
}

// TestConformanceB44PassthroughIsExact pins the property that separates a
// passed-through channel from a compressed one: B44 must not alter a FLOAT or
// UINT sample at all. Each lossy fixture is compared against its uncompressed
// twin, which the golden test above independently ties to the reference.
func TestConformanceB44PassthroughIsExact(t *testing.T) {
	for _, pair := range []struct{ lossy, none string }{
		{"mixed_b44", "mixed_none"},
		{"mixed_b44a", "mixed_none"},
		{"flat_b44a", "flat_none"},
	} {
		t.Run(pair.lossy, func(t *testing.T) {
			base, _, _ := readAllChannels(t, filepath.Join(conformanceDir, pair.none+".exr"))
			got, _, _ := readAllChannels(t, filepath.Join(conformanceDir, pair.lossy+".exr"))

			// Z is FLOAT and id is UINT in these fixtures; R, G and B are HALF
			// and therefore quantised by the codec.
			for _, name := range []string{"Z", "id"} {
				want, ok := base[name]
				if !ok {
					t.Fatalf("%s: uncompressed twin has no channel %s", pair.none, name)
				}
				have, ok := got[name]
				if !ok {
					t.Fatalf("%s: decoded image has no channel %s", pair.lossy, name)
				}
				zeros := 0
				for i := range want {
					if have[i] == 0 {
						zeros++
					}
					if have[i] != want[i] {
						t.Fatalf("%s: channel %s sample %d = %v, uncompressed twin has %v; B44 must pass this channel through untouched",
							pair.lossy, name, i, have[i], want[i])
					}
				}
				if zeros == len(want) {
					t.Fatalf("%s: channel %s decoded to all zeros", pair.lossy, name)
				}
			}

			// The HALF channels are lossy, but they must still be recognisably
			// the same image rather than, say, shifted by the passthrough runs.
			for _, name := range []string{"R", "G", "B"} {
				want := base[name]
				have := got[name]
				for i := range want {
					if diff := math.Abs(have[i] - want[i]); diff > b44HalfTolerance {
						t.Fatalf("%s: channel %s sample %d = %v, uncompressed twin has %v (tolerance %v)",
							pair.lossy, name, i, have[i], want[i], b44HalfTolerance)
					}
				}
			}
		})
	}
}

// b44HalfTolerance bounds B44's quantisation error for these fixtures. B44
// encodes a 4x4 block as a base value plus 6-bit differences at a shared shift,
// so its absolute error scales with the block's dynamic range; the HALF channels
// in the mixed fixtures span 0..1.
const b44HalfTolerance = 0.02

// TestConformanceB44WriteIsReadableByReference closes the loop on the write
// side with the reference implementation itself: it re-encodes a fixture as B44
// and B44A, hands the result to oiiotool, and requires oiiotool's values.
//
// This is the only assertion here that the encoder is interoperable. A
// round-trip through this library's own decoder would pass just as well if the
// encoder wrote the FLOAT and UINT runs at an offset no other implementation
// agrees with.
//
// Skipped when oiiotool is not installed.
func TestConformanceB44WriteIsReadableByReference(t *testing.T) {
	oiiotool, err := exec.LookPath("oiiotool")
	if err != nil {
		t.Skip("oiiotool not installed; install OpenImageIO to run the write-side conformance test")
	}

	src, w, h := readAllChannels(t, filepath.Join(conformanceDir, "mixed_none.exr"))
	types := map[string]PixelType{
		"R": PixelTypeHalf, "G": PixelTypeHalf, "B": PixelTypeHalf,
		"Z": PixelTypeFloat, "id": PixelTypeUint,
	}

	for _, comp := range []Compression{CompressionB44, CompressionB44A} {
		t.Run(comp.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "out.exr")
			writeMixedImage(t, path, comp, src, types, w, h)

			golden := oiiotoolDump(t, oiiotool, path)
			if len(golden.pixels) != w*h {
				t.Fatalf("oiiotool read %d pixels from our file, want %d", len(golden.pixels), w*h)
			}

			for ci, name := range golden.channels {
				want, ok := src[name]
				if !ok {
					t.Fatalf("oiiotool reports an unexpected channel %q in our file", name)
				}
				for i, px := range golden.pixels {
					have := px[ci]
					if golden.types[ci] == "uint" {
						have *= float64(uint64(1) << 32)
					}
					switch types[name] {
					case PixelTypeHalf:
						if diff := math.Abs(have - want[i]); diff > b44HalfTolerance {
							t.Fatalf("%s: oiiotool reads channel %s pixel %d as %v, we encoded %v (tolerance %v)",
								comp, name, i, have, want[i], b44HalfTolerance)
						}
					default:
						// Passed through untouched: the only slack is
						// oiiotool's printing precision.
						if !equalSample(have, want[i]) {
							t.Fatalf("%s: oiiotool reads channel %s pixel %d as %v, we encoded %v; this channel is passed through and must be identical",
								comp, name, i, have, want[i])
						}
					}
				}
			}
		})
	}
}

// writeMixedImage writes a scanline file with per-channel pixel types.
func writeMixedImage(t *testing.T, path string, comp Compression, src map[string][]float64, types map[string]PixelType, w, h int) {
	t.Helper()

	header := NewScanlineHeader(w, h)
	header.SetCompression(comp)
	cl := NewChannelList()
	for name := range types {
		cl.Add(Channel{Name: name, Type: types[name], XSampling: 1, YSampling: 1})
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
	for name, typ := range types {
		vals, ok := src[name]
		if !ok {
			t.Fatalf("source image has no channel %s", name)
		}
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
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// oiiotoolDump runs the reference implementation over a file we wrote and parses
// what it read, in the same transcript form as the committed goldens.
func oiiotoolDump(t *testing.T, oiiotool, path string) *goldenImage {
	t.Helper()

	info, err := exec.Command(oiiotool, "--info", "-v", path).CombinedOutput()
	if err != nil {
		t.Fatalf("oiiotool --info on our own file failed: %v\n%s", err, info)
	}
	var chanLine string
	for _, line := range strings.Split(string(info), "\n") {
		if strings.Contains(line, "channel list:") {
			chanLine = strings.TrimSpace(line)
		}
	}
	if chanLine == "" {
		t.Fatalf("oiiotool --info printed no channel list for our file:\n%s", info)
	}

	dump, err := exec.Command(oiiotool, "--dumpdata", path).Output()
	if err != nil {
		t.Fatalf("oiiotool --dumpdata on our own file failed: %v", err)
	}

	// parseGolden reads a transcript from disk, so reassemble one.
	transcript := filepath.Join(t.TempDir(), "dump.golden")
	lines := strings.SplitN(string(dump), "\n", 2)
	if len(lines) != 2 {
		t.Fatalf("oiiotool --dumpdata produced no pixels:\n%s", dump)
	}
	if err := os.WriteFile(transcript, []byte(fmt.Sprintf("%s\n%s", chanLine, lines[1])), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	return parseGolden(t, transcript)
}
