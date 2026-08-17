package exr

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// The conformance corpus in testdata/conformance is external ground truth: the
// .exr files are written by the OpenEXR reference implementation and the
// .golden files hold the pixel values that same implementation reads back from
// them. Nothing in the corpus is produced by this library.
//
// That distinction is the entire point of these tests. A round-trip test
// (write with go-openexr, read with go-openexr, compare) passes whenever the
// encoder and decoder are inverses of each other, even when both deviate
// identically from the specification. Exactly that happened: the ZIP and RLE
// codecs applied their predictor and byte-reorder passes in the wrong order and
// omitted OpenEXR's +128 predictor bias, and RLE inverted its control-byte
// convention. Every round-trip test passed while the library could not read or
// write a single conforming ZIP or RLE file.
//
// Regenerate the corpus with scripts/gen-conformance-testdata.sh.

const conformanceDir = "testdata/conformance"

// goldenImage is the reference implementation's own view of a fixture.
type goldenImage struct {
	channels []string
	// types[i] is the pixel type oiiotool named for channels[i], when the
	// transcript names one. It only does so for images whose channels are not
	// all the same type, e.g. `channel list: R (half), G (half), Z (float)`.
	types []string
	// pixels[i] holds one value per channel, in the order named by channels.
	pixels [][]float64
}

// parseGolden reads an oiiotool --dumpdata transcript.
func parseGolden(t *testing.T, path string) *goldenImage {
	t.Helper()

	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open golden: %v", err)
	}
	defer f.Close()

	g := &goldenImage{}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if rest, ok := strings.CutPrefix(line, "channel list:"); ok {
			for _, entry := range strings.Split(rest, ",") {
				name, typ, _ := strings.Cut(strings.TrimSpace(entry), " ")
				g.channels = append(g.channels, name)
				g.types = append(g.types, strings.Trim(typ, "()"))
			}
			continue
		}
		_, rest, ok := strings.Cut(line, "):")
		if !ok || !strings.HasPrefix(line, "Pixel (") {
			continue
		}
		// Integer formats print the raw values followed by a normalised copy
		// in parentheses; the raw values are the ones worth asserting on.
		if i := strings.IndexByte(rest, '('); i >= 0 {
			rest = rest[:i]
		}
		var vals []float64
		for _, tok := range strings.Fields(rest) {
			v, err := strconv.ParseFloat(tok, 64)
			if err != nil {
				t.Fatalf("golden %s: bad value %q: %v", path, tok, err)
			}
			vals = append(vals, v)
		}
		g.pixels = append(g.pixels, vals)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if len(g.channels) == 0 {
		t.Fatalf("golden %s: no channel list", path)
	}
	if len(g.pixels) == 0 {
		t.Fatalf("golden %s: no pixels", path)
	}
	return g
}

// readAllChannels decodes every channel of a scanline file as float64, keyed by
// channel name, converting each pixel type through its natural representation.
func readAllChannels(t *testing.T, path string) (map[string][]float64, int, int) {
	t.Helper()

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	defer f.Close()

	r, err := NewScanlineReader(f)
	if err != nil {
		t.Fatalf("NewScanlineReader(%s): %v", path, err)
	}

	dw := r.DataWindow()
	w := int(dw.Max.X-dw.Min.X) + 1
	h := int(dw.Max.Y-dw.Min.Y) + 1

	cl := r.Header().Channels()
	out := make(map[string][]float64, cl.Len())

	// Bind every channel in one pass so the test exercises the same
	// multi-channel framebuffer path real callers use.
	fb := NewFrameBuffer()
	finish := make([]func(), 0, cl.Len())
	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)
		name := ch.Name
		switch ch.Type {
		case PixelTypeHalf:
			buf := make([]half.Half, w*h)
			fb.Set(name, NewSliceFromHalf(buf, w, h))
			finish = append(finish, func() {
				vals := make([]float64, len(buf))
				for j, v := range buf {
					vals[j] = float64(v.Float32())
				}
				out[name] = vals
			})
		case PixelTypeFloat:
			buf := make([]float32, w*h)
			fb.Set(name, NewSliceFromFloat32(buf, w, h))
			finish = append(finish, func() {
				vals := make([]float64, len(buf))
				for j, v := range buf {
					vals[j] = float64(v)
				}
				out[name] = vals
			})
		case PixelTypeUint:
			buf := make([]uint32, w*h)
			fb.Set(name, NewSliceFromUint32(buf, w, h))
			finish = append(finish, func() {
				vals := make([]float64, len(buf))
				for j, v := range buf {
					vals[j] = float64(v)
				}
				out[name] = vals
			})
		default:
			t.Fatalf("%s: unexpected pixel type %v", path, ch.Type)
		}
	}
	r.SetFrameBuffer(fb)

	if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
		t.Fatalf("ReadPixels(%s): %v", path, err)
	}
	for _, fn := range finish {
		fn()
	}
	return out, w, h
}

// compareToGolden asserts every decoded sample equals the reference value.
// uint32 golden values arrive normalised by oiiotool, so they are compared as
// a ratio; float and half values must match bit-for-bit after conversion.
func compareToGolden(t *testing.T, path string, got map[string][]float64, g *goldenImage, normalise float64) {
	t.Helper()

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
			if ci >= len(px) {
				t.Fatalf("%s: golden pixel %d has %d values, need %d", path, i, len(px), len(g.channels))
			}
			want := px[ci]
			have := vals[i]
			if normalise != 0 {
				have /= normalise
			}
			if equalSample(have, want) {
				continue
			}
			mismatches++
			if reported < maxReport {
				reported++
				t.Errorf("%s: channel %s pixel %d = %v, reference says %v",
					path, name, i, have, want)
			}
		}
		if mismatches > maxReport {
			t.Errorf("%s: channel %s: %d of %d samples differ from the reference implementation",
				path, name, mismatches, len(vals))
		}
	}
}

// equalSample compares a decoded sample against the reference transcript.
// oiiotool prints 9 decimal places, which is not enough to round-trip a float32
// exactly, so the comparison allows a relative error just below that printing
// precision rather than demanding bit equality against a truncated decimal.
func equalSample(have, want float64) bool {
	if math.IsNaN(have) || math.IsNaN(want) {
		return math.IsNaN(have) && math.IsNaN(want)
	}
	if have == want {
		return true
	}
	if math.IsInf(have, 0) || math.IsInf(want, 0) {
		return false
	}
	diff := math.Abs(have - want)
	scale := math.Max(math.Abs(have), math.Abs(want))
	if scale < 1e-6 {
		return diff < 1e-9
	}
	return diff/scale < 1e-6
}

func keysOf(m map[string][]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// conformanceGroups lists the fixture groups and the compressions each was
// written with. Every compression of a group must decode to the same golden.
var conformanceGroups = []struct {
	group     string
	normalise float64 // divide decoded samples by this before comparing
}{
	{"grad_half", 0},
	{"grad_float", 0},
	{"grad_uint", 0},
	{"noise_half", 0},
	{"noise_float", 0},
}

var conformanceCompressions = []string{"none", "rle", "zip", "zips", "piz"}

// TestConformanceDecodesReferenceFiles is the core true-value test: for every
// lossless compression, decode a file written by the OpenEXR reference
// implementation and require the exact pixel values that implementation reads
// from it.
func TestConformanceDecodesReferenceFiles(t *testing.T) {
	for _, grp := range conformanceGroups {
		goldenPath := filepath.Join(conformanceDir, grp.group+".golden")
		if _, err := os.Stat(goldenPath); err != nil {
			t.Fatalf("missing golden %s; run scripts/gen-conformance-testdata.sh", goldenPath)
		}
		golden := parseGolden(t, goldenPath)

		for _, comp := range conformanceCompressions {
			name := fmt.Sprintf("%s_%s", grp.group, comp)
			t.Run(name, func(t *testing.T) {
				path := filepath.Join(conformanceDir, name+".exr")
				if _, err := os.Stat(path); err != nil {
					t.Fatalf("missing fixture %s; run scripts/gen-conformance-testdata.sh", path)
				}
				got, _, _ := readAllChannels(t, path)
				compareToGolden(t, path, got, golden, grp.normalise)
			})
		}
	}
}

// TestConformanceCompressionsAgree checks that every compression of a group
// decodes identically to the uncompressed member of that group. This catches a
// codec that is wrong in a way the golden's printed precision might tolerate,
// and localises failures to a specific codec.
func TestConformanceCompressionsAgree(t *testing.T) {
	for _, grp := range conformanceGroups {
		basePath := filepath.Join(conformanceDir, grp.group+"_none.exr")
		if _, err := os.Stat(basePath); err != nil {
			t.Fatalf("missing fixture %s; run scripts/gen-conformance-testdata.sh", basePath)
		}
		base, _, _ := readAllChannels(t, basePath)

		for _, comp := range conformanceCompressions {
			if comp == "none" {
				continue
			}
			name := fmt.Sprintf("%s_%s", grp.group, comp)
			t.Run(name, func(t *testing.T) {
				got, _, _ := readAllChannels(t, filepath.Join(conformanceDir, name+".exr"))
				for ch, want := range base {
					have, ok := got[ch]
					if !ok {
						t.Errorf("%s: missing channel %s", name, ch)
						continue
					}
					diff := 0
					for i := range want {
						if want[i] != have[i] && !(math.IsNaN(want[i]) && math.IsNaN(have[i])) {
							diff++
						}
					}
					if diff > 0 {
						t.Errorf("%s: channel %s differs from %s_none in %d of %d samples",
							name, ch, grp.group, diff, len(want))
					}
				}
			})
		}
	}
}

// TestConformanceRoundTripIsInteroperable closes the loop on the write side.
// It decodes a reference fixture, re-encodes it with every compression, and
// decodes the result again, requiring the reference's values throughout.
//
// This does not fully prove the encoder is spec-conformant on its own — a
// symmetric encoder/decoder pair would still pass. It is meaningful only
// because TestConformanceDecodesReferenceFiles independently pins the decoder
// to external ground truth; together they pin the encoder too.
func TestConformanceRoundTripIsInteroperable(t *testing.T) {
	golden := parseGolden(t, filepath.Join(conformanceDir, "grad_half.golden"))
	src, w, h := readAllChannels(t, filepath.Join(conformanceDir, "grad_half_none.exr"))

	for _, comp := range []Compression{
		CompressionNone, CompressionRLE, CompressionZIP, CompressionZIPS, CompressionPIZ,
	} {
		t.Run(comp.String(), func(t *testing.T) {
			header := NewScanlineHeader(w, h)
			header.SetCompression(comp)
			cl := NewChannelList()
			for _, name := range golden.channels {
				cl.Add(Channel{Name: name, Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
			}
			header.SetChannels(cl)

			path := filepath.Join(t.TempDir(), "out.exr")
			f, err := os.Create(path)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			wr, err := NewScanlineWriter(f, header)
			if err != nil {
				t.Fatalf("NewScanlineWriter: %v", err)
			}
			fb := NewFrameBuffer()
			for _, name := range golden.channels {
				buf := make([]half.Half, w*h)
				for i, v := range src[name] {
					buf[i] = half.FromFloat32(float32(v))
				}
				fb.Set(name, NewSliceFromHalf(buf, w, h))
			}
			wr.SetFrameBuffer(fb)
			if err := wr.WritePixels(0, h-1); err != nil {
				t.Fatalf("WritePixels: %v", err)
			}
			if err := wr.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}
			f.Close()

			got, _, _ := readAllChannels(t, path)
			compareToGolden(t, path, got, golden, 0)
		})
	}
}
