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

// DWA is lossy, so its fixtures cannot be checked against the pattern they
// were made from: the reference implementation does not read its own DWA file
// back as the image it started with. What can be checked, and is the only
// thing worth checking, is that this library reads a reference-written DWA
// file as the same image the reference reads out of it. Each fixture therefore
// carries its own golden, produced by oiiotool reading the fixture.
//
// The tests below hold decoding to two separate standards:
//
//   - Every sample must be within dwaSampleTolerance of the reference's value.
//     That bound is a bug detector, not a quality target: a decoder that got
//     the block order, the colour conversion or the zig-zag wrong would be off
//     by far more than this.
//   - At least dwaExactFraction of the samples must equal the reference's to
//     the precision the golden records them at, which is the assertion that
//     actually has teeth. "Exact" here means equalSample, the same comparison
//     the lossless conformance tests use, since oiiotool prints nine decimal
//     places and that is not enough to round-trip a float32.
//
// The second bound is what the numbers support. Decoding the fixtures here
// reproduces every sample the reference reads; across a wider sweep of
// reference-written DWAA and DWAB files (8x8 up to 800x800, half and float,
// R,G,B,A and Y,RY,BY,A and R,G,B,Z, including high dynamic range content with
// values in the hundreds and files containing NaN and infinity) the worst case
// was 51 samples out of 1920000 differing, each tracing back to a one-step
// difference in the encoded half. The largest relative difference seen was
// 4.4e-3, on a sample near 460 where DWA's exponential coding curve multiplies
// one step of the coded value by about 1000. These come from float rounding
// inside the inverse DCT, where the reference's C compiler and Go do not
// associate and round identically; they are not a difference in what the
// format says.
const (
	dwaSampleTolerance = 0.02
	dwaExactFraction   = 0.999
)

// dwaConformanceFixtures lists the reference-written DWA fixtures. Each has a
// golden of the same name.
var dwaConformanceFixtures = []string{
	"grad_half_dwaa",
	"grad_half_dwab",
	"grad_float_dwaa",
	"grad_float_dwab",
	"gradz_half_dwaa",
	"noise_half_dwaa",
}

// TestConformanceDecodesReferenceDwa is the true-value test for DWA: decode a
// file the OpenEXR reference implementation wrote and require the pixel values
// that implementation reads back from it.
func TestConformanceDecodesReferenceDwa(t *testing.T) {
	for _, name := range dwaConformanceFixtures {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(conformanceDir, name+".exr")
			goldenPath := filepath.Join(conformanceDir, name+".golden")
			for _, p := range []string{path, goldenPath} {
				if _, err := os.Stat(p); err != nil {
					t.Fatalf("missing %s; run scripts/gen-conformance-testdata.sh", p)
				}
			}
			golden := parseGolden(t, goldenPath)
			got, _, _ := readAllChannels(t, path)
			compareToLossyGolden(t, path, got, golden)
		})
	}
}

// TestConformanceDwaTiledDecodesReferenceFile does the same for a tiled DWA
// file, which lays its chunks out by tile rather than by scanline.
func TestConformanceDwaTiledDecodesReferenceFile(t *testing.T) {
	oiiotool := lookOiiotool(t)
	dir := t.TempDir()
	src := filepath.Join(dir, "tiled_dwaa.exr")
	run(t, oiiotool, "--pattern",
		"fill:topleft=0,0,0,1:topright=1,0,0.25,1:bottomleft=0,1,0.5,1:bottomright=1,1,0.75,1",
		"35x40", "4", "-d", "half", "--compression", "dwaa", "--tile", "16", "16", "-o", src)

	golden := dumpGolden(t, oiiotool, src, filepath.Join(dir, "tiled.golden"))
	got := readAllTiledChannels(t, src)
	compareToLossyGolden(t, src, got, golden)
}

// TestConformanceDwaIsReadableByReference closes the loop on the write side:
// a DWA file this library produces has to be readable by the reference
// implementation, and has to hold the image it was given to within DWA's
// loss. A round trip through this library alone would not show that.
func TestConformanceDwaIsReadableByReference(t *testing.T) {
	oiiotool := lookOiiotool(t)

	for _, comp := range []Compression{CompressionDWAA, CompressionDWAB} {
		t.Run(comp.String(), func(t *testing.T) {
			dir := t.TempDir()
			// Start from a reference-written uncompressed file so the input is
			// not something this library invented either.
			srcPath := filepath.Join(conformanceDir, "grad_half_none.exr")
			src, w, h := readAllChannels(t, srcPath)

			header := NewScanlineHeader(w, h)
			header.SetCompression(comp)
			cl := NewChannelList()
			names := []string{"A", "B", "G", "R"}
			for _, name := range names {
				cl.Add(Channel{Name: name, Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
			}
			header.SetChannels(cl)

			outPath := filepath.Join(dir, "ours.exr")
			f, err := os.Create(outPath)
			if err != nil {
				t.Fatalf("create: %v", err)
			}
			wr, err := NewScanlineWriter(f, header)
			if err != nil {
				t.Fatalf("NewScanlineWriter: %v", err)
			}
			fb := NewFrameBuffer()
			for _, name := range names {
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

			// The reference implementation's view of the file we wrote.
			golden := dumpGolden(t, oiiotool, outPath, filepath.Join(dir, "ours.golden"))

			// It must be the image we handed the writer, to within DWA's loss.
			for ci, name := range golden.channels {
				want, ok := src[name]
				if !ok {
					t.Fatalf("reference reports a channel %q we did not write", name)
				}
				for i, px := range golden.pixels {
					have := px[ci]
					if diff := math.Abs(have - want[i]); diff > dwaSampleTolerance {
						t.Fatalf("channel %s sample %d: we wrote %v, the reference reads %v",
							name, i, want[i], have)
					}
				}
			}

			// And reading it back here must agree with the reference too.
			got, _, _ := readAllChannels(t, outPath)
			compareToLossyGolden(t, outPath, got, golden)
		})
	}
}

// compareToLossyGolden asserts that every decoded sample is within
// dwaSampleTolerance of the reference's, and that nearly all of them are exact.
func compareToLossyGolden(t *testing.T, path string, got map[string][]float64, g *goldenImage) {
	t.Helper()

	total, exact := 0, 0
	for ci, name := range g.channels {
		vals, ok := got[name]
		if !ok {
			t.Fatalf("%s: decoded image has no channel %q (has %v)", path, name, keysOf(got))
		}
		if len(vals) != len(g.pixels) {
			t.Fatalf("%s: channel %s has %d samples, golden has %d",
				path, name, len(vals), len(g.pixels))
		}
		for i, px := range g.pixels {
			want, have := px[ci], vals[i]
			total++
			if equalSample(have, want) {
				exact++
				continue
			}
			diff := math.Abs(have - want)
			scale := math.Max(1, math.Max(math.Abs(have), math.Abs(want)))
			if diff/scale > dwaSampleTolerance {
				t.Fatalf("%s: channel %s sample %d = %v, the reference reads %v",
					path, name, i, have, want)
			}
		}
	}
	if total == 0 {
		t.Fatalf("%s: golden has no samples", path)
	}
	if frac := float64(exact) / float64(total); frac < dwaExactFraction {
		t.Errorf("%s: only %d of %d samples (%.4f) match the reference, want at least %.4f",
			path, exact, total, frac, dwaExactFraction)
	}
}

// readAllTiledChannels is readAllChannels for a tiled file.
func readAllTiledChannels(t *testing.T, path string) map[string][]float64 {
	t.Helper()

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	defer f.Close()

	r, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader(%s): %v", path, err)
	}
	dw := r.DataWindow()
	w := int(dw.Max.X-dw.Min.X) + 1
	h := int(dw.Max.Y-dw.Min.Y) + 1

	cl := r.Header().Channels()
	out := make(map[string][]float64, cl.Len())
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
		default:
			t.Fatalf("%s: unexpected pixel type %v", path, ch.Type)
		}
	}
	r.SetFrameBuffer(fb)
	if err := r.ReadTiles(0, 0, r.Header().NumXTiles(0)-1, r.Header().NumYTiles(0)-1); err != nil {
		t.Fatalf("ReadTiles(%s): %v", path, err)
	}
	for _, fn := range finish {
		fn()
	}
	return out
}

// lookOiiotool finds the reference implementation's command line tool, which
// stands in for the reference implementation itself in the write-side tests.
// Tests that need it skip when it is absent so a clean checkout still runs.
func lookOiiotool(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("oiiotool")
	if err != nil {
		t.Skip("oiiotool not found in PATH; install OpenImageIO to run this test")
	}
	return path
}

// run executes a command and returns its standard output, failing the test if
// it does not succeed.
func run(t *testing.T, name string, args ...string) string {
	t.Helper()
	cmd := exec.Command(name, args...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s %v: %v", name, args, err)
	}
	return string(out)
}

// dumpGolden records the reference implementation's own view of a file, in the
// format parseGolden reads: the channel list line followed by the --dumpdata
// transcript with its header line removed.
func dumpGolden(t *testing.T, oiiotool, image, dest string) *goldenImage {
	t.Helper()

	var channelLine string
	for _, line := range strings.Split(run(t, oiiotool, "--info", "-v", image), "\n") {
		if i := strings.Index(line, "channel list:"); i >= 0 {
			channelLine = line[i:]
			break
		}
	}
	if channelLine == "" {
		t.Fatalf("oiiotool --info reported no channel list for %s", image)
	}
	lines := strings.Split(run(t, oiiotool, "--dumpdata", image), "\n")
	if len(lines) < 2 {
		t.Fatalf("oiiotool --dumpdata produced nothing for %s", image)
	}
	body := channelLine + "\n" + strings.Join(lines[1:], "\n")
	if err := os.WriteFile(dest, []byte(body), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
	return parseGolden(t, dest)
}
