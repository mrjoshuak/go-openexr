package exr

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// These tests decode the official ASWF conformance corpus
// (github.com/AcademySoftwareFoundation/openexr-images) and require the exact
// pixel values the OpenEXR reference implementation reads from the same files.
//
// The images are large, so they are not committed; testdata/download.sh fetches
// them and the tests skip when they are absent. What IS committed is
// testdata/openexr_images.golden, a digest per image produced by
// scripts/gen-reference-goldens.py reading each file through OpenImageIO. The
// digest covers every sample, not summary statistics: min/max/mean are
// insensitive to a permutation of the pixels, and a broken PIZ decode of
// AllHalfValues.exr reproduced the reference's min, max, mean, NaN count and
// Inf count exactly while nearly every pixel was wrong.

const (
	referenceCorpusDir  = "../testdata/openexr-images"
	referenceGoldenFile = "testdata/openexr_images.golden"
)

// canonicalNaN is substituted for any NaN before hashing. NaN payload bits are
// not preserved identically by every half-to-float conversion and carry no
// image meaning, so comparing them would be a false failure.
const canonicalNaN = 0x7FC00000

type referenceEntry struct {
	file     string
	width    int
	height   int
	channels []string
	digest   string
}

func loadReferenceGoldens(t *testing.T) []referenceEntry {
	t.Helper()

	f, err := os.Open(referenceGoldenFile)
	if err != nil {
		t.Fatalf("open %s: %v", referenceGoldenFile, err)
	}
	defer f.Close()

	var out []referenceEntry
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 5 {
			t.Fatalf("%s: malformed line %q", referenceGoldenFile, line)
		}
		w, err := strconv.Atoi(fields[1])
		if err != nil {
			t.Fatalf("%s: bad width in %q", referenceGoldenFile, line)
		}
		h, err := strconv.Atoi(fields[2])
		if err != nil {
			t.Fatalf("%s: bad height in %q", referenceGoldenFile, line)
		}
		out = append(out, referenceEntry{
			file:     fields[0],
			width:    w,
			height:   h,
			channels: strings.Split(fields[3], ","),
			digest:   fields[4],
		})
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("read %s: %v", referenceGoldenFile, err)
	}
	if len(out) == 0 {
		t.Fatalf("%s contains no entries", referenceGoldenFile)
	}
	return out
}

// digestChannels computes the canonical digest defined in
// scripts/gen-reference-goldens.py. Both sides must agree exactly.
func digestChannels(samples map[string][]float32) (string, []string) {
	names := make([]string, 0, len(samples))
	for name := range samples {
		names = append(names, name)
	}
	sort.Strings(names)

	h := sha256.New()
	var word [4]byte
	for _, name := range names {
		h.Write([]byte(name))
		h.Write([]byte{0})
		for _, v := range samples[name] {
			bits := math.Float32bits(v)
			if bits&0x7F800000 == 0x7F800000 && bits&0x007FFFFF != 0 {
				bits = canonicalNaN
			}
			binary.LittleEndian.PutUint32(word[:], bits)
			h.Write(word[:])
		}
	}
	return hex.EncodeToString(h.Sum(nil)), names
}

// readSamplesAsFloat32 decodes every channel of a file to float32, which is
// what the golden generator hashed.
func readSamplesAsFloat32(t *testing.T, path string) (map[string][]float32, int, int) {
	t.Helper()

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile(%s): %v", path, err)
	}
	defer f.Close()

	hdr := f.Header(0)
	if hdr.IsTiled() {
		return readTiledSamplesAsFloat32(t, f)
	}

	r, err := NewScanlineReader(f)
	if err != nil {
		t.Fatalf("NewScanlineReader(%s): %v", path, err)
	}

	dw := r.DataWindow()
	w := int(dw.Max.X-dw.Min.X) + 1
	h := int(dw.Max.Y-dw.Min.Y) + 1

	fb := NewFrameBuffer()
	out, finish := bindChannelsToFrameBuffer(t, path, r.Header().Channels(), w, h, fb)
	r.SetFrameBuffer(fb)

	if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
		t.Fatalf("ReadPixels(%s): %v", path, err)
	}
	for _, fn := range finish {
		fn()
	}
	return out, w, h
}

func readTiledSamplesAsFloat32(t *testing.T, f *File) (map[string][]float32, int, int) {
	t.Helper()

	r, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader: %v", err)
	}
	dw := r.DataWindow()
	w := int(dw.Max.X-dw.Min.X) + 1
	h := int(dw.Max.Y-dw.Min.Y) + 1

	fb := NewFrameBuffer()
	out, finish := bindChannelsToFrameBuffer(t, "tiled", r.Header().Channels(), w, h, fb)
	r.SetFrameBuffer(fb)
	if err := r.ReadTiles(0, 0, r.NumXTilesAtLevel(0)-1, r.NumYTilesAtLevel(0)-1); err != nil {
		t.Fatalf("ReadTiles: %v", err)
	}
	for _, fn := range finish {
		fn()
	}
	return out, w, h
}

// bindChannelsToFrameBuffer allocates a destination slice for every channel in
// its native pixel type and returns closures that convert each to float32 once
// the read completes.
func bindChannelsToFrameBuffer(t *testing.T, path string, cl *ChannelList, w, h int, fb *FrameBuffer) (map[string][]float32, []func()) {
	t.Helper()

	out := make(map[string][]float32, cl.Len())
	finish := make([]func(), 0, cl.Len())

	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)
		name := ch.Name
		switch ch.Type {
		case PixelTypeHalf:
			buf := make([]half.Half, w*h)
			fb.Set(name, NewSliceFromHalf(buf, w, h))
			finish = append(finish, func() {
				vals := make([]float32, len(buf))
				for j, v := range buf {
					vals[j] = v.Float32()
				}
				out[name] = vals
			})
		case PixelTypeFloat:
			buf := make([]float32, w*h)
			fb.Set(name, NewSliceFromFloat32(buf, w, h))
			finish = append(finish, func() { out[name] = buf })
		case PixelTypeUint:
			buf := make([]uint32, w*h)
			fb.Set(name, NewSliceFromUint32(buf, w, h))
			finish = append(finish, func() {
				vals := make([]float32, len(buf))
				for j, v := range buf {
					vals[j] = float32(v)
				}
				out[name] = vals
			})
		default:
			t.Fatalf("%s: unexpected pixel type %v", path, ch.Type)
		}
	}
	return out, finish
}

// TestReferenceImagesDecodeExactly is the strongest true-value test in the
// suite: real-world images, written by the reference implementation, at real
// sizes, with the compressions the industry actually ships, compared sample by
// sample.
func TestReferenceImagesDecodeExactly(t *testing.T) {
	if _, err := os.Stat(referenceCorpusDir); os.IsNotExist(err) {
		t.Skipf("official corpus not present; run testdata/download.sh to enable this test")
	}

	for _, want := range loadReferenceGoldens(t) {
		t.Run(want.file, func(t *testing.T) {
			path := filepath.Join(referenceCorpusDir, want.file)
			if _, err := os.Stat(path); os.IsNotExist(err) {
				t.Skipf("%s not downloaded", want.file)
			}

			got, w, h := readSamplesAsFloat32(t, path)
			if w != want.width || h != want.height {
				t.Fatalf("%s: decoded %dx%d, reference says %dx%d", want.file, w, h, want.width, want.height)
			}

			digest, names := digestChannels(got)
			if strings.Join(names, ",") != strings.Join(want.channels, ",") {
				t.Fatalf("%s: channels %v, reference says %v", want.file, names, want.channels)
			}
			if digest != want.digest {
				t.Errorf("%s: decoded pixels do not match the OpenEXR reference implementation\n"+
					"  got digest  %s\n  want digest %s\n"+
					"  (every sample is covered; regenerate with scripts/gen-reference-goldens.py)",
					want.file, digest, want.digest)
			}
		})
	}
}
