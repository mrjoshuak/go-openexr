package compression

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	jpeg2000 "github.com/mrjoshuak/go-jpeg2000"
)

// HTJ2K has no external oracle in this repository — the reference
// implementation cannot read what this library writes — so its tests are
// entirely round trips through its own decoder. Mutation testing
// (scripts/mutation/run.py, id htj2k-chanmap-direction) inverted the meaning
// of the chunk's channel map on the encode and decode side together and every
// HTJ2K test stayed green, including the RGB ones, because most channel
// layouts produce a self-inverse permutation and the round trip cancels the
// rest.
//
// The two tests here assert on the bytes themselves: the chunk header against
// a hand-written vector, and the component order inside the codestream against
// the mapping the header declares.

// TestWriteHTJ2KHeaderMatchesSpecVector pins the chunk header to its bytes.
//
// OpenEXR's HTJ2K chunk begins with the two ASCII bytes 'H','T', a 32-bit
// big-endian payload length, a 16-bit big-endian channel count and then one
// 16-bit big-endian entry per channel. For the identity map {0, 1, 2} the
// payload is 2 + 3*2 = 8 bytes, so the header is exactly
//
//	48 54  00 00 00 08  00 03  00 00  00 01  00 02
//
// The existing tests only fed writeHTJ2KHeader's output back into
// readHTJ2KHeader, which cannot see a change made to both.
func TestWriteHTJ2KHeaderMatchesSpecVector(t *testing.T) {
	var buf bytes.Buffer
	if err := writeHTJ2KHeader(&buf, []uint16{0, 1, 2}); err != nil {
		t.Fatalf("writeHTJ2KHeader failed: %v", err)
	}
	want := []byte{
		'H', 'T',
		0x00, 0x00, 0x00, 0x08,
		0x00, 0x03,
		0x00, 0x00,
		0x00, 0x01,
		0x00, 0x02,
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("writeHTJ2KHeader({0,1,2}) = % x, want % x", buf.Bytes(), want)
	}

	// A non-identity map, so the entry order is pinned too and not just the
	// fact that three entries were written.
	buf.Reset()
	if err := writeHTJ2KHeader(&buf, []uint16{1, 2, 3, 0}); err != nil {
		t.Fatalf("writeHTJ2KHeader failed: %v", err)
	}
	want = []byte{
		'H', 'T',
		0x00, 0x00, 0x00, 0x0a,
		0x00, 0x04,
		0x00, 0x01,
		0x00, 0x02,
		0x00, 0x03,
		0x00, 0x00,
	}
	if !bytes.Equal(buf.Bytes(), want) {
		t.Fatalf("writeHTJ2KHeader({1,2,3,0}) = % x, want % x", buf.Bytes(), want)
	}
}

// TestHTJ2KFloatComponentOrderFollowsChannelMap checks that the codestream's
// component k really carries the EXR channel the header's map says it does.
//
// The map is defined as J2K component -> EXR channel index, and its whole
// purpose is to put R, G and B in components 0, 1 and 2 so that the reversible
// colour transform of ISO/IEC 15444-1 Annex G applies to them. For channels
// named A, R, G, B in that order the map is {1, 2, 3, 0}, and component 0 must
// hold R — not A, and not "whatever the decoder will undo".
//
// This is checked by decoding the codestream directly, bypassing this
// library's decompressor, which is the only part of the pipeline that could
// cancel a mistake here.
func TestHTJ2KFloatComponentOrderFollowsChannelMap(t *testing.T) {
	const width, height = 8, 4

	// Distinct constant per channel, so a mix-up is unambiguous.
	values := map[string]float32{"A": 0.25, "R": 0.5, "G": 0.75, "B": 1.0}
	names := []string{"A", "R", "G", "B"}

	channels := make([]HTJ2KChannelInfo, len(names))
	for i, n := range names {
		channels[i] = HTJ2KChannelInfo{
			Type: HTJ2KPixelTypeFloat, Width: width, Height: height,
			XSampling: 1, YSampling: 1, Name: n,
		}
	}

	// OpenEXR packs a chunk one scanline at a time, and within a scanline one
	// whole channel row at a time in name-sorted order — not pixel-interleaved.
	// See internal_ht.cpp, which walks the packed buffer as
	// line_pixels + raster_line_offset and advances line_pixels by one line per
	// scanline.
	bytesPerLine := width * len(names) * 4
	src := make([]byte, bytesPerLine*height)
	for y := 0; y < height; y++ {
		for c, n := range names {
			row := src[y*bytesPerLine+c*width*4:]
			for x := 0; x < width; x++ {
				putFloat32LE(row[x*4:], values[n])
			}
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 64)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	headerSize, channelMap, err := readHTJ2KHeader(compressed)
	if err != nil {
		t.Fatalf("readHTJ2KHeader failed: %v", err)
	}

	// A, R, G, B -> R is EXR channel 1, G is 2, B is 3, A trails at 0.
	wantMap := []uint16{1, 2, 3, 0}
	if len(channelMap) != len(wantMap) {
		t.Fatalf("channel map has %d entries, want %d", len(channelMap), len(wantMap))
	}
	for i := range wantMap {
		if channelMap[i] != wantMap[i] {
			t.Fatalf("channel map = %v, want %v (component 0 must carry R for the RCT)",
				channelMap, wantMap)
		}
	}

	img, err := jpeg2000.DecodeFloat(bytes.NewReader(compressed[headerSize:]))
	if err != nil {
		t.Fatalf("decoding the codestream directly failed: %v", err)
	}
	if img.ComponentCount() != len(names) {
		t.Fatalf("codestream has %d components, want %d", img.ComponentCount(), len(names))
	}

	for comp, exrCh := range wantMap {
		wantName := names[exrCh]
		want := values[wantName]
		for i, got := range img.Components[comp] {
			if got != want {
				t.Fatalf("codestream component %d sample %d = %v; the header's map says it "+
					"carries EXR channel %d (%q), whose value is %v",
					comp, i, got, exrCh, wantName, want)
			}
		}
	}
}

// putFloat32LE writes a float32 in the little-endian layout EXR pixel data uses.
func putFloat32LE(dst []byte, v float32) {
	binary.LittleEndian.PutUint32(dst, math.Float32bits(v))
}
