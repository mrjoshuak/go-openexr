package exr

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// The deep chunk layout, asserted against the bytes rather than against this
// library's own reader.
//
// Everything asserted here was measured from files the OpenEXR reference
// implementation wrote (via oiiotool 3.1.16 / OpenImageIO), not derived from
// this code:
//
//   - a deep scanline chunk header is 28 bytes: int y, then three uint64 —
//     the packed size of the pixel offset table, the packed size of the sample
//     data, and the unpacked size of the sample data. This library wrote 20,
//     omitting the last, and OpenEXR answered "Some scanline chunks were
//     missing or corrupted";
//   - a deep tile chunk header is 40 bytes: four int tile coordinates then the
//     same three uint64;
//   - the pixel offset table holds counts cumulative along each scanline,
//     restarting at zero on the next one (a reference tile over a 4x3 image
//     with two samples per pixel in rows 1 and 2 reads 1 3 5 7 | 2 4 6 8 |
//     2 4 6 8);
//   - the sample data is stored one scanline at a time and, within a scanline,
//     one channel at a time in alphabetical order — every sample of every
//     pixel of that scanline for one channel before the next channel begins;
//   - a single-part deep tiled file must not set the tiled bit in the version
//     field, only the deep bit.
//
// scripts/validate.sh gates all of this against the reference itself, but only
// where oiiotool is installed. These assertions hold everywhere.

// deepChunkOffsets returns the chunk offset table of a single-part file whose
// header ends at the given offset.
func deepFileParts(t *testing.T, data []byte) (version uint32, offsets []int64) {
	t.Helper()
	if len(data) < 8 {
		t.Fatalf("file is %d bytes", len(data))
	}
	version = xdr.ByteOrder.Uint32(data[4:8])

	// Walk the attributes to find the end of the header.
	p := 8
	readStr := func() string {
		start := p
		for p < len(data) && data[p] != 0 {
			p++
		}
		s := string(data[start:p])
		p++
		return s
	}
	for {
		if p >= len(data) {
			t.Fatal("ran off the end of the header")
		}
		if data[p] == 0 {
			p++
			break
		}
		readStr() // name
		readStr() // type
		size := int(int32(xdr.ByteOrder.Uint32(data[p : p+4])))
		p += 4 + size
	}

	// The offset table follows; its length is not stored, so read offsets
	// while they point somewhere plausible and increase.
	for p+8 <= len(data) {
		off := int64(xdr.ByteOrder.Uint64(data[p : p+8]))
		if off <= int64(p) || off >= int64(len(data)) {
			break
		}
		if len(offsets) > 0 && off <= offsets[len(offsets)-1] {
			break
		}
		offsets = append(offsets, off)
		p += 8
	}
	if len(offsets) == 0 {
		t.Fatal("no chunk offsets found")
	}
	return version, offsets
}

// TestDeepScanlineChunkWireFormat asserts the deep scanline chunk header and
// the order of the sample data, on an uncompressed file so the bytes can be
// read directly.
func TestDeepScanlineChunkWireFormat(t *testing.T) {
	const w, h = 5, 3

	// Sample counts 0..2 with an empty pixel in every row, and a value that
	// identifies its channel, pixel and sample.
	count := func(x, y int) uint32 { return uint32((x + 2*y) % 3) }
	value := func(c, x, y, s int) float32 { return float32(100*c + 10*(y*w+x) + s) }

	chans := []string{"A", "B", "Z"} // alphabetical: the file's storage order
	fb := NewDeepFrameBuffer(w, h)
	for _, name := range chans {
		fb.Insert(name, PixelTypeFloat)
	}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			n := count(x, y)
			fb.SetSampleCount(x, y, n)
			fb.AllocateSamples(x, y)
			for s := 0; s < int(n); s++ {
				for c, name := range chans {
					fb.Slices[name].SetSampleFloat32(x, y, s, value(c, x, y, s))
				}
			}
		}
	}

	var buf bytes.Buffer
	ws := &seekableBuffer{Buffer: buf}
	wr, err := NewDeepScanlineWriter(ws, w, h)
	if err != nil {
		t.Fatalf("NewDeepScanlineWriter: %v", err)
	}
	wr.Header().SetCompression(CompressionNone)
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(h); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if err := wr.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	data := ws.Bytes()
	version, offsets := deepFileParts(t, data)

	if version&VersionFlagDeep == 0 {
		t.Errorf("version field %#x does not set the deep bit", version)
	}
	if version&VersionFlagTiled != 0 {
		t.Errorf("version field %#x sets the tiled bit on a deep scanline file", version)
	}
	if len(offsets) != h {
		t.Fatalf("got %d chunk offsets, want %d (deep chunks hold one scanline)", len(offsets), h)
	}

	for y := 0; y < h; y++ {
		off := offsets[y]
		if int(off)+deepScanlineChunkHeaderSize > len(data) {
			t.Fatalf("chunk %d starts at %d, past the end of a %d byte file", y, off, len(data))
		}
		hdr := data[off : int(off)+deepScanlineChunkHeaderSize]
		gotY := int32(xdr.ByteOrder.Uint32(hdr[0:4]))
		packedTable := int(xdr.ByteOrder.Uint64(hdr[4:12]))
		packedData := int(xdr.ByteOrder.Uint64(hdr[12:20]))
		unpackedData := int(xdr.ByteOrder.Uint64(hdr[20:28]))

		if int(gotY) != y {
			t.Errorf("chunk %d: y = %d", y, gotY)
		}
		if packedTable != w*4 {
			t.Errorf("chunk %d: pixel offset table is %d bytes, want %d", y, packedTable, w*4)
		}

		// Expected row totals and the cumulative table, computed here rather
		// than read from the writer.
		var rowSamples int
		cumulative := make([]uint32, w)
		var run uint32
		for x := 0; x < w; x++ {
			run += count(x, y)
			cumulative[x] = run
			rowSamples += int(count(x, y))
		}
		wantBytes := rowSamples * len(chans) * 4
		if unpackedData != wantBytes {
			t.Errorf("chunk %d: unpacked sample data size is %d, want %d", y, unpackedData, wantBytes)
		}
		if packedData != wantBytes {
			t.Errorf("chunk %d: packed sample data size is %d, want %d (compression is NONE)", y, packedData, wantBytes)
		}

		table := data[int(off)+deepScanlineChunkHeaderSize : int(off)+deepScanlineChunkHeaderSize+packedTable]
		for x := 0; x < w; x++ {
			got := binary.LittleEndian.Uint32(table[x*4:])
			if got != cumulative[x] {
				t.Errorf("chunk %d: offset table[%d] = %d, want %d (cumulative within the scanline)", y, x, got, cumulative[x])
			}
		}

		// The sample data, channel-major within the scanline.
		samples := data[int(off)+deepScanlineChunkHeaderSize+packedTable:]
		if len(samples) < packedData {
			t.Fatalf("chunk %d: only %d bytes of sample data, want %d", y, len(samples), packedData)
		}
		i := 0
		for c, name := range chans {
			for x := 0; x < w; x++ {
				for s := 0; s < int(count(x, y)); s++ {
					got := xdr.ByteOrder.Uint32(samples[i*4 : i*4+4])
					want := value(c, x, y, s)
					if got != math.Float32bits(want) {
						t.Errorf("chunk %d: sample %d is channel %s pixel %d sample %d = %v, want %v",
							y, i, name, x, s, math.Float32frombits(got), want)
					}
					i++
				}
			}
		}
		if i*4 != packedData {
			t.Errorf("chunk %d: consumed %d bytes of sample data, chunk says %d", y, i*4, packedData)
		}
	}
}

// TestDeepTiledChunkWireFormat asserts the deep tile chunk header, the
// per-scanline sample count table inside a tile, the channel-major sample
// order, and that a partial edge tile stores only the pixels it has inside the
// data window.
func TestDeepTiledChunkWireFormat(t *testing.T) {
	const w, h = 5, 3 // one 4x4 tile column is partial, and the tile row is short
	const tw, th = 4, 4

	count := func(x, y int) uint32 { return uint32((x + 2*y) % 3) }
	value := func(c, x, y, s int) float32 { return float32(100*c + 10*(y*w+x) + s) }
	chans := []string{"A", "Z"}

	fb := NewDeepFrameBuffer(w, h)
	for _, name := range chans {
		fb.Insert(name, PixelTypeFloat)
	}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			n := count(x, y)
			fb.SetSampleCount(x, y, n)
			fb.AllocateSamples(x, y)
			for s := 0; s < int(n); s++ {
				for c, name := range chans {
					fb.Slices[name].SetSampleFloat32(x, y, s, value(c, x, y, s))
				}
			}
		}
	}

	var buf bytes.Buffer
	ws := &seekableBuffer{Buffer: buf}
	wr, err := NewDeepTiledWriter(ws, w, h, tw, th)
	if err != nil {
		t.Fatalf("NewDeepTiledWriter: %v", err)
	}
	wr.Header().SetCompression(CompressionNone)
	wr.SetFrameBuffer(fb)
	if err := wr.WriteTiles(0, 0, 1, 0); err != nil {
		t.Fatalf("WriteTiles: %v", err)
	}
	if err := wr.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	data := ws.Bytes()
	version, offsets := deepFileParts(t, data)

	if version&VersionFlagDeep == 0 {
		t.Errorf("version field %#x does not set the deep bit", version)
	}
	if version&VersionFlagTiled != 0 {
		t.Errorf("version field %#x sets the tiled bit; the tiled and deep bits are mutually exclusive", version)
	}
	if len(offsets) != 2 {
		t.Fatalf("got %d chunk offsets, want 2 tiles", len(offsets))
	}

	for tileX := 0; tileX < 2; tileX++ {
		off := int(offsets[tileX])
		hdr := data[off : off+deepTileChunkHeaderSize]
		gotTX := int32(xdr.ByteOrder.Uint32(hdr[0:4]))
		gotTY := int32(xdr.ByteOrder.Uint32(hdr[4:8]))
		gotLX := int32(xdr.ByteOrder.Uint32(hdr[8:12]))
		gotLY := int32(xdr.ByteOrder.Uint32(hdr[12:16]))
		packedTable := int(xdr.ByteOrder.Uint64(hdr[16:24]))
		packedData := int(xdr.ByteOrder.Uint64(hdr[24:32]))
		unpackedData := int(xdr.ByteOrder.Uint64(hdr[32:40]))

		if int(gotTX) != tileX || gotTY != 0 || gotLX != 0 || gotLY != 0 {
			t.Errorf("tile %d: coordinates %d %d %d %d", tileX, gotTX, gotTY, gotLX, gotLY)
		}

		startX := tileX * tw
		tileW := tw
		if startX+tileW > w {
			tileW = w - startX
		}
		tileH := h // the image is shorter than one tile

		if packedTable != tileW*tileH*4 {
			t.Errorf("tile %d: offset table is %d bytes, want %d (%dx%d clipped to the data window)",
				tileX, packedTable, tileW*tileH*4, tileW, tileH)
		}

		table := data[off+deepTileChunkHeaderSize : off+deepTileChunkHeaderSize+packedTable]
		total := 0
		for ly := 0; ly < tileH; ly++ {
			var run uint32
			for lx := 0; lx < tileW; lx++ {
				run += count(startX+lx, ly)
				got := binary.LittleEndian.Uint32(table[(ly*tileW+lx)*4:])
				if got != run {
					t.Errorf("tile %d: offset table[%d,%d] = %d, want %d (cumulative within the tile's scanline)",
						tileX, lx, ly, got, run)
				}
			}
			total += int(run)
		}

		wantBytes := total * len(chans) * 4
		if unpackedData != wantBytes || packedData != wantBytes {
			t.Errorf("tile %d: sizes packed %d unpacked %d, want %d both (compression is NONE)",
				tileX, packedData, unpackedData, wantBytes)
		}

		samples := data[off+deepTileChunkHeaderSize+packedTable:]
		i := 0
		for ly := 0; ly < tileH; ly++ {
			for c, name := range chans {
				for lx := 0; lx < tileW; lx++ {
					x := startX + lx
					for s := 0; s < int(count(x, ly)); s++ {
						got := xdr.ByteOrder.Uint32(samples[i*4 : i*4+4])
						want := value(c, x, ly, s)
						if got != math.Float32bits(want) {
							t.Errorf("tile %d: sample %d is channel %s pixel (%d,%d) sample %d = %v, want %v",
								tileX, i, name, x, ly, s, math.Float32frombits(got), want)
						}
						i++
					}
				}
			}
		}
		if i*4 != packedData {
			t.Errorf("tile %d: consumed %d bytes of sample data, chunk says %d", tileX, i*4, packedData)
		}
	}
}
