package exr

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

// writeTiledTestFile writes a tiled EXR with the given tile size and returns
// its path.
func writeTiledTestFile(t *testing.T, dir string, w, h, tw, th int, comp Compression) string {
	t.Helper()
	path := filepath.Join(dir, "tiled.exr")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	hdr := newScanlineTestHeader(w, h)
	hdr.SetCompression(comp)
	hdr.SetTileDescription(TileDescription{
		XSize: uint32(tw), YSize: uint32(th),
		Mode: LevelModeOne, RoundingMode: LevelRoundDown,
	})
	twr, err := NewTiledWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewTiledWriter: %v", err)
	}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	s := fb.Get("Y")
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			s.SetFloat32(x, y, float32(x*3+y))
		}
	}
	twr.SetFrameBuffer(fb)
	nx, ny := twr.NumTilesX(), twr.NumTilesY()
	if err := twr.WriteTiles(0, 0, nx-1, ny-1); err != nil {
		t.Fatalf("WriteTiles: %v", err)
	}
	if err := twr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// TestChunkRangeNamesTheRealBytes pins the index against the file itself: the
// offset and length must name exactly the bytes ReadChunk consumes.
//
// An index whose ranges are plausible but off by a header is worse than no
// index, because a ranged read would fetch the wrong bytes and only fail later,
// somewhere else.
func TestChunkRangeNamesTheRealBytes(t *testing.T) {
	dir := t.TempDir()
	const w, h = 32, 16
	path := filepath.Join(dir, "scan.exr")
	writeScanlineFile(t, path, w, h, true)

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	n := f.NumChunks(0)
	if n == 0 {
		t.Fatal("file reports no chunks")
	}
	for i := 0; i < n; i++ {
		cr, err := f.ChunkRange(0, i)
		if err != nil {
			t.Fatalf("ChunkRange(0, %d): %v", i, err)
		}
		if cr.Offset <= 0 || cr.Offset+cr.Length > int64(len(raw)) {
			t.Fatalf("chunk %d range %d+%d falls outside the %d-byte file",
				i, cr.Offset, cr.Length, len(raw))
		}
		y, data, err := f.ReadChunk(0, i)
		if err != nil {
			t.Fatalf("ReadChunk(0, %d): %v", i, err)
		}
		if y != cr.Y {
			t.Errorf("chunk %d: ChunkRange says y=%d, ReadChunk says y=%d", i, cr.Y, y)
		}
		if int64(len(data)) != cr.DataLength {
			t.Errorf("chunk %d: ChunkRange says %d data bytes, ReadChunk returned %d",
				i, cr.DataLength, len(data))
		}
		got := raw[cr.DataOffset : cr.DataOffset+cr.DataLength]
		if !bytes.Equal(got, data) {
			t.Errorf("chunk %d: the bytes at DataOffset %d are not the chunk's own data",
				i, cr.DataOffset)
		}
	}
}

// TestChunksForScanlinesCoversTheRows checks the query direction: every chunk
// holding a requested row must be returned, and no chunk that holds none of
// them.
func TestChunksForScanlinesCoversTheRows(t *testing.T) {
	dir := t.TempDir()
	const w, h = 32, 16
	path := filepath.Join(dir, "scan.exr")
	writeScanlineFile(t, path, w, h, true)

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	all, err := f.ChunkRanges(0)
	if err != nil {
		t.Fatalf("ChunkRanges: %v", err)
	}
	perChunk := int32(f.Header(0).Compression().ScanlinesPerChunk())

	for _, q := range []struct{ y0, y1 int32 }{
		{0, 0}, {0, 3}, {5, 5}, {4, 11}, {0, int32(h - 1)},
	} {
		got, err := f.ChunksForScanlines(0, q.y0, q.y1)
		if err != nil {
			t.Fatalf("ChunksForScanlines(%d,%d): %v", q.y0, q.y1, err)
		}
		want := map[int64]bool{}
		for _, cr := range all {
			if cr.Y+perChunk-1 >= q.y0 && cr.Y <= q.y1 {
				want[cr.Offset] = true
			}
		}
		if len(got) != len(want) {
			t.Errorf("rows %d..%d: got %d chunks, want %d", q.y0, q.y1, len(got), len(want))
		}
		for _, cr := range got {
			if !want[cr.Offset] {
				t.Errorf("rows %d..%d: chunk at y=%d does not hold any of them", q.y0, q.y1, cr.Y)
			}
		}
		// A narrow query must cost less than the whole file, or the index is
		// not narrowing anything.
		if q.y1-q.y0 < int32(h)/2 && len(got) >= len(all) {
			t.Errorf("rows %d..%d selected %d of %d chunks; a partial range must select fewer",
				q.y0, q.y1, len(got), len(all))
		}
	}
}

// TestChunksForRegionSelectsOnlyOverlappingTiles is the same property for a
// tiled part, which is the case a viewport actually uses.
func TestChunksForRegionSelectsOnlyOverlappingTiles(t *testing.T) {
	dir := t.TempDir()
	const w, h, tw, th = 64, 64, 16, 16
	path := writeTiledTestFile(t, dir, w, h, tw, th, CompressionNone)

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	all, err := f.ChunkRanges(0)
	if err != nil {
		t.Fatalf("ChunkRanges: %v", err)
	}
	if len(all) != (w/tw)*(h/th) {
		t.Fatalf("expected %d tiles, the table holds %d", (w/tw)*(h/th), len(all))
	}

	// A 16x16 viewport at the origin covers exactly one tile.
	got, err := f.ChunksForRegion(0, 0, 0, 16, 16, 0, 0)
	if err != nil {
		t.Fatalf("ChunksForRegion: %v", err)
	}
	if len(got) != 1 {
		t.Errorf("a 16x16 viewport over 16x16 tiles selected %d tiles, want 1", len(got))
	}

	// One that straddles a boundary covers four.
	got, err = f.ChunksForRegion(0, 8, 8, 24, 24, 0, 0)
	if err != nil {
		t.Fatalf("ChunksForRegion: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("a viewport straddling a tile corner selected %d tiles, want 4", len(got))
	}

	// The whole image covers everything, which is the upper bound.
	got, err = f.ChunksForRegion(0, 0, 0, w, h, 0, 0)
	if err != nil {
		t.Fatalf("ChunksForRegion: %v", err)
	}
	if len(got) != len(all) {
		t.Errorf("the whole image selected %d of %d tiles", len(got), len(all))
	}
}
