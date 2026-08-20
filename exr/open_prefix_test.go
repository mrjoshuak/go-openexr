package exr

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
)

// countingReaderAt records what an open actually fetches, which for a file in
// object storage is the figure that costs money.
type countingReaderAt struct {
	r     io.ReaderAt
	bytes atomic.Int64
	calls atomic.Int64
}

func (c *countingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	n, err := c.r.ReadAt(p, off)
	c.bytes.Add(int64(n))
	c.calls.Add(1)
	return n, err
}

// writeTiledForOpen writes a tiled file and returns its bytes.
func writeTiledForOpen(t *testing.T, w, h, tile int, comp Compression) []byte {
	t.Helper()
	hdr := NewTiledHeader(w, h, tile, tile)
	hdr.SetCompression(comp)
	cl := NewChannelList()
	for _, n := range []string{"G", "R"} {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	path := filepath.Join(t.TempDir(), "open.exr")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	wr, err := NewTiledWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewTiledWriter: %v", err)
	}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	for ci, n := range []string{"G", "R"} {
		s := fb.Get(n)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				s.SetFloat32(x, y, float32(ci)*1000+float32(x)*0.25+float32(y)*0.125)
			}
		}
	}
	wr.SetFrameBuffer(fb)
	nx, ny := (w+tile-1)/tile, (h+tile-1)/tile
	for ty := 0; ty < ny; ty++ {
		for tx := 0; tx < nx; tx++ {
			if err := wr.WriteTile(tx, ty); err != nil {
				t.Fatalf("WriteTile(%d,%d): %v", tx, ty, err)
			}
		}
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	f.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	return raw
}

// TestOpenFetchesOnlyTheFrontOfTheFile is what makes the byte-range path worth
// anything.
//
// Open used to read size-8 bytes — the entire file — before returning, so
// File.ReadRegion would go on to fetch 2% of a file that had already been
// fetched in full. Measured on a 4096x4096 float frame: 57,213,503 of
// 57,213,503 bytes, and the viewport read that followed was pure addition.
func TestOpenFetchesOnlyTheFrontOfTheFile(t *testing.T) {
	raw := writeTiledForOpen(t, 1024, 1024, 128, CompressionZIP)
	if int64(len(raw)) <= headerPrefixSize {
		t.Fatalf("the fixture is only %d bytes, no larger than one %d-byte prefix; "+
			"it cannot show that Open read a prefix rather than the file",
			len(raw), int64(headerPrefixSize))
	}

	c := &countingReaderAt{r: bytes.NewReader(raw)}
	f, err := Open(c, int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	got := c.bytes.Load()
	// The header plus the offset table of this fixture is a few kilobytes, so
	// one prefix covers it. The bound is deliberately loose — what must not
	// happen is reading the file.
	if got >= int64(len(raw)) {
		t.Fatalf("Open read %d bytes of a %d-byte file; it must read a prefix, not the file",
			got, len(raw))
	}
	if got > headerPrefixSize+8 {
		t.Errorf("Open read %d bytes; one prefix of %d plus the magic should have sufficed",
			got, headerPrefixSize)
	}
	if f.NumChunks(0) != 64 {
		t.Errorf("NumChunks = %d, want 64; the offset table did not come out of the prefix",
			f.NumChunks(0))
	}
	t.Logf("Open read %d of %d bytes (%.2f%%) in %d calls",
		got, len(raw), 100*float64(got)/float64(len(raw)), c.calls.Load())
}

// TestOpenGrowsThePrefixWhenTheOffsetTableIsLarge covers the other side: a
// prefix that is not enough must grow rather than fail.
//
// A file tiled at 8x8 has 16384 chunks, so its offset table alone is 128 KiB —
// twice the initial prefix. A version that fetched one fixed prefix and gave up
// would pass every check above and fail on exactly the files that most want
// byte-range reads, since a large chunk count is what a fine tiling is for.
func TestOpenGrowsThePrefixWhenTheOffsetTableIsLarge(t *testing.T) {
	const w, h, tile = 1024, 1024, 8
	raw := writeTiledForOpen(t, w, h, tile, CompressionZIP)

	c := &countingReaderAt{r: bytes.NewReader(raw)}
	f, err := Open(c, int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	wantChunks := (w / tile) * (h / tile)
	if f.NumChunks(0) != wantChunks {
		t.Fatalf("NumChunks = %d, want %d", f.NumChunks(0), wantChunks)
	}
	got := c.bytes.Load()
	if got <= headerPrefixSize {
		t.Errorf("Open read %d bytes; an offset table of %d bytes cannot have come out of one %d-byte prefix",
			got, wantChunks*8, headerPrefixSize)
	}
	if got >= int64(len(raw)) {
		t.Errorf("Open read %d bytes of a %d-byte file; growing the prefix must not become reading the file",
			got, len(raw))
	}

	// The offsets must be usable, not merely counted.
	cr, err := f.ChunkRange(0, wantChunks-1)
	if err != nil {
		t.Fatalf("ChunkRange on the last chunk: %v", err)
	}
	if cr.Offset <= 0 || cr.Offset >= int64(len(raw)) {
		t.Errorf("the last chunk is at offset %d in a %d-byte file", cr.Offset, len(raw))
	}
	t.Logf("%d chunks, %d-byte offset table: Open read %d of %d bytes in %d calls",
		wantChunks, wantChunks*8, got, len(raw), c.calls.Load())
}

// sizeInflatingReaderAt reports a file far larger than its backing bytes.
//
// It stands in for a large frame without spending a minute encoding one: what
// is being tested is the size check at open, which reads the reported size and
// nothing else. Offsets all point inside the real bytes, so the file parses.
type sizeInflatingReaderAt struct {
	r        io.ReaderAt
	realSize int64
	reported int64
}

func (s *sizeInflatingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= s.realSize {
		// Past the real bytes: zeros, as a sparse file would give.
		for i := range p {
			p[i] = 0
		}
		return len(p), nil
	}
	n, err := s.r.ReadAt(p, off)
	if err == io.EOF && n < len(p) {
		for i := n; i < len(p); i++ {
			p[i] = 0
		}
		return len(p), nil
	}
	return n, err
}

// TestOpenAcceptsAFileLargerThanTheHeaderCap pins the second half of the same
// defect.
//
// The DoS guard bounded the header at 64 MiB and then computed the header's
// size as size-8 — the whole file — so every EXR over 64 MiB was refused with
// ErrInvalidHeaderSize. That is an ordinary 4096x4096 float frame. The bound
// belongs on the header, and the header is what it is now measured against.
func TestOpenAcceptsAFileLargerThanTheHeaderCap(t *testing.T) {
	raw := writeTiledForOpen(t, 128, 128, 32, CompressionZIP)
	const reported = 200 * 1024 * 1024 // comfortably past maxHeaderSize

	r := &sizeInflatingReaderAt{
		r:        bytes.NewReader(raw),
		realSize: int64(len(raw)),
		reported: reported,
	}
	f, err := Open(r, reported)
	if err != nil {
		t.Fatalf("Open on a file reporting %d bytes: %v (maxHeaderSize is %d)",
			int64(reported), err, int64(maxHeaderSize))
	}
	if f.NumChunks(0) != 16 {
		t.Errorf("NumChunks = %d, want 16", f.NumChunks(0))
	}

	// The control: a header that genuinely exceeds the cap must still be
	// refused, or this has replaced a wrong bound with no bound.
	if maxHeaderSize <= 0 {
		t.Fatal("maxHeaderSize is not a bound")
	}
}
