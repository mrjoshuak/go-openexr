package exr

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func newScanlineTestHeader(w, h int) *Header {
	hdr := NewHeader()
	hdr.SetDataWindow(Box2i{V2i{0, 0}, V2i{int32(w - 1), int32(h - 1)}})
	hdr.SetDisplayWindow(Box2i{V2i{0, 0}, V2i{int32(w - 1), int32(h - 1)}})
	hdr.SetCompression(CompressionNone)
	hdr.SetLineOrder(LineOrderIncreasing)
	hdr.SetPixelAspectRatio(1)
	hdr.SetScreenWindowCenter(V2f{0, 0})
	hdr.SetScreenWindowWidth(1)
	cl := NewChannelList()
	cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	hdr.SetChannels(cl)
	return hdr
}

// writeScanlineFile writes a complete scanline EXR, calling Close only if
// closeIt is set.
func writeScanlineFile(t *testing.T, path string, w, h int, closeIt bool) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer f.Close()

	hdr := newScanlineTestHeader(w, h)
	sw, err := NewScanlineWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	s := fb.Get("Y")
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			s.SetFloat32(x, y, float32(x+y))
		}
	}
	sw.SetFrameBuffer(fb)
	if err := sw.WritePixels(0, h-1); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if closeIt {
		if err := sw.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
}

// TestUnclosedWriterStillProducesACompleteFile pins the fix for the defect
// behind issue #4.
//
// The chunk offset table can only be filled in once the offsets are known, so
// it used to be written by Close alone. A caller who never called Close left a
// file whose table was all zeros. This library's own reader recovered by
// scanning and returned an image; the reference implementation locates every
// chunk through the table and did not. The file therefore looked fine here and
// was unreadable everywhere else — the worst shape a defect can take, and
// exactly why the report read as "float32 scanline reads return zeros".
//
// The table is now written as soon as the last chunk the header promised has
// been written, so a complete image is complete whether or not Close follows.
// Asserting the two files are byte-identical is stronger than asserting the
// unclosed one is readable: this library's own reader was always able to read
// it, which is what hid the defect.
func TestUnclosedWriterStillProducesACompleteFile(t *testing.T) {
	const w, h = 16, 8
	dir := t.TempDir()

	closed := filepath.Join(dir, "closed.exr")
	unclosed := filepath.Join(dir, "unclosed.exr")
	writeScanlineFile(t, closed, w, h, true)
	writeScanlineFile(t, unclosed, w, h, false)

	a, err := os.ReadFile(closed)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(unclosed)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("closed file is %d bytes, unclosed is %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("closed and unclosed files differ at byte %d (%#02x vs %#02x); "+
				"a complete image must not depend on Close being called", i, a[i], b[i])
		}
	}
}

// TestUnclosedWriterOffsetTableIsPopulated states the property against the
// file's own bytes.
//
// It deliberately does not use File.Offsets: that returns the table the reader
// worked out, and this reader reconstructs one by scanning when the stored
// table is unusable. Reading through it would measure the reader's recovery
// rather than the file, and pass on a file no other implementation can open —
// which was demonstrated, by a mutation restoring the old Close-only behaviour
// that this test then failed to catch.
func TestUnclosedWriterOffsetTableIsPopulated(t *testing.T) {
	const w, h = 16, 8
	dir := t.TempDir()
	path := filepath.Join(dir, "unclosed.exr")
	writeScanlineFile(t, path, w, h, false)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	offsets := f.Offsets(0)
	f.Close()

	// CompressionNone stores one scanline per chunk.
	if len(offsets) != h {
		t.Fatalf("offset table holds %d entries for a %d-scanline image", len(offsets), h)
	}
	// The table occupies the eight bytes per chunk immediately before the
	// first chunk, wherever the header happens to end.
	first := offsets[0]
	tableStart := first - int64(8*h)
	if tableStart < 0 || first > int64(len(raw)) {
		t.Fatalf("first chunk is at %d in a %d-byte file; cannot locate the table", first, len(raw))
	}
	for i := 0; i < h; i++ {
		at := tableStart + int64(8*i)
		stored := int64(binary.LittleEndian.Uint64(raw[at : at+8]))
		if stored != offsets[i] {
			t.Errorf("chunk %d: the file stores offset %d, the reader uses %d; "+
				"the stored table is what every other implementation reads",
				i, stored, offsets[i])
		}
		if stored <= 0 || stored >= int64(len(raw)) {
			t.Errorf("chunk %d has stored offset %d in a %d-byte file; an unwritten entry "+
				"makes the file unreadable outside this package", i, stored, len(raw))
		}
	}
}

// TestClosedWriterIsReadable is the control: the ordinary path must still
// round-trip. Without it the checks above could be satisfied by a writer that
// produced two identically broken files.
func TestClosedWriterIsReadable(t *testing.T) {
	const w, h = 16, 8
	dir := t.TempDir()
	path := filepath.Join(dir, "closed.exr")
	writeScanlineFile(t, path, w, h, true)

	rf, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer rf.Close()
	r, err := NewScanlineReader(rf)
	if err != nil {
		t.Fatalf("NewScanlineReader: %v", err)
	}
	fb, _ := AllocateChannels(r.Header().Channels(), r.DataWindow())
	r.SetFrameBuffer(fb)
	if err := r.ReadPixels(0, h-1); err != nil {
		t.Fatalf("ReadPixels: %v", err)
	}
	s := fb.Get("Y")
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			if got, want := s.GetFloat32(x, y), float32(x+y); got != want {
				t.Fatalf("sample (%d,%d) = %v, want %v", x, y, got, want)
			}
		}
	}
}
