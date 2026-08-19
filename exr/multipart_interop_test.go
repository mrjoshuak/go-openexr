package exr

import (
	"bytes"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// The tests in this file cover what the multi-part writer emits rather than
// what it can read back: each one asserts something scripts/validate.sh
// measured against the OpenEXR reference implementation, so a regression is
// caught by `go test` and not only by a machine with oiiotool installed.

// mpSample is a ramp that depends on the part, the channel and both
// coordinates, so no two parts and no two channels hold the same image and no
// rearrangement of rows, columns or tiles leaves the samples unchanged.
func mpSample(part, ch, x, y, w, h int) float32 {
	fx := float32(x) / float32(w-1)
	fy := float32(y) / float32(h-1)
	return 0.07*float32(part+1) + 0.13*float32(ch+1) + 0.31*fx + 0.19*fy
}

// mpHalfPart builds a frame buffer of half channels holding mpSample, in the
// coordinate system this package's frame buffers use: the pixel at the data
// window's minimum is buffer position (0, 0).
// mpHalfPart builds a frame buffer over bare buffers. ox and oy are the data
// window's minimum: a hand-built slice starts at zero unless told otherwise,
// and the library addresses frame buffers in the window's own coordinates.
func mpHalfPart(part int, names []string, w, h, ox, oy int) (*FrameBuffer, map[string][]half.Half) {
	fb := NewFrameBuffer()
	planes := map[string][]half.Half{}
	for ci, n := range names {
		plane := make([]half.Half, w*h)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				plane[y*w+x] = half.FromFloat32(mpSample(part, ci, x, y, w, h))
			}
		}
		planes[n] = plane
		fb.Set(n, NewSliceFromHalf(plane, w, h).WithOrigin(ox, oy))
	}
	return fb, planes
}

func mpHeader(name string, comp Compression, dw Box2i, display Box2i, names []string) *Header {
	h := NewScanlineHeader(int(dw.Width()), int(dw.Height()))
	h.SetDataWindow(dw)
	h.SetDisplayWindow(display)
	h.SetCompression(comp)
	h.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: name})
	h.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeScanline})
	cl := NewChannelList()
	for _, n := range names {
		cl.Add(Channel{Name: n, Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)
	return h
}

// TestMultiPartWritePixelsIsChunkAligned checks that a part written a scanline
// at a time produces exactly the file a part written in one call does. The
// format anchors a scanline part's chunk grid at the first line of its data
// window, so a chunk that starts anywhere else is unreadable; writing line by
// line used to emit one chunk per line and then fail outright with "too many
// chunks written" against any codec that packs several lines into a chunk.
func TestMultiPartWritePixelsIsChunkAligned(t *testing.T) {
	const w, h = 24, 40
	dw := Box2i{Min: V2i{0, 0}, Max: V2i{w - 1, h - 1}}
	names := []string{"R", "G", "B"}

	write := func(group int) []byte {
		hdr := mpHeader("part", CompressionZIP, dw, dw, names)
		var buf bytes.Buffer
		ws := &seekableWriter{Buffer: &buf}
		mpo, err := NewMultiPartOutputFile(ws, []*Header{hdr})
		if err != nil {
			t.Fatalf("NewMultiPartOutputFile: %v", err)
		}
		fb, _ := mpHalfPart(0, names, w, h, int(dw.Min.X), int(dw.Min.Y))
		if err := mpo.SetFrameBuffer(0, fb); err != nil {
			t.Fatalf("SetFrameBuffer: %v", err)
		}
		for written := 0; written < h; written += group {
			n := group
			if written+n > h {
				n = h - written
			}
			if err := mpo.WritePixels(0, n); err != nil {
				t.Fatalf("WritePixels(0, %d) after %d lines: %v", n, written, err)
			}
		}
		if err := mpo.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		return buf.Bytes()
	}

	whole := write(h)
	for _, group := range []int{1, 3, 16, 17} {
		got := write(group)
		if !bytes.Equal(got, whole) {
			t.Errorf("writing %d scanlines at a time produced %d bytes, writing all %d at once produced %d; the chunk grid depends on the call pattern",
				group, len(got), h, len(whole))
		}
	}
}

// TestMultiPartDataWindowOrigin checks that a part whose data window does not
// start at the origin is written from the frame buffer this package documents
// everywhere else: the pixel at (dataWindow.Min.X, dataWindow.Min.Y) is buffer
// position (0, 0). Reading image coordinates from the buffer shifted every
// such part by the origin and ran off the end of the caller's memory.
func TestMultiPartDataWindowOrigin(t *testing.T) {
	display := Box2i{Min: V2i{0, 0}, Max: V2i{70, 39}}
	names := []string{"R", "G", "B"}

	for _, dw := range []Box2i{
		{Min: V2i{13, 7}, Max: V2i{60, 35}},
		{Min: V2i{-9, -5}, Max: V2i{30, 22}},
	} {
		w, h := int(dw.Width()), int(dw.Height())
		hdr := mpHeader("part", CompressionZIP, dw, display, names)

		var buf bytes.Buffer
		ws := &seekableWriter{Buffer: &buf}
		mpo, err := NewMultiPartOutputFile(ws, []*Header{hdr})
		if err != nil {
			t.Fatalf("NewMultiPartOutputFile: %v", err)
		}
		fb, planes := mpHalfPart(0, names, w, h, int(dw.Min.X), int(dw.Min.Y))
		if err := mpo.SetFrameBuffer(0, fb); err != nil {
			t.Fatalf("SetFrameBuffer: %v", err)
		}
		if err := mpo.WritePixels(0, h); err != nil {
			t.Fatalf("WritePixels: %v", err)
		}
		if err := mpo.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		data := buf.Bytes()
		f, err := OpenReader(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatalf("OpenReader: %v", err)
		}
		sr, err := NewScanlineReaderPart(f, 0)
		if err != nil {
			t.Fatalf("NewScanlineReaderPart: %v", err)
		}
		out := NewFrameBuffer()
		got := map[string][]half.Half{}
		for _, n := range names {
			plane := make([]half.Half, w*h)
			got[n] = plane
			// The same origin the write side used: a hand-built slice starts
			// at zero, and the library addresses the buffer in the data
			// window's own coordinates.
			out.Set(n, NewSliceFromHalf(plane, w, h).WithOrigin(int(dw.Min.X), int(dw.Min.Y)))
		}
		sr.SetFrameBuffer(out)
		if err := sr.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
			t.Fatalf("ReadPixels: %v", err)
		}
		for _, n := range names {
			for i := range planes[n] {
				if got[n][i] != planes[n][i] {
					t.Fatalf("data window %v channel %s sample %d: wrote %v, read %v",
						dw, n, i, planes[n][i].Float32(), got[n][i].Float32())
				}
			}
		}
	}
}

// TestMultiPartSharedAttributes checks that parts are not allowed to disagree
// about the attributes the format requires every part of a file to share.
// OpenEXR refuses such a file as a whole, on writing and on reading, so a file
// written with two display windows cannot be opened by anything else.
func TestMultiPartSharedAttributes(t *testing.T) {
	a := Box2i{Min: V2i{0, 0}, Max: V2i{31, 15}}
	b := Box2i{Min: V2i{0, 0}, Max: V2i{15, 7}}
	names := []string{"R", "G", "B"}

	h1 := mpHeader("one", CompressionZIP, a, a, names)
	h2 := mpHeader("two", CompressionZIP, b, b, names)

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}
	if _, err := NewMultiPartWriter(ws, []*Header{h1, h2}); err == nil {
		t.Fatal("NewMultiPartWriter accepted parts with different display windows; the reference implementation refuses that file")
	}

	// The same two parts, differing only in data window, are legal.
	h2 = mpHeader("two", CompressionZIP, b, a, names)
	buf.Reset()
	ws = &seekableWriter{Buffer: &buf}
	if _, err := NewMultiPartWriter(ws, []*Header{h1, h2}); err != nil {
		t.Fatalf("NewMultiPartWriter rejected parts that differ only in data window: %v", err)
	}
}

// TestMultiPartVersionFlags checks the version field of a multi-part file
// holding a tiled part. The tiled flag and the multi-part flag are mutually
// exclusive — a multi-part file says what each part is in that part's own type
// attribute — and OpenEXR rejects a file with both before reading a pixel.
func TestMultiPartVersionFlags(t *testing.T) {
	tiled := NewTiledHeader(64, 32, 16, 16)
	tiled.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: "tiled"})
	tiled.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeTiled})

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}
	w, err := NewMultiPartWriter(ws, []*Header{tiled})
	if err != nil {
		t.Fatalf("NewMultiPartWriter: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	version := uint32(buf.Bytes()[4]) | uint32(buf.Bytes()[5])<<8 |
		uint32(buf.Bytes()[6])<<16 | uint32(buf.Bytes()[7])<<24
	if version&VersionFlagMultiPart == 0 {
		t.Errorf("version field 0x%04x does not have the multi-part flag", version)
	}
	if version&VersionFlagTiled != 0 {
		t.Errorf("version field 0x%04x has both the tiled and the multi-part flag; OpenEXR reads that as a corrupt file", version)
	}
}

// TestMultiPartChunkCount checks that each part declares its own chunk count,
// which the format requires in every part of a multi-part file and which the
// reference implementation writes in every part it produces.
func TestMultiPartChunkCount(t *testing.T) {
	dw := Box2i{Min: V2i{0, 0}, Max: V2i{63, 47}}
	names := []string{"R", "G", "B"}
	h1 := mpHeader("zip", CompressionZIP, dw, dw, names)   // 16 lines per chunk
	h2 := mpHeader("none", CompressionNone, dw, dw, names) // one line per chunk

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}
	if _, err := NewMultiPartWriter(ws, []*Header{h1, h2}); err != nil {
		t.Fatalf("NewMultiPartWriter: %v", err)
	}
	for i, want := range []int32{3, 48} {
		h := []*Header{h1, h2}[i]
		attr := h.Get(AttrNameChunkCount)
		if attr == nil {
			t.Fatalf("part %d has no chunkCount attribute", i)
		}
		if got, ok := attr.Value.(int32); !ok || got != want {
			t.Errorf("part %d chunkCount = %v, want %d", i, attr.Value, want)
		}
	}
}

// TestMultiPartTilesWrittenOutOfOrder checks that a tile's entry in the chunk
// offset table is fixed by where the tile is and not by when it was written.
// The reader turns (tileX, tileY) into an index and seeks straight there, so
// appending offsets in write order gives a table that is only correct while
// the caller happens to write tiles in that same order.
func TestMultiPartTilesWrittenOutOfOrder(t *testing.T) {
	const tile = 16
	dw := Box2i{Min: V2i{11, 5}, Max: V2i{74, 52}}
	w, h := int(dw.Width()), int(dw.Height())
	names := []string{"R", "G", "B"}

	hdr := NewTiledHeader(w, h, tile, tile)
	hdr.SetDataWindow(dw)
	hdr.SetDisplayWindow(dw)
	hdr.SetCompression(CompressionZIP)
	hdr.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: "tiles"})
	hdr.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeTiled})
	cl := NewChannelList()
	for _, n := range names {
		cl.Add(Channel{Name: n, Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}
	mpo, err := NewMultiPartOutputFile(ws, []*Header{hdr})
	if err != nil {
		t.Fatalf("NewMultiPartOutputFile: %v", err)
	}
	fb, planes := mpHalfPart(0, names, w, h, int(dw.Min.X), int(dw.Min.Y))
	if err := mpo.SetFrameBuffer(0, fb); err != nil {
		t.Fatalf("SetFrameBuffer: %v", err)
	}

	nx := (w + tile - 1) / tile
	ny := (h + tile - 1) / tile
	// Last tile first: the reverse of the order the offset table lists.
	for ty := ny - 1; ty >= 0; ty-- {
		for tx := nx - 1; tx >= 0; tx-- {
			if err := mpo.WriteTile(0, tx, ty); err != nil {
				t.Fatalf("WriteTile(0, %d, %d): %v", tx, ty, err)
			}
		}
	}
	if err := mpo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	f, err := OpenReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	tr, err := NewTiledReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewTiledReaderPart: %v", err)
	}
	out := NewFrameBuffer()
	got := map[string][]half.Half{}
	for _, n := range names {
		plane := make([]half.Half, w*h)
		got[n] = plane
		// The read side needs the same origin as the write side: the library
		// addresses a frame buffer in the data window's own coordinates.
		out.Set(n, NewSliceFromHalf(plane, w, h).WithOrigin(int(dw.Min.X), int(dw.Min.Y)))
	}
	tr.SetFrameBuffer(out)
	if err := tr.ReadTiles(0, 0, nx-1, ny-1); err != nil {
		t.Fatalf("ReadTiles: %v", err)
	}
	for _, n := range names {
		for i := range planes[n] {
			if got[n][i] != planes[n][i] {
				t.Fatalf("channel %s sample %d (x=%d, y=%d): wrote %v, read %v",
					n, i, i%w, i/w, planes[n][i].Float32(), got[n][i].Float32())
			}
		}
	}
}

// TestMultiPartMipmapLevels checks a tiled part that carries a whole mip
// pyramid inside a multi-part file — the shape an embedded proxy wants. Each
// level is written from its own frame buffer and has to come back at its own
// resolution, from its own stretch of the part's chunk offset table.
func TestMultiPartMipmapLevels(t *testing.T) {
	const tile, size = 16, 64
	dw := Box2i{Min: V2i{0, 0}, Max: V2i{size - 1, size - 1}}
	names := []string{"R", "G", "B"}

	hdr := NewTiledHeader(size, size, tile, tile)
	hdr.SetTileDescription(TileDescription{XSize: tile, YSize: tile, Mode: LevelModeMipmap})
	hdr.SetDataWindow(dw)
	hdr.SetDisplayWindow(dw)
	hdr.SetCompression(CompressionZIP)
	hdr.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: "proxy"})
	hdr.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeTiled})
	cl := NewChannelList()
	for _, n := range names {
		cl.Add(Channel{Name: n, Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}
	mpo, err := NewMultiPartOutputFile(ws, []*Header{hdr})
	if err != nil {
		t.Fatalf("NewMultiPartOutputFile: %v", err)
	}

	levels := hdr.NumXLevels()
	if levels < 3 {
		t.Fatalf("a %dx%d part with %d-pixel tiles has %d levels; the test needs several", size, size, tile, levels)
	}
	want := make([]map[string][]half.Half, levels)
	for l := 0; l < levels; l++ {
		lw, lh := hdr.LevelWidth(l), hdr.LevelHeight(l)
		// A different part index per level so no two levels hold the same
		// normalised image: a swapped level has to be visible.
		fb, planes := mpHalfPart(l, names, lw, lh, int(dw.Min.X), int(dw.Min.Y))
		want[l] = planes
		if err := mpo.SetFrameBuffer(0, fb); err != nil {
			t.Fatalf("SetFrameBuffer level %d: %v", l, err)
		}
		nx, ny := hdr.NumXTiles(l), hdr.NumYTiles(l)
		for ty := 0; ty < ny; ty++ {
			for tx := 0; tx < nx; tx++ {
				if err := mpo.WriteTileLevel(0, tx, ty, l, l); err != nil {
					t.Fatalf("WriteTileLevel(0, %d, %d, %d): %v", tx, ty, l, err)
				}
			}
		}
	}
	if err := mpo.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := buf.Bytes()
	f, err := OpenReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader: %v", err)
	}
	tr, err := NewTiledReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewTiledReaderPart: %v", err)
	}
	for l := 0; l < levels; l++ {
		lw, lh := hdr.LevelWidth(l), hdr.LevelHeight(l)
		out := NewFrameBuffer()
		got := map[string][]half.Half{}
		for _, n := range names {
			plane := make([]half.Half, lw*lh)
			got[n] = plane
			out.Set(n, NewSliceFromHalf(plane, lw, lh))
		}
		tr.SetFrameBuffer(out)
		if err := tr.ReadTilesLevel(0, 0, hdr.NumXTiles(l)-1, hdr.NumYTiles(l)-1, l, l); err != nil {
			t.Fatalf("ReadTilesLevel(%d): %v", l, err)
		}
		for _, n := range names {
			for i := range want[l][n] {
				if got[n][i] != want[l][n][i] {
					t.Fatalf("level %d (%dx%d) channel %s sample %d: wrote %v, read %v",
						l, lw, lh, n, i, want[l][n][i].Float32(), got[n][i].Float32())
				}
			}
		}
	}
}

// TestMultiPartCompressionIsApplied checks that a part actually gets the
// compression its header advertises. A codec the multi-part writer does not
// implement used to fall through and store the samples unchanged: the file
// still reads back correctly, because a chunk that is not smaller than its
// unpacked size is by definition stored raw, so nothing but the size says the
// compression the caller asked for was never applied.
func TestMultiPartCompressionIsApplied(t *testing.T) {
	const w, h = 128, 64
	dw := Box2i{Min: V2i{0, 0}, Max: V2i{w - 1, h - 1}}
	names := []string{"R", "G", "B"}

	sizeOf := func(comp Compression) int {
		hdr := mpHeader("part", comp, dw, dw, names)
		var buf bytes.Buffer
		ws := &seekableWriter{Buffer: &buf}
		mpo, err := NewMultiPartOutputFile(ws, []*Header{hdr})
		if err != nil {
			t.Fatalf("%v: NewMultiPartOutputFile: %v", comp, err)
		}
		fb, _ := mpHalfPart(0, names, w, h, int(dw.Min.X), int(dw.Min.Y))
		if err := mpo.SetFrameBuffer(0, fb); err != nil {
			t.Fatalf("%v: SetFrameBuffer: %v", comp, err)
		}
		if err := mpo.WritePixels(0, h); err != nil {
			t.Fatalf("%v: WritePixels: %v", comp, err)
		}
		if err := mpo.Close(); err != nil {
			t.Fatalf("%v: Close: %v", comp, err)
		}
		return buf.Len()
	}

	// The samples themselves, without any chunk headers or offset table: a
	// part stored raw cannot be smaller than this, and every codec here beats
	// it comfortably on a smooth ramp. Comparing against the uncompressed
	// part's *file* size would not do, because codecs differ in how many
	// scanlines a chunk holds and so in how many chunk headers a part has.
	raw := w * h * len(names) * 2
	if got := sizeOf(CompressionNone); got <= raw {
		t.Fatalf("an uncompressed part is %d bytes, no larger than its %d bytes of samples; the measure below is meaningless", got, raw)
	}
	for _, comp := range []Compression{
		CompressionRLE, CompressionZIPS, CompressionZIP, CompressionPIZ,
		CompressionPXR24, CompressionB44, CompressionB44A,
		CompressionDWAA, CompressionDWAB,
		CompressionHTJ2K256, CompressionHTJ2K32,
	} {
		if got := sizeOf(comp); got >= raw {
			t.Errorf("%v: the part is %d bytes and holds %d bytes of samples; it is stored raw, so the header advertises a compression that was never applied",
				comp, got, raw)
		}
	}
}
