package exr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The four deep areas the parity audit named. Two of its claims reproduced
// exactly, one reproduced with a different cause, and one did not reproduce at
// all — each is recorded here as measured rather than as reported.

// deepScanlineFixture writes a deep scanline file whose data window starts at
// (offX, offY) and whose samples encode their own coordinates.
func deepScanlineFixture(t *testing.T, path string, offX, offY, w, h int, extra ...string) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	wr, err := NewDeepScanlineWriter(f, w, h)
	if err != nil {
		t.Fatalf("NewDeepScanlineWriter: %v", err)
	}
	hdr := wr.Header()
	hdr.SetDataWindow(Box2i{
		Min: V2i{X: int32(offX), Y: int32(offY)},
		Max: V2i{X: int32(offX + w - 1), Y: int32(offY + h - 1)},
	})
	names := append([]string{"A", "R", "Z"}, extra...)
	cl := NewChannelList()
	for _, n := range names {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	fb := NewDeepFrameBuffer(w, h)
	for _, n := range names {
		fb.Insert(n, PixelTypeFloat)
	}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			fb.SetSampleCount(x, y, 1)
			fb.AllocateSamples(x, y)
			for _, n := range names {
				v := float32(y*100 + x)
				if n == "A" {
					v = 1
				}
				fb.Slices[n].SetSampleFloat32(x, y, 0, v)
			}
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(h); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if err := wr.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
}

// TestDeepScanlineReadThroughANonOriginWindow pins the behaviour the audit
// reported as broken and which does not reproduce.
//
// It reported that a deep scanline read drops the top rows of any part whose
// data window does not start at y=0, citing 318 of 760 rows zeroed. Measured
// here at windows of (0,0) and (5,7): every pixel present and correct. The
// deep reader derives its chunk index from the window's first row, which is
// what would have caused it, and it does so correctly. The check is kept so
// the behaviour is pinned either way.
func TestDeepScanlineReadThroughANonOriginWindow(t *testing.T) {
	const w, h = 16, 12
	dir := t.TempDir()

	for _, off := range []struct{ x, y int }{{0, 0}, {5, 7}} {
		path := filepath.Join(dir, "deep.exr")
		deepScanlineFixture(t, path, off.x, off.y, w, h)

		in, err := OpenFile(path)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		r, err := NewDeepScanlineReader(in)
		if err != nil {
			in.Close()
			t.Fatalf("NewDeepScanlineReader: %v", err)
		}
		dw := in.Header(0).DataWindow()
		fb := NewDeepFrameBuffer(w, h)
		for _, n := range []string{"A", "R", "Z"} {
			fb.Insert(n, PixelTypeFloat)
		}
		r.SetFrameBuffer(fb)
		if err := r.ReadPixelSampleCounts(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
			in.Close()
			t.Fatalf("window (%d,%d) ReadPixelSampleCounts: %v", off.x, off.y, err)
		}
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				fb.AllocateSamples(x, y)
			}
		}
		if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
			in.Close()
			t.Fatalf("window (%d,%d) ReadPixels: %v", off.x, off.y, err)
		}
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				if fb.GetSampleCount(x, y) == 0 {
					in.Close()
					t.Fatalf("window (%d,%d): pixel (%d,%d) came back with no samples",
						off.x, off.y, x, y)
				}
				if got, want := fb.Slices["Z"].GetSampleFloat32(x, y, 0), float32(y*100+x); got != want {
					in.Close()
					t.Fatalf("window (%d,%d): Z at (%d,%d) = %v, want %v",
						off.x, off.y, x, y, got, want)
				}
			}
		}
		in.Close()
	}
}

// TestReadRegionRefusesADeepPart pins a refusal that used to happen by
// accident.
//
// A deep chunk holds a sample-count table and a variable number of samples per
// pixel, so none of the byte-range addressing applies to it. ReadRegion used to
// attempt it anyway and fail downstream in the codec — "compression: corrupted
// ZIP data" — which names the wrong thing entirely and would send anyone
// debugging it to the compression code.
func TestReadRegionRefusesADeepPart(t *testing.T) {
	const w, h = 16, 12
	dir := t.TempDir()
	path := filepath.Join(dir, "deep.exr")
	deepScanlineFixture(t, path, 0, 0, w, h)

	in, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer in.Close()

	_, err = in.ReadRegion(0, Box2i{Min: V2i{X: 2, Y: 2}, Max: V2i{X: 7, Y: 7}})
	if err == nil {
		t.Fatal("ReadRegion accepted a deep part")
	}
	if !strings.Contains(err.Error(), "deep") {
		t.Errorf("the refusal does not say the part is deep: %v", err)
	}
}

// TestDeepCompositingCoversEveryChannelAndAnyBand pins both compositor defects.
//
// The compositor wrote only R, G, B and A, so Z and every AOV came back
// untouched — measured at 0 of 192 samples written for both, which for deep
// data is most of the point of having it. And it allocated its deep buffers for
// the band being composited while the deep reader addresses a frame buffer from
// the data window's first row, so any band that did not start at the top
// indexed past the end: "index out of range [4] with length 4" for rows 4 to 7
// of a 12-row image.
func TestDeepCompositingCoversEveryChannelAndAnyBand(t *testing.T) {
	const w, h = 16, 12
	dir := t.TempDir()
	path := filepath.Join(dir, "src.exr")
	deepScanlineFixture(t, path, 0, 0, w, h, "B", "G", "myAOV")

	for _, band := range []struct{ y1, y2 int }{{0, h - 1}, {4, 7}} {
		in, err := OpenFile(path)
		if err != nil {
			t.Fatalf("OpenFile: %v", err)
		}
		src, err := NewDeepScanlineReader(in)
		if err != nil {
			in.Close()
			t.Fatalf("NewDeepScanlineReader: %v", err)
		}
		c := NewCompositeDeepScanLine()
		if err := c.AddSource(src); err != nil {
			in.Close()
			t.Fatalf("AddSource: %v", err)
		}
		cl := NewChannelList()
		for _, n := range []string{"A", "B", "G", "R", "Z", "myAOV"} {
			cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
		}
		out, _ := AllocateChannels(cl, c.DataWindow())
		c.SetFrameBuffer(out)

		if err := c.ReadPixels(band.y1, band.y2); err != nil {
			in.Close()
			t.Fatalf("rows %d..%d: %v", band.y1, band.y2, err)
		}

		// Every channel must carry composited data, not just RGBA. Pixel (0,0)
		// has Z of zero by construction, so the count is taken over the band's
		// interior columns.
		for _, name := range []string{"Z", "myAOV"} {
			written := 0
			for y := band.y1; y <= band.y2; y++ {
				for x := 1; x < w; x++ {
					if out.Get(name).GetFloat32(x, y) != 0 {
						written++
					}
				}
			}
			want := (w - 1) * (band.y2 - band.y1 + 1)
			if written != want {
				t.Errorf("rows %d..%d: channel %s has %d of %d samples written; "+
					"the compositor must carry every channel, not only RGBA",
					band.y1, band.y2, name, written, want)
			}
		}
		in.Close()
	}
}

// TestDeepTiledLevelsBelowTheTileSize pins the level clipping.
//
// A tile is short wherever it runs off the edge of the level it belongs to, and
// every level below the tile size is entirely short. The reader clipped against
// the data window instead, so it expected a full-size tile at every level and
// the codec reported "corrupted ZIP data": levels 3 through 6 of a 7-level file
// were unreadable while levels 0 to 2, at or above the tile size, were fine.
func TestDeepTiledLevelsBelowTheTileSize(t *testing.T) {
	const size, tile = 64, 16
	dir := t.TempDir()
	path := filepath.Join(dir, "deeptiled.exr")

	names := []string{"A", "Z"}
	func() {
		f, err := os.Create(path)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		defer f.Close()
		wr, err := NewDeepTiledWriter(f, size, size, tile, tile)
		if err != nil {
			t.Fatalf("NewDeepTiledWriter: %v", err)
		}
		hdr := wr.Header()
		hdr.SetTileDescription(TileDescription{
			XSize: tile, YSize: tile,
			Mode: LevelModeMipmap, RoundingMode: LevelRoundDown,
		})
		cl := NewChannelList()
		for _, n := range names {
			cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
		}
		hdr.SetChannels(cl)

		for lv := 0; lv < hdr.NumXLevels(); lv++ {
			lw, lh := hdr.LevelWidth(lv), hdr.LevelHeight(lv)
			fb := NewDeepFrameBuffer(lw, lh)
			for _, n := range names {
				fb.Insert(n, PixelTypeFloat)
			}
			for y := 0; y < lh; y++ {
				for x := 0; x < lw; x++ {
					fb.SetSampleCount(x, y, 1)
					fb.AllocateSamples(x, y)
					fb.Slices["Z"].SetSampleFloat32(x, y, 0, float32(lv*1000+y*10+x))
					fb.Slices["A"].SetSampleFloat32(x, y, 0, 1)
				}
			}
			wr.SetFrameBuffer(fb)
			for ty := 0; ty < (lh+tile-1)/tile; ty++ {
				for tx := 0; tx < (lw+tile-1)/tile; tx++ {
					if err := wr.WriteTileLevel(tx, ty, lv, lv); err != nil {
						t.Fatalf("level %d WriteTileLevel(%d,%d): %v", lv, tx, ty, err)
					}
				}
			}
		}
		if err := wr.Finalize(); err != nil {
			t.Fatalf("Finalize: %v", err)
		}
	}()

	in, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer in.Close()
	r, err := NewDeepTiledReader(in)
	if err != nil {
		t.Fatalf("NewDeepTiledReader: %v", err)
	}
	hdr := in.Header(0)
	if hdr.NumXLevels() < 5 {
		t.Fatalf("the fixture has %d levels; it needs several below the tile size to "+
			"exercise the clipping", hdr.NumXLevels())
	}

	for lv := 0; lv < hdr.NumXLevels(); lv++ {
		lw, lh := hdr.LevelWidth(lv), hdr.LevelHeight(lv)
		fb := NewDeepFrameBuffer(lw, lh)
		for _, n := range names {
			fb.Insert(n, PixelTypeFloat)
		}
		r.SetFrameBuffer(fb)
		for ty := 0; ty < (lh+tile-1)/tile; ty++ {
			for tx := 0; tx < (lw+tile-1)/tile; tx++ {
				if err := r.ReadTileLevel(tx, ty, lv, lv); err != nil {
					t.Fatalf("level %d (%dx%d) ReadTileLevel(%d,%d): %v",
						lv, lw, lh, tx, ty, err)
				}
			}
		}
		for y := 0; y < lh; y++ {
			for x := 0; x < lw; x++ {
				if fb.GetSampleCount(x, y) == 0 {
					t.Fatalf("level %d: pixel (%d,%d) came back with no samples", lv, x, y)
				}
				got := fb.Slices["Z"].GetSampleFloat32(x, y, 0)
				want := float32(lv*1000 + y*10 + x)
				if got != want {
					t.Fatalf("level %d: Z at (%d,%d) = %v, want %v", lv, x, y, got, want)
				}
			}
		}
	}
}
