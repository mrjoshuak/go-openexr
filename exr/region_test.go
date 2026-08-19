package exr

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeTiledFixture writes a tiled EXR of the given size and compression and
// returns its path, together with the samples it was given.
func writeTiledFixture(t *testing.T, dir string, comp Compression, w, h, tile, offX, offY int) (string, map[string][]float32) {
	t.Helper()

	hdr := NewTiledHeader(w, h, tile, tile)
	hdr.SetDataWindow(Box2i{
		Min: V2i{X: int32(offX), Y: int32(offY)},
		Max: V2i{X: int32(offX + w - 1), Y: int32(offY + h - 1)},
	})
	hdr.SetCompression(comp)
	cl := NewChannelList()
	for _, n := range []string{"B", "G", "R"} {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	want := map[string][]float32{}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	for ci, name := range []string{"B", "G", "R"} {
		// The plane is indexed window-relative; the frame buffer takes
		// window-absolute coordinates, as every accessor in this package does.
		plane := make([]float32, w*h)
		s := fb.Get(name)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				// Depends on the channel and on both coordinates, so no
				// transposition, channel swap or tile mix-up leaves it
				// unchanged.
				v := float32(ci)*1000 + float32(x) + float32(y)*0.125
				plane[y*w+x] = v
				s.SetFloat32(offX+x, offY+y, v)
			}
		}
		want[name] = plane
	}

	path := filepath.Join(dir, fmt.Sprintf("region_%s_%d_%d.exr", comp.String(), offX, offY))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	wr, err := NewTiledWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewTiledWriter: %v", err)
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
	return path, want
}

// TestReadRegionMatchesTheWholeImage checks the viewport read against the
// samples the file was written from, for a codec with an addressable interior
// and one without, and for a data window at the origin and away from it.
//
// Both codecs are run because they take different paths through ReadRegion —
// HTJ2K decodes the region out of the codestream, everything else decompresses
// the chunk whole and crops — and a viewport that worked on one and not the
// other would look fine in any test that only used the codec its author had in
// mind.
//
// Both windows are run because a viewport read has three coordinate systems in
// play — image, tile and codestream — and at a window of (0, 0) they all agree.
// Dropping the window's origin from the tile rectangle is invisible until the
// window moves, and it is the defect the mutation harness applies here.
func TestReadRegionMatchesTheWholeImage(t *testing.T) {
	const w, h, tile = 256, 256, 64
	dir := t.TempDir()

	type placement struct {
		name       string
		offX, offY int
	}
	for _, comp := range []Compression{CompressionHTJ2K256, CompressionZIP} {
		for _, p := range []placement{
			{"origin", 0, 0},
			{"offset", 13, -7},
		} {
			t.Run(comp.String()+"_"+p.name, func(t *testing.T) {
				runReadRegionCase(t, dir, comp, w, h, tile, p.offX, p.offY)
			})
		}
	}
}

func runReadRegionCase(t *testing.T, dir string, comp Compression, w, h, tile, offX, offY int) {
	path, want := writeTiledFixture(t, dir, comp, w, h, tile, offX, offY)

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	f, err := Open(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// A viewport straddling four tiles, so the per-tile clipping is
	// exercised on every side rather than only at the origin.
	region := Box2i{
		Min: V2i{X: int32(offX + 100), Y: int32(offY + 90)},
		Max: V2i{X: int32(offX + 163), Y: int32(offY + 153)},
	}
	got, err := f.ReadRegion(0, region)
	if err != nil {
		t.Fatalf("ReadRegion: %v", err)
	}
	if got.Region != region {
		t.Fatalf("ReadRegion covered %v, want %v", got.Region, region)
	}
	rw := int(region.Max.X-region.Min.X) + 1
	if got.ChunksRead >= got.ChunksTotal {
		t.Errorf("the viewport read %d of %d chunks; it must read fewer",
			got.ChunksRead, got.ChunksTotal)
	}
	if got.FileBytes >= int64(len(raw)) {
		t.Errorf("the viewport read %d bytes of a %d-byte file; it must read fewer",
			got.FileBytes, len(raw))
	}

	for _, name := range got.Channels {
		plane := got.Planes[name]
		for y := int(region.Min.Y); y <= int(region.Max.Y); y++ {
			for x := int(region.Min.X); x <= int(region.Max.X); x++ {
				g := plane[(y-int(region.Min.Y))*rw+(x-int(region.Min.X))]
				// want is indexed window-relative.
				wv := want[name][(y-offY)*w+(x-offX)]
				if g != wv {
					t.Fatalf("channel %s (%d,%d) = %v, want %v", name, x, y, g, wv)
				}
			}
		}
	}
	t.Logf("%s window (%d,%d): viewport %v read %d of %d chunks, %d of %d file bytes, decoded %d and skipped %d code-block bytes",
		comp.String(), offX, offY, region, got.ChunksRead, got.ChunksTotal,
		got.FileBytes, len(raw), got.DecodedBytes, got.SkippedBytes)
}

// TestReadRegionSkipsCodeBlocksOnlyForHTJ2K records the difference honestly.
//
// HTJ2K is the only compression in the format whose chunk has an addressable
// interior; a ZIP chunk decompresses whole or not at all. Reporting a saving
// for ZIP would be the easiest possible way for this API to overstate itself,
// so the zero is asserted rather than assumed.
//
// The tiles are 256x256 rather than the 64x64 used above, because below that
// the codestream saving does not exist to be measured. The reference codec's
// parameters are fixed at 128x32 code-blocks and five decompositions
// (internal_ht.cpp), and a code-block's influence is its band rectangle grown
// by the synthesis margin — 64 samples in every direction at the lowest
// resolution — so inside a 64x64 tile every block reaches every pixel. A
// viewport of such a file still costs less, but the saving is all at the chunk
// level, which is what the check above measures.
func TestReadRegionSkipsCodeBlocksOnlyForHTJ2K(t *testing.T) {
	const w, h, tile = 512, 512, 256
	dir := t.TempDir()
	region := Box2i{Min: V2i{X: 8, Y: 8}, Max: V2i{X: 71, Y: 71}}

	read := func(comp Compression) *RegionSamples {
		path, _ := writeTiledFixture(t, dir, comp, w, h, tile, 0, 0)
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile: %v", err)
		}
		f, err := Open(bytes.NewReader(raw), int64(len(raw)))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		got, err := f.ReadRegion(0, region)
		if err != nil {
			t.Fatalf("ReadRegion(%s): %v", comp.String(), err)
		}
		return got
	}

	zip := read(CompressionZIP)
	if zip.DecodedBytes != 0 || zip.SkippedBytes != 0 {
		t.Errorf("ZIP reported %d decoded and %d skipped code-block bytes; a ZIP chunk has no interior to address",
			zip.DecodedBytes, zip.SkippedBytes)
	}

	ht := read(CompressionHTJ2K256)
	if ht.DecodedBytes == 0 {
		t.Fatal("HTJ2K reported no decoded code-block data; the measurement is not wired up")
	}
	if ht.SkippedBytes == 0 {
		t.Error("HTJ2K skipped nothing for a 64x64 viewport of a 256x256 tile; it decoded the chunk and cropped")
	}
	t.Logf("HTJ2K viewport %v: decoded %d and skipped %d code-block bytes across %d chunks",
		region, ht.DecodedBytes, ht.SkippedBytes, ht.ChunksRead)
}

// TestReadRegionRefusesWhatItCannotDo keeps the API from appearing to offer
// more than it delivers. Without this the guards above are satisfied by a
// function that accepts everything and returns whatever it likes.
func TestReadRegionRefusesWhatItCannotDo(t *testing.T) {
	const w, h, tile = 128, 128, 32
	dir := t.TempDir()
	path, _ := writeTiledFixture(t, dir, CompressionZIP, w, h, tile, 0, 0)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	f, err := Open(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// A region entirely outside the data window has no samples to return, and
	// silently returning an empty plane would read as success.
	outside := Box2i{Min: V2i{X: 500, Y: 500}, Max: V2i{X: 600, Y: 600}}
	if _, err := f.ReadRegion(0, outside); err == nil {
		t.Error("a region outside the data window was accepted")
	}
	if _, err := f.ReadRegion(7, Box2i{Max: V2i{X: 1, Y: 1}}); err == nil {
		t.Error("a part index past the end of the file was accepted")
	}
}
