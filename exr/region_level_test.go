package exr

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// levelSample depends on the level as well as both coordinates, so a region
// read that returned the wrong level — or the right level at the wrong offset —
// cannot pass. Reading level 1 and getting level 0's pixels is the failure this
// is built to catch, and with a per-level constant it is unmistakable.
func levelSample(ci, level, x, y int) float32 {
	return float32(ci)*10000 + float32(level)*1000 + float32(x) + float32(y)*0.125
}

// writeMipFixture writes a mipmapped tiled file whose every level holds
// different content, and returns the path.
func writeMipFixture(t *testing.T, dir string, comp Compression, size, tile, offX, offY int) string {
	t.Helper()

	hdr := NewTiledHeader(size, size, tile, tile)
	hdr.SetDataWindow(Box2i{
		Min: V2i{X: int32(offX), Y: int32(offY)},
		Max: V2i{X: int32(offX + size - 1), Y: int32(offY + size - 1)},
	})
	hdr.SetCompression(comp)
	hdr.SetTileDescription(TileDescription{
		XSize: uint32(tile), YSize: uint32(tile),
		Mode: LevelModeMipmap, RoundingMode: LevelRoundDown,
	})
	cl := NewChannelList()
	for _, n := range []string{"G", "R"} {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	path := filepath.Join(dir, fmt.Sprintf("mip_%s.exr", comp.String()))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	wr, err := NewTiledWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewTiledWriter: %v", err)
	}

	for level := 0; level < hdr.NumXLevels(); level++ {
		lw, lh := hdr.LevelWidth(level), hdr.LevelHeight(level)
		lvl := Box2i{
			Min: V2i{X: int32(offX), Y: int32(offY)},
			Max: V2i{X: int32(offX + lw - 1), Y: int32(offY + lh - 1)},
		}
		fb, _ := AllocateChannels(hdr.Channels(), lvl)
		for ci, name := range []string{"G", "R"} {
			s := fb.Get(name)
			for y := 0; y < lh; y++ {
				for x := 0; x < lw; x++ {
					s.SetFloat32(offX+x, offY+y, levelSample(ci, level, x, y))
				}
			}
		}
		wr.SetFrameBuffer(fb)
		nx := (lw + tile - 1) / tile
		ny := (lh + tile - 1) / tile
		for ty := 0; ty < ny; ty++ {
			for tx := 0; tx < nx; tx++ {
				if err := wr.WriteTileLevel(tx, ty, level, level); err != nil {
					t.Fatalf("WriteTileLevel(%d,%d,%d): %v", tx, ty, level, err)
				}
			}
		}
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path
}

// TestReadRegionLevelServesAMipmapLevel is the capability an HD viewer of a 5K
// plate actually needs.
//
// The codestream's own resolution levels are the wrong mechanism for float EXR
// — they average reinterpreted bit patterns — so the pyramid in the file is the
// answer, and this is the one call that serves a rectangle of it. Everything
// underneath already took a level; ReadRegion was hardcoded to zero.
func TestReadRegionLevelServesAMipmapLevel(t *testing.T) {
	const size, tile = 256, 32
	const offX, offY = 13, -7
	dir := t.TempDir()

	for _, comp := range []Compression{CompressionHTJ2K256, CompressionZIP} {
		t.Run(comp.String(), func(t *testing.T) {
			path := writeMipFixture(t, dir, comp, size, tile, offX, offY)
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			f, err := Open(bytes.NewReader(raw), int64(len(raw)))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			h := f.Header(0)

			for level := 0; level < h.NumXLevels(); level++ {
				lw, lh := h.LevelWidth(level), h.LevelHeight(level)
				if lw < 16 || lh < 16 {
					continue // too small for a meaningful sub-rectangle
				}
				// A rectangle inside the level, deliberately not at its origin.
				region := Box2i{
					Min: V2i{X: int32(offX + 4), Y: int32(offY + 4)},
					Max: V2i{X: int32(offX + lw/2), Y: int32(offY + lh/2)},
				}
				got, err := f.ReadRegionLevel(0, region, level, level)
				if err != nil {
					t.Fatalf("level %d: %v", level, err)
				}
				if got.Region != region {
					t.Fatalf("level %d covered %v, want %v", level, got.Region, region)
				}
				rw := int(region.Max.X-region.Min.X) + 1
				for ci, name := range []string{"G", "R"} {
					plane := got.Planes[name]
					for y := int(region.Min.Y); y <= int(region.Max.Y); y++ {
						for x := int(region.Min.X); x <= int(region.Max.X); x++ {
							g := plane[(y-int(region.Min.Y))*rw+(x-int(region.Min.X))]
							w := levelSample(ci, level, x-offX, y-offY)
							if g != w {
								t.Fatalf("level %d channel %s (%d,%d) = %v, want %v",
									level, name, x, y, g, w)
							}
						}
					}
				}
				if got.ChunksRead >= got.ChunksTotal {
					t.Errorf("level %d read %d of %d chunks; a rectangle of one level "+
						"must read fewer than the whole pyramid",
						level, got.ChunksRead, got.ChunksTotal)
				}
				t.Logf("%s level %d (%dx%d): %v -> %d of %d chunks, %d of %d file bytes",
					comp.String(), level, lw, lh, region,
					got.ChunksRead, got.ChunksTotal, got.FileBytes, len(raw))
			}
		})
	}
}

// TestReadRegionDefaultsToLevelZero pins the compatibility half: the existing
// one-argument call must keep meaning full resolution.
func TestReadRegionDefaultsToLevelZero(t *testing.T) {
	const size, tile = 128, 32
	const offX, offY = 5, 3
	dir := t.TempDir()
	path := writeMipFixture(t, dir, CompressionZIP, size, tile, offX, offY)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	f, err := Open(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	region := Box2i{Min: V2i{X: offX + 8, Y: offY + 8}, Max: V2i{X: offX + 39, Y: offY + 39}}
	plain, err := f.ReadRegion(0, region)
	if err != nil {
		t.Fatalf("ReadRegion: %v", err)
	}
	explicit, err := f.ReadRegionLevel(0, region, 0, 0)
	if err != nil {
		t.Fatalf("ReadRegionLevel(0,0): %v", err)
	}
	for _, name := range plain.Channels {
		a, b := plain.Planes[name], explicit.Planes[name]
		if len(a) != len(b) {
			t.Fatalf("channel %s: %d samples vs %d", name, len(a), len(b))
		}
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("channel %s sample %d differs between ReadRegion and "+
					"ReadRegionLevel(0,0)", name, i)
			}
		}
	}

	// And a level that does not exist must be refused rather than clamped.
	if _, err := f.ReadRegionLevel(0, region, 99, 99); err == nil {
		t.Error("level 99 was accepted")
	}
	// A mipmapped part has one level index; independent ones are a ripmap.
	if _, err := f.ReadRegionLevel(0, region, 1, 0); err == nil {
		t.Error("a mipmapped part accepted independent x and y levels")
	}
}
