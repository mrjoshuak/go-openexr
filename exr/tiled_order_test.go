package exr

import (
	"errors"
	"os"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// These tests assert properties of the bytes on disk, not the result of reading
// them back with this library. A round trip cannot see an offset table indexed
// the wrong way, because the reader indexes it the same wrong way; the external
// gate in scripts/validate.sh caught this one, and these keep it caught without
// the OpenEXR toolchain installed.

// tiledOrderHeader builds a tiled header with a distinguishable channel set.
func tiledOrderHeader(w, h, tw, th int, mode LevelMode) *Header {
	hd := NewTiledHeader(w, h, tw, th)
	hd.SetCompression(CompressionZIP)
	hd.SetTileDescription(TileDescription{XSize: uint32(tw), YSize: uint32(th), Mode: mode})
	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	hd.SetChannels(cl)
	return hd
}

func tiledOrderFrameBuffer(w, h int) *FrameBuffer {
	data := make([]half.Half, w*h)
	for i := range data {
		data[i] = half.FromFloat32(float32(i % 61))
	}
	fb := NewFrameBuffer()
	fb.Set("R", NewSliceFromHalf(data, w, h))
	return fb
}

// tileCoordsInFileOrder walks the chunk offset table of a single-part tiled
// file and reports the (tileX, tileY, levelX, levelY) each slot points at.
func tileCoordsInFileOrder(t *testing.T, data []byte, numChunks int) [][4]int {
	t.Helper()

	r := xdr.NewReader(data[8:]) // magic and version
	if _, err := ReadHeader(r); err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	tableStart := 8 + r.Pos()

	coords := make([][4]int, numChunks)
	for i := 0; i < numChunks; i++ {
		off := int(xdr.ByteOrder.Uint64(data[tableStart+i*8:]))
		if off <= 0 || off+20 > len(data) {
			t.Fatalf("chunk %d: offset %d is outside the file (%d bytes)", i, off, len(data))
		}
		coords[i] = [4]int{
			int(xdr.ByteOrder.Uint32(data[off:])),
			int(xdr.ByteOrder.Uint32(data[off+4:])),
			int(xdr.ByteOrder.Uint32(data[off+8:])),
			int(xdr.ByteOrder.Uint32(data[off+12:])),
		}
	}
	return coords
}

// canonicalTileOrder is the sequence the format assigns to a tiled part's chunk
// offset table, written out here independently of tileChunkIndex so the two
// have to agree.
func canonicalTileOrder(h *Header) [][4]int {
	var want [][4]int
	add := func(lx, ly int) {
		for ty := 0; ty < h.NumYTiles(ly); ty++ {
			for tx := 0; tx < h.NumXTiles(lx); tx++ {
				want = append(want, [4]int{tx, ty, lx, ly})
			}
		}
	}
	switch h.TileDescription().Mode {
	case LevelModeOne:
		add(0, 0)
	case LevelModeMipmap:
		for l := 0; l < h.NumXLevels(); l++ {
			add(l, l)
		}
	case LevelModeRipmap:
		for ly := 0; ly < h.NumYLevels(); ly++ {
			for lx := 0; lx < h.NumXLevels(); lx++ {
				add(lx, ly)
			}
		}
	}
	return want
}

// TestTiledWriterOffsetTableIsIndexedByCoordinate writes every tile of a file in
// reverse order and requires the offset table to still be in the order the
// format defines. Before this was fixed the table followed the writes, and the
// reference implementation rejected the file outright with
// "bad tile x coordinate (2, expect 0)".
func TestTiledWriterOffsetTableIsIndexedByCoordinate(t *testing.T) {
	tests := []struct {
		name         string
		w, h, tw, th int
		mode         LevelMode
	}{
		{"one level, partial edge tiles", 71, 40, 32, 32, LevelModeOne},
		{"mipmap", 71, 40, 16, 16, LevelModeMipmap},
		{"ripmap", 40, 24, 16, 16, LevelModeRipmap},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hd := tiledOrderHeader(tt.w, tt.h, tt.tw, tt.th, tt.mode)
			buf := newMemWriteSeeker()

			wr, err := NewTiledWriter(buf, hd)
			if err != nil {
				t.Fatalf("NewTiledWriter: %v", err)
			}
			wr.SetFrameBuffer(tiledOrderFrameBuffer(tt.w, tt.h))

			// Reverse of the canonical order: last level first, last tile first.
			want := canonicalTileOrder(hd)
			for i := len(want) - 1; i >= 0; i-- {
				c := want[i]
				if err := wr.WriteTileLevel(c[0], c[1], c[2], c[3]); err != nil {
					t.Fatalf("WriteTileLevel%v: %v", c, err)
				}
			}
			if err := wr.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got := tileCoordsInFileOrder(t, buf.Bytes(), len(want))
			if len(got) != len(want) {
				t.Fatalf("offset table has %d slots, want %d", len(got), len(want))
			}
			for i := range want {
				if got[i] != want[i] {
					t.Errorf("offset table slot %d points at tile %v, want %v", i, got[i], want[i])
				}
			}
		})
	}
}

// TestTileChunkIndexMatchesCanonicalOrder pins the index arithmetic itself: the
// slot a tile is assigned must be its position in the canonical sequence, with
// no gaps and no collisions.
func TestTileChunkIndexMatchesCanonicalOrder(t *testing.T) {
	for _, mode := range []LevelMode{LevelModeOne, LevelModeMipmap, LevelModeRipmap} {
		for _, round := range []LevelRoundingMode{LevelRoundDown, LevelRoundUp} {
			hd := tiledOrderHeader(71, 40, 16, 16, mode)
			td := hd.TileDescription()
			td.RoundingMode = round
			hd.SetTileDescription(*td)

			want := canonicalTileOrder(hd)
			if len(want) != hd.ChunksInFile() {
				t.Fatalf("mode %v round %v: canonical order has %d tiles, ChunksInFile says %d",
					mode, round, len(want), hd.ChunksInFile())
			}
			for i, c := range want {
				got, err := tileChunkIndex(hd, c[0], c[1], c[2], c[3])
				if err != nil {
					t.Fatalf("mode %v round %v: tileChunkIndex%v: %v", mode, round, c, err)
				}
				if got != i {
					t.Errorf("mode %v round %v: tile %v got slot %d, want %d", mode, round, c, got, i)
				}
			}
		}
	}
}

func TestTileChunkIndexRejectsOutOfRange(t *testing.T) {
	hd := tiledOrderHeader(71, 40, 16, 16, LevelModeMipmap)

	cases := []struct {
		name                         string
		tileX, tileY, levelX, levelY int
		want                         error
	}{
		{"level past the last", 0, 0, hd.NumXLevels(), hd.NumXLevels(), ErrLevelOutOfRange},
		{"negative level", 0, 0, -1, -1, ErrLevelOutOfRange},
		{"mipmap levels must match", 0, 0, 1, 2, ErrLevelOutOfRange},
		{"tile past the last column", hd.NumXTiles(0), 0, 0, 0, ErrTileOutOfRange},
		{"tile past the last row", 0, hd.NumYTiles(0), 0, 0, ErrTileOutOfRange},
		{"negative tile", -1, 0, 0, 0, ErrTileOutOfRange},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := tileChunkIndex(hd, c.tileX, c.tileY, c.levelX, c.levelY); !errors.Is(err, c.want) {
				t.Errorf("tileChunkIndex = %v, want %v", err, c.want)
			}
		})
	}
}

// TestValidateRejectsSubsampledTiledChannels covers the other half of what the
// gate found: the format forbids subsampled channels in a tiled image, and the
// reference implementation will not open a file that has them
// ("channel 'BY': x subsampling factor is not 1 (2) for a tiled image").
// scripts/testdata/tiled_subsampled_invalid.exr is one this library wrote
// before this check existed.
func TestValidateRejectsSubsampledTiledChannels(t *testing.T) {
	hd := NewTiledHeader(32, 16, 16, 16)
	cl := NewChannelList()
	cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	cl.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	hd.SetChannels(cl)

	if err := hd.Validate(); !errors.Is(err, ErrTiledSubsampling) {
		t.Errorf("Validate = %v, want ErrTiledSubsampling", err)
	}
	if _, err := NewTiledWriter(newMemWriteSeeker(), hd); !errors.Is(err, ErrTiledSubsampling) {
		t.Errorf("NewTiledWriter = %v, want ErrTiledSubsampling", err)
	}

	// A scanline header with the same channels stays legal: subsampling is only
	// forbidden when the image is tiled.
	sl := NewScanlineHeader(32, 16)
	sl.SetChannels(cl)
	if err := sl.Validate(); err != nil {
		t.Errorf("scanline Validate = %v, want nil", err)
	}
}

// TestTiledSubsampledFixtureIsStillIllegal keeps the committed evidence honest:
// if the file the reference refuses ever stops parsing as a tiled header with a
// subsampled channel, the guard above is guarding nothing.
func TestTiledSubsampledFixtureIsStillIllegal(t *testing.T) {
	data, err := os.ReadFile("../scripts/testdata/tiled_subsampled_invalid.exr")
	if err != nil {
		t.Skipf("fixture not present: %v", err)
	}
	r := xdr.NewReader(data[8:])
	hd, err := ReadHeader(r)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if !hd.IsTiled() {
		t.Fatal("fixture is not tiled; it no longer demonstrates the rule")
	}
	if err := hd.Validate(); !errors.Is(err, ErrTiledSubsampling) {
		t.Fatalf("Validate = %v, want ErrTiledSubsampling", err)
	}
}

// memWriteSeeker is an in-memory io.WriteSeeker so these tests never touch the
// filesystem.
type memWriteSeeker struct {
	buf []byte
	pos int64
}

func newMemWriteSeeker() *memWriteSeeker { return &memWriteSeeker{} }

func (m *memWriteSeeker) Bytes() []byte { return m.buf }

func (m *memWriteSeeker) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	if end > int64(len(m.buf)) {
		grown := make([]byte, end)
		copy(grown, m.buf)
		m.buf = grown
	}
	copy(m.buf[m.pos:end], p)
	m.pos = end
	return len(p), nil
}

func (m *memWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		m.pos = offset
	case 1:
		m.pos += offset
	case 2:
		m.pos = int64(len(m.buf)) + offset
	default:
		return 0, errors.New("bad whence")
	}
	if m.pos < 0 {
		return 0, errors.New("negative position")
	}
	return m.pos, nil
}
