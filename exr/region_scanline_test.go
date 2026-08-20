package exr

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// writeScanlineFixture writes a scanline EXR and returns its path and the
// samples it was written from, indexed window-relative.
func writeScanlineFixture(t *testing.T, dir string, comp Compression, w, h, offX, offY int) (string, map[string][]float32) {
	t.Helper()

	hdr := NewScanlineHeader(w, h)
	hdr.SetDataWindow(Box2i{
		Min: V2i{X: int32(offX), Y: int32(offY)},
		Max: V2i{X: int32(offX + w - 1), Y: int32(offY + h - 1)},
	})
	hdr.SetCompression(comp)
	cl := NewChannelList()
	for _, n := range []string{"G", "R"} {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	hdr.SetChannels(cl)

	want := map[string][]float32{}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	for ci, name := range []string{"G", "R"} {
		plane := make([]float32, w*h)
		s := fb.Get(name)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				v := float32(ci)*1000 + float32(x)*0.25 + float32(y)*0.125
				plane[y*w+x] = v
				s.SetFloat32(offX+x, offY+y, v)
			}
		}
		want[name] = plane
	}

	path := filepath.Join(dir, fmt.Sprintf("scan_%s.exr", comp.String()))
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	wr, err := NewScanlineWriter(f, hdr)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(offY, offY+h-1); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if err := wr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return path, want
}

// TestReadRegionOnAScanlinePart is the capability: most EXRs in the world are
// scanline, and ReadRegion refused them outright.
//
// The geometry is the point of the difference. A scanline chunk is the full
// width of the data window by 32 or 256 rows, so a viewport pulls whole rows —
// the chunk-level saving is weaker than a tiled part's — while for HTJ2K the
// viewport is a small part of a very wide chunk, which is where the codestream
// saving is largest.
func TestReadRegionOnAScanlinePart(t *testing.T) {
	const w, h = 512, 512
	dir := t.TempDir()

	for _, comp := range []Compression{CompressionHTJ2K256, CompressionZIP} {
		for _, off := range []struct {
			name       string
			offX, offY int
		}{{"origin", 0, 0}, {"offset", 13, -7}} {
			t.Run(comp.String()+"_"+off.name, func(t *testing.T) {
				path, want := writeScanlineFixture(t, dir, comp, w, h, off.offX, off.offY)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile: %v", err)
				}
				f, err := Open(bytes.NewReader(raw), int64(len(raw)))
				if err != nil {
					t.Fatalf("Open: %v", err)
				}

				region := Box2i{
					Min: V2i{X: int32(off.offX + 100), Y: int32(off.offY + 100)},
					Max: V2i{X: int32(off.offX + 227), Y: int32(off.offY + 227)},
				}
				got, err := f.ReadRegion(0, region)
				if err != nil {
					t.Fatalf("ReadRegion: %v", err)
				}
				if got.Region != region {
					t.Fatalf("covered %v, want %v", got.Region, region)
				}
				rw := int(region.Max.X-region.Min.X) + 1
				for _, name := range got.Channels {
					plane := got.Planes[name]
					for y := int(region.Min.Y); y <= int(region.Max.Y); y++ {
						for x := int(region.Min.X); x <= int(region.Max.X); x++ {
							g := plane[(y-int(region.Min.Y))*rw+(x-int(region.Min.X))]
							wv := want[name][(y-off.offY)*w+(x-off.offX)]
							if g != wv {
								t.Fatalf("channel %s (%d,%d) = %v, want %v", name, x, y, g, wv)
							}
						}
					}
				}
				if got.FileBytes >= int64(len(raw)) {
					t.Errorf("read %d bytes of a %d-byte file; a viewport must read fewer",
						got.FileBytes, len(raw))
				}
				t.Logf("%s window (%d,%d): %d of %d chunks, %d of %d file bytes, decoded %d skipped %d",
					comp.String(), off.offX, off.offY, got.ChunksRead, got.ChunksTotal,
					got.FileBytes, len(raw), got.DecodedBytes, got.SkippedBytes)
			})
		}
	}
}

// TestReadRegionOnAScanlinePartCostsLess is the measurement half.
//
// A scanline HTJ2K chunk is 256 rows at the full width, so this is the geometry
// where the codestream saving is largest — the opposite of a 256x256 tile,
// where the chunk is already viewport-sized and nothing can be skipped. The
// image is wide for that reason.
func TestReadRegionOnAScanlinePartCostsLess(t *testing.T) {
	const w, h = 2048, 512
	dir := t.TempDir()
	path, _ := writeScanlineFixture(t, dir, CompressionHTJ2K256, w, h, 0, 0)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	f, err := Open(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	region := Box2i{Min: V2i{X: 896, Y: 100}, Max: V2i{X: 1151, Y: 355}}
	got, err := f.ReadRegion(0, region)
	if err != nil {
		t.Fatalf("ReadRegion: %v", err)
	}
	if got.SkippedBytes == 0 {
		t.Fatal("a 256-wide viewport of a 2048-wide scanline chunk skipped no code-blocks")
	}
	if got.DecodedBytes >= got.DecodedBytes+got.SkippedBytes {
		t.Fatalf("decoded %d of %d code-block bytes; a viewport must cost less",
			got.DecodedBytes, got.DecodedBytes+got.SkippedBytes)
	}
	total := got.DecodedBytes + got.SkippedBytes
	t.Logf("256x256 viewport of a %dx%d scanline HTJ2K part: %d of %d chunks, "+
		"%d of %d file bytes, decoded %d of %d code-block bytes (%.0f%%)",
		w, h, got.ChunksRead, got.ChunksTotal, got.FileBytes, len(raw),
		got.DecodedBytes, total, 100*float64(got.DecodedBytes)/float64(total))
}
