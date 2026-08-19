package exr

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// These are the cases from issue #7: reads and writes through a frame buffer
// for a file whose data window does not start at the origin.
//
// Two things are checked and they are not the same thing. A frame buffer that
// covers the window must read back exactly what was written, band-shaped
// buffers included — that is the feature. A frame buffer that does not cover it
// must produce an error naming the mismatch — that is the safety, and its
// absence is what made the first report look like wrong pixels when it was
// writing past the end of every plane.

const (
	wrW, wrH       = 10, 8
	wrMinX, wrMinY = 5, 3
)

var wrNames = []string{"A", "B", "G", "R"}

// wrValue encodes the channel and both coordinates, so a shifted row, a
// transposed axis and a channel swap are all distinguishable from each other.
func wrValue(ch, x, y int) float32 {
	return float32(ch)*1000 + float32(y)*100 + float32(x)
}

func writeOffsetWindowFile(t *testing.T, path string) Box2i {
	t.Helper()
	h := NewScanlineHeader(wrW, wrH)
	h.SetCompression(CompressionNone)
	dw := Box2i{
		Min: V2i{X: wrMinX, Y: wrMinY},
		Max: V2i{X: wrMinX + wrW - 1, Y: wrMinY + wrH - 1},
	}
	h.SetDataWindow(dw)
	cl := NewChannelList()
	for _, n := range wrNames {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	w, err := NewScanlineWriter(f, h)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}
	fb, _ := AllocateChannels(h.Channels(), dw)
	for ci, n := range wrNames {
		s := fb.Get(n)
		for y := int(dw.Min.Y); y <= int(dw.Max.Y); y++ {
			for x := int(dw.Min.X); x <= int(dw.Max.X); x++ {
				s.SetFloat32(x, y, wrValue(ci, x, y))
			}
		}
	}
	w.SetFrameBuffer(fb)
	if err := w.WritePixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return dw
}

func openOffsetWindowFile(t *testing.T, path string) *File {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	f, err := Open(bytes.NewReader(raw), int64(len(raw)))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return f
}

// TestReadThroughAnOffsetWindow reads the whole data window and a band of it,
// through buffers allocated for those exact rectangles.
func TestReadThroughAnOffsetWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.exr")
	dw := writeOffsetWindowFile(t, path)
	f := openOffsetWindowFile(t, path)

	check := func(t *testing.T, fb *FrameBuffer, box Box2i) {
		t.Helper()
		for ci, n := range wrNames {
			s := fb.Get(n)
			for y := int(box.Min.Y); y <= int(box.Max.Y); y++ {
				for x := int(box.Min.X); x <= int(box.Max.X); x++ {
					got, want := s.GetFloat32(x, y), wrValue(ci, x, y)
					if got != want {
						t.Fatalf("channel %s at (%d,%d) = %v, want %v", n, x, y, got, want)
					}
				}
			}
		}
	}

	t.Run("whole window", func(t *testing.T) {
		fb, _ := AllocateChannels(f.Header(0).Channels(), dw)
		r, err := NewScanlineReaderPart(f, 0)
		if err != nil {
			t.Fatalf("NewScanlineReaderPart: %v", err)
		}
		r.SetFrameBuffer(fb)
		if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
			t.Fatalf("ReadPixels: %v", err)
		}
		check(t, fb, dw)
	})

	// A band-shaped buffer is the case the report was really about: staging a
	// few rows at a time rather than a whole frame. It works, and the rows it
	// is allocated for are the absolute rows it is asked to read.
	t.Run("band", func(t *testing.T) {
		band := Box2i{
			Min: V2i{X: dw.Min.X, Y: dw.Min.Y + 4},
			Max: V2i{X: dw.Max.X, Y: dw.Min.Y + 7},
		}
		fb, _ := AllocateChannels(f.Header(0).Channels(), band)
		r, err := NewScanlineReaderPart(f, 0)
		if err != nil {
			t.Fatalf("NewScanlineReaderPart: %v", err)
		}
		r.SetFrameBuffer(fb)
		if err := r.ReadPixels(int(band.Min.Y), int(band.Max.Y)); err != nil {
			t.Fatalf("ReadPixels: %v", err)
		}
		check(t, fb, band)
	})
}

// TestFrameBufferThatCannotHoldTheReadIsRefused is the safety half.
//
// The guard band is the point of the test rather than decoration: before this,
// a buffer allocated for a window at the origin against a data window at (5, 3)
// overwrote 30 float32 words past the end of every plane and ReadPixels
// returned nil. An error message alone would not prove that stopped.
func TestFrameBufferThatCannotHoldTheReadIsRefused(t *testing.T) {
	path := filepath.Join(t.TempDir(), "offset.exr")
	dw := writeOffsetWindowFile(t, path)
	f := openOffsetWindowFile(t, path)

	const sentinel = -12345
	const guard = wrW * 8
	planes := map[string][]float32{}
	fb := NewFrameBuffer()
	for _, n := range wrNames {
		buf := make([]float32, wrW*wrH+guard)
		for i := range buf {
			buf[i] = sentinel
		}
		// The mismatch: a buffer whose window starts at the origin, for a file
		// whose data window does not.
		fb.Set(n, NewSliceFromFloat32(buf[:wrW*wrH], wrW, wrH))
		planes[n] = buf
	}

	r, err := NewScanlineReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewScanlineReaderPart: %v", err)
	}
	r.SetFrameBuffer(fb)
	err = r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y))
	if !errors.Is(err, ErrFrameBufferTooSmall) {
		t.Fatalf("ReadPixels returned %v, want ErrFrameBufferTooSmall", err)
	}

	for _, n := range wrNames {
		buf := planes[n]
		for i := wrW * wrH; i < len(buf); i++ {
			if buf[i] != sentinel {
				t.Fatalf("channel %s: word %d past the plane was overwritten (%v)",
					n, i-wrW*wrH, buf[i])
			}
		}
	}
}

// TestWriteThroughAFrameBufferThatIsTooSmallIsRefused covers the other
// direction. A writer reading past the end of a plane produces a file full of
// whatever followed it in memory, which is worse than an error and harder to
// notice.
func TestWriteThroughAFrameBufferThatIsTooSmallIsRefused(t *testing.T) {
	h := NewScanlineHeader(wrW, wrH)
	h.SetCompression(CompressionNone)
	dw := Box2i{
		Min: V2i{X: wrMinX, Y: wrMinY},
		Max: V2i{X: wrMinX + wrW - 1, Y: wrMinY + wrH - 1},
	}
	h.SetDataWindow(dw)
	cl := NewChannelList()
	for _, n := range wrNames {
		cl.Add(Channel{Name: n, Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)

	f, err := os.Create(filepath.Join(t.TempDir(), "out.exr"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	w, err := NewScanlineWriter(f, h)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}

	fb := NewFrameBuffer()
	for _, n := range wrNames {
		buf := make([]float32, wrW*wrH)
		fb.Set(n, NewSliceFromFloat32(buf, wrW, wrH))
	}
	w.SetFrameBuffer(fb)
	if err := w.WritePixels(int(dw.Min.Y), int(dw.Max.Y)); !errors.Is(err, ErrFrameBufferTooSmall) {
		t.Fatalf("WritePixels returned %v, want ErrFrameBufferTooSmall", err)
	}
}

// TestTiledReadRefusesAFrameBufferThatCannotHoldATile is the same guard on the
// tiled path, which writes through the per-pixel accessors and so had the same
// hole. Without it this fix would cover the codec every reporter happened to
// use and leave the other one open.
func TestTiledReadRefusesAFrameBufferThatCannotHoldATile(t *testing.T) {
	const w, h, tile = 64, 64, 32
	dir := t.TempDir()
	path, _ := writeTiledFixture(t, dir, CompressionZIP, w, h, tile, 11, 5)
	f := openOffsetWindowFile(t, path)

	fb := NewFrameBuffer()
	for _, n := range []string{"B", "G", "R"} {
		buf := make([]float32, w*h)
		// Origin left at zero while the data window starts at (11, 5).
		fb.Set(n, NewSliceFromFloat32(buf, w, h))
	}
	r, err := NewTiledReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewTiledReaderPart: %v", err)
	}
	r.SetFrameBuffer(fb)
	// The last tile is the one that runs off the end: at a window of (11, 5)
	// it covers (43,37)-(74,68), and the buffer stops at 63. The first tile
	// fits inside a zero-origin buffer of the same size, which is why a check
	// written against tile (0,0) would pass while the corruption stayed.
	if err := r.ReadTile(1, 1); !errors.Is(err, ErrFrameBufferTooSmall) {
		t.Fatalf("ReadTile(1,1) returned %v, want ErrFrameBufferTooSmall", err)
	}

	// The control: the same read through a buffer that does cover the window
	// must succeed, or this is satisfied by a reader that refuses everything.
	good, _ := AllocateChannels(f.Header(0).Channels(), f.Header(0).DataWindow())
	r2, err := NewTiledReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewTiledReaderPart: %v", err)
	}
	r2.SetFrameBuffer(good)
	if err := r2.ReadTile(1, 1); err != nil {
		t.Fatalf("ReadTile through a covering buffer: %v", err)
	}
}
