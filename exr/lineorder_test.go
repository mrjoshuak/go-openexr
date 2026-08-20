package exr

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// lineOrderLayout returns the y of each chunk in the order the chunks sit in
// the file.
//
// The chunk offset table is always ordered by increasing y, whatever the line
// order is: it is the index a reader seeks with. lineOrder describes where the
// chunks are laid out, so the offsets have to be sorted and the y read at each.
// Reading the table in table order would show ascending y for every file and
// measure nothing, which is part of why this went unnoticed.
func lineOrderLayout(t *testing.T, path string, numChunks int) []int32 {
	t.Helper()
	d, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	i := 8 // past the magic and version
	for {
		start := i
		for d[i] != 0 {
			i++
		}
		if i == start { // the empty name that ends the header
			i++
			break
		}
		i++
		for d[i] != 0 { // type
			i++
		}
		i++
		size := int32(binary.LittleEndian.Uint32(d[i : i+4]))
		i += 4 + int(size)
	}
	offs := make([]int64, 0, numChunks)
	for c := 0; c < numChunks; c++ {
		offs = append(offs, int64(binary.LittleEndian.Uint64(d[i:i+8])))
		i += 8
	}
	sort.Slice(offs, func(a, b int) bool { return offs[a] < offs[b] })
	ys := make([]int32, 0, numChunks)
	for _, off := range offs {
		ys = append(ys, int32(binary.LittleEndian.Uint32(d[off:off+4])))
	}
	return ys
}

func writeLineOrderFile(t *testing.T, path string, order LineOrder, w, h int) error {
	t.Helper()
	hdr := NewScanlineHeader(w, h)
	hdr.SetCompression(CompressionZIPS)
	hdr.SetLineOrder(order)
	cl := NewChannelList()
	cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	hdr.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()
	wr, err := NewScanlineWriter(f, hdr)
	if err != nil {
		return err
	}
	fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
	s := fb.Get("Y")
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			s.SetFloat32(x, y, float32(y*100+x))
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		return err
	}
	return wr.Close()
}

// TestLineOrderIsHonoured checks that the file is laid out the way its header
// says it is.
//
// lineOrder was stored and ignored: every file was written ascending whatever
// it declared. Nothing could catch it. The reference reads through the offset
// table and does not care about the physical order, so it accepts such a file
// without complaint, and a round trip through this library is equally blind.
// The header claimed one thing and the bytes did another, and only the layout
// shows it.
func TestLineOrderIsHonoured(t *testing.T) {
	const w, h = 16, 12
	dir := t.TempDir()

	for _, c := range []struct {
		name  string
		order LineOrder
		first int32
		last  int32
	}{
		{"increasing", LineOrderIncreasing, 0, int32(h - 1)},
		{"decreasing", LineOrderDecreasing, int32(h - 1), 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			path := filepath.Join(dir, c.name+".exr")
			if err := writeLineOrderFile(t, path, c.order, w, h); err != nil {
				t.Fatalf("write: %v", err)
			}
			ys := lineOrderLayout(t, path, h)
			if len(ys) != h {
				t.Fatalf("found %d chunks, want %d", len(ys), h)
			}
			if ys[0] != c.first || ys[len(ys)-1] != c.last {
				t.Errorf("chunks are laid out %v; the header promises the first at y=%d "+
					"and the last at y=%d", ys, c.first, c.last)
			}

			// And the file must still read back correctly, since reordering
			// the chunks must not disturb the offset table.
			f, err := OpenFile(path)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			defer f.Close()
			r, err := NewScanlineReader(f)
			if err != nil {
				t.Fatalf("NewScanlineReader: %v", err)
			}
			fb, _ := AllocateChannels(f.Header(0).Channels(), f.Header(0).DataWindow())
			r.SetFrameBuffer(fb)
			if err := r.ReadPixels(0, h-1); err != nil {
				t.Fatalf("ReadPixels: %v", err)
			}
			s := fb.Get("Y")
			for y := 0; y < h; y++ {
				for x := 0; x < w; x++ {
					if got, want := s.GetFloat32(x, y), float32(y*100+x); got != want {
						t.Fatalf("(%d,%d) = %v, want %v", x, y, got, want)
					}
				}
			}
		})
	}
}

// TestRandomYIsRefusedOnAScanlinePart pins the other half.
//
// RANDOM_Y is tiled-only — ImfLineOrder.h says so in as many words — and a
// scanline part has no way to express it, since scanlines are chunks of
// consecutive rows. It used to be accepted and written as increasing, producing
// a file whose header claimed something the format does not allow.
func TestRandomYIsRefusedOnAScanlinePart(t *testing.T) {
	dir := t.TempDir()
	err := writeLineOrderFile(t, filepath.Join(dir, "random.exr"), LineOrderRandom, 16, 12)
	if err == nil {
		t.Fatal("RANDOM_Y was accepted on a scanline part; it is only for tiled files")
	}
	if !strings.Contains(err.Error(), "tiled") {
		t.Errorf("the refusal does not say why: %v", err)
	}

	// The control: the two orders a scanline part may declare must still work,
	// or this is satisfied by a writer that refuses everything.
	for _, o := range []LineOrder{LineOrderIncreasing, LineOrderDecreasing} {
		if err := writeLineOrderFile(t, filepath.Join(dir, "ok.exr"), o, 16, 12); err != nil {
			t.Errorf("line order %v was refused: %v", o, err)
		}
	}
}
