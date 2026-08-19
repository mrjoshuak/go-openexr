package exr

import (
	"testing"
	"unsafe"
)

// TestAllocateChannelsAddressesStayInBounds pins the invariant that every
// address AllocateChannels' slices can produce for the window they were
// allocated for lies inside the buffer allocated for them.
//
// This is the check that was missing. AllocateChannels biased Slice.Base by the
// data window origin, so PixelAddr expected absolute image coordinates, while
// every reader and writer in the package addresses the frame buffer relative to
// the data window — "(minX, minY) maps to buffer position (0, 0)", as
// ScanlineWriter puts it. For any data window that did not start at (0, 0) the
// two disagreed, and because Slice writes through an unchecked unsafe.Pointer,
// the disagreement did not produce an error: it wrote outside the buffer.
//
// Nothing caught it, because a caller that reads back through the same wrong
// addressing gets the values it wrote. Only an external reader — or the Go
// runtime noticing the corrupted heap — can see it. This test needs neither.
func TestAllocateChannelsAddressesStayInBounds(t *testing.T) {
	windows := []struct {
		name string
		box  Box2i
	}{
		{"origin", Box2i{V2i{0, 0}, V2i{63, 47}}},
		{"positive_origin", Box2i{V2i{17, 9}, V2i{80, 56}}},
		{"negative_y", Box2i{V2i{17, -9}, V2i{80, 38}}},
		{"negative_both", Box2i{V2i{-13, -7}, V2i{50, 40}}},
		{"single_pixel_offset", Box2i{V2i{5, 5}, V2i{5, 5}}},
	}

	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	cl.Add(Channel{Name: "Z", Type: PixelTypeFloat, XSampling: 1, YSampling: 1})

	for _, w := range windows {
		t.Run(w.name, func(t *testing.T) {
			fb, bufs := AllocateChannels(cl, w.box)
			if fb == nil {
				t.Fatal("AllocateChannels returned no frame buffer")
			}
			width := int(w.box.Width())
			height := int(w.box.Height())

			for name, buf := range bufs {
				s := fb.Get(name)
				if s == nil {
					t.Fatalf("channel %q has a buffer but no slice", name)
				}
				lo := uintptr(unsafe.Pointer(&buf[0]))
				hi := lo + uintptr(len(buf))

				// The corners are enough: the addressing is affine, so if
				// (0,0) and (width-1, height-1) are inside, everything between
				// them is too.
				ox, oy := int(w.box.Min.X), int(w.box.Min.Y)
				for _, p := range [][2]int{{ox, oy}, {ox + width - 1, oy + height - 1}} {
					addr := uintptr(s.PixelAddr(p[0], p[1]))
					if addr < lo {
						t.Errorf("channel %q pixel (%d,%d): address %#x is %d bytes BEFORE the buffer (%#x..%#x); a write here corrupts the heap",
							name, p[0], p[1], addr, lo-addr, lo, hi)
					}
					if addr+uintptr(s.Type.Size()) > hi {
						t.Errorf("channel %q pixel (%d,%d): address %#x runs %d bytes PAST the buffer (%#x..%#x); a write here corrupts the heap",
							name, p[0], p[1], addr, addr+uintptr(s.Type.Size())-hi, lo, hi)
					}
				}
			}
		})
	}
}

// TestAllocateChannelsIsWindowAbsolute states the convention directly.
//
// Both halves matter: a Base biased outside its allocation is what makes the
// collector throw, and dropping the origin silently changes what every
// coordinate means.
func TestAllocateChannelsIsWindowAbsolute(t *testing.T) {
	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})

	box := Box2i{V2i{17, -9}, V2i{80, 38}}
	fb, bufs := AllocateChannels(cl, box)
	s := fb.Get("R")
	if s == nil {
		t.Fatal("no slice for R")
	}

	start := uintptr(unsafe.Pointer(&bufs["R"][0]))
	if got := uintptr(s.PixelAddr(17, -9)); got != start {
		t.Errorf("PixelAddr(17,-9) = %#x, want the buffer start %#x: coordinates "+
			"are the data window's own", got, start)
	}
	if got := uintptr(s.Base); got != start {
		t.Errorf("Slice.Base = %#x, want %#x: a base outside its allocation is what "+
			"makes the garbage collector throw", got, start)
	}
	if s.OriginX != 17 || s.OriginY != -9 {
		t.Errorf("origin recorded as (%d,%d), want (17,-9)", s.OriginX, s.OriginY)
	}
}
