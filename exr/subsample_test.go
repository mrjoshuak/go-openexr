package exr

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// Subsampling is the format's luminance/chroma mechanism, and it was broken on
// both axes in a way no round trip could see, because both defects were applied
// identically to the reader and the writer.
//
// Horizontally, the row functions are indexed by stored column — a channel with
// xSampling 2 has half as many columns as the window is wide — and their
// per-pixel fallbacks passed that index to an accessor that divides by
// xSampling again. Since the fast paths require xSampling of 1, the fallback is
// the *only* path a subsampled channel ever takes. Measured against libOpenEXR:
// 316 of 512 samples wrong, on ten of twelve codecs, with the reference reading
// the files without complaint.
//
// Vertically, every channel contributed a row to every scanline, so the chunks
// were the wrong size entirely: five codecs produced files the reference could
// not decompress, and pxr24 produced one it read with 359 of 384 samples wrong.
//
// The comparison against the reference lives in the gate. These are the
// oracle-free checks, and what makes them able to see the horizontal defect at
// all is that they compare against the values the fixture was built from rather
// than against another read through this library.

// subValue is an integer below 2048, which a half holds exactly, so a mismatch
// is a packing error rather than the fixture exceeding the format.
func subValue(ci, x, y int) float32 {
	return float32(ci)*500 + float32(x)*10 + float32(y)
}

// TestSubsampledChannelsRoundTrip writes and reads a luminance/chroma file for
// every codec that can carry one, at 4:2:2 and 4:2:0.
func TestSubsampledChannelsRoundTrip(t *testing.T) {
	const w, h = 16, 16
	dir := t.TempDir()

	codecs := []struct {
		name string
		c    Compression
	}{
		{"none", CompressionNone},
		{"rle", CompressionRLE},
		{"zips", CompressionZIPS},
		{"zip", CompressionZIP},
		{"piz", CompressionPIZ},
		{"pxr24", CompressionPXR24},
	}
	samplings := []struct{ xs, ys int32 }{{2, 1}, {2, 2}}

	for _, c := range codecs {
		for _, s := range samplings {
			name := fmt.Sprintf("%s_%dx%d", c.name, s.xs, s.ys)
			t.Run(name, func(t *testing.T) {
				hdr := NewScanlineHeader(w, h)
				hdr.SetCompression(c.c)
				cl := NewChannelList()
				cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
				cl.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: s.xs, YSampling: s.ys})
				cl.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: s.xs, YSampling: s.ys})
				hdr.SetChannels(cl)

				path := filepath.Join(dir, name+".exr")
				f, err := os.Create(path)
				if err != nil {
					t.Fatalf("Create: %v", err)
				}
				wr, err := NewScanlineWriter(f, hdr)
				if err != nil {
					t.Fatalf("NewScanlineWriter: %v", err)
				}
				fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
				for ci, n := range []string{"BY", "RY", "Y"} {
					sl := fb.Get(n)
					xs, ys := int(s.xs), int(s.ys)
					if n == "Y" {
						xs, ys = 1, 1
					}
					for y := 0; y < h; y += ys {
						for x := 0; x < w; x += xs {
							sl.SetFloat32(x, y, subValue(ci, x, y))
						}
					}
				}
				wr.SetFrameBuffer(fb)
				if err := wr.WritePixels(0, h-1); err != nil {
					t.Fatalf("WritePixels: %v", err)
				}
				if err := wr.Close(); err != nil {
					t.Fatalf("Close: %v", err)
				}
				f.Close()

				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("ReadFile: %v", err)
				}
				in, err := Open(bytes.NewReader(raw), int64(len(raw)))
				if err != nil {
					t.Fatalf("Open: %v", err)
				}
				r, err := NewScanlineReader(in)
				if err != nil {
					t.Fatalf("NewScanlineReader: %v", err)
				}
				rfb, _ := AllocateChannels(in.Header(0).Channels(), in.Header(0).DataWindow())
				r.SetFrameBuffer(rfb)
				if err := r.ReadPixels(0, h-1); err != nil {
					t.Fatalf("ReadPixels: %v", err)
				}

				for ci, n := range []string{"BY", "RY", "Y"} {
					sl := rfb.Get(n)
					xs, ys := int(s.xs), int(s.ys)
					if n == "Y" {
						xs, ys = 1, 1
					}
					for y := 0; y < h; y += ys {
						for x := 0; x < w; x += xs {
							got, want := sl.GetFloat32(x, y), subValue(ci, x, y)
							if got != want {
								t.Fatalf("channel %s at (%d,%d) = %v, want %v",
									n, x, y, got, want)
							}
						}
					}
				}
			})
		}
	}
}

// TestMultiPartPIZSubsampledDoesNotPanic covers the multi-part path, which
// handed PIZ the window's width for every channel.
//
// PIZ models per-channel dimensions explicitly, so telling it a subsampled
// channel is full width makes it read past the end of the data. That is not a
// wrong file but a panic — "index out of range [128] with length 128" — which
// no comparison of samples would ever reach.
func TestMultiPartPIZSubsampledDoesNotPanic(t *testing.T) {
	const w, h = 16, 16

	part := func(name string, xs int32) *Header {
		hdr := NewHeader()
		hdr.SetDataWindow(Box2i{Min: V2i{X: 0, Y: 0}, Max: V2i{X: w - 1, Y: h - 1}})
		hdr.SetDisplayWindow(Box2i{Min: V2i{X: 0, Y: 0}, Max: V2i{X: w - 1, Y: h - 1}})
		hdr.SetCompression(CompressionPIZ)
		hdr.SetLineOrder(LineOrderIncreasing)
		hdr.SetPixelAspectRatio(1)
		hdr.SetScreenWindowCenter(V2f{X: 0, Y: 0})
		hdr.SetScreenWindowWidth(1)
		cl := NewChannelList()
		cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		cl.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: xs, YSampling: 1})
		hdr.SetChannels(cl)
		hdr.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: name})
		hdr.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeScanline})
		return hdr
	}

	path := filepath.Join(t.TempDir(), "mp.exr")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	defer f.Close()

	hdrs := []*Header{part("main", 1), part("chroma", 2)}
	mp, err := NewMultiPartOutputFile(f, hdrs)
	if err != nil {
		t.Fatalf("NewMultiPartOutputFile: %v", err)
	}
	for p, hdr := range hdrs {
		fb, _ := AllocateChannels(hdr.Channels(), hdr.DataWindow())
		for _, n := range []string{"Y", "BY"} {
			sl := fb.Get(n)
			step := 1
			if n == "BY" && p == 1 {
				step = 2
			}
			for y := 0; y < h; y++ {
				for x := 0; x < w; x += step {
					sl.SetFloat32(x, y, float32(x*10+y))
				}
			}
		}
		if err := mp.SetFrameBuffer(p, fb); err != nil {
			t.Fatalf("SetFrameBuffer(%d): %v", p, err)
		}
		if err := mp.WritePixels(p, h); err != nil {
			t.Fatalf("WritePixels(%d): %v", p, err)
		}
	}
	if err := mp.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
