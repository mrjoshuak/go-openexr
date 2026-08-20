// Command subsampgen writes subsampled scanline EXRs, one per compression, and
// reads them back, printing each channel's samples in the form
// scripts/exrpartdump prints so the two readings can be compared by key.
//
// Subsampling is the format's luminance/chroma mechanism and it was broken in
// both axes. Horizontally, the row functions are indexed by stored column while
// their per-pixel fallbacks passed that index to an accessor that divides by
// XSampling again — and since the fast paths require XSampling of 1, the
// fallback is the only path a subsampled channel ever takes, in both
// directions. A round trip through this library agreed with itself perfectly
// while libOpenEXR read 316 of 512 samples differently, on ten of twelve
// codecs. Vertically, every channel contributed a row to every scanline, so the
// chunks were the wrong size: five codecs produced files the reference could
// not decompress and pxr24 produced one it read with 359 of 384 samples wrong.
//
//	subsampgen <outdir> <codec> <xSampling> <ySampling>
//
// Output on stdout is "<channel> <x> <y> <value>" in each channel's own
// coordinates, which for a channel with xSampling 2 means column x of the
// channel is column 2x of the image.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mrjoshuak/go-openexr/exr"
)

const w, h = 16, 16

var codecs = map[string]exr.Compression{
	"b44":   exr.CompressionB44,
	"dwaa":  exr.CompressionDWAA,
	"none":  exr.CompressionNone,
	"rle":   exr.CompressionRLE,
	"zips":  exr.CompressionZIPS,
	"zip":   exr.CompressionZIP,
	"piz":   exr.CompressionPIZ,
	"pxr24": exr.CompressionPXR24,
}

// sample is an integer below 2048, which a half holds exactly, so a mismatch is
// a packing error rather than the fixture exceeding the format.
func sample(ci, x, y int) float32 {
	return float32(ci)*500 + float32(x)*10 + float32(y)
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) != 5 {
		fail("usage: subsampgen <outdir> <codec> <xSampling> <ySampling>")
	}
	dir, name := os.Args[1], os.Args[2]
	xs, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fail("xSampling: %v", err)
	}
	ys, err := strconv.Atoi(os.Args[4])
	if err != nil {
		fail("ySampling: %v", err)
	}
	comp, ok := codecs[name]
	if !ok {
		fail("unknown codec %q", name)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fail("%v", err)
	}

	hdr := exr.NewScanlineHeader(w, h)
	hdr.SetCompression(comp)
	cl := exr.NewChannelList()
	cl.Add(exr.Channel{Name: "Y", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	cl.Add(exr.Channel{Name: "BY", Type: exr.PixelTypeHalf, XSampling: int32(xs), YSampling: int32(ys)})
	cl.Add(exr.Channel{Name: "RY", Type: exr.PixelTypeHalf, XSampling: int32(xs), YSampling: int32(ys)})
	hdr.SetChannels(cl)

	path := filepath.Join(dir, fmt.Sprintf("sub_%s_%dx%d.exr", name, xs, ys))
	f, err := os.Create(path)
	if err != nil {
		fail("%v", err)
	}
	wr, err := exr.NewScanlineWriter(f, hdr)
	if err != nil {
		fail("NewScanlineWriter: %v", err)
	}
	fb, _ := exr.AllocateChannels(hdr.Channels(), hdr.DataWindow())
	for ci, n := range []string{"BY", "RY", "Y"} {
		s := fb.Get(n)
		cxs, cys := xs, ys
		if n == "Y" {
			cxs, cys = 1, 1
		}
		for y := 0; y < h; y += cys {
			for x := 0; x < w; x += cxs {
				s.SetFloat32(x, y, sample(ci, x, y))
			}
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		fail("WritePixels: %v", err)
	}
	if err := wr.Close(); err != nil {
		fail("Close: %v", err)
	}
	f.Close()

	// Read it back with this library and print in exrpartdump's coordinates.
	in, err := exr.OpenFile(path)
	if err != nil {
		fail("open: %v", err)
	}
	defer in.Close()
	r, err := exr.NewScanlineReader(in)
	if err != nil {
		fail("NewScanlineReader: %v", err)
	}
	rfb, _ := exr.AllocateChannels(in.Header(0).Channels(), in.Header(0).DataWindow())
	r.SetFrameBuffer(rfb)
	if err := r.ReadPixels(0, h-1); err != nil {
		fail("ReadPixels: %v", err)
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	fmt.Fprintf(out, "# file %s\n", path)
	for _, n := range []string{"BY", "RY", "Y"} {
		s := rfb.Get(n)
		cxs, cys := xs, ys
		if n == "Y" {
			cxs, cys = 1, 1
		}
		for y := 0; y < h; y += cys {
			for x := 0; x < w; x += cxs {
				// The channel's own coordinates are the image's divided by the
				// sampling, which is what exrpartdump prints.
				fmt.Fprintf(out, "%s %d %d %.9g\n", n, x/cxs, y/cys, s.GetFloat32(x, y))
			}
		}
	}
}
