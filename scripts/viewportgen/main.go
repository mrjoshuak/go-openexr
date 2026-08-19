// Command viewportgen writes a tiled HTJ2K EXR big enough for a viewport read
// to have something to save.
//
//	viewportgen <outdir>
//
// Writes vp_htj2k.exr: 512x512 in 256x256 tiles, two float channels, data window
// at (13, -7).
//
// The sizes are not arbitrary. The reference codec's HTJ2K parameters are fixed
// at 128x32 code-blocks and five decompositions (internal_ht.cpp), and a
// code-block's influence on the image is its band rectangle grown by the
// synthesis margin — 64 samples in every direction at the lowest resolution. In
// a tile smaller than about 256x256 every code-block therefore reaches every
// pixel, and no region decode can skip any of them. A fixture built at the
// tile sizes the rest of the gate uses would measure the fixture rather than
// the code, and would report a saving of zero for a reader that works.
//
// The data window is off the origin because a viewport read has three
// coordinate systems in play — image, tile and codestream — and every one of
// them is the same as the others when the window starts at (0, 0).
//
// Nothing here reads an EXR file: the samples are computed from the definition
// below, so this library's reader cannot agree with its writer about a defect
// they share. scripts/exrtiledump reads the result with libOpenEXR.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/mrjoshuak/go-openexr/exr"
)

const (
	imgW, imgH = 512, 512
	tileSize   = 256
	offX, offY = 13, -7
)

// sample varies per channel and per coordinate in both directions, so no
// transposition, channel swap, tile mix-up or off-by-one in the region
// arithmetic leaves it unchanged.
func sample(ci, x, y int) float32 {
	return float32(ci)*1000 + float32(x) + float32(y)*0.125
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: viewportgen <outdir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	h := exr.NewTiledHeader(imgW, imgH, tileSize, tileSize)
	h.SetDataWindow(exr.Box2i{
		Min: exr.V2i{X: offX, Y: offY},
		Max: exr.V2i{X: offX + imgW - 1, Y: offY + imgH - 1},
	})
	h.SetCompression(exr.CompressionHTJ2K256)
	cl := exr.NewChannelList()
	for _, n := range []string{"G", "R"} {
		cl.Add(exr.Channel{Name: n, Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)

	dw := h.DataWindow()
	fb, _ := exr.AllocateChannels(h.Channels(), dw)
	for ci, name := range []string{"G", "R"} {
		s := fb.Get(name)
		for y := int(dw.Min.Y); y <= int(dw.Max.Y); y++ {
			for x := int(dw.Min.X); x <= int(dw.Max.X); x++ {
				s.SetFloat32(x, y, sample(ci, x-int(dw.Min.X), y-int(dw.Min.Y)))
			}
		}
	}

	path := filepath.Join(dir, "vp_htj2k.exr")
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	w, err := exr.NewTiledWriter(f, h)
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewTiledWriter:", err)
		os.Exit(1)
	}
	w.SetFrameBuffer(fb)
	nx := (imgW + tileSize - 1) / tileSize
	ny := (imgH + tileSize - 1) / tileSize
	for ty := 0; ty < ny; ty++ {
		for tx := 0; tx < nx; tx++ {
			if err := w.WriteTile(tx, ty); err != nil {
				fmt.Fprintf(os.Stderr, "WriteTile(%d,%d): %v\n", tx, ty, err)
				os.Exit(1)
			}
		}
	}
	if err := w.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "Close:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: %dx%d, %dx%d tiles, window at (%d,%d)\n",
		path, imgW, imgH, tileSize, tileSize, offX, offY)
}
