// Command mipcmp writes a mipmapped tiled EXR from the level 0 of an existing
// file, using this library's own mipmap generation, so the generated levels can
// be compared against another implementation's for the same input.
//
// The format specifies no downsampling filter, so no implementation's levels
// are "correct" in the way a codec's output is. What can be asked is whether
// this library's box filter agrees with the reference tool's, which is the
// closest thing to an external check that exists for generated level content —
// and it is a real one, because a generator that placed levels wrongly, scaled
// them, or filtered along the wrong axis would disagree immediately.
//
//	mipcmp <source.exr> <out.exr> <tileSize>
package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/mrjoshuak/go-openexr/exr"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Fprintln(os.Stderr, "usage: mipcmp <source.exr> <out.exr> <tileSize>")
		os.Exit(2)
	}
	tile, err := strconv.Atoi(os.Args[3])
	if err != nil || tile <= 0 {
		fmt.Fprintln(os.Stderr, "bad tile size:", os.Args[3])
		os.Exit(2)
	}

	in, err := exr.OpenFile(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer in.Close()

	// Read level 0, whether the source is tiled or scanline.
	hdr := in.Header(0)
	dw := hdr.DataWindow()
	w, h := int(dw.Width()), int(dw.Height())
	src, _ := exr.AllocateChannels(hdr.Channels(), dw)

	if in.IsTiled() {
		r, err := exr.NewTiledReader(in)
		if err != nil {
			fmt.Fprintln(os.Stderr, "tiled reader:", err)
			os.Exit(1)
		}
		r.SetFrameBuffer(src)
		if err := r.ReadTiles(0, 0, r.NumTilesX()-1, r.NumTilesY()-1); err != nil {
			fmt.Fprintln(os.Stderr, "read tiles:", err)
			os.Exit(1)
		}
	} else {
		r, err := exr.NewScanlineReader(in)
		if err != nil {
			fmt.Fprintln(os.Stderr, "scanline reader:", err)
			os.Exit(1)
		}
		r.SetFrameBuffer(src)
		if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
			fmt.Fprintln(os.Stderr, "read pixels:", err)
			os.Exit(1)
		}
	}

	out, err := os.Create(os.Args[2])
	if err != nil {
		fmt.Fprintln(os.Stderr, "create:", err)
		os.Exit(1)
	}
	defer out.Close()

	oh := exr.NewHeader()
	oh.SetDataWindow(dw)
	oh.SetDisplayWindow(hdr.DisplayWindow())
	oh.SetChannels(hdr.Channels())
	oh.SetCompression(exr.CompressionZIP)
	oh.SetLineOrder(exr.LineOrderIncreasing)
	oh.SetPixelAspectRatio(1)
	oh.SetScreenWindowCenter(exr.V2f{X: 0, Y: 0})
	oh.SetScreenWindowWidth(1)
	oh.SetTileDescription(exr.TileDescription{
		XSize: uint32(tile), YSize: uint32(tile),
		Mode: exr.LevelModeMipmap, RoundingMode: exr.LevelRoundDown,
	})

	tw, err := exr.NewTiledWriter(out, oh)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tiled writer:", err)
		os.Exit(1)
	}
	if err := exr.WriteMipmapTiledImage(tw, src, w, h, exr.FilterBox); err != nil {
		fmt.Fprintln(os.Stderr, "WriteMipmapTiledImage:", err)
		os.Exit(1)
	}
	if err := tw.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "close:", err)
		os.Exit(1)
	}
}
