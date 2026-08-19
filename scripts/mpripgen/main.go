// Command mpripgen writes a multi-part EXR whose second part is a ripmapped
// tiled image, together with the samples that part is meant to hold.
//
// It exists because a ripmap's x and y levels are independent, so its chunk
// offset table has a different layout from a mipmap's — the mipmapped part in
// the main multi-part fixture walks one level per step and never exercises it.
// The fixture generator there indexes levels linearly for the same reason,
// which is why this is a separate program rather than another row in that
// table.
//
//	mpripgen <outdir>
//
// Writes mp_ripmap.exr and mp_ripmap.expect, the latter in the "lx ly x y
// CHANNEL value" form scripts/tilecmp.awk compares, so the ripmapped part can
// be checked by exactly the machinery the single-part tiled section uses.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
)

import "github.com/mrjoshuak/go-openexr/exr"

const (
	masterW, masterH = 96, 64
	ripW, ripH       = 64, 64
	tileSize         = 16
)

// sample depends on the level and both coordinates, so no two levels hold the
// same image and no rearrangement of rows, columns or tiles leaves it
// unchanged.
func sample(ch, lx, ly, x, y int) float32 {
	return float32(ch)*1000 +
		float32(lx)*100 + float32(ly)*10 +
		float32(x)/float32(ripW) + float32(y)/float32(ripH)*0.5
}

func levelSize(n, level int) int {
	for i := 0; i < level; i++ {
		n /= 2
		if n < 1 {
			n = 1
		}
	}
	return n
}

func numLevels(n int) int {
	c := 1
	for n > 1 {
		n /= 2
		c++
	}
	return c
}

func chanList() *exr.ChannelList {
	cl := exr.NewChannelList()
	for _, n := range []string{"B", "G", "R"} {
		cl.Add(exr.Channel{Name: n, Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	}
	return cl
}

func baseHeader(w, h int, display exr.Box2i, name string) *exr.Header {
	hdr := exr.NewHeader()
	hdr.SetDataWindow(exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: int32(w - 1), Y: int32(h - 1)}})
	hdr.SetDisplayWindow(display)
	hdr.SetCompression(exr.CompressionZIP)
	hdr.SetLineOrder(exr.LineOrderIncreasing)
	hdr.SetPixelAspectRatio(1)
	hdr.SetScreenWindowCenter(exr.V2f{X: 0, Y: 0})
	hdr.SetScreenWindowWidth(1)
	hdr.SetChannels(chanList())
	hdr.Set(&exr.Attribute{Name: exr.AttrNameName, Type: exr.AttrTypeString, Value: name})
	return hdr
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: mpripgen <outdir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	display := exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: masterW - 1, Y: masterH - 1}}

	master := baseHeader(masterW, masterH, display, "master")
	master.Set(&exr.Attribute{Name: exr.AttrNameType, Type: exr.AttrTypeString, Value: exr.PartTypeScanline})

	rip := baseHeader(ripW, ripH, display, "ripproxy")
	rip.Set(&exr.Attribute{Name: exr.AttrNameType, Type: exr.AttrTypeString, Value: exr.PartTypeTiled})
	rip.SetTileDescription(exr.TileDescription{
		XSize: tileSize, YSize: tileSize,
		Mode: exr.LevelModeRipmap, RoundingMode: exr.LevelRoundDown,
	})

	path := filepath.Join(dir, "mp_ripmap.exr")
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	mp, err := exr.NewMultiPartOutputFile(f, []*exr.Header{master, rip})
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewMultiPartOutputFile:", err)
		os.Exit(1)
	}

	// Part 0: the scanline master, so the file is genuinely mixed.
	mfb, _ := exr.AllocateChannels(master.Channels(), master.DataWindow())
	for ci, name := range []string{"B", "G", "R"} {
		s := mfb.Get(name)
		for y := 0; y < masterH; y++ {
			for x := 0; x < masterW; x++ {
				s.SetFloat32(x, y, float32(ci)+float32(x)/masterW+float32(y)/masterH)
			}
		}
	}
	if err := mp.SetFrameBuffer(0, mfb); err != nil {
		fmt.Fprintln(os.Stderr, "SetFrameBuffer(0):", err)
		os.Exit(1)
	}
	if err := mp.WritePixels(0, masterH); err != nil {
		fmt.Fprintln(os.Stderr, "WritePixels:", err)
		os.Exit(1)
	}

	// Part 1: the ripmap, every (lx, ly) level from its own frame buffer.
	expect, err := os.Create(filepath.Join(dir, "mp_ripmap.expect"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer expect.Close()
	ew := bufio.NewWriter(expect)
	defer ew.Flush()

	nx, ny := numLevels(ripW), numLevels(ripH)
	for ly := 0; ly < ny; ly++ {
		for lx := 0; lx < nx; lx++ {
			lw, lh := levelSize(ripW, lx), levelSize(ripH, ly)
			lvl := exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: int32(lw - 1), Y: int32(lh - 1)}}
			fb, _ := exr.AllocateChannels(rip.Channels(), lvl)
			for ci, name := range []string{"B", "G", "R"} {
				s := fb.Get(name)
				for y := 0; y < lh; y++ {
					for x := 0; x < lw; x++ {
						v := sample(ci, lx, ly, x, y)
						s.SetFloat32(x, y, v)
						fmt.Fprintf(ew, "%d %d %d %d %s %.9g\n", lx, ly, x, y, name, v)
					}
				}
			}
			if err := mp.SetFrameBuffer(1, fb); err != nil {
				fmt.Fprintln(os.Stderr, "SetFrameBuffer(1):", err)
				os.Exit(1)
			}
			tx := (lw + tileSize - 1) / tileSize
			ty := (lh + tileSize - 1) / tileSize
			for y := 0; y < ty; y++ {
				for x := 0; x < tx; x++ {
					if err := mp.WriteTileLevel(1, x, y, lx, ly); err != nil {
						fmt.Fprintf(os.Stderr, "WriteTileLevel(%d,%d,%d,%d): %v\n", x, y, lx, ly, err)
						os.Exit(1)
					}
				}
			}
		}
	}

	if err := mp.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "Close:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s with a %dx%d ripmap of %d x %d levels\n", path, ripW, ripH, nx, ny)
}
