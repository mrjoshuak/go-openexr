// Command deepmipgen writes a mipmapped deep tiled EXR, one level at a time,
// together with the samples each level is meant to hold.
//
// DeepTiledWriter wrote LevelModeOne only: its chunk offset table was sized for
// one level and indexed by tileY*tilesX+tileX, so every level after the first
// overwrote the first one's slots. The reader had always derived the index per
// level, so the two disagreed the moment a second level existed.
//
//	deepmipgen <outdir> [ripmap]
//
// Writes deep_mip.exr (or deep_rip.exr) and one expectation file per level, in
// the shape scripts/deepdiff.awk parses.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mrjoshuak/go-openexr/exr"
)

const (
	imgW, imgH = 32, 32
	tileSize   = 8
)

// sampleCount varies per pixel and per level, including pixels with none: a
// level whose every pixel holds the same number of samples cannot distinguish a
// correct pixel offset table from one that is merely monotonic.
func sampleCount(lx, ly, x, y int) int {
	if (x+y+lx+ly)%5 == 0 {
		return 0
	}
	return 1 + (x*2+y*3+lx+ly)%3
}

func valueOf(lx, ly, x, y, s int) float32 {
	return float32(lx)*100 + float32(ly)*10 +
		float32(x) + float32(y)*0.25 + float32(s)*0.0625
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

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: deepmipgen <outdir> [ripmap]")
		os.Exit(2)
	}
	dir := os.Args[1]
	ripmap := len(os.Args) > 2 && os.Args[2] == "ripmap"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	name := "deep_mip.exr"
	mode := exr.LevelModeMipmap
	if ripmap {
		name = "deep_rip.exr"
		mode = exr.LevelModeRipmap
	}
	path := filepath.Join(dir, name)

	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	w, err := exr.NewDeepTiledWriter(f, imgW, imgH, tileSize, tileSize)
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewDeepTiledWriter:", err)
		os.Exit(1)
	}
	h := w.Header()
	h.SetCompression(exr.CompressionZIPS)
	h.SetTileDescription(exr.TileDescription{
		XSize: tileSize, YSize: tileSize,
		Mode: mode, RoundingMode: exr.LevelRoundDown,
	})
	cl := exr.NewChannelList()
	cl.Add(exr.Channel{Name: "A", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	cl.Add(exr.Channel{Name: "Z", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	h.SetChannels(cl)

	nx, ny := numLevels(imgW), 1
	if ripmap {
		ny = numLevels(imgH)
	}

	type lvl struct{ lx, ly int }
	var levels []lvl
	if ripmap {
		for ly := 0; ly < ny; ly++ {
			for lx := 0; lx < nx; lx++ {
				levels = append(levels, lvl{lx, ly})
			}
		}
	} else {
		for l := 0; l < nx; l++ {
			levels = append(levels, lvl{l, l})
		}
	}

	ef, err := os.Create(filepath.Join(dir, name+".txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer ef.Close()
	ew := bufio.NewWriter(ef)
	defer ew.Flush()

	total := 0
	for _, l := range levels {
		lw, lh := levelSize(imgW, l.lx), levelSize(imgH, l.ly)

		fb := exr.NewDeepFrameBuffer(lw, lh)
		fb.Insert("A", exr.PixelTypeFloat)
		fb.Insert("Z", exr.PixelTypeFloat)
		for y := 0; y < lh; y++ {
			for x := 0; x < lw; x++ {
				n := sampleCount(l.lx, l.ly, x, y)
				fb.SetSampleCount(x, y, uint32(n))
				fb.AllocateSamples(x, y)
				for s := 0; s < n; s++ {
					fb.Slices["A"].SetSampleFloat32(x, y, s, valueOf(l.lx, l.ly, x, y, s))
					fb.Slices["Z"].SetSampleFloat32(x, y, s, valueOf(l.lx, l.ly, x, y, s)*2)
				}
				total += n
			}
		}
		w.SetFrameBuffer(fb)

		tx := (lw + tileSize - 1) / tileSize
		ty := (lh + tileSize - 1) / tileSize
		for y := 0; y < ty; y++ {
			for x := 0; x < tx; x++ {
				if err := w.WriteTileLevel(x, y, l.lx, l.ly); err != nil {
					fmt.Fprintf(os.Stderr, "WriteTileLevel(%d,%d,%d,%d): %v\n", x, y, l.lx, l.ly, err)
					os.Exit(1)
				}
			}
		}

		// One expectation file per level, in the shape deepdiff.awk parses.
		lf, err := os.Create(filepath.Join(dir, fmt.Sprintf("%s.l%d_%d.txt", name, l.lx, l.ly)))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		lw2 := bufio.NewWriter(lf)
		for y := 0; y < lh; y++ {
			for x := 0; x < lw; x++ {
				n := sampleCount(l.lx, l.ly, x, y)
				fmt.Fprintf(lw2, "Pixel (%d, %d): %d samples ", x, y, n)
				for s := 0; s < n; s++ {
					if s > 0 {
						lw2.WriteString(" / ")
					} else {
						lw2.WriteString(": ")
					}
					v := valueOf(l.lx, l.ly, x, y, s)
					fmt.Fprintf(lw2, "A=%s Z=%s",
						strconv.FormatFloat(float64(v), 'g', -1, 32),
						strconv.FormatFloat(float64(v*2), 'g', -1, 32))
				}
				lw2.WriteString("\n")
			}
		}
		lw2.Flush()
		lf.Close()
	}

	if err := w.Finalize(); err != nil {
		fmt.Fprintln(os.Stderr, "Finalize:", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s: %d levels holding %d samples\n", path, len(levels), total)
}
