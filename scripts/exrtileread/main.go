// Command exrtileread reads a tiled EXR with this library and prints every
// sample of every resolution level, in the format scripts/exrtiledump prints
// from the reference implementation.
//
// This closes the read direction for tiled files. The write-direction gate asks
// the reference to read files this library wrote; nothing there asks this
// library to read a file the reference wrote, so the tiled read path rested on
// round trips — and a round trip cannot see a convention the reader and the
// writer share. Here the fixture comes from exrmaketiled or oiiotool, the truth
// comes from exrtiledump, and this program contributes only its own reading.
//
//	exrtileread [-part N] file.exr
//
// Output is one line per sample:
//
//	<levelX> <levelY> <x> <y> <channel> <value>
//
// preceded by '#' structure lines that mirror exrtiledump's, so the two dumps
// are directly comparable with scripts/tilecmp.awk. Coordinates are relative to
// the level's origin, matching the reference's dump.
package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
)

func modeName(m exr.LevelMode) string {
	switch m {
	case exr.LevelModeOne:
		return "ONE_LEVEL"
	case exr.LevelModeMipmap:
		return "MIPMAP_LEVELS"
	case exr.LevelModeRipmap:
		return "RIPMAP_LEVELS"
	}
	return "UNKNOWN"
}

// sampleValue renders one sample as the reference's "%.9g" does, whatever the
// channel's pixel type. Comparing as text is what makes the two dumps
// comparable; %.9g round-trips a float32 exactly.
func sampleValue(s *exr.Slice, pt exr.PixelType, x, y int) string {
	switch pt {
	case exr.PixelTypeUint:
		return fmt.Sprintf("%.9g", float64(s.GetUint32(x, y)))
	case exr.PixelTypeHalf:
		return fmt.Sprintf("%.9g", float64(s.GetHalf(x, y).Float32()))
	default:
		return fmt.Sprintf("%.9g", float64(s.GetFloat32(x, y)))
	}
}

// dumpLevel reads every tile of one level and prints its samples. The frame
// buffer is allocated to the level's own dimensions, since that is the extent
// the reader fills.
func dumpLevel(out *bufio.Writer, r *exr.TiledReader, lx, ly int) error {
	w, h := r.LevelWidth(lx), r.LevelHeight(ly)
	if w <= 0 || h <= 0 {
		return nil
	}

	dw := r.DataWindow()
	level := exr.Box2i{
		Min: exr.V2i{X: dw.Min.X, Y: dw.Min.Y},
		Max: exr.V2i{X: dw.Min.X + int32(w) - 1, Y: dw.Min.Y + int32(h) - 1},
	}

	cl := r.Header().Channels()
	fb, bufs := exr.AllocateChannels(cl, level)
	r.SetFrameBuffer(fb)
	defer runtime.KeepAlive(bufs)

	nx, ny := r.NumXTilesAtLevel(lx), r.NumYTilesAtLevel(ly)
	if err := r.ReadTilesLevel(0, 0, nx-1, ny-1, lx, ly); err != nil {
		return fmt.Errorf("ReadTilesLevel(0,0,%d,%d,%d,%d): %w", nx-1, ny-1, lx, ly, err)
	}

	fmt.Fprintf(out, "# level %d %d %d %d %d %d\n", lx, ly, w, h, nx, ny)

	// Sort the channel names so the dump is stable regardless of how the
	// channel list is ordered; tilecmp.awk compares by key, but a stable order
	// keeps a raw diff of two dumps readable.
	type chinfo struct {
		name string
		pt   exr.PixelType
	}
	chans := make([]chinfo, 0, cl.Len())
	for i := 0; i < cl.Len(); i++ {
		c := cl.At(i)
		chans = append(chans, chinfo{c.Name, c.Type})
	}
	sort.Slice(chans, func(i, j int) bool { return chans[i].name < chans[j].name })

	for _, c := range chans {
		s := fb.Get(c.name)
		if s == nil {
			return fmt.Errorf("channel %q missing from frame buffer at level %d,%d", c.name, lx, ly)
		}
		// The frame buffer is addressed in the data window's own coordinates,
		// so a level whose window starts at (17, -9) is read from there. The
		// dump's coordinates stay level-relative, which is what exrtiledump
		// prints and what the comparison is against.
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				fmt.Fprintf(out, "%d %d %d %d %s %s\n",
					lx, ly, x, y, c.name,
					sampleValue(s, c.pt, int(dw.Min.X)+x, int(dw.Min.Y)+y))
			}
		}
	}
	return nil
}

func run(path string, part int) error {
	f, err := exr.OpenFile(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	// A part index addresses one tiled part of a multi-part file. Without it
	// the file is read as single-part, which is what every existing caller
	// wants.
	var r *exr.TiledReader
	if part >= 0 {
		m := exr.NewMultiPartInputFile(f)
		if part >= m.NumParts() {
			return fmt.Errorf("part %d of %d", part, m.NumParts())
		}
		r, err = m.TiledReader(part)
		if err != nil {
			return fmt.Errorf("TiledReader(part %d): %w", part, err)
		}
	} else {
		r, err = exr.NewTiledReader(f)
		if err != nil {
			return fmt.Errorf("NewTiledReader: %w", err)
		}
	}

	out := bufio.NewWriterSize(os.Stdout, 1<<20)
	defer out.Flush()

	td := r.Header().TileDescription()
	fmt.Fprintf(out, "# mode %s\n", modeName(r.LevelMode()))
	if td != nil {
		fmt.Fprintf(out, "# tile %d %d %d\n", td.XSize, td.YSize, td.RoundingMode)
	}
	fmt.Fprintf(out, "# levels %d %d\n", r.NumXLevels(), r.NumYLevels())

	switch r.LevelMode() {
	case exr.LevelModeOne:
		return dumpLevel(out, r, 0, 0)
	case exr.LevelModeMipmap:
		for l := 0; l < r.NumXLevels(); l++ {
			if err := dumpLevel(out, r, l, l); err != nil {
				return err
			}
		}
	case exr.LevelModeRipmap:
		for ly := 0; ly < r.NumYLevels(); ly++ {
			for lx := 0; lx < r.NumXLevels(); lx++ {
				if err := dumpLevel(out, r, lx, ly); err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unknown level mode %v", r.LevelMode())
	}
	return nil
}

func main() {
	part := -1
	args := os.Args[1:]
	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		if args[0] == "-part" && len(args) > 1 {
			n, err := strconv.Atoi(args[1])
			if err != nil {
				fmt.Fprintln(os.Stderr, "bad part index:", args[1])
				os.Exit(2)
			}
			part = n
			args = args[2:]
			continue
		}
		break
	}
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: exrtileread [-part N] file.exr")
		os.Exit(2)
	}
	if err := run(args[0], part); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR %s: %v\n", args[0], err)
		os.Exit(1)
	}
}
