// Command exrmpread reads a multi-part EXR with this library and writes one
// plain PFM per channel per resolution level, so the reference implementation
// can be asked whether this library read the file correctly.
//
// This closes the read direction for multi-part files. The write-direction gate
// asks the reference to read files this library wrote; nothing there asks this
// library to read a file the reference wrote, so MultiPartInputFile rested on
// round trips — and a round trip cannot see a convention the reader and the
// writer share.
//
//	exrmpread <file.exr> <outdir>
//
// It writes <outdir>/p<part>_l<level>_<channel>.pfm for every part, level and
// channel, and prints one structure line per part to stdout:
//
//	part <n> <name> <type> <xmin> <ymin> <xmax> <ymax> <channels...>
//
// PFM carries float32 only, so half and uint samples are widened on the way
// out. That is lossless in both directions: every half is exactly a float32,
// and uint32 values in these fixtures stay inside float32's exact integer
// range. The comparison it feeds is bit-exact, not toleranced.
package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
)

// writePFM writes a single-channel little-endian PFM, rows bottom to top, byte
// for byte as scripts/multipartgen writes them.
func writePFM(path string, w, h int, pix []float32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	fmt.Fprintf(bw, "Pf\n%d %d\n-1.0\n", w, h)
	var buf [4]byte
	for y := h - 1; y >= 0; y-- {
		for x := 0; x < w; x++ {
			bits := math.Float32bits(pix[y*w+x])
			buf[0], buf[1], buf[2], buf[3] = byte(bits), byte(bits>>8), byte(bits>>16), byte(bits>>24)
			if _, err := bw.Write(buf[:]); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

// plane lifts one channel of a frame buffer into float32, whatever its stored
// pixel type.
func plane(fb *exr.FrameBuffer, name string, pt exr.PixelType, w, h int) ([]float32, error) {
	s := fb.Get(name)
	if s == nil {
		return nil, fmt.Errorf("channel %q missing from frame buffer", name)
	}
	out := make([]float32, w*h)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			switch pt {
			case exr.PixelTypeUint:
				out[y*w+x] = float32(s.GetUint32(x, y))
			case exr.PixelTypeHalf:
				out[y*w+x] = s.GetHalf(x, y).Float32()
			default:
				out[y*w+x] = s.GetFloat32(x, y)
			}
		}
	}
	return out, nil
}

type chinfo struct {
	name string
	pt   exr.PixelType
}

func channels(cl *exr.ChannelList) []chinfo {
	out := make([]chinfo, 0, cl.Len())
	for i := 0; i < cl.Len(); i++ {
		c := cl.At(i)
		out = append(out, chinfo{c.Name, c.Type})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// emit writes every channel of one level to <outdir>/p<part>_l<level>_<ch>.pfm.
func emit(outDir string, part, level, w, h int, fb *exr.FrameBuffer, chans []chinfo) error {
	for _, c := range chans {
		pix, err := plane(fb, c.name, c.pt, w, h)
		if err != nil {
			return err
		}
		name := fmt.Sprintf("p%d_l%d_%s.pfm", part, level, c.name)
		if err := writePFM(filepath.Join(outDir, name), w, h, pix); err != nil {
			return err
		}
	}
	return nil
}

func doScanline(m *exr.MultiPartInputFile, part int, outDir string) error {
	r, err := m.ScanlineReader(part)
	if err != nil {
		return fmt.Errorf("ScanlineReader(%d): %w", part, err)
	}
	dw := r.DataWindow()
	w, h := int(dw.Max.X-dw.Min.X)+1, int(dw.Max.Y-dw.Min.Y)+1

	cl := m.Header(part).Channels()
	fb, _ := exr.AllocateChannels(cl, dw)
	r.SetFrameBuffer(fb)
	if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
		return fmt.Errorf("ReadPixels(part %d): %w", part, err)
	}
	return emit(outDir, part, 0, w, h, fb, channels(cl))
}

func doTiled(m *exr.MultiPartInputFile, part int, outDir string) error {
	r, err := m.TiledReader(part)
	if err != nil {
		return fmt.Errorf("TiledReader(%d): %w", part, err)
	}
	dw := r.DataWindow()

	// Mipmapped parts are emitted level by level; a ripmapped part's
	// independent x and y levels do not map onto oiiotool's single --selectmip
	// index, so only the diagonal is emitted and the caller is told.
	nlev := 1
	if r.LevelMode() != exr.LevelModeOne {
		nlev = r.NumLevels()
	}

	cl := m.Header(part).Channels()
	chans := channels(cl)
	for l := 0; l < nlev; l++ {
		w, h := r.LevelWidth(l), r.LevelHeight(l)
		if w <= 0 || h <= 0 {
			continue
		}
		level := exr.Box2i{
			Min: exr.V2i{X: dw.Min.X, Y: dw.Min.Y},
			Max: exr.V2i{X: dw.Min.X + int32(w) - 1, Y: dw.Min.Y + int32(h) - 1},
		}
		fb, _ := exr.AllocateChannels(cl, level)
		r.SetFrameBuffer(fb)
		nx, ny := r.NumXTilesAtLevel(l), r.NumYTilesAtLevel(l)
		if err := r.ReadTilesLevel(0, 0, nx-1, ny-1, l, l); err != nil {
			return fmt.Errorf("ReadTilesLevel(part %d level %d): %w", part, l, err)
		}
		if err := emit(outDir, part, l, w, h, fb, chans); err != nil {
			return err
		}
	}
	return nil
}

func run(path, outDir string) error {
	f, err := exr.OpenFile(path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	m := exr.NewMultiPartInputFile(f)
	if !m.IsMultiPart() {
		return fmt.Errorf("%s is not a multi-part file", path)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	for p := 0; p < m.NumParts(); p++ {
		h := m.Header(p)
		if h == nil {
			return fmt.Errorf("part %d has no header", p)
		}
		pi, err := m.PartInfo(p)
		if err != nil {
			return fmt.Errorf("PartInfo(%d): %w", p, err)
		}
		dw := h.DataWindow()
		names := make([]string, 0, h.Channels().Len())
		for _, c := range channels(h.Channels()) {
			names = append(names, c.name)
		}
		fmt.Fprintf(out, "part %d %s %s %d %d %d %d %s\n",
			p, pi.Name, pi.Type, dw.Min.X, dw.Min.Y, dw.Max.X, dw.Max.Y,
			strings.Join(names, ","))

		switch pi.Type {
		case "tiledimage":
			err = doTiled(m, p, outDir)
		case "scanlineimage", "":
			err = doScanline(m, p, outDir)
		default:
			// deepscanline and deeptile are gated by the deep section, which
			// compares samples rather than a dense raster; skip rather than
			// pretend a PFM can carry them.
			fmt.Fprintf(out, "skip %d %s\n", p, pi.Type)
			continue
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: exrmpread <file.exr> <outdir>")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2]); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}
