// Command tiledgen writes tiled EXR fixtures — plain, mipmapped and ripmapped —
// together with, for every resolution level, the exact samples that level is
// meant to contain and the geometry the file claims. An external reader
// (scripts/exrtiledump, built against the OpenEXR reference implementation)
// decodes each file level by level and scripts/validate.sh compares sample for
// sample.
//
// Nothing in this program reads an EXR file. The expected samples are computed
// from the fixture definition below, so a defect applied identically to this
// library's tiled writer and its tiled reader — the failure mode that hid four
// non-interoperable codecs until v1.4.0 — cannot hide here.
//
// FIXTURE SIGNAL. A constant image compares equal against almost any defect, so
// every fixture varies per pixel, per channel and, for multi-level files, per
// level:
//
//	sharp (used with the lossless codecs)
//	  R = x + 0.5 + 64*lx     unique per column, and per X level
//	  G = y + 0.5 + 64*ly     unique per row, and per Y level
//	  B = ((3x + 7y) mod 17) + 0.25   varies within a tile
//	  A = 1 or 1.5, in 8x8 blocks     a checker finer than most tiles
//
//	lossy (used with B44 and DWA)
//	  R = 0.5 + 0.5*x/(w-1), G = 0.5 + 0.5*y/(h-1),
//	  B = 0.5 + 0.25*((3x + 7y) mod 17)/17,
//	  A = 1, i.e. the same gradients confined to [0.5, 1].
//
// A swapped tile changes R or G. A tile placed at the wrong level changes R and
// G by 64. A transposed tile changes B. Every sharp value is exactly
// representable as a half, which this program asserts rather than assumes, so
// the "exact" rows really are held to bit-identity.
//
// WHY THE LOSSY FIXTURE LIVES IN [0.5, 1]. B44's error bound is relative to
// each 4x4 block's own maximum, and only blocks spanning at most one binade are
// held to 2^-5 of it (scripts/validate.sh derives this). A gradient that runs
// down to zero — as the scanline fixture in scripts/interopgen does — puts a
// block boundary across many binades at the small mipmap levels, where a 4x4
// block covers a quarter of the image. Measured on level 3 of a 71x40 mipmap,
// that costs 0.107, and the OpenEXR reference encoder is off by exactly the
// same 0.107 on the same samples: it is a property of the codec and the
// content, not of an encoder. Confining the lossy fixture to a single binade
// keeps the derived bound valid at every level, so the row gates on a number
// derived from the format rather than on one fitted to this library.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

var channels = []string{"A", "B", "G", "R"}

func sharpSample(ch string, x, y, lx, ly int) float32 {
	switch ch {
	case "R":
		return float32(x) + 0.5 + float32(64*lx)
	case "G":
		return float32(y) + 0.5 + float32(64*ly)
	case "B":
		return float32((3*x+7*y)%17) + 0.25
	default: // A
		if ((x>>3)+(y>>3))&1 == 0 {
			return 1
		}
		return 1.5
	}
}

func lossySample(ch string, x, y, w, h int) float32 {
	fx, fy := float32(0), float32(0)
	if w > 1 {
		fx = float32(x) / float32(w-1)
	}
	if h > 1 {
		fy = float32(y) / float32(h-1)
	}
	switch ch {
	case "R":
		return 0.5 + 0.5*fx
	case "G":
		return 0.5 + 0.5*fy
	case "B":
		// High frequency inside every 4x4 block, so B44 has something to
		// quantise even at level 0: a fixture a lossy codec reproduces exactly
		// asserts nothing about the codec.
		return 0.5 + 0.25*float32((3*x+7*y)%17)/17
	default:
		return 1
	}
}

// level holds one resolution level's pixels.
type level struct {
	lx, ly int
	w, h   int
	vals   map[string][]float32
}

func makeLevel(lx, ly, w, h int, lossy bool, pt exr.PixelType) (*level, error) {
	l := &level{lx: lx, ly: ly, w: w, h: h, vals: map[string][]float32{}}
	for _, ch := range channels {
		v := make([]float32, w*h)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				if lossy {
					v[y*w+x] = lossySample(ch, x, y, w, h)
					continue
				}
				s := sharpSample(ch, x, y, lx, ly)
				// The gate holds lossless rows to bit-identity, so a fixture
				// value a half cannot represent would make a passing row depend
				// on rounding rather than on the writer. Assert it instead.
				if pt == exr.PixelTypeHalf && half.FromFloat32(s).Float32() != s {
					return nil, fmt.Errorf("fixture value %v for %s at (%d,%d) level (%d,%d) is not exact as a half", s, ch, x, y, lx, ly)
				}
				v[y*w+x] = s
			}
		}
		l.vals[ch] = v
	}
	return l, nil
}

func (l *level) frameBuffer(pt exr.PixelType) *exr.FrameBuffer {
	fb := exr.NewFrameBuffer()
	for _, ch := range channels {
		switch pt {
		case exr.PixelTypeHalf:
			hv := make([]half.Half, l.w*l.h)
			for i, v := range l.vals[ch] {
				hv[i] = half.FromFloat32(v)
			}
			fb.Set(ch, exr.NewSliceFromHalf(hv, l.w, l.h))
		case exr.PixelTypeFloat:
			cp := make([]float32, len(l.vals[ch]))
			copy(cp, l.vals[ch])
			fb.Set(ch, exr.NewSliceFromFloat32(cp, l.w, l.h))
		}
	}
	return fb
}

// quantize returns the value as it reads back from a file of this pixel type,
// ignoring the codec: what an exact codec must reproduce, and what a lossy
// one's error is measured against.
func quantize(v float32, pt exr.PixelType) float32 {
	if pt == exr.PixelTypeHalf {
		return half.FromFloat32(v).Float32()
	}
	return v
}

// writeExpect writes the samples the reference reader must find, in the same
// "lx ly x y channel value" form scripts/exrtiledump prints.
func writeExpect(path string, levels []*level, pt exr.PixelType) error {
	var b strings.Builder
	for _, l := range levels {
		for _, ch := range channels {
			for y := 0; y < l.h; y++ {
				for x := 0; x < l.w; x++ {
					fmt.Fprintf(&b, "%d %d %d %d %s %.9g\n",
						l.lx, l.ly, x, y, ch, quantize(l.vals[ch][y*l.w+x], pt))
				}
			}
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// writeStructure writes the geometry this library believes the file has, in the
// exact form scripts/exrtiledump prints the geometry the reference computes
// from the file. validate.sh diffs the two: level counts, per-level sizes and
// per-level tile counts are then two independent implementations of the
// format's level arithmetic, compared byte for byte.
func writeStructure(path string, f fixture, h *exr.Header) error {
	var b strings.Builder
	round := "down"
	if f.round == exr.LevelRoundUp {
		round = "up"
	}
	fmt.Fprintf(&b, "# mode %s\n", levelModeTag(f.mode))
	fmt.Fprintf(&b, "# tile %d %d %s\n", f.tw, f.th, round)
	fmt.Fprintf(&b, "# levels %d %d\n", h.NumXLevels(), h.NumYLevels())
	line := func(lx, ly int) {
		fmt.Fprintf(&b, "# level %d %d %d %d %d %d\n", lx, ly,
			h.LevelWidth(lx), h.LevelHeight(ly), h.NumXTiles(lx), h.NumYTiles(ly))
	}
	switch f.mode {
	case exr.LevelModeOne:
		line(0, 0)
	case exr.LevelModeMipmap:
		for l := 0; l < h.NumXLevels(); l++ {
			line(l, l)
		}
	case exr.LevelModeRipmap:
		for ly := 0; ly < h.NumYLevels(); ly++ {
			for lx := 0; lx < h.NumXLevels(); lx++ {
				line(lx, ly)
			}
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// levelFromFrameBuffer reads back a level this library's mipmap or ripmap
// generator produced. For those two fixtures the container is what is gated:
// the format does not specify a downsampling filter, so there is no external
// truth for the *contents* of level N — only for where those samples must land
// in the file and how they must be encoded.
func levelFromFrameBuffer(lx, ly, w, h int, fb *exr.FrameBuffer, pt exr.PixelType) *level {
	l := &level{lx: lx, ly: ly, w: w, h: h, vals: map[string][]float32{}}
	for _, ch := range channels {
		v := make([]float32, w*h)
		s := fb.Get(ch)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				switch pt {
				case exr.PixelTypeHalf:
					v[y*w+x] = s.GetHalf(x, y).Float32()
				case exr.PixelTypeFloat:
					v[y*w+x] = s.GetFloat32(x, y)
				}
			}
		}
		l.vals[ch] = v
	}
	return l
}

type fixture struct {
	name       string
	w, h       int
	offX, offY int // data window origin, to catch geometry that assumes (0,0)
	tw, th     int
	mode       exr.LevelMode
	round      exr.LevelRoundingMode
	pt         exr.PixelType
	codec      exr.Compression
	codecTag   string
	typeTag    string
	api        string // manual | manual-reverse | mipauto | ripauto
	lossy      bool
	expect     string // exact | lossy
	note       string
}

func header(f fixture) *exr.Header {
	h := exr.NewTiledHeader(f.w, f.h, f.tw, f.th)
	dw := exr.Box2i{
		Min: exr.V2i{X: int32(f.offX), Y: int32(f.offY)},
		Max: exr.V2i{X: int32(f.offX + f.w - 1), Y: int32(f.offY + f.h - 1)},
	}
	h.SetDataWindow(dw)
	h.SetDisplayWindow(dw)
	h.SetCompression(f.codec)
	h.SetTileDescription(exr.TileDescription{
		XSize:        uint32(f.tw),
		YSize:        uint32(f.th),
		Mode:         f.mode,
		RoundingMode: f.round,
	})
	cl := exr.NewChannelList()
	for _, ch := range channels {
		cl.Add(exr.Channel{Name: ch, Type: f.pt, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)
	return h
}

// levelPairs lists the (lx,ly) levels a fixture holds, in the order the
// format's tile offset table is indexed: for a ripmap, y level major.
func levelPairs(f fixture, h *exr.Header) [][2]int {
	var out [][2]int
	switch f.mode {
	case exr.LevelModeOne:
		out = append(out, [2]int{0, 0})
	case exr.LevelModeMipmap:
		for l := 0; l < h.NumXLevels(); l++ {
			out = append(out, [2]int{l, l})
		}
	case exr.LevelModeRipmap:
		for ly := 0; ly < h.NumYLevels(); ly++ {
			for lx := 0; lx < h.NumXLevels(); lx++ {
				out = append(out, [2]int{lx, ly})
			}
		}
	}
	return out
}

func writeFixture(dir string, f fixture) ([]*level, error) {
	h := header(f)
	path := filepath.Join(dir, f.name+".exr")
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer out.Close()

	w, err := exr.NewTiledWriter(out, h)
	if err != nil {
		return nil, err
	}

	var levels []*level

	switch f.api {
	case "manual", "manual-reverse":
		pairs := levelPairs(f, h)
		if f.api == "manual-reverse" {
			// A legal but non-canonical order: deepest level first, and within
			// each level the last tile first. The OpenEXR reference writer
			// documents that tiles may be written in any order, and a reader
			// finds them through the tile offset table rather than by position,
			// so this file must read back identical to its canonical twin.
			for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
				pairs[i], pairs[j] = pairs[j], pairs[i]
			}
		}
		for _, p := range pairs {
			lx, ly := p[0], p[1]
			lw, lh := h.LevelWidth(lx), h.LevelHeight(ly)
			l, err := makeLevel(lx, ly, lw, lh, f.lossy, f.pt)
			if err != nil {
				return nil, err
			}
			levels = append(levels, l)
			w.SetFrameBuffer(l.frameBuffer(f.pt))
			nx, ny := h.NumXTiles(lx), h.NumYTiles(ly)
			for i := 0; i < ny; i++ {
				for j := 0; j < nx; j++ {
					ty, tx := i, j
					if f.api == "manual-reverse" {
						ty, tx = ny-1-i, nx-1-j
					}
					if err := w.WriteTileLevel(tx, ty, lx, ly); err != nil {
						return nil, fmt.Errorf("write tile (%d,%d) level (%d,%d): %w", tx, ty, lx, ly, err)
					}
				}
			}
		}

	case "mipauto":
		src, err := makeLevel(0, 0, f.w, f.h, f.lossy, f.pt)
		if err != nil {
			return nil, err
		}
		fb := src.frameBuffer(f.pt)
		gen, err := exr.GenerateMipmapsFromFrameBuffer(fb, f.w, f.h, h, exr.FilterBox)
		if err != nil {
			return nil, err
		}
		for i, g := range gen {
			levels = append(levels, levelFromFrameBuffer(i, i, g.Width, g.Height, g.FrameBuffer, f.pt))
		}
		if err := exr.WriteMipmapTiledImage(w, fb, f.w, f.h, exr.FilterBox); err != nil {
			return nil, err
		}

	case "ripauto":
		src, err := makeLevel(0, 0, f.w, f.h, f.lossy, f.pt)
		if err != nil {
			return nil, err
		}
		fb := src.frameBuffer(f.pt)
		gen, err := exr.GenerateRipmapsFromFrameBuffer(fb, f.w, f.h, h, exr.FilterBox)
		if err != nil {
			return nil, err
		}
		for ly := range gen {
			for lx := range gen[ly] {
				g := gen[ly][lx]
				levels = append(levels, levelFromFrameBuffer(lx, ly, g.Width, g.Height, g.FrameBuffer, f.pt))
			}
		}
		if err := exr.WriteRipmapTiledImage(w, fb, f.w, f.h, exr.FilterBox); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("unknown api %q", f.api)
	}

	if err := w.Close(); err != nil {
		return nil, err
	}
	sort.SliceStable(levels, func(i, j int) bool {
		if levels[i].ly != levels[j].ly {
			return levels[i].ly < levels[j].ly
		}
		return levels[i].lx < levels[j].lx
	})
	if err := writeStructure(filepath.Join(dir, f.name+".structure"), f, h); err != nil {
		return nil, err
	}
	return levels, writeExpect(filepath.Join(dir, f.name+".expect"), levels, f.pt)
}

// writeScanlineTwin writes the fixture's level-0 samples through the scanline
// path, which the gate already holds against the reference for every pixel type
// and codec. It gives validate.sh a second, independent oracle: oiiotool is
// asked whether the tiled file and the scanline file hold the same image.
func writeScanlineTwin(dir string, f fixture, l *level) error {
	h := exr.NewScanlineHeader(f.w, f.h)
	dw := exr.Box2i{
		Min: exr.V2i{X: int32(f.offX), Y: int32(f.offY)},
		Max: exr.V2i{X: int32(f.offX + f.w - 1), Y: int32(f.offY + f.h - 1)},
	}
	h.SetDataWindow(dw)
	h.SetDisplayWindow(dw)
	h.SetCompression(exr.CompressionNone)
	cl := exr.NewChannelList()
	for _, ch := range channels {
		cl.Add(exr.Channel{Name: ch, Type: f.pt, XSampling: 1, YSampling: 1})
	}
	h.SetChannels(cl)

	out, err := os.Create(filepath.Join(dir, f.name+"_twin.exr"))
	if err != nil {
		return err
	}
	defer out.Close()

	wr, err := exr.NewScanlineWriter(out, h)
	if err != nil {
		return err
	}
	wr.SetFrameBuffer(l.frameBuffer(f.pt))
	if err := wr.WritePixels(f.offY, f.offY+f.h-1); err != nil {
		return err
	}
	return wr.Close()
}

// writeGuards records what this library does with a tiled header the format
// forbids. OpenEXR's sanity check refuses subsampled channels in a tiled image
// ("channel 'BY': x subsampling factor is not 1 (2) for a tiled image"), so a
// file this library is willing to write with one is a file no reader can open.
// validate.sh requires the answer to be "rejected".
func writeGuards(dir string) error {
	var b strings.Builder
	b.WriteString("# guard\tresult\tdetail\n")

	h := exr.NewTiledHeader(32, 16, 16, 16)
	cl := exr.NewChannelList()
	cl.Add(exr.Channel{Name: "Y", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	cl.Add(exr.Channel{Name: "RY", Type: exr.PixelTypeHalf, XSampling: 2, YSampling: 2})
	cl.Add(exr.Channel{Name: "BY", Type: exr.PixelTypeHalf, XSampling: 2, YSampling: 2})
	h.SetChannels(cl)

	result, detail := "accepted", "no error"
	if err := h.Validate(); err != nil {
		result, detail = "rejected", err.Error()
	}
	fmt.Fprintf(&b, "subsampled-tiled-header\t%s\t%s\n", result, detail)

	// The writer must refuse it too, not only the header check.
	result, detail = "accepted", "no error"
	f, err := os.CreateTemp(dir, "guard-*.exr")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, err := exr.NewTiledWriter(f, h); err != nil {
		result, detail = "rejected", err.Error()
	}
	fmt.Fprintf(&b, "subsampled-tiled-writer\t%s\t%s\n", result, detail)

	return os.WriteFile(filepath.Join(dir, "guards.tsv"), []byte(b.String()), 0o644)
}

const (
	imgW, imgH = 71, 40 // neither dimension is a multiple of any tile size below
	divW, divH = 64, 40 // divides exactly by 16 x 8
)

func fixtures() []fixture {
	return []fixture{
		// Plain tiled, one level. Two tile sizes: one that leaves partial tiles
		// on both edges, one that divides the image exactly, and one larger
		// than the image so the only tile is partial in both directions.
		{name: "t_one_partial_half_zip", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, 3x2 tiles, partial edge tiles in x and y"},
		{name: "t_one_exact_half_zip", w: divW, h: divH, tw: 16, th: 8,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, 4x5 tiles, tile size divides the image exactly"},
		{name: "t_one_bigtile_half_zip", w: imgW, h: imgH, tw: 128, th: 128,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, a single tile larger than the image"},
		{name: "t_one_offset_half_zip", w: imgW, h: imgH, offX: 7, offY: 3, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, data window origin at (7,3)"},
		{name: "t_one_partial_half_none", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionNone,
			codecTag: "none", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, uncompressed"},
		{name: "t_one_partial_half_piz", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionPIZ,
			codecTag: "piz", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, PIZ over partial tiles"},
		{name: "t_one_partial_float_zips", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeFloat, codec: exr.CompressionZIPS,
			codecTag: "zips", typeTag: "float", api: "manual", expect: "exact",
			note: "one level, FLOAT samples"},
		{name: "t_one_partial_float_rle", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeFloat, codec: exr.CompressionRLE,
			codecTag: "rle", typeTag: "float", api: "manual", expect: "exact",
			note: "one level, RLE"},
		{name: "t_one_partial_float_pxr24", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeFloat, codec: exr.CompressionPXR24,
			codecTag: "pxr24", typeTag: "float", api: "manual", lossy: true, expect: "lossy",
			note: "one level, PXR24 over FLOAT"},

		// Lossy. The [0.5,1] fixture keeps every 4x4 block inside one binade so
		// the bounds derived in scripts/validate.sh apply at every level.
		{name: "t_one_partial_half_b44", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionB44,
			codecTag: "b44", typeTag: "half", api: "manual", lossy: true, expect: "lossy",
			note: "one level, B44 over partial tiles"},
		{name: "t_one_partial_half_dwaa", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionDWAA,
			codecTag: "dwaa", typeTag: "half", api: "manual", lossy: true, expect: "lossy",
			note: "one level, DWAA over partial tiles"},
		{name: "t_one_partial_half_dwab", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionDWAB,
			codecTag: "dwab", typeTag: "half", api: "manual", lossy: true, expect: "lossy",
			note: "one level, DWAB over partial tiles"},
		{name: "t_one_partial_half_b44a", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionB44A,
			codecTag: "b44a", typeTag: "half", api: "manual", lossy: true, expect: "lossy",
			note: "one level, B44A over partial tiles"},

		// HTJ2K over tiles. Both identifiers were missing from the tiled
		// compression switch entirely, so a tiled header declaring either
		// produced "compression not yet implemented" from the writer — the one
		// compression a tiled cloud workflow most wants, since an HTJ2K chunk
		// is a JPEG 2000 codestream whose packets are individually
		// addressable. Lossless, so these are exact rather than toleranced.
		{name: "t_one_half_htj2k256", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionHTJ2K256,
			codecTag: "htj2k256", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, HTJ2K 256 over partial tiles"},
		{name: "t_one_half_htj2k32", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionHTJ2K32,
			codecTag: "htj2k32", typeTag: "half", api: "manual", expect: "exact",
			note: "one level, HTJ2K 32 over partial tiles"},
		{name: "t_one_float_htj2k256", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeFloat, codec: exr.CompressionHTJ2K256,
			codecTag: "htj2k256", typeTag: "float", api: "manual", expect: "exact",
			note: "one level, HTJ2K 256 over float, partial tiles"},
		{name: "t_mip_half_htj2k256", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionHTJ2K256,
			codecTag: "htj2k256", typeTag: "half", api: "manual", expect: "exact",
			note: "mipmap, HTJ2K 256, every level"},

		// Mipmaps, every level written with content that differs per level.
		{name: "t_mip_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "mipmap, round down, 7 levels, distinct content per level"},
		{name: "t_mip_up_float_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, round: exr.LevelRoundUp, pt: exr.PixelTypeFloat,
			codec: exr.CompressionZIP, codecTag: "zip", typeTag: "float", api: "manual",
			expect: "exact", note: "mipmap, round up, 8 levels"},
		{name: "t_mip_half_piz", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionPIZ,
			codecTag: "piz", typeTag: "half", api: "manual", expect: "exact",
			note: "mipmap, PIZ: every level down to the 1x1 tip"},
		{name: "t_mip_half_b44", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionB44,
			codecTag: "b44", typeTag: "half", api: "manual", lossy: true, expect: "lossy",
			note: "mipmap, B44: 4x4 blocks larger than the levels they cover"},

		// Ripmaps: levels are independent in x and y.
		{name: "t_rip_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeRipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual", expect: "exact",
			note: "ripmap, round down, 7x6 levels, distinct content per level"},
		{name: "t_rip_up_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeRipmap, round: exr.LevelRoundUp, pt: exr.PixelTypeHalf,
			codec: exr.CompressionZIP, codecTag: "zip", typeTag: "half", api: "manual",
			expect: "exact", note: "ripmap, round up, 8x7 levels"},
		{name: "t_rip_float_none", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeRipmap, pt: exr.PixelTypeFloat, codec: exr.CompressionNone,
			codecTag: "none", typeTag: "float", api: "manual", expect: "exact",
			note: "ripmap, uncompressed"},

		// The same content, written in a legal but non-canonical order. The
		// format locates a tile through the offset table, so the file must read
		// back identical to its canonical twin.
		{name: "t_one_revorder_half_zip", w: imgW, h: imgH, tw: 32, th: 32,
			mode: exr.LevelModeOne, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual-reverse", expect: "exact",
			note: "one level, tiles written last to first"},
		{name: "t_mip_revorder_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual-reverse", expect: "exact",
			note: "mipmap, deepest level first, tiles last to first"},
		{name: "t_rip_revorder_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeRipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "manual-reverse", expect: "exact",
			note: "ripmap, levels and tiles in reverse order"},

		// The library's own level generators, which choose both the level
		// contents and the order the tiles are written in.
		{name: "t_mipauto_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeMipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "mipauto", expect: "exact",
			note: "WriteMipmapTiledImage: every generated level, where it belongs"},
		{name: "t_ripauto_half_zip", w: imgW, h: imgH, tw: 16, th: 16,
			mode: exr.LevelModeRipmap, pt: exr.PixelTypeHalf, codec: exr.CompressionZIP,
			codecTag: "zip", typeTag: "half", api: "ripauto", expect: "exact",
			note: "WriteRipmapTiledImage: every generated level, where it belongs"},
	}
}

func levelModeTag(m exr.LevelMode) string {
	switch m {
	case exr.LevelModeMipmap:
		return "mipmap"
	case exr.LevelModeRipmap:
		return "ripmap"
	default:
		return "one"
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tiledgen <outdir>")
		os.Exit(2)
	}
	dir := os.Args[1]

	var manifest strings.Builder
	manifest.WriteString("# file\ttype\tcodec\tmode\texpect\tlevels\tchunks\ttile\tnote\n")

	written, failed := 0, 0
	for _, f := range fixtures() {
		levels, err := writeFixture(dir, f)
		if err != nil {
			fmt.Printf("FAIL write %s: %v\n", f.name, err)
			failed++
			continue
		}
		if err := writeScanlineTwin(dir, f, levels[0]); err != nil {
			fmt.Printf("FAIL twin %s: %v\n", f.name, err)
			failed++
			continue
		}
		h := header(f)
		fmt.Fprintf(&manifest, "%s.exr\t%s\t%s\t%s\t%s\t%dx%d\t%d\t%dx%d\t%s\n",
			f.name, f.typeTag, f.codecTag, levelModeTag(f.mode), f.expect,
			h.NumXLevels(), h.NumYLevels(), h.ChunksInFile(), f.tw, f.th, f.note)
		written++
	}

	if err := writeGuards(dir); err != nil {
		fmt.Fprintf(os.Stderr, "guards: %v\n", err)
		os.Exit(1)
	}
	if err := os.WriteFile(filepath.Join(dir, "manifest.tsv"), []byte(manifest.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "manifest: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d tiled fixtures, %d failures\n", written, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
