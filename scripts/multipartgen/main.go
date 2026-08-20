// Command multipartgen writes multi-part EXR files whose parts deliberately
// disagree — in data window, in compression, in channel layout and in storage
// type — so that an external reader can be asked whether each part still holds
// the image this library put in it.
//
// Nothing here reads an EXR file. Alongside every part it writes the samples
// it intended, one plain PFM per channel (a fifteen-line binary float format
// with no relationship to EXR) and, for integer channels, a text table. The
// oracle is the OpenEXR reference implementation, invoked by
// scripts/validate.sh on the results; a defect applied identically to this
// library's multi-part writer and its multi-part reader is invisible to a
// round trip and fails there.
//
// The fixture is a ramp whose value depends on the part index, the channel
// index and both pixel coordinates, so a swapped part, a swapped channel, a
// transposed tile and a shifted scanline all change the samples that come
// back. A constant image would compare equal against almost any defect.
package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

// chanSpec is one channel of one part.
type chanSpec struct {
	name string
	pt   exr.PixelType
	// xs is the channel's XSampling: it stores every xs-th column, so it
	// contributes ceil(width/xs) samples per row rather than width of them.
	// Zero means 1. YSampling is not varied because it removes whole rows
	// from a scanline, which this library's chunk layout cannot express and
	// NewMultiPartWriter refuses.
	xs int
}

// sampling returns the channel's XSampling, treating the zero value as 1.
func (c chanSpec) sampling() int {
	if c.xs < 1 {
		return 1
	}
	return c.xs
}

// partSpec is one part of one multi-part file. A zero tile means the part is
// a scanline part; otherwise it is a tiled part with square tiles.
type partSpec struct {
	name     string
	compName string
	comp     exr.Compression
	// dw is the part's data window as minX, minY, maxX, maxY. Parts of one
	// file may differ here; they may not differ in display window, which the
	// format requires every part of a file to agree on.
	dw    [4]int
	chans []chanSpec
	tile  int
	// lineByLine writes this part one scanline at a time rather than in a
	// single call, which is how most callers drive a writer.
	lineByLine bool
	// scrambleTiles writes this part's tiles in an order that is not the
	// order the chunk offset table lists them in.
	scrambleTiles bool
	// mipmap gives the tiled part a full pyramid of levels, each written
	// from its own frame buffer.
	mipmap bool
}

// levels is the number of mipmap levels this part has, counting the full
// resolution one. Round-down is the default rounding mode.
func (p partSpec) levels() int {
	if !p.mipmap {
		return 1
	}
	n := 1
	w, h := p.width(), p.height()
	for w > 1 || h > 1 {
		w /= 2
		h /= 2
		if w < 1 {
			w = 1
		}
		if h < 1 {
			h = 1
		}
		n++
	}
	return n
}

// levelDims is the size of one level, by the same round-down rule.
func (p partSpec) levelDims(level int) (int, int) {
	w, h := p.width(), p.height()
	for i := 0; i < level; i++ {
		w /= 2
		h /= 2
		if w < 1 {
			w = 1
		}
		if h < 1 {
			h = 1
		}
	}
	return w, h
}

func (p partSpec) minX() int  { return p.dw[0] }
func (p partSpec) minY() int  { return p.dw[1] }
func (p partSpec) width() int { return p.dw[2] - p.dw[0] + 1 }
func (p partSpec) height() int {
	return p.dw[3] - p.dw[1] + 1
}

// fileSpec is one multi-part file.
type fileSpec struct {
	file string
	// display is the display window shared by every part. The reference
	// implementation refuses a file whose parts disagree about it.
	display [4]int
	parts   []partSpec
	// order is the order the parts are written in, which need not be the
	// order they are declared in: the chunk offset tables have to survive
	// interleaving.
	order []int
	// groupLines, when non-zero, writes every part in groups of that many
	// scanlines, round-robin across parts, so the parts' chunks interleave
	// in the file.
	groupLines int
	note       string
}

// typeName is the pixel type as the manifest and the reference name it.
func typeName(pt exr.PixelType) string {
	switch pt {
	case exr.PixelTypeHalf:
		return "half"
	case exr.PixelTypeFloat:
		return "float"
	case exr.PixelTypeUint:
		return "uint"
	}
	return "unknown"
}

func rgb(pt exr.PixelType) []chanSpec {
	return []chanSpec{{"R", pt, 1}, {"G", pt, 1}, {"B", pt, 1}}
}

func rgba(pt exr.PixelType) []chanSpec {
	return []chanSpec{{"R", pt, 1}, {"G", pt, 1}, {"B", pt, 1}, {"A", pt, 1}}
}

var files = []fileSpec{
	{
		// The embedded-proxy shape: a full-resolution master and a reduced
		// proxy in one file. Their data windows differ; their display window
		// cannot.
		file:    "mp_proxy.exr",
		display: [4]int{0, 0, 95, 63},
		note:    "master plus half-resolution proxy, written a scanline at a time",
		parts: []partSpec{
			{name: "master", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 95, 63}, chans: rgba(exr.PixelTypeHalf)},
			{name: "proxy", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 47, 31}, chans: rgba(exr.PixelTypeHalf),
				lineByLine: true},
		},
	},
	{
		// Data windows that differ in size and in origin, including an origin
		// left of and above the display window, which is legal and which the
		// scanline chunk grid is anchored to.
		file:    "mp_windows.exr",
		display: [4]int{0, 0, 70, 39},
		note:    "three data windows: full, inset, and negative origin",
		order:   []int{2, 0, 1},
		parts: []partSpec{
			{name: "full", comp: exr.CompressionZIPS, compName: "zips",
				dw: [4]int{0, 0, 70, 39}, chans: rgb(exr.PixelTypeHalf)},
			{name: "inset", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{13, 7, 60, 35}, chans: rgb(exr.PixelTypeHalf)},
			{name: "shifted", comp: exr.CompressionNone, compName: "none",
				dw: [4]int{-9, -5, 30, 22}, chans: rgb(exr.PixelTypeFloat)},
		},
	},
	{
		// One compression per part, including HTJ2K beside codecs that are
		// nothing like it. Every codec here is lossless for the pixel type it
		// is paired with, so every comparison is exact.
		file:       "mp_codecs.exr",
		display:    [4]int{0, 0, 63, 47},
		note:       "one codec per part, htj2k beside zip, piz, pxr24, rle and b44",
		groupLines: 16,
		parts: []partSpec{
			{name: "none", comp: exr.CompressionNone, compName: "none",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "zip", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "piz", comp: exr.CompressionPIZ, compName: "piz",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "htj2k256", comp: exr.CompressionHTJ2K256, compName: "htj2k256",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "htj2k32", comp: exr.CompressionHTJ2K32, compName: "htj2k32",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "pxr24", comp: exr.CompressionPXR24, compName: "pxr24",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "rle", comp: exr.CompressionRLE, compName: "rle",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "b44float", comp: exr.CompressionB44, compName: "b44",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeFloat)},
		},
	},
	{
		// Channel lists that have nothing in common: a different count, a
		// different set of names, a different pixel type per part, and a
		// layered name whose sort order differs from its declaration order.
		file:    "mp_channels.exr",
		display: [4]int{0, 0, 63, 47},
		note:    "four different channel layouts, including uint and layered names",
		parts: []partSpec{
			{name: "rgba", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 63, 47}, chans: rgba(exr.PixelTypeHalf)},
			{name: "depth", comp: exr.CompressionZIPS, compName: "zips",
				dw: [4]int{0, 0, 63, 47}, chans: []chanSpec{{"Z", exr.PixelTypeFloat, 1}}},
			{name: "aov", comp: exr.CompressionPIZ, compName: "piz",
				dw: [4]int{0, 0, 63, 47}, chans: []chanSpec{
					{"diffuse.R", exr.PixelTypeFloat, 1},
					{"diffuse.G", exr.PixelTypeFloat, 1},
					{"diffuse.B", exr.PixelTypeFloat, 1},
					{"mask", exr.PixelTypeHalf, 1},
				}},
			{name: "id", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 63, 47}, chans: []chanSpec{{"id", exr.PixelTypeUint, 1}}},
		},
	},
	{
		// Mixed storage: a scanline part and two tiled parts in one file, one
		// of the tiled parts on an inset data window, and one written in an
		// order that is not the order of the chunk offset table.
		file:    "mp_mixed.exr",
		display: [4]int{0, 0, 95, 63},
		note:    "scanline part beside tiled parts, one inset, one written out of order",
		parts: []partSpec{
			{name: "beauty", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 95, 63}, chans: rgb(exr.PixelTypeHalf)},
			{name: "tiled", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 95, 63}, chans: rgb(exr.PixelTypeHalf), tile: 32},
			{name: "tilecrop", comp: exr.CompressionNone, compName: "none",
				dw: [4]int{11, 5, 74, 52}, chans: rgb(exr.PixelTypeHalf), tile: 16,
				scrambleTiles: true},
		},
	},
	{
		// Subsampled channels. XSampling above 1 narrows each row, so a
		// channel with xs=2 contributes half as many samples per line as its
		// neighbours; packing the full width for it makes the chunk longer
		// than the format says and reads every channel after it at the wrong
		// offset. Chroma-style 2x and 4x sit beside full-resolution channels
		// in one part, which is the case that fails when the packer uses one
		// width for all of them.
		//
		// YSampling is not varied: it removes whole rows from a scanline,
		// which this library's chunk layout cannot express and
		// NewMultiPartWriter refuses, as ScanlineWriter does.
		file:    "mp_subsampled.exr",
		display: [4]int{0, 0, 63, 47},
		note:    "a part mixing full-resolution and 2x and 4x subsampled channels",
		parts: []partSpec{
			{name: "full", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 63, 47}, chans: rgb(exr.PixelTypeHalf)},
			{name: "chroma", comp: exr.CompressionZIPS, compName: "zips",
				dw: [4]int{0, 0, 63, 47}, chans: []chanSpec{
					{"Y", exr.PixelTypeHalf, 1},
					{"BY", exr.PixelTypeHalf, 2},
					{"RY", exr.PixelTypeHalf, 2},
					{"quarter", exr.PixelTypeFloat, 4},
				}},
			// PIZ is here because it is the codec that models per-channel
			// dimensions explicitly, and this path passed it the window's
			// width for every channel. That is not a wrong answer but an
			// index out of range: writing a subsampled PIZ part panicked with
			// "index out of range [128] with length 128" until the width was
			// computed per channel, as the scanline path always did.
			{name: "chromapiz", comp: exr.CompressionPIZ, compName: "piz",
				dw: [4]int{0, 0, 63, 47}, chans: []chanSpec{
					{"Y", exr.PixelTypeHalf, 1},
					{"BY", exr.PixelTypeHalf, 2},
					{"RY", exr.PixelTypeHalf, 2},
				}},
		},
	},
	{
		// The proxy shape a player actually wants: a full-resolution scanline
		// master beside a tiled part carrying a whole mip pyramid, every
		// level written from its own frame buffer and compared at its own
		// resolution.
		file:    "mp_mipmap.exr",
		display: [4]int{0, 0, 95, 63},
		note:    "scanline master beside a mipmapped tiled proxy, every level compared",
		parts: []partSpec{
			{name: "master", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 95, 63}, chans: rgb(exr.PixelTypeHalf)},
			{name: "proxy", comp: exr.CompressionZIP, compName: "zip",
				dw: [4]int{0, 0, 63, 63}, chans: rgb(exr.PixelTypeHalf),
				tile: 16, mipmap: true},
		},
	},
}

// sample is the fixture. It depends on the part, the channel and both
// coordinates, so no two parts and no two channels hold the same image and no
// rearrangement of rows, columns or tiles leaves the samples unchanged.
func sample(part, ch, level, x, y, w, h int) float32 {
	fx, fy := float32(0), float32(0)
	if w > 1 {
		fx = float32(x) / float32(w-1)
	}
	if h > 1 {
		fy = float32(y) / float32(h-1)
	}
	// The level term matters: without it the ramp is the same normalised
	// image at every resolution, and two levels swapped for one another would
	// still compare equal wherever their sizes happen to match.
	base := 0.07*float32(part+1) + 0.13*float32(ch+1) + 0.023*float32(level)
	return base + 0.31*fx + 0.19*fy + 0.11*fx*fy
}

// quantise returns the value a channel of this type can actually hold, so the
// truth file records what the reader must find rather than what the writer was
// handed.
func quantise(pt exr.PixelType, v float32) float32 {
	switch pt {
	case exr.PixelTypeHalf:
		return half.FromFloat32(v).Float32()
	case exr.PixelTypeUint:
		return float32(uintValue(v))
	}
	return v
}

// uintValue maps the fixture onto integers wide enough that a byte-order or
// stride defect changes them.
func uintValue(v float32) uint32 {
	return uint32(math.Round(float64(v) * 100000))
}

// writePFM writes a single-channel little-endian PFM. PFM stores its rows
// bottom to top; this is the only place that ordering appears.
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
			buf[0] = byte(bits)
			buf[1] = byte(bits >> 8)
			buf[2] = byte(bits >> 16)
			buf[3] = byte(bits >> 24)
			if _, err := bw.Write(buf[:]); err != nil {
				return err
			}
		}
	}
	return bw.Flush()
}

// writeUintTable writes one integer per line in raster order.
func writeUintTable(path string, w, h int, pix []uint32) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	bw := bufio.NewWriter(f)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			fmt.Fprintf(bw, "%d\n", pix[y*w+x])
		}
	}
	return bw.Flush()
}

func box(v [4]int) exr.Box2i {
	return exr.Box2i{
		Min: exr.V2i{X: int32(v[0]), Y: int32(v[1])},
		Max: exr.V2i{X: int32(v[2]), Y: int32(v[3])},
	}
}

// buildHeader builds one part's header the way a caller would.
func buildHeader(fs fileSpec, p partSpec) *exr.Header {
	var h *exr.Header
	if p.tile > 0 {
		h = exr.NewTiledHeader(p.width(), p.height(), p.tile, p.tile)
		h.Set(&exr.Attribute{Name: exr.AttrNameType, Type: exr.AttrTypeString, Value: exr.PartTypeTiled})
		if p.mipmap {
			h.SetTileDescription(exr.TileDescription{
				XSize: uint32(p.tile), YSize: uint32(p.tile),
				Mode: exr.LevelModeMipmap,
			})
		}
	} else {
		h = exr.NewScanlineHeader(p.width(), p.height())
		h.Set(&exr.Attribute{Name: exr.AttrNameType, Type: exr.AttrTypeString, Value: exr.PartTypeScanline})
	}
	h.SetDataWindow(box(p.dw))
	h.SetDisplayWindow(box(fs.display))
	h.SetCompression(p.comp)
	h.Set(&exr.Attribute{Name: exr.AttrNameName, Type: exr.AttrTypeString, Value: p.name})

	cl := exr.NewChannelList()
	for _, c := range p.chans {
		cl.Add(exr.Channel{Name: c.name, Type: c.pt, XSampling: int32(c.sampling()), YSampling: 1})
	}
	h.SetChannels(cl)
	return h
}

// partPixels returns the fixture for one part, one plane per channel, in the
// part's own data window coordinates starting at (0,0) — the coordinate system
// this library's frame buffers use, where the pixel at the data window's
// minimum maps to buffer position (0,0).
func partPixels(partIdx int, p partSpec, level int) [][]float32 {
	w, h := p.levelDims(level)
	planes := make([][]float32, len(p.chans))
	for ci, c := range p.chans {
		// A subsampled channel's plane is its own size: ceil(w/xs) columns.
		// The sample it holds at column x is the one at full-resolution column
		// x*xs, which is what the reference reads back for it.
		xs := c.sampling()
		cw := (w + xs - 1) / xs
		plane := make([]float32, cw*h)
		for y := 0; y < h; y++ {
			for x := 0; x < cw; x++ {
				plane[y*cw+x] = quantise(c.pt, sample(partIdx, ci, level, x*xs, y, w, h))
			}
		}
		planes[ci] = plane
	}
	return planes
}

// frameBuffer builds the part's frame buffer. Slices carry the part's data
// window origin: the library addresses a frame buffer in the window's own
// coordinates, and a slice built over a bare buffer starts at zero unless it
// is told otherwise.
func frameBuffer(p partSpec, w, h int, planes [][]float32) *exr.FrameBuffer {
	ox, oy := p.minX(), p.minY()
	fb := exr.NewFrameBuffer()
	for ci, c := range p.chans {
		// The slice is the channel's own width; Slice divides the caller's x
		// by XSampling, so the reader and writer address it the same way.
		xs := c.sampling()
		w := (w + xs - 1) / xs
		switch c.pt {
		case exr.PixelTypeHalf:
			hv := make([]half.Half, w*h)
			for i, v := range planes[ci] {
				hv[i] = half.FromFloat32(v)
			}
			fb.Set(c.name, exr.NewSliceFromHalf(hv, w, h).WithOrigin(ox, oy))
		case exr.PixelTypeFloat:
			fv := make([]float32, w*h)
			copy(fv, planes[ci])
			fb.Set(c.name, exr.NewSliceFromFloat32(fv, w, h).WithOrigin(ox, oy))
		case exr.PixelTypeUint:
			uv := make([]uint32, w*h)
			for i, v := range planes[ci] {
				uv[i] = uint32(v)
			}
			fb.Set(c.name, exr.NewSliceFromUint32(uv, w, h).WithOrigin(ox, oy))
		}
	}
	return fb
}

// writeFile writes one multi-part fixture and returns the manifest rows it
// earned: one per part, and one per channel.
func writeFile(outDir string, fs fileSpec) (parts, chans []string, err error) {
	headers := make([]*exr.Header, len(fs.parts))
	for i, p := range fs.parts {
		headers[i] = buildHeader(fs, p)
	}

	f, err := os.Create(filepath.Join(outDir, fs.file))
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	mpo, err := exr.NewMultiPartOutputFile(f, headers)
	if err != nil {
		return nil, nil, fmt.Errorf("new multi-part output file: %w", err)
	}

	// part -> level -> channel -> plane. Only tiled parts have more than one
	// level; the frame buffer set here is the one for level 0.
	allPlanes := make([][][][]float32, len(fs.parts))
	for i, p := range fs.parts {
		allPlanes[i] = make([][][]float32, p.levels())
		for l := range allPlanes[i] {
			allPlanes[i][l] = partPixels(i, p, l)
		}
		if err := mpo.SetFrameBuffer(i, frameBuffer(p, p.width(), p.height(), allPlanes[i][0])); err != nil {
			return nil, nil, fmt.Errorf("set frame buffer for part %d: %w", i, err)
		}
	}

	order := fs.order
	if order == nil {
		order = make([]int, len(fs.parts))
		for i := range order {
			order[i] = i
		}
	}

	// Scanline parts, either in one call each or interleaved in groups.
	if fs.groupLines > 0 {
		done := 0
		for pass := 0; done < len(fs.parts); pass++ {
			done = 0
			for _, i := range order {
				p := fs.parts[i]
				if p.tile > 0 {
					done++
					continue
				}
				start := pass * fs.groupLines
				if start >= p.height() {
					done++
					continue
				}
				n := fs.groupLines
				if start+n > p.height() {
					n = p.height() - start
				}
				if err := mpo.WritePixels(i, n); err != nil {
					return nil, nil, fmt.Errorf("write %d scanlines of part %d: %w", n, i, err)
				}
			}
		}
	} else {
		for _, i := range order {
			p := fs.parts[i]
			if p.tile > 0 {
				continue
			}
			if p.lineByLine {
				for y := 0; y < p.height(); y++ {
					if err := mpo.WritePixels(i, 1); err != nil {
						return nil, nil, fmt.Errorf("write line %d of part %d: %w", y, i, err)
					}
				}
				continue
			}
			if err := mpo.WritePixels(i, p.height()); err != nil {
				return nil, nil, fmt.Errorf("write %d scanlines of part %d: %w", p.height(), i, err)
			}
		}
	}

	// Tiled parts.
	for _, i := range order {
		p := fs.parts[i]
		if p.tile == 0 {
			continue
		}
		for level := 0; level < p.levels(); level++ {
			lw, lh := p.levelDims(level)
			if level > 0 {
				// Each level is written from its own frame buffer, which is
				// the level's size and not the part's.
				if err := mpo.SetFrameBuffer(i, frameBuffer(p, lw, lh, allPlanes[i][level])); err != nil {
					return nil, nil, fmt.Errorf("set frame buffer for part %d level %d: %w", i, level, err)
				}
			}
			nx := (lw + p.tile - 1) / p.tile
			ny := (lh + p.tile - 1) / p.tile
			type xy struct{ x, y int }
			var tiles []xy
			for ty := 0; ty < ny; ty++ {
				for tx := 0; tx < nx; tx++ {
					tiles = append(tiles, xy{tx, ty})
				}
			}
			if p.scrambleTiles {
				// Reverse: the last tile of the offset table is written first.
				for a, b := 0, len(tiles)-1; a < b; a, b = a+1, b-1 {
					tiles[a], tiles[b] = tiles[b], tiles[a]
				}
			}
			for _, t := range tiles {
				if err := mpo.WriteTileLevel(i, t.x, t.y, level, level); err != nil {
					return nil, nil, fmt.Errorf("write tile (%d, %d) of part %d level %d: %w", t.x, t.y, i, level, err)
				}
			}
		}
	}

	if err := mpo.Close(); err != nil {
		return nil, nil, fmt.Errorf("close: %w", err)
	}

	// Manifest rows and truth files.
	for i, p := range fs.parts {
		typ := "scanlineimage"
		tile := "-"
		if p.tile > 0 {
			typ = "tiledimage"
			tile = fmt.Sprint(p.tile)
		}
		names := make([]string, len(p.chans))
		for ci, c := range p.chans {
			names[ci] = c.name + ":" + typeName(c.pt)
		}
		parts = append(parts, strings.Join([]string{
			fs.file, fmt.Sprint(i), p.name, typ, p.compName,
			fmt.Sprint(p.minX()), fmt.Sprint(p.minY()),
			fmt.Sprint(p.width()), fmt.Sprint(p.height()),
			strings.Join(names, ","), tile, fmt.Sprint(p.levels()),
		}, "\t"))

		for level := 0; level < p.levels(); level++ {
			lw, lh := p.levelDims(level)
			for ci, c := range p.chans {
				// The truth raster is the channel's own size: a subsampled
				// channel holds ceil(width/XSampling) columns, and that is
				// what the reference reads back for it.
				cw := (lw + c.sampling() - 1) / c.sampling()
				stem := fmt.Sprintf("%s.p%d.%s", strings.TrimSuffix(fs.file, ".exr"), i,
					strings.ReplaceAll(c.name, ".", "_"))
				if level > 0 {
					stem = fmt.Sprintf("%s.l%d", stem, level)
				}
				kind := "float"
				truth := stem + ".pfm"
				if c.pt == exr.PixelTypeUint {
					kind = "uint"
					truth = stem + ".txt"
					u := make([]uint32, len(allPlanes[i][level][ci]))
					for k, v := range allPlanes[i][level][ci] {
						u[k] = uint32(v)
					}
					if err := writeUintTable(filepath.Join(outDir, truth), cw, lh, u); err != nil {
						return nil, nil, err
					}
				} else {
					if err := writePFM(filepath.Join(outDir, truth), cw, lh, allPlanes[i][level][ci]); err != nil {
						return nil, nil, err
					}
				}
				chans = append(chans, strings.Join([]string{
					fs.file, fmt.Sprint(i), c.name, kind, truth, fmt.Sprint(level),
				}, "\t"))
			}
		}
	}
	return parts, chans, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: multipartgen <outdir>")
		os.Exit(2)
	}
	outDir := os.Args[1]
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var partRows, chanRows []string
	var fileRows []string
	failed := 0
	for _, fs := range files {
		p, c, err := writeFile(outDir, fs)
		if err != nil {
			fmt.Printf("FAIL write %s: %v\n", fs.file, err)
			failed++
			continue
		}
		partRows = append(partRows, p...)
		chanRows = append(chanRows, c...)
		fileRows = append(fileRows, fmt.Sprintf("%s\t%d\t%s", fs.file, len(fs.parts), fs.note))
	}

	write := func(name, header string, rows []string) {
		body := header + "\n"
		if len(rows) > 0 {
			body += strings.Join(rows, "\n") + "\n"
		}
		if err := os.WriteFile(filepath.Join(outDir, name), []byte(body), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
	}
	write("files.tsv", "# file\tparts\tnote", fileRows)
	write("parts.tsv", "# file\tpart\tname\ttype\tcompression\tminx\tminy\twidth\theight\tchannels(name:type)\ttile\tlevels", partRows)
	write("chans.tsv", "# file\tpart\tchannel\tkind\ttruth\tlevel", chanRows)

	fmt.Printf("wrote %d multi-part files, %d parts, %d channels, %d write failures\n",
		len(fileRows), len(partRows), len(chanRows), failed)
	if failed > 0 {
		os.Exit(1)
	}
}
