package exr

import (
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"math"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/half"
)

// RegionSamples is what a viewport read produced: one plane per channel, in the
// region's own coordinates, together with what reading it cost.
//
// Region is the rectangle actually covered, which is the requested one clipped
// to the data window. A plane has Region.Width()*Region.Height() samples in row
// order, and sample (x, y) of the image lives at index
// (y-Region.Min.Y)*width + (x-Region.Min.X).
type RegionSamples struct {
	Region   Box2i
	Channels []string
	Planes   map[string][]float32

	// FileBytes is how much of the file was read: the chunks the region
	// touches, headers included, and nothing else.
	FileBytes int64
	// ChunksRead and ChunksTotal say how much of the part was skipped at the
	// chunk level, before any decompression.
	ChunksRead, ChunksTotal int
	// DecodedBytes and SkippedBytes are the code-block data the block coder
	// ran on, and the data the region let it leave alone. They are zero for a
	// compression other than HTJ2K, which has no addressable interior.
	DecodedBytes, SkippedBytes int
}

// ReadRegion reads a rectangle of a tiled part without decompressing the whole
// of it, and without reading the parts of the file it does not need.
//
// This is the composition the chunk index and the codestream index were built
// for. ChunksForRegion turns the viewport into the tiles that hold it, reading
// only chunk headers; each of those chunks is fetched by byte range; and for
// HTJ2K the chunk's own codestream is decoded for the viewport alone, so the
// entropy coder never runs on the code-blocks the viewport cannot reach. A
// 32768-row image can be read at 256 rows for the cost of 256 rows.
//
// Every other compression decompresses its chunks whole — there is nothing
// inside a ZIP chunk to address — so for those this saves the chunk reads and
// nothing more, and reports SkippedBytes of zero rather than implying
// otherwise. Samples are returned as float32 whatever the channel's stored
// type, since a viewport is for looking at.
func (f *File) ReadRegion(part int, region Box2i) (*RegionSamples, error) {
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}
	if !f.partIsTiled(part) {
		return nil, errors.New("exr: ReadRegion needs a tiled part; a scanline part has no viewport structure below the chunk")
	}
	td := h.TileDescription()
	if td == nil || td.XSize == 0 || td.YSize == 0 {
		return nil, errors.New("exr: tiled part has no usable tile description")
	}

	dw := h.DataWindow()
	clipped := Box2i{
		Min: V2i{X: maxi32(region.Min.X, dw.Min.X), Y: maxi32(region.Min.Y, dw.Min.Y)},
		Max: V2i{X: mini32(region.Max.X, dw.Max.X), Y: mini32(region.Max.Y, dw.Max.Y)},
	}
	if clipped.Min.X > clipped.Max.X || clipped.Min.Y > clipped.Max.Y {
		return nil, fmt.Errorf("exr: region %v does not meet the data window %v", region, dw)
	}
	rw := int(clipped.Max.X-clipped.Min.X) + 1
	rh := int(clipped.Max.Y-clipped.Min.Y) + 1

	sorted := h.Channels().SortedByName()
	for _, ch := range sorted {
		// A subsampled channel stores a smaller plane, so a packed line is not
		// one width for every channel and the scatter below would put the
		// samples of one channel into another. Refuse rather than guess.
		if ch.XSampling != 1 || ch.YSampling != 1 {
			return nil, fmt.Errorf("exr: ReadRegion does not handle subsampled channels: %s has sampling %d,%d",
				ch.Name, ch.XSampling, ch.YSampling)
		}
	}
	out := &RegionSamples{
		Region:      clipped,
		Channels:    make([]string, len(sorted)),
		Planes:      make(map[string][]float32, len(sorted)),
		ChunksTotal: f.NumChunks(part),
	}
	for i, ch := range sorted {
		out.Channels[i] = ch.Name
		out.Planes[ch.Name] = make([]float32, rw*rh)
	}

	// ChunksForRegion takes a half-open rectangle; a Box2i is inclusive.
	chunks, err := f.ChunksForRegion(part, clipped.Min.X, clipped.Min.Y,
		clipped.Max.X+1, clipped.Max.Y+1, 0, 0)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("exr: no chunk of part %d covers %v", part, clipped)
	}

	htj2k := h.Compression() == CompressionHTJ2K256 || h.Compression() == CompressionHTJ2K32
	var tiled *TiledReader
	if !htj2k {
		// Every other codec decompresses a chunk whole, through the same
		// switch the ordinary tiled reader uses.
		tiled, err = NewTiledReaderPart(f, part)
		if err != nil {
			return nil, err
		}
	}
	tw, th := int(td.XSize), int(td.YSize)
	dwW := int(dw.Max.X-dw.Min.X) + 1
	dwH := int(dw.Max.Y-dw.Min.Y) + 1

	for _, cr := range chunks {
		data := make([]byte, cr.DataLength)
		if _, err := f.reader.ReadAt(data, cr.DataOffset); err != nil {
			return nil, fmt.Errorf("exr: reading chunk at %d: %w", cr.Offset, err)
		}
		out.FileBytes += cr.Length
		out.ChunksRead++

		// An edge tile is smaller than the tile size, and a codec told the
		// wrong extent reads past its input.
		tileW := mini(tw, dwW-cr.TileX*tw)
		tileH := mini(th, dwH-cr.TileY*th)
		if tileW <= 0 || tileH <= 0 {
			return nil, fmt.Errorf("exr: tile (%d,%d) lies outside the data window", cr.TileX, cr.TileY)
		}
		// The tile's rectangle in image coordinates, and the part of the
		// viewport inside it — in the tile's own coordinates, which is what
		// the codestream is indexed by.
		tx0 := dw.Min.X + int32(cr.TileX*tw)
		ty0 := dw.Min.Y + int32(cr.TileY*th)
		sx0 := int(maxi32(clipped.Min.X, tx0) - tx0)
		sy0 := int(maxi32(clipped.Min.Y, ty0) - ty0)
		sx1 := int(mini32(clipped.Max.X, tx0+int32(tileW)-1)-tx0) + 1
		sy1 := int(mini32(clipped.Max.Y, ty0+int32(tileH)-1)-ty0) + 1

		channels := htj2kTileChannels(h.Channels(), tileW, tileH)
		var (
			samples      []byte
			bytesPerLine int
			offsets      []int
			gotX0, gotY0 int
			gotW, gotH   int
		)
		if htj2k {
			sub := image.Rect(sx0, sy0, sx1, sy1)
			res, err := compression.HTJ2KDecompressPartial(data, channels,
				&compression.HTJ2KDecodeOptions{Region: &sub})
			if err != nil {
				return nil, fmt.Errorf("exr: tile (%d,%d): %w", cr.TileX, cr.TileY, err)
			}
			samples, bytesPerLine = res.Data, res.BytesPerLine
			offsets = htj2kPlaneOffsets(channels, res.Width)
			gotX0, gotY0 = sx0, sy0
			gotW, gotH = res.Width, res.Height
			out.DecodedBytes += res.DecodedBytes
			out.SkippedBytes += res.SkippedBytes
		} else {
			whole, err := tiled.decompressTileData(data, cr.TileX, cr.TileY, tileW, tileH)
			if err != nil {
				return nil, fmt.Errorf("exr: tile (%d,%d): %w", cr.TileX, cr.TileY, err)
			}
			samples = whole
			offsets = htj2kPlaneOffsets(channels, tileW)
			bytesPerLine = htj2kLineBytes(channels, tileW)
			gotX0, gotY0 = 0, 0
			gotW, gotH = tileW, tileH
		}

		for ci, ch := range channels {
			plane := out.Planes[ch.Name]
			for y := sy0; y < sy1; y++ {
				sy := y - gotY0
				if sy < 0 || sy >= gotH {
					continue
				}
				line := samples[sy*bytesPerLine:]
				row := line[offsets[ci]:]
				dstY := int(ty0) + y - int(clipped.Min.Y)
				for x := sx0; x < sx1; x++ {
					sx := x - gotX0
					if sx < 0 || sx >= gotW {
						continue
					}
					dstX := int(tx0) + x - int(clipped.Min.X)
					plane[dstY*rw+dstX] = htj2kSampleAt(row, sx, ch.Type)
				}
			}
		}
	}

	return out, nil
}

// htj2kPlaneOffsets returns the byte offset of each channel within one packed
// line of a decoded chunk, for a decode that produced the given width.
func htj2kPlaneOffsets(channels []compression.HTJ2KChannelInfo, width int) []int {
	offsets := make([]int, len(channels))
	pos := 0
	for i, ch := range channels {
		offsets[i] = pos
		pos += width * htj2kSampleSize(ch.Type)
	}
	return offsets
}

func htj2kLineBytes(channels []compression.HTJ2KChannelInfo, width int) int {
	total := 0
	for _, ch := range channels {
		total += width * htj2kSampleSize(ch.Type)
	}
	return total
}

func htj2kSampleSize(t int) int {
	if t == compression.HTJ2KPixelTypeHalf {
		return 2
	}
	return 4
}

// htj2kSampleAt reads sample x of a packed row as a float32, whatever the
// channel's stored type. A viewport is for looking at, so uint and half both
// widen rather than forcing the caller to switch on the type.
func htj2kSampleAt(row []byte, x, t int) float32 {
	switch t {
	case compression.HTJ2KPixelTypeHalf:
		return half.Half(binary.LittleEndian.Uint16(row[x*2:])).Float32()
	case compression.HTJ2KPixelTypeUint:
		return float32(binary.LittleEndian.Uint32(row[x*4:]))
	default:
		return math.Float32frombits(binary.LittleEndian.Uint32(row[x*4:]))
	}
}

func maxi32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func mini32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func mini(a, b int) int {
	if a < b {
		return a
	}
	return b
}
