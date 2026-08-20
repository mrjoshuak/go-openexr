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

// ReadRegion reads a rectangle of a part without decompressing the whole of it,
// and without reading the parts of the file it does not need.
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
//
// A scanline part works too, and for HTJ2K it is where the codestream saving is
// largest: a scanline chunk is the full width of the image by 32 or 256 rows,
// so a viewport is a small fraction of it horizontally and there is a great
// deal inside the chunk to skip. Measured on a 256-row chunk 8192 samples wide,
// a 256x256 viewport puts 9.3% of the code-block bytes through the block coder.
// A scanline part cannot be addressed below a band of rows at the chunk level,
// though, so the chunk-level saving is smaller than a tiled part's.
func (f *File) ReadRegion(part int, region Box2i) (*RegionSamples, error) {
	return f.ReadRegionLevel(part, region, 0, 0)
}

// ReadRegionLevel reads a rectangle of one resolution level of a tiled part.
//
// This is what serves HD playback from a 5K plate, and it is worth being clear
// about why it rather than HTJ2K's own resolution levels. A mipmap level is
// computed by a real downsample filter when the file is written, so level 1 of
// a 5120x2700 frame is a proper 2560x1350 image. A reduced-resolution decode of
// the codestream is not: an EXR chunk carries float samples as reinterpreted
// bit patterns, so the wavelet averages bit patterns and the result is unusable
// for display wherever the image spans exponents or touches zero. The pyramid
// costs about a third more storage and is the answer; see
// HTJ2KDecodeOptions.ReduceResolution for what the other mechanism is good for.
//
// The rectangle is in the level's own coordinates, which share the data
// window's origin and run to the level's width and height. Level (0, 0) is the
// full-resolution image and is what ReadRegion asks for.
//
// A part with only one level accepts (0, 0) and nothing else. A ripmapped part
// takes the two indices independently; a mipmapped one requires them equal, as
// the format does.
func (f *File) ReadRegionLevel(part int, region Box2i, levelX, levelY int) (*RegionSamples, error) {
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}
	// A deep part's chunks hold a sample-count table and a variable number of
	// samples per pixel, not a rectangle of fixed-size ones, so none of the
	// addressing below applies to it. It used to be attempted anyway and
	// failed downstream in the codec — "compression: corrupted ZIP data" —
	// which is a refusal by accident, naming the wrong thing.
	if headerIsDeep(h) {
		return nil, fmt.Errorf("exr: part %d is deep; ReadRegion addresses chunks of "+
			"fixed-size pixels and a deep chunk holds a sample count table and a "+
			"variable number of samples per pixel", part)
	}
	if !f.partIsTiled(part) {
		if levelX != 0 || levelY != 0 {
			return nil, fmt.Errorf("exr: a scanline part has only level (0,0); asked for (%d,%d)",
				levelX, levelY)
		}
		return f.readScanlineRegion(part, h, region)
	}
	td := h.TileDescription()
	if td == nil || td.XSize == 0 || td.YSize == 0 {
		return nil, errors.New("exr: tiled part has no usable tile description")
	}
	if levelX < 0 || levelX >= h.NumXLevels() || levelY < 0 || levelY >= h.NumYLevels() {
		return nil, fmt.Errorf("exr: level (%d,%d) is outside the part's %dx%d levels",
			levelX, levelY, h.NumXLevels(), h.NumYLevels())
	}
	if td.Mode == LevelModeMipmap && levelX != levelY {
		return nil, fmt.Errorf("exr: a mipmapped part has one level index; asked for (%d,%d)",
			levelX, levelY)
	}

	// The level's own window: same origin as the data window, its own extent.
	dw := Box2i{
		Min: h.DataWindow().Min,
		Max: V2i{
			X: h.DataWindow().Min.X + int32(h.LevelWidth(levelX)) - 1,
			Y: h.DataWindow().Min.Y + int32(h.LevelHeight(levelY)) - 1,
		},
	}
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
		clipped.Max.X+1, clipped.Max.Y+1, levelX, levelY)
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
	// The level's extent, not the image's: an edge tile of level 2 is short
	// against level 2's width, and a codec told the full-resolution one reads
	// past its input.
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

// readScanlineRegion is ReadRegion for a part stored as scanlines.
//
// The shape differs from the tiled path in what a chunk is: a band of rows at
// the full width of the data window rather than a rectangle. That makes the
// chunk-level saving weaker — a viewport pulls whole rows — and the
// codestream-level saving stronger for HTJ2K, since the viewport is a small
// part of a very wide chunk.
func (f *File) readScanlineRegion(part int, h *Header, region Box2i) (*RegionSamples, error) {
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

	chunks, err := f.ChunksForScanlines(part, clipped.Min.Y, clipped.Max.Y)
	if err != nil {
		return nil, err
	}
	if len(chunks) == 0 {
		return nil, fmt.Errorf("exr: no chunk of part %d covers rows %d to %d",
			part, clipped.Min.Y, clipped.Max.Y)
	}

	reader, err := NewScanlineReaderPart(f, part)
	if err != nil {
		return nil, err
	}
	comp := h.Compression()
	htj2k := comp == CompressionHTJ2K256 || comp == CompressionHTJ2K32
	linesPerChunk := comp.ScanlinesPerChunk()
	dwW := int(dw.Max.X-dw.Min.X) + 1

	for _, cr := range chunks {
		data := make([]byte, cr.DataLength)
		if _, err := f.reader.ReadAt(data, cr.DataOffset); err != nil {
			return nil, fmt.Errorf("exr: reading chunk at %d: %w", cr.Offset, err)
		}
		out.FileBytes += cr.Length
		out.ChunksRead++

		// The chunk's own rows, clipped to the data window: the last chunk of
		// a part is short whenever the height is not a multiple.
		chunkY0 := int(cr.Y)
		numLines := mini(linesPerChunk, int(dw.Max.Y)-chunkY0+1)
		if numLines <= 0 {
			return nil, fmt.Errorf("exr: chunk at row %d lies outside the data window", chunkY0)
		}
		// The rows of this chunk the region wants, chunk-relative.
		sy0 := int(maxi32(clipped.Min.Y, int32(chunkY0))) - chunkY0
		sy1 := int(mini32(clipped.Max.Y, int32(chunkY0+numLines-1))) - chunkY0 + 1
		sx0 := int(clipped.Min.X - dw.Min.X)
		sx1 := int(clipped.Max.X-dw.Min.X) + 1

		channels := htj2kTileChannels(h.Channels(), dwW, numLines)
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
				return nil, fmt.Errorf("exr: chunk at row %d: %w", chunkY0, err)
			}
			samples, bytesPerLine = res.Data, res.BytesPerLine
			offsets = htj2kPlaneOffsets(channels, res.Width)
			gotX0, gotY0 = sx0, sy0
			gotW, gotH = res.Width, res.Height
			out.DecodedBytes += res.DecodedBytes
			out.SkippedBytes += res.SkippedBytes
		} else {
			whole, err := reader.decompressChunk(data, chunkY0, numLines, comp)
			if err != nil {
				return nil, fmt.Errorf("exr: chunk at row %d: %w", chunkY0, err)
			}
			samples = whole
			offsets = htj2kPlaneOffsets(channels, dwW)
			bytesPerLine = htj2kLineBytes(channels, dwW)
			gotX0, gotY0 = 0, 0
			gotW, gotH = dwW, numLines
		}

		for ci, ch := range channels {
			plane := out.Planes[ch.Name]
			for y := sy0; y < sy1; y++ {
				sy := y - gotY0
				if sy < 0 || sy >= gotH {
					continue
				}
				row := samples[sy*bytesPerLine:][offsets[ci]:]
				dstY := chunkY0 + y - int(clipped.Min.Y)
				for x := sx0; x < sx1; x++ {
					sx := x - gotX0
					if sx < 0 || sx >= gotW {
						continue
					}
					dstX := int(dw.Min.X) + x - int(clipped.Min.X)
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
