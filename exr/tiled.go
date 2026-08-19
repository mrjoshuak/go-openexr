package exr

import (
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/half"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Tiled I/O errors
var (
	ErrNotTiled        = errors.New("exr: image is not tiled")
	ErrTileOutOfRange  = errors.New("exr: tile coordinates out of range")
	ErrLevelOutOfRange = errors.New("exr: level out of range")
)

// TiledReader reads tiled images from an EXR file.
type TiledReader struct {
	file        *File
	part        int
	header      *Header
	frameBuffer *FrameBuffer
	dataWindow  Box2i
	channelList *ChannelList
	tileDesc    *TileDescription
	tilesX      int
	tilesY      int

	// flevelOnce ensures FLEVEL detection happens only once (thread-safe)
	flevelOnce sync.Once
}

// NewTiledReader creates a reader for a single-part tiled file.
func NewTiledReader(f *File) (*TiledReader, error) {
	return NewTiledReaderPart(f, 0)
}

// NewTiledReaderPart creates a reader for a specific part of a tiled file.
func NewTiledReaderPart(f *File, part int) (*TiledReader, error) {
	if f == nil {
		return nil, ErrInvalidFile
	}
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}

	if !h.IsTiled() {
		return nil, ErrNotTiled
	}

	td := h.TileDescription()
	if td == nil || td.XSize == 0 || td.YSize == 0 {
		return nil, errors.New("exr: missing or invalid tile description")
	}

	cl := h.Channels()
	if cl == nil || cl.Len() == 0 {
		return nil, errors.New("exr: missing or empty channels attribute")
	}
	// Validate all channels have known pixel types and valid sampling
	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)
		if ch.Type.Size() == 0 {
			return nil, errors.New("exr: channel has unknown pixel type")
		}
		if ch.XSampling == 0 || ch.YSampling == 0 {
			return nil, errors.New("exr: channel has invalid sampling (zero)")
		}
	}

	dw := h.DataWindow()

	// Validate data window dimensions
	width := int(dw.Width())
	height := int(dw.Height())
	if width <= 0 || height <= 0 {
		return nil, errors.New("exr: invalid data window dimensions")
	}
	// Limit to reasonable size to prevent OOM (64K x 64K max)
	const maxDimension = 65536
	if width > maxDimension || height > maxDimension {
		return nil, errors.New("exr: data window dimensions too large")
	}

	return &TiledReader{
		file:        f,
		part:        part,
		header:      h,
		dataWindow:  dw,
		channelList: cl,
		tileDesc:    td,
		tilesX:      (width + int(td.XSize) - 1) / int(td.XSize),
		tilesY:      (height + int(td.YSize) - 1) / int(td.YSize),
	}, nil
}

// NumXLevels returns the number of resolution levels in X direction.
func (r *TiledReader) NumXLevels() int {
	return r.header.NumXLevels()
}

// NumYLevels returns the number of resolution levels in Y direction.
func (r *TiledReader) NumYLevels() int {
	return r.header.NumYLevels()
}

// NumLevels returns the number of resolution levels (for mipmap mode).
func (r *TiledReader) NumLevels() int {
	return r.header.NumXLevels()
}

// LevelWidth returns the width of the image at level lx.
func (r *TiledReader) LevelWidth(lx int) int {
	return r.header.LevelWidth(lx)
}

// LevelHeight returns the height of the image at level ly.
func (r *TiledReader) LevelHeight(ly int) int {
	return r.header.LevelHeight(ly)
}

// NumXTilesAtLevel returns the number of tiles in X at level lx.
func (r *TiledReader) NumXTilesAtLevel(lx int) int {
	return r.header.NumXTiles(lx)
}

// NumYTilesAtLevel returns the number of tiles in Y at level ly.
func (r *TiledReader) NumYTilesAtLevel(ly int) int {
	return r.header.NumYTiles(ly)
}

// LevelMode returns the level mode for this tiled image.
func (r *TiledReader) LevelMode() LevelMode {
	return r.tileDesc.Mode
}

// Header returns the header for this part.
func (r *TiledReader) Header() *Header {
	return r.header
}

// DataWindow returns the data window for this part.
func (r *TiledReader) DataWindow() Box2i {
	return r.dataWindow
}

// TileDescription returns the tile description.
func (r *TiledReader) TileDescription() *TileDescription {
	return r.tileDesc
}

// NumTilesX returns the number of tiles in the X direction.
func (r *TiledReader) NumTilesX() int {
	return r.tilesX
}

// NumTilesY returns the number of tiles in the Y direction.
func (r *TiledReader) NumTilesY() int {
	return r.tilesY
}

// SetFrameBuffer sets the frame buffer to read pixels into.
func (r *TiledReader) SetFrameBuffer(fb *FrameBuffer) {
	r.frameBuffer = fb
}

// chunkIndex calculates the chunk index for a tile at the given coordinates and level.
// For single-level (LevelModeOne), levelX and levelY should be 0.
// For mipmap, levelX and levelY should be the same.
// For ripmap, levelX and levelY can be different.
func (r *TiledReader) chunkIndex(tileX, tileY, levelX, levelY int) int {
	// One definition of this mapping, shared with the writer: a table the
	// writer fills in one order and the reader consults in another is the
	// defect these two used to be able to drift into.
	return r.header.tileChunkIndex(tileX, tileY, levelX, levelY)
}

// ReadTile reads a single tile at level 0 into the frame buffer.
func (r *TiledReader) ReadTile(tileX, tileY int) error {
	return r.ReadTileLevel(tileX, tileY, 0, 0)
}

// ReadTileLevel reads a single tile at the specified level into the frame buffer.
// For mipmap mode, levelX and levelY should be the same.
// For ripmap mode, levelX and levelY can be different.
// For single-level mode (LevelModeOne), both should be 0.
func (r *TiledReader) ReadTileLevel(tileX, tileY, levelX, levelY int) error {
	if r.frameBuffer == nil {
		return ErrNoFrameBuffer
	}

	// Validate level
	if levelX < 0 || levelX >= r.NumXLevels() || levelY < 0 || levelY >= r.NumYLevels() {
		return ErrLevelOutOfRange
	}

	// Validate tile coordinates for this level
	numTilesX := r.header.NumXTiles(levelX)
	numTilesY := r.header.NumYTiles(levelY)
	if tileX < 0 || tileX >= numTilesX || tileY < 0 || tileY >= numTilesY {
		return ErrTileOutOfRange
	}

	// Calculate chunk index using level-aware calculation
	chunkIdx := r.chunkIndex(tileX, tileY, levelX, levelY)

	// Read tile data from file
	_, data, err := r.file.ReadTileChunk(r.part, chunkIdx)
	if err != nil {
		return err
	}

	// Get level dimensions
	levelWidth := r.header.LevelWidth(levelX)
	levelHeight := r.header.LevelHeight(levelY)

	// Calculate actual tile dimensions (may be smaller at edges)
	tileWidth := min(int(r.tileDesc.XSize), levelWidth-tileX*int(r.tileDesc.XSize))
	tileHeight := min(int(r.tileDesc.YSize), levelHeight-tileY*int(r.tileDesc.YSize))

	decompressedData, err := r.decompressTileData(data, tileX, tileY, tileWidth, tileHeight)
	if err != nil {
		return err
	}

	// Decode tile data - use level-aware decoding
	return r.decodeTileLevel(tileX, tileY, levelX, levelY, tileWidth, tileHeight, decompressedData)
}

// decompressTileData expands one tile chunk's bytes to packed samples.
//
// A tile that is not smaller than its uncompressed size was stored raw by the
// writer (see storeUncompressed); the size is the only signal for this. Small
// mipmap levels hit this constantly, since a 2x2 tile never compresses.
//
// It is a function rather than part of ReadTileLevel because File.ReadRegion
// decompresses tiles it fetched by byte range, without a frame buffer or a
// level walk, and a second copy of this switch would be a second place for a
// codec to go missing — which is exactly how HTJ2K came to be absent from the
// tiled path in the first place.
func (r *TiledReader) decompressTileData(data []byte, tileX, tileY, tileWidth, tileHeight int) ([]byte, error) {
	comp := r.header.Compression()
	if comp != CompressionNone && len(data) >= r.calculateTileSize(tileWidth, tileHeight) {
		return data, nil
	}
	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		return r.decompressTileRLE(data, tileWidth, tileHeight)
	case CompressionZIPS, CompressionZIP:
		return r.decompressTileZIP(data, tileWidth, tileHeight)
	case CompressionPIZ:
		return r.decompressTilePIZ(data, tileWidth, tileHeight)
	case CompressionPXR24:
		return r.decompressTilePXR24(data, tileWidth, tileHeight)
	case CompressionB44, CompressionB44A:
		return r.decompressTileB44(data, tileWidth, tileHeight)
	case CompressionDWAA, CompressionDWAB:
		return r.decompressTileDWA(data, tileX, tileY, tileWidth, tileHeight)
	case CompressionHTJ2K256, CompressionHTJ2K32:
		return r.decompressTileHTJ2K(data, tileWidth, tileHeight)
	default:
		return nil, errors.New("exr: compression not yet implemented: " + comp.String())
	}
}

// ReadTiles reads all tiles in a range at level 0.
func (r *TiledReader) ReadTiles(tileX1, tileY1, tileX2, tileY2 int) error {
	return r.ReadTilesLevel(tileX1, tileY1, tileX2, tileY2, 0, 0)
}

// ReadTilesLevel reads all tiles in a range at the specified level.
func (r *TiledReader) ReadTilesLevel(tileX1, tileY1, tileX2, tileY2, levelX, levelY int) error {
	if tileX1 > tileX2 || tileY1 > tileY2 {
		return ErrTileOutOfRange
	}

	for ty := tileY1; ty <= tileY2; ty++ {
		for tx := tileX1; tx <= tileX2; tx++ {
			if err := r.ReadTileLevel(tx, ty, levelX, levelY); err != nil {
				return err
			}
		}
	}

	return nil
}

// calculateTileSize calculates the uncompressed size of a tile.
func (r *TiledReader) calculateTileSize(tileWidth, tileHeight int) int {
	bytesPerPixel := 0
	for i := 0; i < r.channelList.Len(); i++ {
		ch := r.channelList.At(i)
		bytesPerPixel += ch.Type.Size()
	}
	return bytesPerPixel * tileWidth * tileHeight
}

// decompressTileRLE decompresses RLE-compressed tile data.
func (r *TiledReader) decompressTileRLE(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	expectedSize := r.calculateTileSize(tileWidth, tileHeight)

	decompressed, err := compression.RLEDecompress(data, expectedSize)
	if err != nil {
		return nil, err
	}

	// Reverse the predictor, then undo the byte reordering.
	output := make([]byte, expectedSize)
	predictor.ReconstructBytes(output, decompressed)
	return output, nil
}

// decompressTileZIP decompresses ZIP-compressed tile data.
// Uses combined deinterleave + predictor decode for better performance.
// Thread-safe: uses pooled buffers instead of shared state.
// Also detects and records the FLEVEL for deterministic round-trip.
func (r *TiledReader) decompressTileZIP(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	expectedSize := r.calculateTileSize(tileWidth, tileHeight)

	// Detect FLEVEL from compressed data for deterministic round-trip
	// Use sync.Once to ensure thread-safe detection (first detection wins)
	r.flevelOnce.Do(func() {
		if flevel, ok := compression.DetectZlibFLevel(data); ok {
			r.header.SetDetectedFLevel(flevel)
		}
	})

	// Get pooled buffer for decompression (thread-safe)
	buf := zipDecompressBufPool.Get().(*zipDecompressBuf)
	defer zipDecompressBufPool.Put(buf)

	// Ensure buffer is large enough
	if cap(buf.data) < expectedSize {
		buf.data = make([]byte, expectedSize)
	} else {
		buf.data = buf.data[:expectedSize]
	}

	// zlib decompress into pooled buffer
	if err := compression.ZIPDecompressTo(buf.data, data); err != nil {
		return nil, err
	}

	// Use optimized combined deinterleave + predictor decode
	output := make([]byte, expectedSize)
	predictor.ReconstructBytes(output, buf.data)
	return output, nil
}

// decompressTilePIZ decompresses PIZ-compressed tile data.
func (r *TiledReader) decompressTilePIZ(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	// Build channel info sorted by name (EXR channel order).
	sortedChannels := r.channelList.SortedByName()
	pizChannels := make([]compression.PIZChannel, len(sortedChannels))
	for i, ch := range sortedChannels {
		size := 1
		if ch.Type == PixelTypeFloat || ch.Type == PixelTypeUint {
			size = 2
		}
		pizChannels[i] = compression.PIZChannel{
			Size: size,
			NX:   tileWidth,
			NY:   tileHeight,
		}
	}

	// Check for passthrough: when PIZ compression produces output >= input size,
	// C++ stores the data uncompressed in per-scanline interleaved format.
	expectedSize := r.calculateTileSize(tileWidth, tileHeight)
	if len(data) == expectedSize {
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}

	// Decompress with channel-aware wavelet.
	channelData, err := compression.PIZDecompressBytesChannels(data, pizChannels)
	if err != nil {
		return nil, err
	}

	// Rearrange from channel-contiguous to per-scanline.
	return pizChannelContiguousToScanline(channelData, sortedChannels, tileWidth, tileHeight), nil
}

// decompressTilePXR24 decompresses PXR24-compressed tile data.
func (r *TiledReader) decompressTilePXR24(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	expectedSize := r.calculateTileSize(tileWidth, tileHeight)

	// Build channel info - channels are sorted by name in the file
	sortedChannels := r.channelList.SortedByName()

	channels := make([]compression.ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		var pxrType int
		switch ch.Type {
		case PixelTypeUint:
			pxrType = 0 // pxr24PixelTypeUint
		case PixelTypeHalf:
			pxrType = 1 // pxr24PixelTypeHalf
		case PixelTypeFloat:
			pxrType = 2 // pxr24PixelTypeFloat
		}
		channels[i] = compression.ChannelInfo{
			Type:   pxrType,
			Width:  tileWidth,
			Height: tileHeight,
		}
	}

	return compression.PXR24Decompress(data, channels, tileWidth, tileHeight, expectedSize)
}

// decodeTileLevel decodes uncompressed tile data at a specific level into the frame buffer.
func (r *TiledReader) decodeTileLevel(tileX, tileY, levelX, levelY, tileWidth, tileHeight int, data []byte) error {
	// Where this tile begins in the frame buffer's coordinates.
	//
	// A tile's position within its level is level-relative; a frame buffer is
	// addressed in the data window's own coordinates, which is what Slice
	// carries an origin for. So the window's minimum is added here rather than
	// subtracted from every access. For a window at the origin the two are the
	// same, which is why the level-relative position worked for every file
	// whose data window started at (0, 0) and read outside the buffer for the
	// rest.
	dw := r.header.DataWindow()
	startX := int(dw.Min.X) + tileX*int(r.tileDesc.XSize)
	startY := int(dw.Min.Y) + tileY*int(r.tileDesc.YSize)

	// Channels are sorted by name
	sortedChannels := r.channelList.SortedByName()

	reader := xdr.NewReader(data)

	for y := 0; y < tileHeight; y++ {
		for _, ch := range sortedChannels {
			slice := r.frameBuffer.Get(ch.Name)
			if slice == nil {
				// Skip this channel's data
				if err := reader.Skip(tileWidth * ch.Type.Size()); err != nil {
					return err
				}
				continue
			}

			for x := 0; x < tileWidth; x++ {
				switch ch.Type {
				case PixelTypeHalf:
					bits, err := reader.ReadUint16()
					if err != nil {
						return err
					}
					slice.SetHalf(startX+x, startY+y, half.FromBits(bits))

				case PixelTypeFloat:
					val, err := reader.ReadFloat32()
					if err != nil {
						return err
					}
					slice.SetFloat32(startX+x, startY+y, val)

				case PixelTypeUint:
					val, err := reader.ReadUint32()
					if err != nil {
						return err
					}
					slice.SetUint32(startX+x, startY+y, val)
				}
			}
		}
	}

	return nil
}

// TiledWriter writes tiled images to an EXR file.
type TiledWriter struct {
	writer      *Writer
	header      *Header
	frameBuffer *FrameBuffer
	dataWindow  Box2i
	channelList *ChannelList
	tileDesc    *TileDescription
	tilesX      int
	tilesY      int
}

// NewTiledHeader creates a header for a tiled image with default values.
func NewTiledHeader(width, height, tileWidth, tileHeight int) *Header {
	h := NewHeader()

	dataWindow := Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}}
	h.SetDataWindow(dataWindow)
	h.SetDisplayWindow(dataWindow)
	h.SetCompression(CompressionZIP)
	h.SetLineOrder(LineOrderIncreasing)
	h.SetPixelAspectRatio(1.0)
	h.SetScreenWindowCenter(V2f{0, 0})
	h.SetScreenWindowWidth(1.0)

	h.SetTileDescription(TileDescription{
		XSize: uint32(tileWidth),
		YSize: uint32(tileHeight),
		Mode:  LevelModeOne,
	})

	// Add default RGB channels
	cl := NewChannelList()
	cl.Add(NewChannel("R", PixelTypeHalf))
	cl.Add(NewChannel("G", PixelTypeHalf))
	cl.Add(NewChannel("B", PixelTypeHalf))
	h.SetChannels(cl)

	return h
}

// NewTiledWriter creates a writer for a tiled image.
func NewTiledWriter(w io.WriteSeeker, h *Header) (*TiledWriter, error) {
	if !h.IsTiled() {
		return nil, ErrNotTiled
	}

	writer, err := NewWriter(w, h)
	if err != nil {
		return nil, err
	}

	td := h.TileDescription()
	dw := h.DataWindow()

	return &TiledWriter{
		writer:      writer,
		header:      h,
		dataWindow:  dw,
		channelList: h.Channels(),
		tileDesc:    td,
		tilesX:      (int(dw.Width()) + int(td.XSize) - 1) / int(td.XSize),
		tilesY:      (int(dw.Height()) + int(td.YSize) - 1) / int(td.YSize),
	}, nil
}

// Header returns the header for this file.
func (w *TiledWriter) Header() *Header {
	return w.header
}

// NumTilesX returns the number of tiles in the X direction.
func (w *TiledWriter) NumTilesX() int {
	return w.tilesX
}

// NumTilesY returns the number of tiles in the Y direction.
func (w *TiledWriter) NumTilesY() int {
	return w.tilesY
}

// SetFrameBuffer sets the frame buffer to write pixels from.
func (w *TiledWriter) SetFrameBuffer(fb *FrameBuffer) {
	w.frameBuffer = fb
}

// NumXLevels returns the number of resolution levels in X direction.
func (w *TiledWriter) NumXLevels() int {
	return w.header.NumXLevels()
}

// NumYLevels returns the number of resolution levels in Y direction.
func (w *TiledWriter) NumYLevels() int {
	return w.header.NumYLevels()
}

// NumLevels returns the number of resolution levels (for mipmap mode).
func (w *TiledWriter) NumLevels() int {
	return w.header.NumXLevels()
}

// LevelWidth returns the width of the image at level lx.
func (w *TiledWriter) LevelWidth(lx int) int {
	return w.header.LevelWidth(lx)
}

// LevelHeight returns the height of the image at level ly.
func (w *TiledWriter) LevelHeight(ly int) int {
	return w.header.LevelHeight(ly)
}

// NumXTilesAtLevel returns the number of tiles in X at level lx.
func (w *TiledWriter) NumXTilesAtLevel(lx int) int {
	return w.header.NumXTiles(lx)
}

// NumYTilesAtLevel returns the number of tiles in Y at level ly.
func (w *TiledWriter) NumYTilesAtLevel(ly int) int {
	return w.header.NumYTiles(ly)
}

// LevelMode returns the level mode for this tiled image.
func (w *TiledWriter) LevelMode() LevelMode {
	return w.tileDesc.Mode
}

// WriteTile writes a single tile at level 0 from the frame buffer.
func (w *TiledWriter) WriteTile(tileX, tileY int) error {
	return w.WriteTileLevel(tileX, tileY, 0, 0)
}

// WriteTileLevel writes a single tile at the specified level from the frame buffer.
// For mipmap mode, levelX and levelY should be the same.
// For ripmap mode, levelX and levelY can be different.
// For single-level mode (LevelModeOne), both should be 0.
func (w *TiledWriter) WriteTileLevel(tileX, tileY, levelX, levelY int) error {
	if w.frameBuffer == nil {
		return ErrNoFrameBuffer
	}

	// Validate level
	if levelX < 0 || levelX >= w.NumXLevels() || levelY < 0 || levelY >= w.NumYLevels() {
		return ErrLevelOutOfRange
	}

	// Validate tile coordinates for this level
	numTilesX := w.header.NumXTiles(levelX)
	numTilesY := w.header.NumYTiles(levelY)
	if tileX < 0 || tileX >= numTilesX || tileY < 0 || tileY >= numTilesY {
		return ErrTileOutOfRange
	}

	// Get level dimensions
	levelWidth := w.header.LevelWidth(levelX)
	levelHeight := w.header.LevelHeight(levelY)

	// Calculate actual tile dimensions (may be smaller at edges)
	tileWidth := min(int(w.tileDesc.XSize), levelWidth-tileX*int(w.tileDesc.XSize))
	tileHeight := min(int(w.tileDesc.YSize), levelHeight-tileY*int(w.tileDesc.YSize))

	// Encode tile - use level-aware encoding
	rawData, err := w.encodeTileLevel(tileX, tileY, levelX, levelY, tileWidth, tileHeight)
	if err != nil {
		return err
	}

	// Compress
	compression := w.header.Compression()
	var data []byte

	switch compression {
	case CompressionNone:
		data = rawData
	case CompressionRLE:
		data = w.compressTileRLE(rawData)
	case CompressionZIPS, CompressionZIP:
		data, err = w.compressTileZIP(rawData)
		if err != nil {
			return err
		}
	case CompressionPIZ:
		data, err = w.compressTilePIZ(rawData, tileWidth, tileHeight)
		if err != nil {
			return err
		}
	case CompressionPXR24:
		data, err = w.compressTilePXR24(rawData, tileWidth, tileHeight)
		if err != nil {
			return err
		}
	case CompressionB44:
		data, err = w.compressTileB44(rawData, tileWidth, tileHeight, false)
		if err != nil {
			return err
		}
	case CompressionB44A:
		data, err = w.compressTileB44(rawData, tileWidth, tileHeight, true)
		if err != nil {
			return err
		}
	case CompressionDWAA, CompressionDWAB:
		data, err = w.compressTileDWA(rawData, tileX, tileY, tileWidth, tileHeight)
		if err != nil {
			return err
		}
	case CompressionHTJ2K256, CompressionHTJ2K32:
		data, err = w.compressTileHTJ2K(rawData, tileWidth, tileHeight, htj2kBlockWidth(compression))
		if err != nil {
			return err
		}
	default:
		return errors.New("exr: compression not yet implemented: " + compression.String())
	}

	// A chunk that did not shrink must be stored raw; see storeUncompressed.
	data = storeUncompressed(data, rawData, compression)

	// Write tile chunk with level coordinates
	return w.writer.WriteTileChunk(tileX, tileY, levelX, levelY, data)
}

// WriteTiles writes all tiles in a range at level 0.
func (w *TiledWriter) WriteTiles(tileX1, tileY1, tileX2, tileY2 int) error {
	return w.WriteTilesLevel(tileX1, tileY1, tileX2, tileY2, 0, 0)
}

// WriteTilesLevel writes all tiles in a range at the specified level.
func (w *TiledWriter) WriteTilesLevel(tileX1, tileY1, tileX2, tileY2, levelX, levelY int) error {
	if tileX1 > tileX2 || tileY1 > tileY2 {
		return ErrTileOutOfRange
	}

	for ty := tileY1; ty <= tileY2; ty++ {
		for tx := tileX1; tx <= tileX2; tx++ {
			if err := w.WriteTileLevel(tx, ty, levelX, levelY); err != nil {
				return err
			}
		}
	}

	return nil
}

// encodeTile encodes tile data from the frame buffer (level 0).
func (w *TiledWriter) encodeTile(tileX, tileY, tileWidth, tileHeight int) ([]byte, error) {
	return w.encodeTileLevel(tileX, tileY, 0, 0, tileWidth, tileHeight)
}

// encodeTileLevel encodes tile data at a specific level from the frame buffer.
func (w *TiledWriter) encodeTileLevel(tileX, tileY, levelX, levelY, tileWidth, tileHeight int) ([]byte, error) {
	// For multi-level images, we calculate coordinates based on the level
	// The frame buffer should be sized for the specific level being written
	// The same absolute addressing the reader uses: a frame buffer is indexed
	// in the data window's coordinates, not the level's.
	wdw := w.header.DataWindow()
	startX := int(wdw.Min.X) + tileX*int(w.tileDesc.XSize)
	startY := int(wdw.Min.Y) + tileY*int(w.tileDesc.YSize)

	// Calculate buffer size
	bytesPerPixel := 0
	for i := 0; i < w.channelList.Len(); i++ {
		ch := w.channelList.At(i)
		bytesPerPixel += ch.Type.Size()
	}
	bufSize := bytesPerPixel * tileWidth * tileHeight

	buf := xdr.NewBufferWriter(bufSize)

	// Channels should be sorted by name
	sortedChannels := w.channelList.SortedByName()

	for y := 0; y < tileHeight; y++ {
		for _, ch := range sortedChannels {
			slice := w.frameBuffer.Get(ch.Name)

			for x := 0; x < tileWidth; x++ {
				if slice == nil {
					// Write zeros for missing channel
					switch ch.Type {
					case PixelTypeHalf:
						buf.WriteUint16(0)
					case PixelTypeFloat:
						buf.WriteFloat32(0)
					case PixelTypeUint:
						buf.WriteUint32(0)
					}
					continue
				}

				switch ch.Type {
				case PixelTypeHalf:
					h := slice.GetHalf(startX+x, startY+y)
					buf.WriteUint16(h.Bits())
				case PixelTypeFloat:
					val := slice.GetFloat32(startX+x, startY+y)
					buf.WriteFloat32(val)
				case PixelTypeUint:
					val := slice.GetUint32(startX+x, startY+y)
					buf.WriteUint32(val)
				}
			}
		}
	}

	return buf.Bytes(), nil
}

// compressTileRLE compresses tile data using RLE.
// The pipeline is reorder bytes -> apply predictor -> RLE compress.
func (w *TiledWriter) compressTileRLE(data []byte) []byte {
	scratch := make([]byte, len(data))
	predictor.DeconstructBytes(scratch, data)
	return compression.RLECompress(scratch)
}

// compressTileZIP compresses tile data using ZIP.
// The pipeline is reorder bytes -> apply predictor -> zlib compress.
// Uses the compression level from the header for deterministic round-trip.
func (w *TiledWriter) compressTileZIP(data []byte) ([]byte, error) {
	scratch := make([]byte, len(data))
	predictor.DeconstructBytes(scratch, data)
	level := w.header.ZIPLevel()
	return compression.ZIPCompressLevel(scratch, level)
}

// compressTilePIZ compresses tile data using PIZ.
func (w *TiledWriter) compressTilePIZ(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	// Build channel info.
	sortedChannels := w.channelList.SortedByName()
	pizChannels := make([]compression.PIZChannel, len(sortedChannels))
	for i, ch := range sortedChannels {
		size := 1
		if ch.Type == PixelTypeFloat || ch.Type == PixelTypeUint {
			size = 2
		}
		pizChannels[i] = compression.PIZChannel{
			Size: size,
			NX:   tileWidth,
			NY:   tileHeight,
		}
	}

	// Rearrange from per-scanline to channel-contiguous.
	channelData := pizScanlineToChannelContiguous(data, sortedChannels, tileWidth, tileHeight)

	// Compress with channel-aware wavelet.
	return compression.PIZCompressBytesChannels(channelData, pizChannels)
}

// compressTilePXR24 compresses tile data using PXR24.
func (w *TiledWriter) compressTilePXR24(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	// Build channel info - channels are sorted by name in the file
	sortedChannels := w.channelList.SortedByName()

	channels := make([]compression.ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		var pxrType int
		switch ch.Type {
		case PixelTypeUint:
			pxrType = 0 // pxr24PixelTypeUint
		case PixelTypeHalf:
			pxrType = 1 // pxr24PixelTypeHalf
		case PixelTypeFloat:
			pxrType = 2 // pxr24PixelTypeFloat
		}
		channels[i] = compression.ChannelInfo{
			Type:   pxrType,
			Width:  tileWidth,
			Height: tileHeight,
		}
	}

	return compression.PXR24Compress(data, channels, tileWidth, tileHeight)
}

// decompressTileB44 decompresses B44/B44A-compressed tile data.
func (r *TiledReader) decompressTileB44(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	expectedSize := r.calculateTileSize(tileWidth, tileHeight)

	// Build channel info - channels are sorted by name in the file
	sortedChannels := r.channelList.SortedByName()

	channels := make([]compression.B44ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		var b44Type int
		switch ch.Type {
		case PixelTypeUint:
			b44Type = 0 // b44PixelTypeUint
		case PixelTypeHalf:
			b44Type = 1 // b44PixelTypeHalf
		case PixelTypeFloat:
			b44Type = 2 // b44PixelTypeFloat
		}
		channels[i] = compression.B44ChannelInfo{
			Type:   b44Type,
			Width:  tileWidth,
			Height: tileHeight,
		}
	}

	return compression.B44Decompress(data, channels, tileWidth, tileHeight, expectedSize)
}

// compressTileB44 compresses tile data using B44/B44A.
func (w *TiledWriter) compressTileB44(data []byte, tileWidth, tileHeight int, flatfields bool) ([]byte, error) {
	// Build channel info - channels are sorted by name in the file
	sortedChannels := w.channelList.SortedByName()

	channels := make([]compression.B44ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		var b44Type int
		switch ch.Type {
		case PixelTypeUint:
			b44Type = 0 // b44PixelTypeUint
		case PixelTypeHalf:
			b44Type = 1 // b44PixelTypeHalf
		case PixelTypeFloat:
			b44Type = 2 // b44PixelTypeFloat
		}
		channels[i] = compression.B44ChannelInfo{
			Type:   b44Type,
			Width:  tileWidth,
			Height: tileHeight,
		}
	}

	return compression.B44Compress(data, channels, tileWidth, tileHeight, flatfields)
}

// tileOrigin returns a tile's top-left corner in image coordinates. DWA
// classifies and lays out its data by absolute position, so a tile has to say
// where it sits rather than just how big it is.
func tileOrigin(dw Box2i, td *TileDescription, tileX, tileY int) (int, int) {
	return int(dw.Min.X) + tileX*int(td.XSize), int(dw.Min.Y) + tileY*int(td.YSize)
}

// decompressTileDWA decompresses tile data using DWAA or DWAB.
func (r *TiledReader) decompressTileDWA(data []byte, tileX, tileY, tileWidth, tileHeight int) ([]byte, error) {
	dst := make([]byte, r.calculateTileSize(tileWidth, tileHeight))
	minX, minY := tileOrigin(r.dataWindow, r.tileDesc, tileX, tileY)
	err := compression.DWADecompress(data, dwaChannels(r.channelList),
		minX, minX+tileWidth-1, minY, minY+tileHeight-1, dst)
	if err != nil {
		return nil, err
	}
	return dst, nil
}

// compressTileDWA compresses tile data using DWAA or DWAB. The two differ only
// in scanline chunk height, which does not apply to tiles, so the chunk format
// is identical.
func (w *TiledWriter) compressTileDWA(data []byte, tileX, tileY, tileWidth, tileHeight int) ([]byte, error) {
	// Get compression level from header (defaults to 45.0 if not set)
	level := w.header.DWACompressionLevel()

	minX, minY := tileOrigin(w.dataWindow, w.tileDesc, tileX, tileY)
	return compression.DWACompress(data, dwaChannels(w.channelList),
		minX, minX+tileWidth-1, minY, minY+tileHeight-1, level)
}

// Close finalizes the file.
// After Close is called, the TiledWriter should not be used.
func (w *TiledWriter) Close() error {
	err := w.writer.Close()
	// Clear references to help Windows release file handles
	w.writer = nil
	w.header = nil
	w.frameBuffer = nil
	w.channelList = nil
	w.tileDesc = nil

	// Force garbage collection to ensure immediate release of file handles on Windows
	runtime.GC()

	return err
}
