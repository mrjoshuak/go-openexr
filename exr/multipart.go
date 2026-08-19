package exr

import (
	"errors"
	"io"
	"math"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
)

// Multi-part file errors
var (
	ErrNotMultiPart    = errors.New("exr: file is not multi-part")
	ErrInvalidPartType = errors.New("exr: invalid part type")
	ErrPartNotFound    = errors.New("exr: part not found")

	// ErrConflictingAttributes reports that the parts of a multi-part file
	// disagree about an attribute the format requires every part to share.
	// Such a file is rejected by the reference implementation as a whole, so
	// it is refused here rather than written.
	ErrConflictingAttributes = errors.New("exr: parts disagree about a shared attribute")
)

// PartInfo describes a part in a multi-part file.
type PartInfo struct {
	Index      int
	Name       string
	Type       string
	DataWindow Box2i
	Channels   []string
}

// MultiPartInputFile provides access to multi-part EXR files.
type MultiPartInputFile struct {
	file *File
}

// NewMultiPartInputFile creates a reader for a multi-part file.
// Works with both single-part and multi-part files.
func NewMultiPartInputFile(f *File) *MultiPartInputFile {
	return &MultiPartInputFile{file: f}
}

// File returns the underlying File.
func (m *MultiPartInputFile) File() *File {
	return m.file
}

// NumParts returns the number of parts in the file.
func (m *MultiPartInputFile) NumParts() int {
	return m.file.NumParts()
}

// IsMultiPart returns true if the file is a multi-part file.
func (m *MultiPartInputFile) IsMultiPart() bool {
	return m.file.IsMultiPart()
}

// PartInfo returns information about a specific part.
func (m *MultiPartInputFile) PartInfo(part int) (*PartInfo, error) {
	h := m.file.Header(part)
	if h == nil {
		return nil, ErrPartNotFound
	}

	info := &PartInfo{
		Index:      part,
		DataWindow: h.DataWindow(),
	}

	// Get name (may not exist in single-part files)
	if attr := h.Get(AttrNameName); attr != nil {
		if name, ok := attr.Value.(string); ok {
			info.Name = name
		}
	}

	// Get type
	if attr := h.Get(AttrNameType); attr != nil {
		if typ, ok := attr.Value.(string); ok {
			info.Type = typ
		}
	} else {
		// Infer type from header attributes
		if h.IsTiled() {
			info.Type = PartTypeTiled
		} else {
			info.Type = PartTypeScanline
		}
	}

	// Get channel names
	if cl := h.Channels(); cl != nil {
		info.Channels = make([]string, cl.Len())
		for i := 0; i < cl.Len(); i++ {
			info.Channels[i] = cl.At(i).Name
		}
	}

	return info, nil
}

// ListParts returns information about all parts in the file.
func (m *MultiPartInputFile) ListParts() []*PartInfo {
	parts := make([]*PartInfo, m.NumParts())
	for i := 0; i < m.NumParts(); i++ {
		parts[i], _ = m.PartInfo(i)
	}
	return parts
}

// FindPartByName returns the index of a part by name, or -1 if not found.
func (m *MultiPartInputFile) FindPartByName(name string) int {
	for i := 0; i < m.NumParts(); i++ {
		h := m.file.Header(i)
		if h != nil {
			if attr := h.Get(AttrNameName); attr != nil {
				if n, ok := attr.Value.(string); ok && n == name {
					return i
				}
			}
		}
	}
	return -1
}

// Header returns the header for a specific part.
func (m *MultiPartInputFile) Header(part int) *Header {
	return m.file.Header(part)
}

// ScanlineReader returns a ScanlineReader for the specified part.
// Returns an error if the part is not a scanline part.
func (m *MultiPartInputFile) ScanlineReader(part int) (*ScanlineReader, error) {
	h := m.file.Header(part)
	if h == nil {
		return nil, ErrPartNotFound
	}
	if h.IsTiled() {
		return nil, ErrInvalidPartType
	}
	return NewScanlineReaderPart(m.file, part)
}

// TiledReader returns a TiledReader for the specified part.
// Returns an error if the part is not a tiled part.
func (m *MultiPartInputFile) TiledReader(part int) (*TiledReader, error) {
	h := m.file.Header(part)
	if h == nil {
		return nil, ErrPartNotFound
	}
	if !h.IsTiled() {
		return nil, ErrInvalidPartType
	}
	return NewTiledReaderPart(m.file, part)
}

// DeepScanlineReader returns a DeepScanlineReader for the specified part.
// Returns an error if the part is not a deep scanline part.
func (m *MultiPartInputFile) DeepScanlineReader(part int) (*DeepScanlineReader, error) {
	h := m.file.Header(part)
	if h == nil {
		return nil, ErrPartNotFound
	}
	partType := ""
	if attr := h.Get(AttrNameType); attr != nil {
		if t, ok := attr.Value.(string); ok {
			partType = t
		}
	}
	if partType != PartTypeDeepScanline {
		return nil, ErrInvalidPartType
	}
	if part != 0 {
		return nil, errors.New("exr: deep scanline reader only supports part 0")
	}
	return NewDeepScanlineReader(m.file)
}

// DeepTiledReader returns a DeepTiledReader for the specified part.
// Returns an error if the part is not a deep tiled part.
func (m *MultiPartInputFile) DeepTiledReader(part int) (*DeepTiledReader, error) {
	h := m.file.Header(part)
	if h == nil {
		return nil, ErrPartNotFound
	}
	partType := ""
	if attr := h.Get(AttrNameType); attr != nil {
		if t, ok := attr.Value.(string); ok {
			partType = t
		}
	}
	if partType != PartTypeDeepTiled {
		return nil, ErrInvalidPartType
	}
	return NewDeepTiledReaderPart(m.file, part)
}

// MultiPartOutputFile provides a high-level interface for writing multi-part EXR files.
type MultiPartOutputFile struct {
	writer *Writer
	parts  []*partWriter
}

// partWriter tracks the state of writing to a single part.
type partWriter struct {
	index       int
	header      *Header
	frameBuffer *FrameBuffer
	// currentY is the first scanline of the next chunk to be emitted, in
	// image coordinates. It only ever moves by whole chunks, so it stays on
	// the chunk grid the format anchors at the data window's first scanline.
	currentY int
	// pending counts scanlines the caller has declared written that do not
	// yet complete a chunk.
	pending int
}

// NewMultiPartOutputFile creates a new multi-part output file.
func NewMultiPartOutputFile(w io.WriteSeeker, headers []*Header) (*MultiPartOutputFile, error) {
	writer, err := NewMultiPartWriter(w, headers)
	if err != nil {
		return nil, err
	}

	parts := make([]*partWriter, len(headers))
	for i, h := range headers {
		dw := h.DataWindow()
		parts[i] = &partWriter{
			index:    i,
			header:   h,
			currentY: int(dw.Min.Y),
		}
	}

	return &MultiPartOutputFile{
		writer: writer,
		parts:  parts,
	}, nil
}

// NumParts returns the number of parts.
func (m *MultiPartOutputFile) NumParts() int {
	return len(m.parts)
}

// Header returns the header for a specific part.
func (m *MultiPartOutputFile) Header(part int) *Header {
	if part < 0 || part >= len(m.parts) {
		return nil
	}
	return m.parts[part].header
}

// SetFrameBuffer sets the frame buffer for a specific part.
func (m *MultiPartOutputFile) SetFrameBuffer(part int, fb *FrameBuffer) error {
	if part < 0 || part >= len(m.parts) {
		return ErrPartNotFound
	}
	m.parts[part].frameBuffer = fb
	return nil
}

// WritePixels writes scanlines for a specific part.
//
// numScanlines is how many further scanlines of the part the caller has
// filled in, so a part can be written in one call, in groups, or a line at a
// time. What reaches the file is always a whole chunk. The format anchors a
// scanline part's chunk grid at the first scanline of its data window and a
// reader computes a chunk's position from that grid, so a chunk that begins
// anywhere else makes the part unreadable; scanlines that do not yet complete
// a chunk are held until a later call completes it, or until the last
// scanline of the data window, whose chunk may be short. Writing a part one
// line at a time used to emit one chunk per line and then fail with "too many
// chunks written" against any codec that packs several lines per chunk.
func (m *MultiPartOutputFile) WritePixels(part int, numScanlines int) error {
	if part < 0 || part >= len(m.parts) {
		return ErrPartNotFound
	}
	p := m.parts[part]
	if p.frameBuffer == nil {
		return ErrInvalidSlice
	}
	if numScanlines < 0 {
		return errors.New("exr: negative scanline count")
	}

	h := p.header
	dw := h.DataWindow()
	width := int(dw.Width())
	minY, maxY := int(dw.Min.Y), int(dw.Max.Y)
	comp := h.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	cl := h.Channels()
	if cl == nil {
		return ErrInvalidHeader
	}

	target := p.currentY + p.pending + numScanlines
	if target > maxY+1 {
		target = maxY + 1
	}

	for p.currentY < target {
		chunkY := p.currentY
		linesInChunk := linesPerChunk
		if chunkY+linesInChunk-1 > maxY {
			linesInChunk = maxY - chunkY + 1
		}
		if linesInChunk <= 0 {
			break
		}
		if chunkY+linesInChunk > target {
			// The caller has not filled this chunk yet.
			break
		}

		// Build uncompressed chunk data. The frame buffer is addressed
		// relative to the data window, as everywhere else in this package:
		// the pixel at (dataWindow.Min.X, dataWindow.Min.Y) is buffer
		// position (0, 0).
		uncompressed := buildScanlineData(p.frameBuffer, cl, width, chunkY-minY, linesInChunk)

		// Compress. The codecs that care about position — DWA — want the
		// chunk's place in the image, not in the buffer.
		compressed, err := compressChunkData(uncompressed, int(dw.Min.X), chunkY, width, linesInChunk, cl, comp)
		if err != nil {
			return err
		}

		// A chunk that did not shrink must be stored raw; see storeUncompressed.
		compressed = storeUncompressed(compressed, uncompressed, comp)

		if err := m.writer.WriteChunkPart(part, int32(chunkY), compressed); err != nil {
			return err
		}

		p.currentY += linesInChunk
	}

	p.pending = target - p.currentY
	return nil
}

// WriteTile writes a tile for a specific tiled part.
func (m *MultiPartOutputFile) WriteTile(part, tileX, tileY int) error {
	return m.WriteTileLevel(part, tileX, tileY, 0, 0)
}

// WriteTileLevel writes a tile at a specific level for a tiled part.
func (m *MultiPartOutputFile) WriteTileLevel(part, tileX, tileY, levelX, levelY int) error {
	if part < 0 || part >= len(m.parts) {
		return ErrPartNotFound
	}
	p := m.parts[part]
	if p.frameBuffer == nil {
		return ErrInvalidSlice
	}

	h := p.header
	if !h.IsTiled() {
		return ErrInvalidPartType
	}

	td := h.TileDescription()
	if td == nil {
		return ErrInvalidHeader
	}

	comp := h.Compression()
	cl := h.Channels()
	if cl == nil {
		return ErrInvalidHeader
	}

	// Build tile data
	dw := h.DataWindow()
	tileW := int(td.XSize)
	tileH := int(td.YSize)

	levelW := h.LevelWidth(levelX)
	levelH := h.LevelHeight(levelY)

	startX := tileX * tileW
	startY := tileY * tileH
	endX := startX + tileW
	endY := startY + tileH

	if endX > levelW {
		endX = levelW
	}
	if endY > levelH {
		endY = levelH
	}

	actualW := endX - startX
	actualH := endY - startY

	// Build uncompressed tile data. The frame buffer holds the level being
	// written and is addressed from its own origin, as in TiledWriter: the
	// tile at (0, 0) reads from buffer position (0, 0) whatever the data
	// window's origin is. Only the codecs are told where the tile sits in the
	// image.
	uncompressed := buildTileData(p.frameBuffer, cl, startX, startY, actualW, actualH)

	// Compress
	compressed, err := compressChunkData(uncompressed, int(dw.Min.X)+startX, int(dw.Min.Y)+startY, actualW, actualH, cl, comp)
	if err != nil {
		return err
	}

	// A chunk that did not shrink must be stored raw; see storeUncompressed.
	compressed = storeUncompressed(compressed, uncompressed, comp)

	return m.writer.WriteTileChunkPart(part, tileX, tileY, levelX, levelY, compressed)
}

// Close completes writing the file.
func (m *MultiPartOutputFile) Close() error {
	return m.writer.Close()
}

// buildScanlineData builds uncompressed scanline data. startY is a frame
// buffer row, counted from the first row of the data window, not an image
// coordinate: the caller subtracts the data window's origin. Reading image
// coordinates here shifted every part whose data window did not start at y=0
// and ran off the end of the caller's buffer.
func buildScanlineData(fb *FrameBuffer, cl *ChannelList, width, startY, numLines int) []byte {
	// Calculate size
	bytesPerPixel := 0
	for i := 0; i < cl.Len(); i++ {
		bytesPerPixel += cl.At(i).Type.Size()
	}
	size := width * numLines * bytesPerPixel
	data := make([]byte, size)

	// Sort channels by name
	sortedChannels := cl.SortedByName()

	offset := 0
	for y := startY; y < startY+numLines; y++ {
		for _, ch := range sortedChannels {
			slice := fb.Get(ch.Name)
			for x := 0; x < width; x++ {
				if slice == nil {
					switch ch.Type {
					case PixelTypeHalf:
						offset += 2
					case PixelTypeFloat, PixelTypeUint:
						offset += 4
					}
					continue
				}

				switch ch.Type {
				case PixelTypeHalf:
					v := slice.GetHalf(x, y)
					data[offset] = byte(v.Bits())
					data[offset+1] = byte(v.Bits() >> 8)
					offset += 2
				case PixelTypeFloat:
					v := slice.GetFloat32(x, y)
					bits := math.Float32bits(v)
					data[offset] = byte(bits)
					data[offset+1] = byte(bits >> 8)
					data[offset+2] = byte(bits >> 16)
					data[offset+3] = byte(bits >> 24)
					offset += 4
				case PixelTypeUint:
					v := slice.GetUint32(x, y)
					data[offset] = byte(v)
					data[offset+1] = byte(v >> 8)
					data[offset+2] = byte(v >> 16)
					data[offset+3] = byte(v >> 24)
					offset += 4
				}
			}
		}
	}

	return data
}

// buildTileData builds uncompressed tile data.
func buildTileData(fb *FrameBuffer, cl *ChannelList, startX, startY, width, height int) []byte {
	bytesPerPixel := 0
	for i := 0; i < cl.Len(); i++ {
		bytesPerPixel += cl.At(i).Type.Size()
	}
	size := width * height * bytesPerPixel
	data := make([]byte, size)

	sortedChannels := cl.SortedByName()

	offset := 0
	for y := 0; y < height; y++ {
		for _, ch := range sortedChannels {
			slice := fb.Get(ch.Name)
			for x := 0; x < width; x++ {
				if slice == nil {
					switch ch.Type {
					case PixelTypeHalf:
						offset += 2
					case PixelTypeFloat, PixelTypeUint:
						offset += 4
					}
					continue
				}

				switch ch.Type {
				case PixelTypeHalf:
					v := slice.GetHalf(startX+x, startY+y)
					data[offset] = byte(v.Bits())
					data[offset+1] = byte(v.Bits() >> 8)
					offset += 2
				case PixelTypeFloat:
					v := slice.GetFloat32(startX+x, startY+y)
					bits := math.Float32bits(v)
					data[offset] = byte(bits)
					data[offset+1] = byte(bits >> 8)
					data[offset+2] = byte(bits >> 16)
					data[offset+3] = byte(bits >> 24)
					offset += 4
				case PixelTypeUint:
					v := slice.GetUint32(startX+x, startY+y)
					data[offset] = byte(v)
					data[offset+1] = byte(v >> 8)
					data[offset+2] = byte(v >> 16)
					data[offset+3] = byte(v >> 24)
					offset += 4
				}
			}
		}
	}

	return data
}

// compressChunkData compresses chunk data using the specified compression.
// minX and minY are the chunk's top-left corner in image coordinates, which
// DWA needs; the other codecs work from the chunk's size alone.
func compressChunkData(data []byte, minX, minY, width, height int, cl *ChannelList, comp Compression) ([]byte, error) {
	switch comp {
	case CompressionNone:
		return data, nil

	case CompressionRLE:
		// Reorder bytes, then predict over the reordered stream.
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		return compression.RLECompress(scratch), nil

	case CompressionZIPS, CompressionZIP:
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		return compression.ZIPCompress(scratch)

	case CompressionPIZ:
		sortedChs := cl.SortedByName()
		pizChs := make([]compression.PIZChannel, len(sortedChs))
		for i, ch := range sortedChs {
			size := 1
			if ch.Type == PixelTypeFloat || ch.Type == PixelTypeUint {
				size = 2
			}
			pizChs[i] = compression.PIZChannel{Size: size, NX: width, NY: height}
		}
		channelData := pizScanlineToChannelContiguous(data, sortedChs, width, height)
		return compression.PIZCompressBytesChannels(channelData, pizChs)

	case CompressionPXR24:
		sortedChannels := cl.SortedByName()

		channels := make([]compression.ChannelInfo, len(sortedChannels))
		for i, ch := range sortedChannels {
			chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
			var pxrType int
			switch ch.Type {
			case PixelTypeUint:
				pxrType = 0
			case PixelTypeHalf:
				pxrType = 1
			case PixelTypeFloat:
				pxrType = 2
			}
			channels[i] = compression.ChannelInfo{
				Type:   pxrType,
				Width:  chWidth,
				Height: height,
			}
		}
		return compression.PXR24Compress(data, channels, width, height)

	case CompressionB44, CompressionB44A:
		sortedChannels := cl.SortedByName()
		if err := checkNoYSubsampling(sortedChannels); err != nil {
			return nil, err
		}

		channels := make([]compression.B44ChannelInfo, len(sortedChannels))
		for i, ch := range sortedChannels {
			chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
			var b44Type int
			switch ch.Type {
			case PixelTypeUint:
				b44Type = 0
			case PixelTypeHalf:
				b44Type = 1
			case PixelTypeFloat:
				b44Type = 2
			}
			channels[i] = compression.B44ChannelInfo{
				Type:   b44Type,
				Width:  chWidth,
				Height: height,
			}
		}
		return compression.B44Compress(data, channels, width, height, comp == CompressionB44A)

	case CompressionDWAA, CompressionDWAB:
		return compression.DWACompress(data, dwaChannels(cl),
			minX, minX+width-1, minY, minY+height-1, DefaultDWACompressionLevel)

	case CompressionHTJ2K256, CompressionHTJ2K32:
		// Both HTJ2K codecs use the same 128x32 code-blocks; they differ only
		// in how many scanlines a chunk holds, which the caller has already
		// decided. Falling through to the default here stored the samples
		// unchanged. That still reads back — a chunk no smaller than its
		// unpacked size is raw by definition, whatever the header says — so
		// the only symptom was a part that advertised HTJ2K and had not been
		// compressed at all. TestMultiPartCompressionIsApplied measures it.
		sortedChannels := cl.SortedByName()
		channels := make([]compression.HTJ2KChannelInfo, len(sortedChannels))
		for i, ch := range sortedChannels {
			var htType int
			switch ch.Type {
			case PixelTypeUint:
				htType = compression.HTJ2KPixelTypeUint
			case PixelTypeHalf:
				htType = compression.HTJ2KPixelTypeHalf
			case PixelTypeFloat:
				htType = compression.HTJ2KPixelTypeFloat
			}
			channels[i] = compression.HTJ2KChannelInfo{
				Type:      htType,
				Width:     (width + int(ch.XSampling) - 1) / int(ch.XSampling),
				Height:    (height + int(ch.YSampling) - 1) / int(ch.YSampling),
				XSampling: int(ch.XSampling),
				YSampling: int(ch.YSampling),
				Name:      ch.Name,
			}
		}
		return compression.HTJ2KCompress(data, height, channels, 128)

	default:
		// Never return the samples unchanged for a compression this function
		// does not implement: the chunk would say one thing and hold another.
		return nil, errors.New("exr: compression not yet implemented: " + comp.String())
	}
}
