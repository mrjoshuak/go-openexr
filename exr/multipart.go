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
	currentY    int
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
func (m *MultiPartOutputFile) WritePixels(part int, numScanlines int) error {
	if part < 0 || part >= len(m.parts) {
		return ErrPartNotFound
	}
	p := m.parts[part]
	if p.frameBuffer == nil {
		return ErrInvalidSlice
	}

	h := p.header
	dw := h.DataWindow()
	width := int(dw.Width())
	comp := h.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	cl := h.Channels()
	if cl == nil {
		return ErrInvalidHeader
	}

	for i := 0; i < numScanlines; {
		chunkY := p.currentY
		linesInChunk := linesPerChunk
		remaining := numScanlines - i
		if linesInChunk > remaining {
			linesInChunk = remaining
		}

		if chunkY+linesInChunk-1 > int(dw.Max.Y) {
			linesInChunk = int(dw.Max.Y) - chunkY + 1
		}
		if linesInChunk <= 0 {
			break
		}

		// Build uncompressed chunk data
		uncompressed := buildScanlineData(p.frameBuffer, cl, width, chunkY, linesInChunk)

		// Compress
		compressed, err := compressChunkData(uncompressed, width, linesInChunk, cl, comp)
		if err != nil {
			return err
		}

		if err := m.writer.WriteChunkPart(part, int32(chunkY), compressed); err != nil {
			return err
		}

		p.currentY += linesInChunk
		i += linesInChunk
	}

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

	absStartY := int(dw.Min.Y) + startY

	// Build uncompressed tile data
	uncompressed := buildTileData(p.frameBuffer, cl, startX, absStartY, actualW, actualH)

	// Compress
	compressed, err := compressChunkData(uncompressed, actualW, actualH, cl, comp)
	if err != nil {
		return err
	}

	return m.writer.WriteTileChunkPart(part, tileX, tileY, levelX, levelY, compressed)
}

// Close completes writing the file.
func (m *MultiPartOutputFile) Close() error {
	return m.writer.Close()
}

// buildScanlineData builds uncompressed scanline data.
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
func compressChunkData(data []byte, width, height int, cl *ChannelList, comp Compression) ([]byte, error) {
	switch comp {
	case CompressionNone:
		return data, nil

	case CompressionRLE:
		encoded := make([]byte, len(data))
		copy(encoded, data)
		predictor.EncodeSIMD(encoded)
		return compression.RLECompress(encoded), nil

	case CompressionZIPS, CompressionZIP:
		encoded := make([]byte, len(data))
		copy(encoded, data)
		predictor.EncodeSIMD(encoded)
		var interleaved []byte
		if len(encoded) >= 32 {
			interleaved = compression.InterleaveFast(encoded)
		} else {
			interleaved = compression.Interleave(encoded)
		}
		return compression.ZIPCompress(interleaved)

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

	case CompressionDWAA:
		return compression.CompressDWAA(data, width, height, 45.0)

	case CompressionDWAB:
		return compression.CompressDWAB(data, width, height, 45.0)

	default:
		return data, nil
	}
}
