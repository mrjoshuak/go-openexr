package exr

import (
	"errors"
	"io"
	"runtime"
	"sync"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
)

// Scanline I/O errors
var (
	ErrNoFrameBuffer      = errors.New("exr: no frame buffer set")
	ErrScanlineOutOfRange = errors.New("exr: scanline out of range")
)

// zipDecompressBufPool provides reusable buffers for ZIP decompression.
// This is needed because decompressZIP is called from multiple goroutines
// in parallel read mode, so we can't use a shared buffer on ScanlineReader.
type zipDecompressBuf struct {
	data []byte
}

var zipDecompressBufPool = sync.Pool{
	New: func() any {
		return &zipDecompressBuf{}
	},
}

// channelInfo caches per-channel metadata to avoid map lookups in inner loops.
type channelInfo struct {
	ch              Channel
	slice           *Slice
	pixelsInChannel int
	bytesInChannel  int
}

// ScanlineReader reads scanline images from an EXR file.
type ScanlineReader struct {
	file        *File
	part        int
	header      *Header
	frameBuffer *FrameBuffer
	dataWindow  Box2i
	channelList *ChannelList

	// Cached data for performance (computed once, reused per chunk)
	sortedChannels []Channel
	cachedChannels []channelInfo // Cached channel info with slice pointers
	halfBuf        []uint16      // Reusable buffer for half conversion
	chunkHeaderBuf []byte        // Reusable buffer for chunk headers (8 or 12 bytes)
	chunkDataBuf   []byte        // Reusable buffer for chunk data
	// Note: ZIP decompression uses zipDecompressBufPool instead of a shared buffer
	// because decompressZIP is called from multiple goroutines in parallel mode

	// flevelOnce ensures FLEVEL detection happens only once (thread-safe)
	flevelOnce sync.Once
}

// NewScanlineReader creates a reader for a single-part scanline file.
func NewScanlineReader(f *File) (*ScanlineReader, error) {
	return NewScanlineReaderPart(f, 0)
}

// NewScanlineReaderPart creates a reader for a specific part of a file.
func NewScanlineReaderPart(f *File, part int) (*ScanlineReader, error) {
	if f == nil {
		return nil, ErrInvalidFile
	}
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}

	if h.IsTiled() {
		return nil, errors.New("exr: cannot use ScanlineReader for tiled images")
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

	// Pre-sort channels by name (file order) once
	sortedChannels := cl.SortedByName()

	// Pre-allocate reusable buffers for performance
	halfBuf := make([]uint16, width)
	chunkHeaderSize := 8
	if f.IsMultiPart() {
		chunkHeaderSize = 12
	}
	chunkHeaderBuf := make([]byte, chunkHeaderSize)

	// Calculate max chunk size for buffer pre-allocation
	compression := h.Compression()
	linesPerChunk := compression.ScanlinesPerChunk()
	bytesPerLine := 0
	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)
		pixelsInChannel := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		bytesPerLine += pixelsInChannel * ch.Type.Size()
	}
	maxChunkSize := bytesPerLine * linesPerChunk

	return &ScanlineReader{
		file:           f,
		part:           part,
		header:         h,
		dataWindow:     dw,
		channelList:    cl,
		sortedChannels: sortedChannels,
		halfBuf:        halfBuf,
		chunkHeaderBuf: chunkHeaderBuf,
		chunkDataBuf:   make([]byte, 0, maxChunkSize),
	}, nil
}

// Header returns the header for this part.
func (r *ScanlineReader) Header() *Header {
	return r.header
}

// DataWindow returns the data window for this part.
func (r *ScanlineReader) DataWindow() Box2i {
	return r.dataWindow
}

// SetFrameBuffer sets the frame buffer to read pixels into.
func (r *ScanlineReader) SetFrameBuffer(fb *FrameBuffer) {
	r.frameBuffer = fb

	// Pre-compute channel info with slice pointers to avoid map lookups in inner loop
	width := int(r.dataWindow.Width())
	r.cachedChannels = make([]channelInfo, 0, len(r.sortedChannels))
	for _, ch := range r.sortedChannels {
		pixelsInChannel := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		bytesInChannel := pixelsInChannel * ch.Type.Size()
		r.cachedChannels = append(r.cachedChannels, channelInfo{
			ch:              ch,
			slice:           fb.Get(ch.Name),
			pixelsInChannel: pixelsInChannel,
			bytesInChannel:  bytesInChannel,
		})
	}
}

// readChunkReuse reads a chunk using cached buffers to avoid allocation.
// Returns the y coordinate and a slice of the cached buffer containing the data.
func (r *ScanlineReader) readChunkReuse(chunkIndex int) (int32, []byte, error) {
	offsets := r.file.OffsetsRef(r.part)
	if chunkIndex < 0 || chunkIndex >= len(offsets) {
		return 0, nil, errors.New("exr: invalid chunk index")
	}

	offset := offsets[chunkIndex]

	// Multipart chunks have a 4-byte part number prefix before the header
	headerSize := int64(8)
	headerStart := 0
	if r.file.IsMultiPart() {
		headerSize = 12
		headerStart = 4 // skip part number
	}

	// Fast path: zero-copy with mmap
	if r.file.sliceReader != nil {
		// Get chunk header directly from mmap
		header := r.file.sliceReader.Slice(offset, headerSize)
		if header == nil {
			return 0, nil, errors.New("exr: failed to read chunk header")
		}

		y := int32(header[headerStart]) | int32(header[headerStart+1])<<8 |
			int32(header[headerStart+2])<<16 | int32(header[headerStart+3])<<24
		packedSize := int64(header[headerStart+4]) | int64(header[headerStart+5])<<8 |
			int64(header[headerStart+6])<<16 | int64(header[headerStart+7])<<24

		// Get chunk data directly from mmap (zero-copy!)
		data := r.file.sliceReader.Slice(offset+headerSize, packedSize)
		if data == nil {
			return 0, nil, errors.New("exr: failed to read chunk data")
		}

		return y, data, nil
	}

	// Read chunk header using cached buffer
	if _, err := r.file.reader.ReadAt(r.chunkHeaderBuf, offset); err != nil {
		return 0, nil, err
	}

	y := int32(r.chunkHeaderBuf[headerStart]) | int32(r.chunkHeaderBuf[headerStart+1])<<8 |
		int32(r.chunkHeaderBuf[headerStart+2])<<16 | int32(r.chunkHeaderBuf[headerStart+3])<<24
	packedSize := int(r.chunkHeaderBuf[headerStart+4]) | int(r.chunkHeaderBuf[headerStart+5])<<8 |
		int(r.chunkHeaderBuf[headerStart+6])<<16 | int(r.chunkHeaderBuf[headerStart+7])<<24

	// Ensure chunkDataBuf has enough capacity
	if cap(r.chunkDataBuf) < packedSize {
		r.chunkDataBuf = make([]byte, packedSize)
	} else {
		r.chunkDataBuf = r.chunkDataBuf[:packedSize]
	}

	// Read chunk data into cached buffer
	if _, err := r.file.reader.ReadAt(r.chunkDataBuf, offset+headerSize); err != nil {
		return 0, nil, err
	}

	return y, r.chunkDataBuf, nil
}

// chunkInfo holds pre-read chunk data for parallel processing.
type chunkInfo struct {
	index    int
	chunkY   int32
	data     []byte // compressed data (copy, safe for parallel use)
	numLines int
}

// ReadPixels reads scanlines from y1 to y2 (inclusive) into the frame buffer.
func (r *ScanlineReader) ReadPixels(y1, y2 int) error {
	if r.frameBuffer == nil {
		return ErrNoFrameBuffer
	}

	minY := int(r.dataWindow.Min.Y)
	maxY := int(r.dataWindow.Max.Y)

	if y1 < minY || y2 > maxY || y1 > y2 {
		return ErrScanlineOutOfRange
	}

	comp := r.header.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	// Calculate which chunks we need
	firstChunk := (y1 - minY) / linesPerChunk
	lastChunk := (y2 - minY) / linesPerChunk
	numChunks := lastChunk - firstChunk + 1

	// Check if we should use parallel processing
	// Skip parallel for CompressionNone - the work is trivial and parallel overhead dominates
	config := GetParallelConfig()
	numWorkers := effectiveWorkers(config)
	useParallel := numWorkers > 1 && numChunks >= config.GrainSize && comp != CompressionNone

	if !useParallel {
		// Sequential processing (original path)
		return r.readPixelsSequential(y1, y2)
	}

	// Parallel processing path
	return r.readPixelsParallel(firstChunk, lastChunk, minY, maxY, comp, linesPerChunk)
}

// readPixelsSequential is the original sequential implementation.
func (r *ScanlineReader) readPixelsSequential(y1, y2 int) error {
	minY := int(r.dataWindow.Min.Y)
	maxY := int(r.dataWindow.Max.Y)

	comp := r.header.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	firstChunk := (y1 - minY) / linesPerChunk
	lastChunk := (y2 - minY) / linesPerChunk

	for chunkIdx := firstChunk; chunkIdx <= lastChunk; chunkIdx++ {
		chunkY, data, err := r.readChunkReuse(chunkIdx)
		if err != nil {
			return err
		}

		// Calculate actual number of lines in this chunk (may be fewer for last chunk)
		chunkStartY := minY + chunkIdx*linesPerChunk
		chunkEndY := chunkStartY + linesPerChunk - 1
		if chunkEndY > maxY {
			chunkEndY = maxY
		}
		numLinesInChunk := chunkEndY - chunkStartY + 1

		// Decompress the chunk data
		var decompressedData []byte
		switch comp {
		case CompressionNone:
			decompressedData = data
		case CompressionRLE:
			decompressedData, err = r.decompressRLE(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionZIPS, CompressionZIP:
			decompressedData, err = r.decompressZIP(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionPIZ:
			decompressedData, err = r.decompressPIZ(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionPXR24:
			decompressedData, err = r.decompressPXR24(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionB44, CompressionB44A:
			decompressedData, err = r.decompressB44(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionDWAA, CompressionDWAB:
			decompressedData, err = r.decompressDWA(data, numLinesInChunk)
			if err != nil {
				return err
			}
		case CompressionHTJ2K256, CompressionHTJ2K32:
			decompressedData, err = r.decompressHTJ2K(data, numLinesInChunk)
			if err != nil {
				return err
			}
		default:
			return errors.New("exr: compression not yet implemented: " + comp.String())
		}

		err = r.decodeUncompressedChunk(int(chunkY), decompressedData)

		if err != nil {
			return err
		}
	}

	return nil
}

// readPixelsParallel processes chunks in parallel.
func (r *ScanlineReader) readPixelsParallel(firstChunk, lastChunk, minY, maxY int, comp Compression, linesPerChunk int) error {
	numChunks := lastChunk - firstChunk + 1

	// Phase 1: Read all chunk data sequentially (I/O bound)
	chunks := make([]chunkInfo, numChunks)
	for i := 0; i < numChunks; i++ {
		chunkIdx := firstChunk + i
		chunkY, data, err := r.readChunkReuse(chunkIdx)
		if err != nil {
			return err
		}

		// Calculate number of lines in this chunk
		chunkStartY := minY + chunkIdx*linesPerChunk
		chunkEndY := chunkStartY + linesPerChunk - 1
		if chunkEndY > maxY {
			chunkEndY = maxY
		}

		// Make a copy of the data for parallel processing
		dataCopy := make([]byte, len(data))
		copy(dataCopy, data)

		chunks[i] = chunkInfo{
			index:    chunkIdx,
			chunkY:   chunkY,
			data:     dataCopy,
			numLines: chunkEndY - chunkStartY + 1,
		}
	}

	// Phase 2: Decompress and decode chunks in parallel (CPU bound)
	var mu sync.Mutex
	var firstErr error

	err := ParallelForWithError(numChunks, func(i int) error {
		chunk := &chunks[i]

		// Decompress the chunk data
		decompressedData, err := r.decompressChunk(chunk.data, chunk.numLines, comp)
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return err
		}

		// Decode into framebuffer - each chunk writes to different scanlines, so no conflict
		if err := r.decodeUncompressedChunkParallel(int(chunk.chunkY), decompressedData); err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}
	return firstErr
}

// decompressChunk decompresses a chunk based on compression type.
// This is a thread-safe version that doesn't use shared reader buffers.
func (r *ScanlineReader) decompressChunk(data []byte, numLines int, comp Compression) ([]byte, error) {
	switch comp {
	case CompressionNone:
		// Make a copy since caller might modify
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	case CompressionRLE:
		return r.decompressRLE(data, numLines)
	case CompressionZIPS, CompressionZIP:
		return r.decompressZIP(data, numLines)
	case CompressionPIZ:
		return r.decompressPIZ(data, numLines)
	case CompressionPXR24:
		return r.decompressPXR24(data, numLines)
	case CompressionB44, CompressionB44A:
		return r.decompressB44(data, numLines)
	case CompressionDWAA, CompressionDWAB:
		return r.decompressDWA(data, numLines)
	case CompressionHTJ2K256, CompressionHTJ2K32:
		return r.decompressHTJ2K(data, numLines)
	default:
		return nil, errors.New("exr: compression not yet implemented: " + comp.String())
	}
}

// decodeUncompressedChunkParallel is a thread-safe version of decodeUncompressedChunk.
// It uses pre-computed channel info from SetFrameBuffer for performance.
func (r *ScanlineReader) decodeUncompressedChunkParallel(chunkY int, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	minY := int(r.dataWindow.Min.Y)
	maxY := int(r.dataWindow.Max.Y)
	minX := int(r.dataWindow.Min.X)

	comp := r.header.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()
	numLines := linesPerChunk
	if chunkY+numLines-1 > maxY {
		numLines = maxY - chunkY + 1
	}

	// Use pre-computed channel info (cached at SetFrameBuffer time)
	channelInfos := r.cachedChannels
	pos := 0

	for lineIdx := 0; lineIdx < numLines; lineIdx++ {
		y := chunkY + lineIdx
		if y < minY || y > maxY {
			continue
		}

		for i := range channelInfos {
			ci := &channelInfos[i]
			if ci.slice == nil {
				pos += ci.bytesInChannel
				continue
			}

			if pos+ci.bytesInChannel > len(data) {
				return errors.New("exr: truncated chunk data")
			}

			switch ci.ch.Type {
			case PixelTypeHalf:
				ci.slice.WriteRowHalfBytes(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			case PixelTypeFloat:
				ci.slice.WriteRowFloat(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			case PixelTypeUint:
				ci.slice.WriteRowUint(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			}
			pos += ci.bytesInChannel
		}
	}

	return nil
}

// decodeUncompressedChunk decodes an uncompressed chunk containing one or more scanlines.
// The chunkY parameter is the first scanline Y coordinate in the chunk.
// The data contains numLines scanlines.
func (r *ScanlineReader) decodeUncompressedChunk(chunkY int, data []byte) error {
	if len(data) == 0 {
		return nil // Empty chunk, nothing to decode
	}

	minY := int(r.dataWindow.Min.Y)
	maxY := int(r.dataWindow.Max.Y)
	minX := int(r.dataWindow.Min.X)

	// Calculate how many lines are in this chunk
	compression := r.header.Compression()
	linesPerChunk := compression.ScanlinesPerChunk()
	numLines := linesPerChunk
	if chunkY+numLines-1 > maxY {
		numLines = maxY - chunkY + 1
	}

	// Use pre-computed channel info (cached at SetFrameBuffer time)
	channelInfos := r.cachedChannels
	pos := 0

	// Process all scanlines in this chunk
	for lineIdx := 0; lineIdx < numLines; lineIdx++ {
		y := chunkY + lineIdx
		if y < minY || y > maxY {
			continue
		}

		for i := range channelInfos {
			ci := &channelInfos[i]
			if ci.slice == nil {
				pos += ci.bytesInChannel
				continue
			}

			if pos+ci.bytesInChannel > len(data) {
				return errors.New("exr: truncated chunk data")
			}

			switch ci.ch.Type {
			case PixelTypeHalf:
				ci.slice.WriteRowHalfBytes(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			case PixelTypeFloat:
				ci.slice.WriteRowFloat(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			case PixelTypeUint:
				ci.slice.WriteRowUint(y, data[pos:pos+ci.bytesInChannel], minX, ci.pixelsInChannel)
			}
			pos += ci.bytesInChannel
		}
	}

	return nil
}

// calculateChunkSize calculates the uncompressed size of a chunk.
func (r *ScanlineReader) calculateChunkSize(numLines int) int {
	width := int(r.dataWindow.Width())
	bytesPerLine := 0
	for i := 0; i < r.channelList.Len(); i++ {
		ch := r.channelList.At(i)
		pixelsInChannel := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		bytesPerLine += pixelsInChannel * ch.Type.Size()
	}
	return bytesPerLine * numLines
}

// decompressRLE decompresses RLE-compressed chunk data.
// The OpenEXR RLE pipeline is: RLE decompress -> reverse predictor
// (interleaving is NOT used for RLE, only for ZIP/PIZ)
func (r *ScanlineReader) decompressRLE(data []byte, numLines int) ([]byte, error) {
	expectedSize := r.calculateChunkSize(numLines)

	// RLE decompress
	decompressed, err := compression.RLEDecompress(data, expectedSize)
	if err != nil {
		return nil, err
	}

	// Reverse predictor (in place)
	predictor.DecodeSIMD(decompressed)

	return decompressed, nil
}

// decompressZIP decompresses ZIP/ZIPS-compressed chunk data.
// The OpenEXR ZIP pipeline is: zlib decompress -> deinterleave -> reverse predictor
// This function is thread-safe and can be called from multiple goroutines.
// It also detects and records the FLEVEL from the compressed data for deterministic round-trip.
func (r *ScanlineReader) decompressZIP(data []byte, numLines int) ([]byte, error) {
	expectedSize := r.calculateChunkSize(numLines)

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

// decompressPIZ decompresses PIZ-compressed chunk data.
// PIZ uses wavelet transform + Huffman coding on 16-bit data.
// For 32-bit types (float, uint), each value becomes two 16-bit samples.
func (r *ScanlineReader) decompressPIZ(data []byte, numLines int) ([]byte, error) {
	width := int(r.dataWindow.Width())

	// Count 16-bit samples per pixel (not channels).
	// Half = 1 sample, Float/Uint = 2 samples per pixel.
	samplesPerPixel := 0
	for i := 0; i < r.channelList.Len(); i++ {
		ch := r.channelList.At(i)
		switch ch.Type {
		case PixelTypeHalf:
			samplesPerPixel += 1
		case PixelTypeFloat, PixelTypeUint:
			samplesPerPixel += 2
		}
	}

	// PIZ works on 16-bit data, decompress directly to bytes
	return compression.PIZDecompressBytes(data, width, numLines, samplesPerPixel)
}

// decompressPXR24 decompresses PXR24-compressed chunk data.
// PXR24 uses 24-bit float representation with zlib compression.
func (r *ScanlineReader) decompressPXR24(data []byte, numLines int) ([]byte, error) {
	width := int(r.dataWindow.Width())
	expectedSize := r.calculateChunkSize(numLines)

	// Build channel info - channels are sorted by name in the file
	sortedChannels := r.channelList.SortedByName()

	channels := make([]compression.ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
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
			Width:  chWidth,
			Height: numLines,
		}
	}

	return compression.PXR24Decompress(data, channels, width, numLines, expectedSize)
}

// ScanlineWriter writes scanline images to an EXR file.
type ScanlineWriter struct {
	writer      *Writer
	header      *Header
	frameBuffer *FrameBuffer
	dataWindow  Box2i
	channelList *ChannelList
	currentY    int32
}

// NewScanlineWriter creates a writer for a scanline image.
func NewScanlineWriter(w io.WriteSeeker, h *Header) (*ScanlineWriter, error) {
	if h.IsTiled() {
		return nil, errors.New("exr: cannot use ScanlineWriter for tiled images")
	}

	writer, err := NewWriter(w, h)
	if err != nil {
		return nil, err
	}

	return &ScanlineWriter{
		writer:      writer,
		header:      h,
		dataWindow:  h.DataWindow(),
		channelList: h.Channels(),
		currentY:    h.DataWindow().Min.Y,
	}, nil
}

// Header returns the header for this file.
func (w *ScanlineWriter) Header() *Header {
	return w.header
}

// SetFrameBuffer sets the frame buffer to write pixels from.
func (w *ScanlineWriter) SetFrameBuffer(fb *FrameBuffer) {
	w.frameBuffer = fb
}

// writeChunkInfo holds data for a chunk to be written.
type writeChunkInfo struct {
	chunkStart int
	chunkEnd   int
	rawData    []byte // uncompressed pixel data
	compressed []byte // compressed data (filled during compression phase)
	err        error  // any error during compression
}

// WritePixels writes scanlines from y1 to y2 (inclusive) from the frame buffer.
func (w *ScanlineWriter) WritePixels(y1, y2 int) error {
	if w.frameBuffer == nil {
		return ErrNoFrameBuffer
	}

	minY := int(w.dataWindow.Min.Y)
	maxY := int(w.dataWindow.Max.Y)

	if y1 < minY || y2 > maxY || y1 > y2 {
		return ErrScanlineOutOfRange
	}

	comp := w.header.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	// Calculate number of chunks
	numChunks := 0
	for y := y1; y <= y2; {
		chunkEnd := y + linesPerChunk - 1
		if chunkEnd > y2 {
			chunkEnd = y2
		}
		if chunkEnd > maxY {
			chunkEnd = maxY
		}
		numChunks++
		y = chunkEnd + 1
	}

	// Check if we should use parallel processing
	config := GetParallelConfig()
	numWorkers := effectiveWorkers(config)
	useParallel := numWorkers > 1 && numChunks >= config.GrainSize

	if !useParallel {
		// Sequential processing (original path)
		return w.writePixelsSequential(y1, y2)
	}

	// Parallel processing path
	return w.writePixelsParallel(y1, y2, minY, maxY, comp, linesPerChunk, numChunks)
}

// writePixelsSequential is the original sequential implementation.
func (w *ScanlineWriter) writePixelsSequential(y1, y2 int) error {
	maxY := int(w.dataWindow.Max.Y)

	comp := w.header.Compression()
	linesPerChunk := comp.ScanlinesPerChunk()

	for y := y1; y <= y2; {
		chunkStart := y
		chunkEnd := chunkStart + linesPerChunk - 1
		if chunkEnd > y2 {
			chunkEnd = y2
		}
		if chunkEnd > maxY {
			chunkEnd = maxY
		}

		var data []byte
		var err error

		rawData, err := w.encodeUncompressedChunk(chunkStart, chunkEnd)
		if err != nil {
			return err
		}

		switch comp {
		case CompressionNone:
			data = rawData
		case CompressionRLE:
			data = w.compressRLE(rawData)
		case CompressionZIPS, CompressionZIP:
			data, err = w.compressZIP(rawData)
			if err != nil {
				return err
			}
		case CompressionPIZ:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressPIZ(rawData, numLines)
			if err != nil {
				return err
			}
		case CompressionPXR24:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressPXR24(rawData, numLines)
			if err != nil {
				return err
			}
		case CompressionB44:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressB44(rawData, numLines, false)
			if err != nil {
				return err
			}
		case CompressionB44A:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressB44(rawData, numLines, true)
			if err != nil {
				return err
			}
		case CompressionDWAA:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressDWA(rawData, numLines, false)
			if err != nil {
				return err
			}
		case CompressionDWAB:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressDWA(rawData, numLines, true)
			if err != nil {
				return err
			}
		case CompressionHTJ2K256:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressHTJ2K(rawData, numLines, 128)
			if err != nil {
				return err
			}
		case CompressionHTJ2K32:
			numLines := chunkEnd - chunkStart + 1
			data, err = w.compressHTJ2K(rawData, numLines, 32)
			if err != nil {
				return err
			}
		default:
			return errors.New("exr: compression not yet implemented: " + comp.String())
		}

		if err != nil {
			return err
		}

		if err := w.writer.WriteChunk(int32(chunkStart), data); err != nil {
			return err
		}

		y = chunkEnd + 1
	}

	return nil
}

// writePixelsParallel processes chunks with parallel compression.
func (w *ScanlineWriter) writePixelsParallel(y1, y2, minY, maxY int, comp Compression, linesPerChunk, numChunks int) error {
	// Phase 1: Calculate chunk boundaries and encode raw data
	// (encoding can be parallel since each chunk reads from different scanlines)
	chunks := make([]writeChunkInfo, numChunks)
	y := y1
	for i := 0; i < numChunks; i++ {
		chunkStart := y
		chunkEnd := chunkStart + linesPerChunk - 1
		if chunkEnd > y2 {
			chunkEnd = y2
		}
		if chunkEnd > maxY {
			chunkEnd = maxY
		}
		chunks[i] = writeChunkInfo{
			chunkStart: chunkStart,
			chunkEnd:   chunkEnd,
		}
		y = chunkEnd + 1
	}

	// Phase 2: Encode raw chunks in parallel (each reads from different scanlines)
	err := ParallelForWithError(numChunks, func(i int) error {
		rawData, err := w.encodeUncompressedChunk(chunks[i].chunkStart, chunks[i].chunkEnd)
		if err != nil {
			chunks[i].err = err
			return err
		}
		chunks[i].rawData = rawData
		return nil
	})
	if err != nil {
		return err
	}

	// Phase 3: Compress chunks in parallel (CPU bound)
	err = ParallelForWithError(numChunks, func(i int) error {
		chunk := &chunks[i]
		numLines := chunk.chunkEnd - chunk.chunkStart + 1

		var compressed []byte
		var compErr error

		switch comp {
		case CompressionNone:
			compressed = chunk.rawData
		case CompressionRLE:
			compressed = w.compressRLE(chunk.rawData)
		case CompressionZIPS, CompressionZIP:
			compressed, compErr = w.compressZIP(chunk.rawData)
		case CompressionPIZ:
			compressed, compErr = w.compressPIZ(chunk.rawData, numLines)
		case CompressionPXR24:
			compressed, compErr = w.compressPXR24(chunk.rawData, numLines)
		case CompressionB44:
			compressed, compErr = w.compressB44(chunk.rawData, numLines, false)
		case CompressionB44A:
			compressed, compErr = w.compressB44(chunk.rawData, numLines, true)
		case CompressionDWAA:
			compressed, compErr = w.compressDWA(chunk.rawData, numLines, false)
		case CompressionDWAB:
			compressed, compErr = w.compressDWA(chunk.rawData, numLines, true)
		case CompressionHTJ2K256:
			compressed, compErr = w.compressHTJ2K(chunk.rawData, numLines, 128)
		case CompressionHTJ2K32:
			compressed, compErr = w.compressHTJ2K(chunk.rawData, numLines, 32)
		default:
			compErr = errors.New("exr: compression not yet implemented: " + comp.String())
		}

		if compErr != nil {
			chunk.err = compErr
			return compErr
		}
		chunk.compressed = compressed
		return nil
	})
	if err != nil {
		return err
	}

	// Phase 4: Write chunks sequentially (must maintain file order)
	for i := 0; i < numChunks; i++ {
		chunk := &chunks[i]
		if chunk.err != nil {
			return chunk.err
		}
		if err := w.writer.WriteChunk(int32(chunk.chunkStart), chunk.compressed); err != nil {
			return err
		}
	}

	return nil
}

// encodeUncompressedChunk encodes scanlines as uncompressed data.
func (w *ScanlineWriter) encodeUncompressedChunk(y1, y2 int) ([]byte, error) {
	width := int(w.dataWindow.Width())
	minX := int(w.dataWindow.Min.X)

	// Calculate buffer size
	bufSize := 0
	for i := 0; i < w.channelList.Len(); i++ {
		ch := w.channelList.At(i)
		pixelsInChannel := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		bufSize += pixelsInChannel * ch.Type.Size() * (y2 - y1 + 1)
	}

	// Allocate output buffer directly
	output := make([]byte, bufSize)

	// Channels should be sorted by name
	sortedChannels := w.channelList.SortedByName()

	// Pre-allocate buffer for half conversion
	halfBuf := make([]uint16, width)

	pos := 0 // Current position in output

	for y := y1; y <= y2; y++ {
		for _, ch := range sortedChannels {
			pixelsInChannel := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
			bytesInChannel := pixelsInChannel * ch.Type.Size()

			slice := w.frameBuffer.Get(ch.Name)
			if slice == nil {
				// Write zeros for missing channel
				for i := 0; i < bytesInChannel; i++ {
					output[pos+i] = 0
				}
				pos += bytesInChannel
				continue
			}

			// Use optimized bulk row operations
			switch ch.Type {
			case PixelTypeHalf:
				slice.ReadRowHalf(y, halfBuf, minX, pixelsInChannel)
				// Convert uint16 to bytes
				for i := 0; i < pixelsInChannel; i++ {
					output[pos] = byte(halfBuf[i])
					output[pos+1] = byte(halfBuf[i] >> 8)
					pos += 2
				}

			case PixelTypeFloat:
				slice.ReadRowFloat(y, output[pos:pos+bytesInChannel], minX, pixelsInChannel)
				pos += bytesInChannel

			case PixelTypeUint:
				slice.ReadRowUint(y, output[pos:pos+bytesInChannel], minX, pixelsInChannel)
				pos += bytesInChannel
			}
		}
	}

	return output, nil
}

// compressRLE compresses chunk data using RLE.
// The OpenEXR RLE pipeline is: apply predictor -> RLE compress
// (interleaving is NOT used for RLE, only for ZIP/PIZ)
func (w *ScanlineWriter) compressRLE(data []byte) []byte {
	// Make a copy since predictor modifies in place
	encoded := make([]byte, len(data))
	copy(encoded, data)

	// Apply predictor (in place, using optimized version)
	predictor.EncodeSIMD(encoded)

	// RLE compress
	return compression.RLECompress(encoded)
}

// compressZIP compresses chunk data using ZIP.
// The OpenEXR ZIP pipeline is: apply predictor -> interleave -> zlib compress
// Uses the compression level from the header for deterministic round-trip.
func (w *ScanlineWriter) compressZIP(data []byte) ([]byte, error) {
	// Make a copy since predictor modifies in place
	encoded := make([]byte, len(data))
	copy(encoded, data)

	// Apply predictor (in place, using optimized version)
	predictor.EncodeSIMD(encoded)

	// Interleave (use fast version for larger data)
	var interleaved []byte
	if len(encoded) >= 32 {
		interleaved = compression.InterleaveFast(encoded)
	} else {
		interleaved = compression.Interleave(encoded)
	}

	// zlib compress with configured level
	level := w.header.ZIPLevel()
	return compression.ZIPCompressLevel(interleaved, level)
}

// compressPIZ compresses chunk data using PIZ.
// PIZ uses wavelet transform + Huffman coding on 16-bit data.
// For 32-bit types (float, uint), each value becomes two 16-bit samples.
func (w *ScanlineWriter) compressPIZ(data []byte, numLines int) ([]byte, error) {
	width := int(w.dataWindow.Width())

	// Count 16-bit samples per pixel (not channels).
	// Half = 1 sample, Float/Uint = 2 samples per pixel.
	samplesPerPixel := 0
	for i := 0; i < w.channelList.Len(); i++ {
		ch := w.channelList.At(i)
		switch ch.Type {
		case PixelTypeHalf:
			samplesPerPixel += 1
		case PixelTypeFloat, PixelTypeUint:
			samplesPerPixel += 2
		}
	}

	// Convert bytes to uint16 slice
	uint16Data := make([]uint16, len(data)/2)
	for i := 0; i < len(uint16Data); i++ {
		uint16Data[i] = uint16(data[i*2]) | uint16(data[i*2+1])<<8
	}

	// PIZ compress - use samplesPerPixel as the "channel" count
	return compression.PIZCompress(uint16Data, width, numLines, samplesPerPixel)
}

// compressPXR24 compresses chunk data using PXR24.
// PXR24 converts floats to 24-bit and uses zlib compression.
func (w *ScanlineWriter) compressPXR24(data []byte, numLines int) ([]byte, error) {
	width := int(w.dataWindow.Width())

	// Build channel info - channels are sorted by name in the file
	sortedChannels := w.channelList.SortedByName()

	channels := make([]compression.ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
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
			Width:  chWidth,
			Height: numLines,
		}
	}

	return compression.PXR24Compress(data, channels, width, numLines)
}

// decompressB44 decompresses B44/B44A-compressed chunk data.
func (r *ScanlineReader) decompressB44(data []byte, numLines int) ([]byte, error) {
	width := int(r.dataWindow.Width())
	expectedSize := r.calculateChunkSize(numLines)

	// Build channel info - channels are sorted by name in the file
	sortedChannels := r.channelList.SortedByName()

	channels := make([]compression.B44ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
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
			Width:  chWidth,
			Height: numLines,
		}
	}

	return compression.B44Decompress(data, channels, width, numLines, expectedSize)
}

// compressB44 compresses chunk data using B44/B44A.
func (w *ScanlineWriter) compressB44(data []byte, numLines int, flatfields bool) ([]byte, error) {
	width := int(w.dataWindow.Width())

	// Build channel info - channels are sorted by name in the file
	sortedChannels := w.channelList.SortedByName()

	channels := make([]compression.B44ChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
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
			Width:  chWidth,
			Height: numLines,
		}
	}

	return compression.B44Compress(data, channels, width, numLines, flatfields)
}

// decompressDWA decompresses chunk data using DWAA or DWAB.
func (r *ScanlineReader) decompressDWA(data []byte, numLines int) ([]byte, error) {
	width := int(r.dataWindow.Width())
	expectedSize := r.calculateChunkSize(numLines)

	// Create output buffer
	dst := make([]byte, expectedSize)

	// Decompress using DWA
	if err := compression.DecompressDWAA(data, dst, width, numLines); err != nil {
		return nil, err
	}

	return dst, nil
}

// decompressHTJ2K decompresses chunk data using HTJ2K (High-Throughput JPEG 2000).
func (r *ScanlineReader) decompressHTJ2K(data []byte, numLines int) ([]byte, error) {
	width := int(r.dataWindow.Width())

	// Build channel info
	sortedChannels := r.channelList.SortedByName()

	channels := make([]compression.HTJ2KChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		chHeight := (numLines + int(ch.YSampling) - 1) / int(ch.YSampling)
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
			Width:     chWidth,
			Height:    chHeight,
			XSampling: int(ch.XSampling),
			YSampling: int(ch.YSampling),
			Name:      ch.Name,
		}
	}

	expectedSize := r.calculateChunkSize(numLines)
	return compression.HTJ2KDecompress(data, expectedSize, channels)
}

// compressHTJ2K compresses chunk data using HTJ2K (High-Throughput JPEG 2000).
func (w *ScanlineWriter) compressHTJ2K(data []byte, numLines int, blockSize int) ([]byte, error) {
	width := int(w.dataWindow.Width())

	// Build channel info
	sortedChannels := w.channelList.SortedByName()

	channels := make([]compression.HTJ2KChannelInfo, len(sortedChannels))
	for i, ch := range sortedChannels {
		chWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)
		chHeight := (numLines + int(ch.YSampling) - 1) / int(ch.YSampling)
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
			Width:     chWidth,
			Height:    chHeight,
			XSampling: int(ch.XSampling),
			YSampling: int(ch.YSampling),
			Name:      ch.Name,
		}
	}

	return compression.HTJ2KCompress(data, numLines, channels, blockSize)
}

// compressDWA compresses chunk data using DWAA or DWAB.
func (w *ScanlineWriter) compressDWA(data []byte, numLines int, isDWAB bool) ([]byte, error) {
	width := int(w.dataWindow.Width())

	// Get compression level from header (defaults to 45.0 if not set)
	level := w.header.DWACompressionLevel()

	if isDWAB {
		return compression.CompressDWAB(data, width, numLines, level)
	}
	return compression.CompressDWAA(data, width, numLines, level)
}

// Close finalizes the file.
// After Close is called, the ScanlineWriter should not be used.
func (w *ScanlineWriter) Close() error {
	err := w.writer.Close()
	// Clear references to help Windows release file handles
	w.writer = nil
	w.header = nil
	w.frameBuffer = nil
	w.channelList = nil

	// Force garbage collection to ensure immediate release of file handles on Windows
	// This is a workaround for Windows file handle behavior where file handles
	// may not be released immediately after Close() even with cleared references
	runtime.GC()

	return err
}
