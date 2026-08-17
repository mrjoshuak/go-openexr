package exr

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Deep data errors
var (
	ErrDeepInvalidSampleCount  = errors.New("exr: invalid deep sample count")
	ErrDeepSampleCountMismatch = errors.New("exr: deep sample count mismatch")
	ErrDeepNotSupported        = errors.New("exr: deep data not supported for this compression")
	ErrDeepDataCorrupt         = errors.New("exr: deep data corrupted")
)

// DeepSlice describes a region of memory that holds deep pixel data for one channel.
// Unlike Slice, DeepSlice uses a pointer-to-pointer layout where each pixel
// contains a pointer to an array of samples.
type DeepSlice struct {
	// Type is the pixel data type stored in this slice.
	Type PixelType

	// Pointers is a 2D array of pointers. Each Pointers[y][x] points to
	// an array of samples for that pixel. The array length is determined
	// by the corresponding sample count.
	Pointers [][]interface{}

	// XSampling is the horizontal subsampling factor (1 = full resolution).
	XSampling int

	// YSampling is the vertical subsampling factor (1 = full resolution).
	YSampling int

	// FillValue is used for missing data (default 0).
	FillValue float64
}

// NewDeepSlice creates a new DeepSlice for the given dimensions.
func NewDeepSlice(pixelType PixelType, width, height int) DeepSlice {
	pointers := make([][]interface{}, height)
	for y := 0; y < height; y++ {
		pointers[y] = make([]interface{}, width)
	}
	return DeepSlice{
		Type:      pixelType,
		Pointers:  pointers,
		XSampling: 1,
		YSampling: 1,
	}
}

// AllocateSamples allocates sample storage for a pixel based on the sample count.
func (ds *DeepSlice) AllocateSamples(x, y, sampleCount int) {
	if y >= len(ds.Pointers) || x >= len(ds.Pointers[y]) {
		return
	}
	switch ds.Type {
	case PixelTypeFloat:
		ds.Pointers[y][x] = make([]float32, sampleCount)
	case PixelTypeHalf:
		ds.Pointers[y][x] = make([]uint16, sampleCount)
	case PixelTypeUint:
		ds.Pointers[y][x] = make([]uint32, sampleCount)
	}
}

// SetSampleFloat32 sets a sample value for a float channel.
func (ds *DeepSlice) SetSampleFloat32(x, y, sample int, value float32) {
	if data, ok := ds.Pointers[y][x].([]float32); ok && sample < len(data) {
		data[sample] = value
	}
}

// GetSampleFloat32 gets a sample value from a float channel.
func (ds *DeepSlice) GetSampleFloat32(x, y, sample int) float32 {
	if data, ok := ds.Pointers[y][x].([]float32); ok && sample < len(data) {
		return data[sample]
	}
	return float32(ds.FillValue)
}

// SetSampleHalf sets a sample value for a half channel.
func (ds *DeepSlice) SetSampleHalf(x, y, sample int, value uint16) {
	if data, ok := ds.Pointers[y][x].([]uint16); ok && sample < len(data) {
		data[sample] = value
	}
}

// GetSampleHalf gets a sample value from a half channel.
func (ds *DeepSlice) GetSampleHalf(x, y, sample int) uint16 {
	if data, ok := ds.Pointers[y][x].([]uint16); ok && sample < len(data) {
		return data[sample]
	}
	return 0
}

// SetSampleUint sets a sample value for a uint channel.
func (ds *DeepSlice) SetSampleUint(x, y, sample int, value uint32) {
	if data, ok := ds.Pointers[y][x].([]uint32); ok && sample < len(data) {
		data[sample] = value
	}
}

// GetSampleUint gets a sample value from a uint channel.
func (ds *DeepSlice) GetSampleUint(x, y, sample int) uint32 {
	if data, ok := ds.Pointers[y][x].([]uint32); ok && sample < len(data) {
		return data[sample]
	}
	return 0
}

// DeepFrameBuffer holds deep pixel data for multiple channels.
type DeepFrameBuffer struct {
	// Slices holds the deep slice for each channel.
	Slices map[string]*DeepSlice

	// SampleCounts holds the number of samples per pixel.
	// SampleCounts[y][x] is the number of samples at pixel (x, y).
	SampleCounts [][]uint32

	// Width and Height of the frame buffer.
	Width  int
	Height int
}

// NewDeepFrameBuffer creates a new DeepFrameBuffer for the given dimensions.
func NewDeepFrameBuffer(width, height int) *DeepFrameBuffer {
	sampleCounts := make([][]uint32, height)
	for y := 0; y < height; y++ {
		sampleCounts[y] = make([]uint32, width)
	}
	return &DeepFrameBuffer{
		Slices:       make(map[string]*DeepSlice),
		SampleCounts: sampleCounts,
		Width:        width,
		Height:       height,
	}
}

// Insert adds a channel to the frame buffer.
func (dfb *DeepFrameBuffer) Insert(name string, pixelType PixelType) {
	slice := NewDeepSlice(pixelType, dfb.Width, dfb.Height)
	dfb.Slices[name] = &slice
}

// SetSampleCount sets the number of samples for a pixel.
func (dfb *DeepFrameBuffer) SetSampleCount(x, y int, count uint32) {
	if y < len(dfb.SampleCounts) && x < len(dfb.SampleCounts[y]) {
		dfb.SampleCounts[y][x] = count
	}
}

// GetSampleCount returns the number of samples for a pixel.
func (dfb *DeepFrameBuffer) GetSampleCount(x, y int) uint32 {
	if y < len(dfb.SampleCounts) && x < len(dfb.SampleCounts[y]) {
		return dfb.SampleCounts[y][x]
	}
	return 0
}

// AllocateSamples allocates sample storage for all channels at a pixel.
func (dfb *DeepFrameBuffer) AllocateSamples(x, y int) {
	count := int(dfb.GetSampleCount(x, y))
	for _, slice := range dfb.Slices {
		slice.AllocateSamples(x, y, count)
	}
}

// TotalSampleCount returns the total number of samples across all pixels.
func (dfb *DeepFrameBuffer) TotalSampleCount() uint64 {
	var total uint64
	for y := 0; y < dfb.Height; y++ {
		for x := 0; x < dfb.Width; x++ {
			total += uint64(dfb.SampleCounts[y][x])
		}
	}
	return total
}

// MaxSamplesPerPixel returns the maximum sample count across all pixels.
func (dfb *DeepFrameBuffer) MaxSamplesPerPixel() uint32 {
	var max uint32
	for y := 0; y < dfb.Height; y++ {
		for x := 0; x < dfb.Width; x++ {
			if dfb.SampleCounts[y][x] > max {
				max = dfb.SampleCounts[y][x]
			}
		}
	}
	return max
}

// PackedSampleCountTable creates the packed sample count table for serialization.
// The table stores cumulative sample counts for efficient offset calculation.
func (dfb *DeepFrameBuffer) PackedSampleCountTable() []byte {
	numPixels := dfb.Width * dfb.Height
	table := make([]byte, numPixels*4) // 4 bytes per cumulative count

	cumulative := uint32(0)
	for y := 0; y < dfb.Height; y++ {
		for x := 0; x < dfb.Width; x++ {
			cumulative += dfb.SampleCounts[y][x]
			offset := (y*dfb.Width + x) * 4
			binary.LittleEndian.PutUint32(table[offset:], cumulative)
		}
	}
	return table
}

// UnpackSampleCountTable unpacks a sample count table and populates SampleCounts.
func (dfb *DeepFrameBuffer) UnpackSampleCountTable(table []byte) error {
	numPixels := dfb.Width * dfb.Height
	if len(table) < numPixels*4 {
		return ErrDeepDataCorrupt
	}

	prevCumulative := uint32(0)
	for y := 0; y < dfb.Height; y++ {
		for x := 0; x < dfb.Width; x++ {
			offset := (y*dfb.Width + x) * 4
			cumulative := binary.LittleEndian.Uint32(table[offset:])
			dfb.SampleCounts[y][x] = cumulative - prevCumulative
			prevCumulative = cumulative
		}
	}
	return nil
}

// DeepScanlineReader reads deep scanline images.
type DeepScanlineReader struct {
	file     *File
	header   *Header
	channels *ChannelList
	fb       *DeepFrameBuffer
}

// NewDeepScanlineReader creates a reader for deep scanline data.
func NewDeepScanlineReader(f *File) (*DeepScanlineReader, error) {
	if !f.IsDeep() {
		return nil, ErrDeepNotSupported
	}
	header := f.Header(0)
	if header == nil {
		return nil, ErrInvalidHeader
	}

	cl := header.Channels()
	if cl == nil {
		return nil, ErrInvalidHeader
	}

	return &DeepScanlineReader{
		file:     f,
		header:   header,
		channels: cl,
	}, nil
}

// SetFrameBuffer sets the frame buffer for reading.
func (r *DeepScanlineReader) SetFrameBuffer(fb *DeepFrameBuffer) {
	r.fb = fb
}

// ReadPixelSampleCounts reads the sample counts for a range of scanlines.
func (r *DeepScanlineReader) ReadPixelSampleCounts(y1, y2 int) error {
	if r.fb == nil {
		return ErrInvalidSlice
	}

	dw := r.header.DataWindow()
	width := int(dw.Max.X - dw.Min.X + 1)
	yMin := int(dw.Min.Y)
	yMax := int(dw.Max.Y)

	linesPerBlock := r.header.Compression().ScanlinesPerChunk()
	offsets := r.file.OffsetsRef(0)
	comp := r.header.Compression()

	// Track which chunks we've already processed
	processedChunks := make(map[int]bool)

	for y := y1; y <= y2; {
		chunkIndex := (y - yMin) / linesPerBlock
		if chunkIndex < 0 || chunkIndex >= len(offsets) {
			y++
			continue
		}

		// Skip if already processed
		if processedChunks[chunkIndex] {
			y++
			continue
		}
		processedChunks[chunkIndex] = true

		// Read compressed chunk data
		chunkY, compressedSampleCounts, _, err := r.file.ReadDeepChunk(0, chunkIndex)
		if err != nil {
			return err
		}

		// Calculate lines in this chunk
		linesInChunk := linesPerBlock
		if chunkIndex == len(offsets)-1 {
			totalLines := yMax - yMin + 1
			linesInChunk = totalLines - chunkIndex*linesPerBlock
			if linesInChunk <= 0 {
				linesInChunk = 1
			}
		}

		// Decompress sample count table
		numPixelsInChunk := width * linesInChunk
		expectedSampleCountSize := numPixelsInChunk * 4
		sampleCounts, err := r.decompressSampleCountTable(compressedSampleCounts, expectedSampleCountSize, comp)
		if err != nil {
			return err
		}

		// Unpack sample counts for lines in this chunk
		prevCumulative := uint32(0)
		for ly := 0; ly < linesInChunk; ly++ {
			absY := int(chunkY) + ly
			if absY < y1 || absY > y2 {
				// Still need to track cumulative count
				for x := 0; x < width; x++ {
					tableOffset := (ly*width + x) * 4
					if tableOffset+4 <= len(sampleCounts) {
						prevCumulative = binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
					}
				}
				continue
			}

			for x := 0; x < width; x++ {
				tableOffset := (ly*width + x) * 4
				if tableOffset+4 <= len(sampleCounts) {
					cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
					r.fb.SetSampleCount(x, absY-yMin, cumulative-prevCumulative)
					prevCumulative = cumulative
				}
			}
		}

		// Move to next chunk
		y = int(chunkY) + linesInChunk
	}

	return nil
}

// ReadPixels reads deep pixel data for a range of scanlines.
func (r *DeepScanlineReader) ReadPixels(y1, y2 int) error {
	if r.fb == nil {
		return ErrInvalidSlice
	}

	dw := r.header.DataWindow()
	width := int(dw.Max.X - dw.Min.X + 1)
	yMin := int(dw.Min.Y)
	yMax := int(dw.Max.Y)

	linesPerBlock := r.header.Compression().ScanlinesPerChunk()
	offsets := r.file.OffsetsRef(0)
	comp := r.header.Compression()

	// Allocate sample storage based on sample counts
	for y := y1; y <= y2; y++ {
		for x := 0; x < r.fb.Width; x++ {
			r.fb.AllocateSamples(x, y)
		}
	}

	// Get sorted channel list for reading (channels are stored sorted by name)
	sortedChannels := r.getSortedChannels()

	// Process each chunk that overlaps our scanline range
	for y := y1; y <= y2; {
		chunkIndex := (y - yMin) / linesPerBlock
		if chunkIndex < 0 || chunkIndex >= len(offsets) {
			y++
			continue
		}

		// Read chunk
		chunkY, sampleCountData, pixelData, err := r.file.ReadDeepChunk(0, chunkIndex)
		if err != nil {
			return err
		}

		// Calculate lines in this chunk
		linesInChunk := linesPerBlock
		if chunkIndex == len(offsets)-1 {
			totalLines := yMax - yMin + 1
			linesInChunk = totalLines - chunkIndex*linesPerBlock
			if linesInChunk <= 0 {
				linesInChunk = 1
			}
		}

		// Decompress sample count table if needed
		numPixelsInChunk := width * linesInChunk
		expectedSampleCountSize := numPixelsInChunk * 4
		sampleCounts, err := r.decompressSampleCountTable(sampleCountData, expectedSampleCountSize, comp)
		if err != nil {
			return err
		}

		// Calculate total samples in chunk from cumulative counts
		var totalSamples uint64
		if len(sampleCounts) >= 4 {
			lastIdx := (numPixelsInChunk - 1) * 4
			if lastIdx+4 <= len(sampleCounts) {
				totalSamples = uint64(binary.LittleEndian.Uint32(sampleCounts[lastIdx:]))
			}
		}

		// Calculate expected uncompressed pixel data size
		bytesPerSample := 0
		for _, ch := range sortedChannels {
			bytesPerSample += ch.Type.Size()
		}
		expectedPixelDataSize := int(totalSamples) * bytesPerSample

		// Decompress pixel data
		decompressedPixelData, err := r.decompressPixelData(pixelData, expectedPixelDataSize, comp)
		if err != nil {
			return err
		}

		// Parse pixel data into frame buffer
		err = r.parseDeepPixelData(decompressedPixelData, sampleCounts, sortedChannels,
			int(chunkY), linesInChunk, width, yMin, y1, y2)
		if err != nil {
			return err
		}

		// Move to next chunk
		y = int(chunkY) + linesInChunk
	}

	return nil
}

// getSortedChannels returns channels sorted by name (file storage order)
func (r *DeepScanlineReader) getSortedChannels() []Channel {
	channels := make([]Channel, r.channels.Len())
	for i := 0; i < r.channels.Len(); i++ {
		channels[i] = r.channels.At(i)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})
	return channels
}

// decompressSampleCountTable decompresses the sample count table
func (r *DeepScanlineReader) decompressSampleCountTable(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 {
		return make([]byte, expectedSize), nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		decompressed, err := compression.RLEDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionZIPS, CompressionZIP:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	default:
		// For unsupported compression, try zlib as fallback
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return data, nil // Return as-is
		}
		return decompressed, nil
	}
}

// decompressPixelData decompresses the pixel data
func (r *DeepScanlineReader) decompressPixelData(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 || expectedSize == 0 {
		return nil, nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		decompressed, err := compression.RLEDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionZIPS, CompressionZIP:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionPIZ:
		// PIZ for deep data - decompress and convert
		width := int(r.header.DataWindow().Width())
		numChannels := r.channels.Len()
		numSamples := expectedSize / 2 // Approximate for 16-bit data
		return compression.PIZDecompressBytes(data, width, numSamples/width/numChannels+1, numChannels)
	default:
		return data, nil
	}
}

// parseDeepPixelData parses decompressed pixel data into the frame buffer
func (r *DeepScanlineReader) parseDeepPixelData(data, sampleCounts []byte, channels []Channel,
	chunkY, linesInChunk, width, yMin, y1, y2 int) error {

	if len(data) == 0 {
		return nil
	}

	reader := xdr.NewReader(data)
	prevCumulative := uint32(0)

	for ly := 0; ly < linesInChunk; ly++ {
		absY := chunkY + ly
		inRange := absY >= y1 && absY <= y2
		fbY := absY - yMin

		for x := 0; x < width; x++ {
			// Get sample count for this pixel
			tableOffset := (ly*width + x) * 4
			var sampleCount uint32
			if tableOffset+4 <= len(sampleCounts) {
				cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
				sampleCount = cumulative - prevCumulative
				prevCumulative = cumulative
			}

			if sampleCount == 0 {
				continue
			}

			// Read samples for each channel
			for _, ch := range channels {
				slice := r.fb.Slices[ch.Name]

				for s := uint32(0); s < sampleCount; s++ {
					switch ch.Type {
					case PixelTypeHalf:
						val, err := reader.ReadUint16()
						if err != nil {
							return err
						}
						if inRange && slice != nil {
							slice.SetSampleHalf(x, fbY, int(s), val)
						}
					case PixelTypeFloat:
						val, err := reader.ReadFloat32()
						if err != nil {
							return err
						}
						if inRange && slice != nil {
							slice.SetSampleFloat32(x, fbY, int(s), val)
						}
					case PixelTypeUint:
						val, err := reader.ReadUint32()
						if err != nil {
							return err
						}
						if inRange && slice != nil {
							slice.SetSampleUint(x, fbY, int(s), val)
						}
					}
				}
			}
		}
	}

	return nil
}

// DeepScanlineWriter writes deep scanline images.
type DeepScanlineWriter struct {
	w              io.WriteSeeker
	header         *Header
	channels       *ChannelList
	fb             *DeepFrameBuffer
	currentY       int
	dataWindow     Box2i
	initialized    bool
	chunkOffsets   []int64
	offsetTablePos int64
}

// NewDeepScanlineWriter creates a writer for deep scanline data.
func NewDeepScanlineWriter(w io.WriteSeeker, width, height int) (*DeepScanlineWriter, error) {
	header := NewHeader()
	header.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeDeepScanline})
	header.SetCompression(CompressionZIPS)
	header.SetDataWindow(Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}})
	header.SetDisplayWindow(Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}})

	return &DeepScanlineWriter{
		w:          w,
		header:     header,
		dataWindow: Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}},
	}, nil
}

// Header returns the header for configuration.
func (dsw *DeepScanlineWriter) Header() *Header {
	return dsw.header
}

// SetFrameBuffer sets the frame buffer for writing.
func (dsw *DeepScanlineWriter) SetFrameBuffer(fb *DeepFrameBuffer) {
	dsw.fb = fb

	// Create channel list from frame buffer slices
	if dsw.channels == nil {
		dsw.channels = NewChannelList()
		for name, slice := range fb.Slices {
			dsw.channels.Add(Channel{
				Name:      name,
				Type:      slice.Type,
				XSampling: 1,
				YSampling: 1,
			})
		}
		dsw.header.SetChannels(dsw.channels)
	}
}

// initialize writes the file header and offset table placeholder
func (dsw *DeepScanlineWriter) initialize() error {
	if dsw.initialized {
		return nil
	}

	// Update data window from frame buffer
	if dsw.fb != nil {
		dsw.dataWindow = Box2i{
			Min: V2i{0, 0},
			Max: V2i{int32(dsw.fb.Width - 1), int32(dsw.fb.Height - 1)},
		}
		dsw.header.SetDataWindow(dsw.dataWindow)
		dsw.header.SetDisplayWindow(dsw.dataWindow)
	}

	// Write magic number
	if _, err := dsw.w.Write(MagicNumber); err != nil {
		return err
	}

	// Write version field with deep flag
	versionField := MakeVersionField(2, false, false, true, false)
	versionBuf := make([]byte, 4)
	xdr.ByteOrder.PutUint32(versionBuf, versionField)
	if _, err := dsw.w.Write(versionBuf); err != nil {
		return err
	}

	// Write header (WriteHeader includes the terminator byte)
	headerBuf := xdr.NewBufferWriter(1024)
	if err := WriteHeader(headerBuf, dsw.header); err != nil {
		return err
	}
	if _, err := dsw.w.Write(headerBuf.Bytes()); err != nil {
		return err
	}

	// Calculate number of chunks
	height := int(dsw.dataWindow.Height())
	linesPerChunk := dsw.header.Compression().ScanlinesPerChunk()
	numChunks := (height + linesPerChunk - 1) / linesPerChunk

	// Save position of offset table
	dsw.offsetTablePos, _ = dsw.w.Seek(0, io.SeekCurrent)

	// Write placeholder offset table
	dsw.chunkOffsets = make([]int64, numChunks)
	offsetTable := make([]byte, numChunks*8)
	if _, err := dsw.w.Write(offsetTable); err != nil {
		return err
	}

	dsw.currentY = int(dsw.dataWindow.Min.Y)
	dsw.initialized = true
	return nil
}

// WritePixels writes deep pixel data for a range of scanlines.
func (dsw *DeepScanlineWriter) WritePixels(numScanlines int) error {
	if dsw.fb == nil {
		return ErrInvalidSlice
	}

	if !dsw.initialized {
		if err := dsw.initialize(); err != nil {
			return err
		}
	}

	width := int(dsw.dataWindow.Width())
	linesPerChunk := dsw.header.Compression().ScanlinesPerChunk()
	comp := dsw.header.Compression()

	// Get sorted channels
	sortedChannels := dsw.getSortedChannels()

	y := dsw.currentY
	endY := y + numScanlines
	yMin := int(dsw.dataWindow.Min.Y)

	for y < endY {
		chunkIndex := (y - yMin) / linesPerChunk
		chunkY := yMin + chunkIndex*linesPerChunk

		linesInChunk := linesPerChunk
		remaining := int(dsw.dataWindow.Max.Y) - chunkY + 1
		if linesInChunk > remaining {
			linesInChunk = remaining
		}

		// Record chunk offset
		offset, _ := dsw.w.Seek(0, io.SeekCurrent)
		if chunkIndex < len(dsw.chunkOffsets) {
			dsw.chunkOffsets[chunkIndex] = offset
		}

		// Write chunk
		if err := dsw.writeChunk(chunkY, linesInChunk, width, sortedChannels, comp); err != nil {
			return err
		}

		y = chunkY + linesInChunk
	}

	dsw.currentY = y
	return nil
}

// writeChunk writes a single deep chunk
func (dsw *DeepScanlineWriter) writeChunk(chunkY, linesInChunk, width int, channels []Channel, comp Compression) error {
	yMin := int(dsw.dataWindow.Min.Y)
	numPixels := width * linesInChunk

	// Build sample count table (cumulative)
	sampleCountTable := make([]byte, numPixels*4)
	cumulative := uint32(0)
	for ly := 0; ly < linesInChunk; ly++ {
		fbY := chunkY + ly - yMin
		for x := 0; x < width; x++ {
			cumulative += dsw.fb.GetSampleCount(x, fbY)
			offset := (ly*width + x) * 4
			binary.LittleEndian.PutUint32(sampleCountTable[offset:], cumulative)
		}
	}
	totalSamples := cumulative

	// Build pixel data
	bytesPerSample := 0
	for _, ch := range channels {
		bytesPerSample += ch.Type.Size()
	}
	pixelDataSize := int(totalSamples) * bytesPerSample

	writer := xdr.NewBufferWriter(pixelDataSize)
	prevCumulative := uint32(0)

	for ly := 0; ly < linesInChunk; ly++ {
		fbY := chunkY + ly - yMin
		for x := 0; x < width; x++ {
			tableOffset := (ly*width + x) * 4
			cumulative := binary.LittleEndian.Uint32(sampleCountTable[tableOffset:])
			sampleCount := cumulative - prevCumulative
			prevCumulative = cumulative

			for _, ch := range channels {
				slice := dsw.fb.Slices[ch.Name]
				if slice == nil {
					// Write zeros for missing channels
					for s := uint32(0); s < sampleCount; s++ {
						switch ch.Type {
						case PixelTypeHalf:
							writer.WriteUint16(0)
						case PixelTypeFloat:
							writer.WriteFloat32(0)
						case PixelTypeUint:
							writer.WriteUint32(0)
						}
					}
					continue
				}

				for s := uint32(0); s < sampleCount; s++ {
					switch ch.Type {
					case PixelTypeHalf:
						val := slice.GetSampleHalf(x, fbY, int(s))
						writer.WriteUint16(val)
					case PixelTypeFloat:
						val := slice.GetSampleFloat32(x, fbY, int(s))
						writer.WriteFloat32(val)
					case PixelTypeUint:
						val := slice.GetSampleUint(x, fbY, int(s))
						writer.WriteUint32(val)
					}
				}
			}
		}
	}

	// Compress sample count table and pixel data
	compressedSampleCount, err := dsw.compressData(sampleCountTable, comp)
	if err != nil {
		return err
	}

	compressedPixelData, err := dsw.compressData(writer.Bytes(), comp)
	if err != nil {
		return err
	}

	// Write chunk header
	// For deep data: y (4 bytes) + packed sample count size (8 bytes) + packed pixel data size (8 bytes)
	chunkHeader := make([]byte, 20)
	xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(chunkY))
	xdr.ByteOrder.PutUint64(chunkHeader[4:12], uint64(len(compressedSampleCount)))
	xdr.ByteOrder.PutUint64(chunkHeader[12:20], uint64(len(compressedPixelData)))

	if _, err := dsw.w.Write(chunkHeader); err != nil {
		return err
	}

	// Write compressed sample count table
	if _, err := dsw.w.Write(compressedSampleCount); err != nil {
		return err
	}

	// Write compressed pixel data
	if _, err := dsw.w.Write(compressedPixelData); err != nil {
		return err
	}

	return nil
}

// compressData compresses data based on compression type.
// Uses the compression level from the header for deterministic round-trip.
func (dsw *DeepScanlineWriter) compressData(data []byte, comp Compression) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		// Reorder bytes, apply the predictor, then RLE compress
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		return compression.RLECompress(scratch), nil
	case CompressionZIPS, CompressionZIP:
		// Reorder bytes, apply the predictor, then zlib compress
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		level := dsw.header.ZIPLevel()
		return compression.ZIPCompressLevel(scratch, level)
	default:
		// Fallback to ZIP
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		level := dsw.header.ZIPLevel()
		return compression.ZIPCompressLevel(scratch, level)
	}
}

// getSortedChannels returns channels sorted by name
func (dsw *DeepScanlineWriter) getSortedChannels() []Channel {
	if dsw.channels == nil {
		return nil
	}
	channels := make([]Channel, dsw.channels.Len())
	for i := 0; i < dsw.channels.Len(); i++ {
		channels[i] = dsw.channels.At(i)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})
	return channels
}

// Finalize writes the offset table and closes the file.
func (dsw *DeepScanlineWriter) Finalize() error {
	if !dsw.initialized {
		return nil
	}

	// Write offset table at saved position
	currentPos, _ := dsw.w.Seek(0, io.SeekCurrent)
	if _, err := dsw.w.Seek(dsw.offsetTablePos, io.SeekStart); err != nil {
		return err
	}

	offsetTable := make([]byte, len(dsw.chunkOffsets)*8)
	for i, offset := range dsw.chunkOffsets {
		xdr.ByteOrder.PutUint64(offsetTable[i*8:], uint64(offset))
	}
	if _, err := dsw.w.Write(offsetTable); err != nil {
		return err
	}

	// Seek back to end
	_, err := dsw.w.Seek(currentPos, io.SeekStart)
	return err
}

// IsDeepCompressionSupported returns true if the compression type supports deep data.
func IsDeepCompressionSupported(c Compression) bool {
	switch c {
	case CompressionNone, CompressionRLE, CompressionZIPS, CompressionZIP, CompressionPIZ:
		return true
	case CompressionPXR24, CompressionB44, CompressionB44A, CompressionDWAA, CompressionDWAB:
		// Lossy compression not supported for deep data
		return false
	default:
		return false
	}
}

// DeepTiledReader reads deep tiled images.
type DeepTiledReader struct {
	file       *File
	part       int
	header     *Header
	channels   *ChannelList
	fb         *DeepFrameBuffer
	dataWindow Box2i
	tileDesc   *TileDescription
	tilesX     int
	tilesY     int
}

// NewDeepTiledReader creates a reader for deep tiled data.
func NewDeepTiledReader(f *File) (*DeepTiledReader, error) {
	return NewDeepTiledReaderPart(f, 0)
}

// NewDeepTiledReaderPart creates a reader for a specific part of a deep tiled file.
func NewDeepTiledReaderPart(f *File, part int) (*DeepTiledReader, error) {
	if !f.IsDeep() {
		return nil, ErrDeepNotSupported
	}
	header := f.Header(part)
	if header == nil {
		return nil, ErrInvalidHeader
	}

	if !header.IsTiled() {
		return nil, ErrNotTiled
	}

	cl := header.Channels()
	if cl == nil {
		return nil, ErrInvalidHeader
	}

	td := header.TileDescription()
	dw := header.DataWindow()

	return &DeepTiledReader{
		file:       f,
		part:       part,
		header:     header,
		channels:   cl,
		dataWindow: dw,
		tileDesc:   td,
		tilesX:     (int(dw.Width()) + int(td.XSize) - 1) / int(td.XSize),
		tilesY:     (int(dw.Height()) + int(td.YSize) - 1) / int(td.YSize),
	}, nil
}

// Header returns the header for this part.
func (r *DeepTiledReader) Header() *Header {
	return r.header
}

// DataWindow returns the data window for this part.
func (r *DeepTiledReader) DataWindow() Box2i {
	return r.dataWindow
}

// TileDescription returns the tile description.
func (r *DeepTiledReader) TileDescription() *TileDescription {
	return r.tileDesc
}

// NumTilesX returns the number of tiles in the X direction.
func (r *DeepTiledReader) NumTilesX() int {
	return r.tilesX
}

// NumTilesY returns the number of tiles in the Y direction.
func (r *DeepTiledReader) NumTilesY() int {
	return r.tilesY
}

// NumXLevels returns the number of resolution levels in X direction.
func (r *DeepTiledReader) NumXLevels() int {
	return r.header.NumXLevels()
}

// NumYLevels returns the number of resolution levels in Y direction.
func (r *DeepTiledReader) NumYLevels() int {
	return r.header.NumYLevels()
}

// SetFrameBuffer sets the frame buffer for reading.
func (r *DeepTiledReader) SetFrameBuffer(fb *DeepFrameBuffer) {
	r.fb = fb
}

// chunkIndex calculates the chunk index for a tile at the given coordinates and level.
func (r *DeepTiledReader) chunkIndex(tileX, tileY, levelX, levelY int) int {
	if r.tileDesc.Mode == LevelModeOne {
		return tileY*r.tilesX + tileX
	}

	offset := 0
	switch r.tileDesc.Mode {
	case LevelModeMipmap:
		for l := 0; l < levelX; l++ {
			numX := r.header.NumXTiles(l)
			numY := r.header.NumYTiles(l)
			offset += numX * numY
		}
		numXAtLevel := r.header.NumXTiles(levelX)
		offset += tileY*numXAtLevel + tileX
	case LevelModeRipmap:
		for ly := 0; ly < levelY; ly++ {
			numY := r.header.NumYTiles(ly)
			for lx := 0; lx < r.header.NumXLevels(); lx++ {
				numX := r.header.NumXTiles(lx)
				offset += numX * numY
			}
		}
		for lx := 0; lx < levelX; lx++ {
			numX := r.header.NumXTiles(lx)
			numY := r.header.NumYTiles(levelY)
			offset += numX * numY
		}
		numXAtLevel := r.header.NumXTiles(levelX)
		offset += tileY*numXAtLevel + tileX
	}
	return offset
}

// ReadTileSampleCounts reads sample counts for a single tile at level 0.
func (r *DeepTiledReader) ReadTileSampleCounts(tileX, tileY int) error {
	return r.ReadTileSampleCountsLevel(tileX, tileY, 0, 0)
}

// ReadTileSampleCountsLevel reads sample counts for a single tile at the specified level.
func (r *DeepTiledReader) ReadTileSampleCountsLevel(tileX, tileY, levelX, levelY int) error {
	if r.fb == nil {
		return ErrInvalidSlice
	}

	chunkIdx := r.chunkIndex(tileX, tileY, levelX, levelY)
	offsets := r.file.OffsetsRef(r.part)
	if chunkIdx < 0 || chunkIdx >= len(offsets) {
		return ErrTileOutOfRange
	}

	// Read deep tile chunk
	_, sampleCountTable, _, err := r.file.ReadDeepTileChunk(r.part, chunkIdx)
	if err != nil {
		return err
	}

	// Calculate tile boundaries
	tileW := int(r.tileDesc.XSize)
	tileH := int(r.tileDesc.YSize)
	tileStartX := tileX * tileW
	tileStartY := tileY * tileH

	// Decompress sample count table
	numPixelsInTile := tileW * tileH
	expectedSize := numPixelsInTile * 4
	comp := r.header.Compression()
	sampleCounts, err := r.decompressSampleCountTable(sampleCountTable, expectedSize, comp)
	if err != nil {
		return err
	}

	// Unpack sample counts
	prevCumulative := uint32(0)
	for ly := 0; ly < tileH; ly++ {
		y := tileStartY + ly
		if y >= r.fb.Height {
			continue
		}
		for lx := 0; lx < tileW; lx++ {
			x := tileStartX + lx
			if x >= r.fb.Width {
				continue
			}
			tableOffset := (ly*tileW + lx) * 4
			if tableOffset+4 <= len(sampleCounts) {
				cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
				r.fb.SetSampleCount(x, y, cumulative-prevCumulative)
				prevCumulative = cumulative
			}
		}
	}

	return nil
}

// ReadTile reads a single deep tile at level 0.
func (r *DeepTiledReader) ReadTile(tileX, tileY int) error {
	return r.ReadTileLevel(tileX, tileY, 0, 0)
}

// ReadTileLevel reads a single deep tile at the specified level.
func (r *DeepTiledReader) ReadTileLevel(tileX, tileY, levelX, levelY int) error {
	if r.fb == nil {
		return ErrInvalidSlice
	}

	chunkIdx := r.chunkIndex(tileX, tileY, levelX, levelY)
	offsets := r.file.OffsetsRef(r.part)
	if chunkIdx < 0 || chunkIdx >= len(offsets) {
		return ErrTileOutOfRange
	}

	// Read deep tile chunk
	_, sampleCountTable, pixelData, err := r.file.ReadDeepTileChunk(r.part, chunkIdx)
	if err != nil {
		return err
	}

	// Calculate tile boundaries
	tileW := int(r.tileDesc.XSize)
	tileH := int(r.tileDesc.YSize)
	tileStartX := tileX * tileW
	tileStartY := tileY * tileH

	comp := r.header.Compression()
	sortedChannels := r.getSortedChannels()

	// Decompress sample count table
	numPixelsInTile := tileW * tileH
	expectedSampleCountSize := numPixelsInTile * 4
	sampleCounts, err := r.decompressSampleCountTable(sampleCountTable, expectedSampleCountSize, comp)
	if err != nil {
		return err
	}

	// Calculate total samples from cumulative counts
	var totalSamples uint64
	if len(sampleCounts) >= 4 {
		lastIdx := (numPixelsInTile - 1) * 4
		if lastIdx+4 <= len(sampleCounts) {
			totalSamples = uint64(binary.LittleEndian.Uint32(sampleCounts[lastIdx:]))
		}
	}

	// Allocate samples based on sample counts
	prevCumulative := uint32(0)
	for ly := 0; ly < tileH; ly++ {
		y := tileStartY + ly
		if y >= r.fb.Height {
			continue
		}
		for lx := 0; lx < tileW; lx++ {
			x := tileStartX + lx
			if x >= r.fb.Width {
				continue
			}
			tableOffset := (ly*tileW + lx) * 4
			if tableOffset+4 <= len(sampleCounts) {
				cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
				sampleCount := cumulative - prevCumulative
				r.fb.SetSampleCount(x, y, sampleCount)
				r.fb.AllocateSamples(x, y)
				prevCumulative = cumulative
			}
		}
	}

	// Calculate expected pixel data size
	bytesPerSample := 0
	for _, ch := range sortedChannels {
		bytesPerSample += ch.Type.Size()
	}
	expectedPixelDataSize := int(totalSamples) * bytesPerSample

	// Decompress pixel data
	decompressedPixelData, err := r.decompressPixelData(pixelData, expectedPixelDataSize, comp)
	if err != nil {
		return err
	}

	// Parse pixel data
	if len(decompressedPixelData) > 0 {
		reader := xdr.NewReader(decompressedPixelData)
		prevCumulative = 0

		for ly := 0; ly < tileH; ly++ {
			y := tileStartY + ly
			if y >= r.fb.Height {
				continue
			}
			for lx := 0; lx < tileW; lx++ {
				x := tileStartX + lx
				if x >= r.fb.Width {
					continue
				}

				tableOffset := (ly*tileW + lx) * 4
				var sampleCount uint32
				if tableOffset+4 <= len(sampleCounts) {
					cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
					sampleCount = cumulative - prevCumulative
					prevCumulative = cumulative
				}

				if sampleCount == 0 {
					continue
				}

				for _, ch := range sortedChannels {
					slice := r.fb.Slices[ch.Name]
					for s := uint32(0); s < sampleCount; s++ {
						switch ch.Type {
						case PixelTypeHalf:
							val, err := reader.ReadUint16()
							if err != nil {
								return err
							}
							if slice != nil {
								slice.SetSampleHalf(x, y, int(s), val)
							}
						case PixelTypeFloat:
							val, err := reader.ReadFloat32()
							if err != nil {
								return err
							}
							if slice != nil {
								slice.SetSampleFloat32(x, y, int(s), val)
							}
						case PixelTypeUint:
							val, err := reader.ReadUint32()
							if err != nil {
								return err
							}
							if slice != nil {
								slice.SetSampleUint(x, y, int(s), val)
							}
						}
					}
				}
			}
		}
	}

	return nil
}

// ReadTiles reads all tiles in a range at level 0.
func (r *DeepTiledReader) ReadTiles(tileX1, tileY1, tileX2, tileY2 int) error {
	return r.ReadTilesLevel(tileX1, tileY1, tileX2, tileY2, 0, 0)
}

// ReadTilesLevel reads all tiles in a range at the specified level.
func (r *DeepTiledReader) ReadTilesLevel(tileX1, tileY1, tileX2, tileY2, levelX, levelY int) error {
	for ty := tileY1; ty <= tileY2; ty++ {
		for tx := tileX1; tx <= tileX2; tx++ {
			if err := r.ReadTileLevel(tx, ty, levelX, levelY); err != nil {
				return err
			}
		}
	}
	return nil
}

// getSortedChannels returns channels sorted by name
func (r *DeepTiledReader) getSortedChannels() []Channel {
	channels := make([]Channel, r.channels.Len())
	for i := 0; i < r.channels.Len(); i++ {
		channels[i] = r.channels.At(i)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})
	return channels
}

// decompressSampleCountTable decompresses the sample count table
func (r *DeepTiledReader) decompressSampleCountTable(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 {
		return make([]byte, expectedSize), nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		decompressed, err := compression.RLEDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionZIPS, CompressionZIP:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	default:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return data, nil
		}
		return decompressed, nil
	}
}

// decompressPixelData decompresses the pixel data
func (r *DeepTiledReader) decompressPixelData(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 || expectedSize == 0 {
		return nil, nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		decompressed, err := compression.RLEDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionZIPS, CompressionZIP:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
	case CompressionPIZ:
		width := int(r.tileDesc.XSize)
		numChannels := r.channels.Len()
		numSamples := expectedSize / 2
		return compression.PIZDecompressBytes(data, width, numSamples/width/numChannels+1, numChannels)
	default:
		return data, nil
	}
}

// DeepTiledWriter writes deep tiled images.
type DeepTiledWriter struct {
	w              io.WriteSeeker
	header         *Header
	channels       *ChannelList
	fb             *DeepFrameBuffer
	dataWindow     Box2i
	tileDesc       TileDescription
	initialized    bool
	chunkOffsets   []int64
	offsetTablePos int64
	tilesX         int
	tilesY         int
}

// NewDeepTiledWriter creates a writer for deep tiled data.
func NewDeepTiledWriter(w io.WriteSeeker, width, height int, tileW, tileH uint32) (*DeepTiledWriter, error) {
	header := NewHeader()
	header.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeDeepTiled})
	header.SetCompression(CompressionZIPS)
	header.SetDataWindow(Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}})
	header.SetDisplayWindow(Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}})

	td := TileDescription{
		XSize:        tileW,
		YSize:        tileH,
		Mode:         LevelModeOne,
		RoundingMode: LevelRoundDown,
	}
	header.SetTileDescription(td)

	tilesX := (width + int(tileW) - 1) / int(tileW)
	tilesY := (height + int(tileH) - 1) / int(tileH)

	return &DeepTiledWriter{
		w:          w,
		header:     header,
		dataWindow: Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}},
		tileDesc:   td,
		tilesX:     tilesX,
		tilesY:     tilesY,
	}, nil
}

// Header returns the header for configuration.
func (dtw *DeepTiledWriter) Header() *Header {
	return dtw.header
}

// SetFrameBuffer sets the frame buffer for writing.
func (dtw *DeepTiledWriter) SetFrameBuffer(fb *DeepFrameBuffer) {
	dtw.fb = fb

	if dtw.channels == nil {
		dtw.channels = NewChannelList()
		for name, slice := range fb.Slices {
			dtw.channels.Add(Channel{
				Name:      name,
				Type:      slice.Type,
				XSampling: 1,
				YSampling: 1,
			})
		}
		dtw.header.SetChannels(dtw.channels)
	}
}

// initialize writes the file header and offset table placeholder
func (dtw *DeepTiledWriter) initialize() error {
	if dtw.initialized {
		return nil
	}

	if dtw.fb != nil {
		dtw.dataWindow = Box2i{
			Min: V2i{0, 0},
			Max: V2i{int32(dtw.fb.Width - 1), int32(dtw.fb.Height - 1)},
		}
		dtw.header.SetDataWindow(dtw.dataWindow)
		dtw.header.SetDisplayWindow(dtw.dataWindow)
	}

	// Write magic number
	if _, err := dtw.w.Write(MagicNumber); err != nil {
		return err
	}

	// Write version field with deep and tiled flags
	versionField := MakeVersionField(2, true, false, true, false)
	versionBuf := make([]byte, 4)
	xdr.ByteOrder.PutUint32(versionBuf, versionField)
	if _, err := dtw.w.Write(versionBuf); err != nil {
		return err
	}

	// Write header (WriteHeader includes the terminator byte)
	headerBuf := xdr.NewBufferWriter(1024)
	if err := WriteHeader(headerBuf, dtw.header); err != nil {
		return err
	}
	if _, err := dtw.w.Write(headerBuf.Bytes()); err != nil {
		return err
	}

	// Calculate number of chunks
	numChunks := dtw.tilesX * dtw.tilesY

	// Save position of offset table
	dtw.offsetTablePos, _ = dtw.w.Seek(0, io.SeekCurrent)

	// Write placeholder offset table
	dtw.chunkOffsets = make([]int64, numChunks)
	offsetTable := make([]byte, numChunks*8)
	if _, err := dtw.w.Write(offsetTable); err != nil {
		return err
	}

	dtw.initialized = true
	return nil
}

// WriteTile writes a single deep tile at level 0.
func (dtw *DeepTiledWriter) WriteTile(tileX, tileY int) error {
	return dtw.WriteTileLevel(tileX, tileY, 0, 0)
}

// WriteTileLevel writes a single deep tile at the specified level.
func (dtw *DeepTiledWriter) WriteTileLevel(tileX, tileY, levelX, levelY int) error {
	if dtw.fb == nil {
		return ErrInvalidSlice
	}

	if !dtw.initialized {
		if err := dtw.initialize(); err != nil {
			return err
		}
	}

	// Calculate chunk index
	chunkIndex := tileY*dtw.tilesX + tileX

	// Record chunk offset
	offset, _ := dtw.w.Seek(0, io.SeekCurrent)
	if chunkIndex < len(dtw.chunkOffsets) {
		dtw.chunkOffsets[chunkIndex] = offset
	}

	// Calculate tile boundaries
	tileW := int(dtw.tileDesc.XSize)
	tileH := int(dtw.tileDesc.YSize)
	tileStartX := tileX * tileW
	tileStartY := tileY * tileH

	// Clamp to data window
	tileEndX := tileStartX + tileW
	tileEndY := tileStartY + tileH
	if tileEndX > dtw.fb.Width {
		tileEndX = dtw.fb.Width
	}
	if tileEndY > dtw.fb.Height {
		tileEndY = dtw.fb.Height
	}
	actualTileW := tileEndX - tileStartX
	actualTileH := tileEndY - tileStartY

	numPixels := actualTileW * actualTileH
	comp := dtw.header.Compression()
	sortedChannels := dtw.getSortedChannels()

	// Build sample count table (cumulative)
	sampleCountTable := make([]byte, numPixels*4)
	cumulative := uint32(0)
	for ly := 0; ly < actualTileH; ly++ {
		y := tileStartY + ly
		for lx := 0; lx < actualTileW; lx++ {
			x := tileStartX + lx
			cumulative += dtw.fb.GetSampleCount(x, y)
			offset := (ly*actualTileW + lx) * 4
			binary.LittleEndian.PutUint32(sampleCountTable[offset:], cumulative)
		}
	}
	totalSamples := cumulative

	// Build pixel data
	bytesPerSample := 0
	for _, ch := range sortedChannels {
		bytesPerSample += ch.Type.Size()
	}
	pixelDataSize := int(totalSamples) * bytesPerSample
	writer := xdr.NewBufferWriter(pixelDataSize)

	prevCumulative := uint32(0)
	for ly := 0; ly < actualTileH; ly++ {
		y := tileStartY + ly
		for lx := 0; lx < actualTileW; lx++ {
			x := tileStartX + lx
			tableOffset := (ly*actualTileW + lx) * 4
			cumulative := binary.LittleEndian.Uint32(sampleCountTable[tableOffset:])
			sampleCount := cumulative - prevCumulative
			prevCumulative = cumulative

			for _, ch := range sortedChannels {
				slice := dtw.fb.Slices[ch.Name]
				if slice == nil {
					for s := uint32(0); s < sampleCount; s++ {
						switch ch.Type {
						case PixelTypeHalf:
							writer.WriteUint16(0)
						case PixelTypeFloat:
							writer.WriteFloat32(0)
						case PixelTypeUint:
							writer.WriteUint32(0)
						}
					}
					continue
				}

				for s := uint32(0); s < sampleCount; s++ {
					switch ch.Type {
					case PixelTypeHalf:
						val := slice.GetSampleHalf(x, y, int(s))
						writer.WriteUint16(val)
					case PixelTypeFloat:
						val := slice.GetSampleFloat32(x, y, int(s))
						writer.WriteFloat32(val)
					case PixelTypeUint:
						val := slice.GetSampleUint(x, y, int(s))
						writer.WriteUint32(val)
					}
				}
			}
		}
	}

	// Compress sample count table and pixel data
	compressedSampleCount, err := dtw.compressData(sampleCountTable, comp)
	if err != nil {
		return err
	}

	compressedPixelData, err := dtw.compressData(writer.Bytes(), comp)
	if err != nil {
		return err
	}

	// Write deep tile chunk header
	// For deep tiled: tileX (4) + tileY (4) + levelX (4) + levelY (4) +
	//                 packed sample count size (8) + packed pixel data size (8) = 32 bytes
	chunkHeader := make([]byte, 32)
	xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(tileX))
	xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(tileY))
	xdr.ByteOrder.PutUint32(chunkHeader[8:12], uint32(levelX))
	xdr.ByteOrder.PutUint32(chunkHeader[12:16], uint32(levelY))
	xdr.ByteOrder.PutUint64(chunkHeader[16:24], uint64(len(compressedSampleCount)))
	xdr.ByteOrder.PutUint64(chunkHeader[24:32], uint64(len(compressedPixelData)))

	if _, err := dtw.w.Write(chunkHeader); err != nil {
		return err
	}

	// Write compressed sample count table
	if _, err := dtw.w.Write(compressedSampleCount); err != nil {
		return err
	}

	// Write compressed pixel data
	if _, err := dtw.w.Write(compressedPixelData); err != nil {
		return err
	}

	return nil
}

// WriteTiles writes all tiles in a range at level 0.
func (dtw *DeepTiledWriter) WriteTiles(tileX1, tileY1, tileX2, tileY2 int) error {
	for ty := tileY1; ty <= tileY2; ty++ {
		for tx := tileX1; tx <= tileX2; tx++ {
			if err := dtw.WriteTile(tx, ty); err != nil {
				return err
			}
		}
	}
	return nil
}

// compressData compresses data based on compression type.
// Uses the compression level from the header for deterministic round-trip.
func (dtw *DeepTiledWriter) compressData(data []byte, comp Compression) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		// Reorder bytes, apply the predictor, then RLE compress
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		return compression.RLECompress(scratch), nil
	case CompressionZIPS, CompressionZIP:
		// Reorder bytes, apply the predictor, then zlib compress
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		level := dtw.header.ZIPLevel()
		return compression.ZIPCompressLevel(scratch, level)
	default:
		// Fallback to ZIP
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		level := dtw.header.ZIPLevel()
		return compression.ZIPCompressLevel(scratch, level)
	}
}

// getSortedChannels returns channels sorted by name
func (dtw *DeepTiledWriter) getSortedChannels() []Channel {
	if dtw.channels == nil {
		return nil
	}
	channels := make([]Channel, dtw.channels.Len())
	for i := 0; i < dtw.channels.Len(); i++ {
		channels[i] = dtw.channels.At(i)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})
	return channels
}

// Finalize writes the offset table and closes the file.
func (dtw *DeepTiledWriter) Finalize() error {
	if !dtw.initialized {
		return nil
	}

	currentPos, _ := dtw.w.Seek(0, io.SeekCurrent)
	if _, err := dtw.w.Seek(dtw.offsetTablePos, io.SeekStart); err != nil {
		return err
	}

	offsetTable := make([]byte, len(dtw.chunkOffsets)*8)
	for i, offset := range dtw.chunkOffsets {
		xdr.ByteOrder.PutUint64(offsetTable[i*8:], uint64(offset))
	}
	if _, err := dtw.w.Write(offsetTable); err != nil {
		return err
	}

	_, err := dtw.w.Seek(currentPos, io.SeekStart)
	return err
}
