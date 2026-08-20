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
	part     int
	header   *Header
	channels *ChannelList
	fb       *DeepFrameBuffer
}

// NewDeepScanlineReader creates a reader for deep scanline data in part 0.
func NewDeepScanlineReader(f *File) (*DeepScanlineReader, error) {
	return NewDeepScanlineReaderPart(f, 0)
}

// NewDeepScanlineReaderPart creates a reader for the deep scanline data in the
// given part of a multi-part file. Part 0 of a single-part file is what
// NewDeepScanlineReader reads.
func NewDeepScanlineReaderPart(f *File, part int) (*DeepScanlineReader, error) {
	if !f.IsDeep() {
		return nil, ErrDeepNotSupported
	}
	header := f.Header(part)
	if header == nil {
		return nil, ErrInvalidHeader
	}

	cl := header.Channels()
	if cl == nil {
		return nil, ErrInvalidHeader
	}

	return &DeepScanlineReader{
		file:     f,
		part:     part,
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
	offsets := r.file.OffsetsRef(r.part)
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
		chunkY, compressedSampleCounts, _, err := r.file.ReadDeepChunk(r.part, chunkIndex)
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

		// Unpack sample counts for lines in this chunk. The table is
		// cumulative along each scanline and restarts on the next one, so the
		// running total resets per line rather than carrying across the chunk.
		for ly := 0; ly < linesInChunk; ly++ {
			absY := int(chunkY) + ly
			if absY < y1 || absY > y2 {
				continue
			}
			prevCumulative := uint32(0)

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
	offsets := r.file.OffsetsRef(r.part)
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
		chunkY, sampleCountData, pixelData, err := r.file.ReadDeepChunk(r.part, chunkIndex)
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

		// Calculate total samples in chunk from the cumulative counts. Each
		// scanline's counts restart at zero, so the chunk total is the sum of
		// the last entry of every scanline, not the last entry of the table.
		var totalSamples uint64
		for ly := 0; ly < linesInChunk; ly++ {
			lastIdx := (ly*width + width - 1) * 4
			if lastIdx >= 0 && lastIdx+4 <= len(sampleCounts) {
				totalSamples += uint64(binary.LittleEndian.Uint32(sampleCounts[lastIdx:]))
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
	return decompressDeepBlock(data, expectedSize, comp)
}

// decompressPixelData decompresses the pixel data
func (r *DeepScanlineReader) decompressPixelData(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 || expectedSize == 0 {
		return nil, nil
	}
	return decompressDeepBlock(data, expectedSize, comp)
}

// parseDeepPixelData parses decompressed pixel data into the frame buffer
func (r *DeepScanlineReader) parseDeepPixelData(data, sampleCounts []byte, channels []Channel,
	chunkY, linesInChunk, width, yMin, y1, y2 int) error {

	if len(data) == 0 {
		return nil
	}

	reader := xdr.NewReader(data)

	// The sample data is stored one scanline at a time and, within a
	// scanline, one channel at a time: every sample of every pixel of that
	// scanline for one channel, then the next channel. The counts in the
	// table are cumulative along the scanline and restart on the next one.
	for ly := 0; ly < linesInChunk; ly++ {
		absY := chunkY + ly
		inRange := absY >= y1 && absY <= y2
		fbY := absY - yMin

		for _, ch := range channels {
			slice := r.fb.Slices[ch.Name]
			prevCumulative := uint32(0)

			for x := 0; x < width; x++ {
				// Get sample count for this pixel
				tableOffset := (ly*width + x) * 4
				var sampleCount uint32
				if tableOffset+4 <= len(sampleCounts) {
					cumulative := binary.LittleEndian.Uint32(sampleCounts[tableOffset:])
					sampleCount = cumulative - prevCumulative
					prevCumulative = cumulative
				}

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

	// A deep file compressed with anything but NONE, RLE or ZIPS is rejected
	// by the reference implementation before a single pixel is read, so refuse
	// to produce one rather than write a file no other reader will open.
	if !IsDeepCompressionSupported(dsw.header.Compression()) {
		return ErrDeepNotSupported
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
// deepChunkBody packs one deep scanline chunk's two payloads: the compressed
// pixel offset (sample count) table and the compressed sample data, with the
// sample data's unpacked size, which the chunk header carries and which nothing
// else in the chunk implies.
//
// It is separate from writing so a deep part of a multi-part file can produce
// the same bytes and hand them to the multi-part writer, which prefixes each
// chunk with its part number and records the offset in that part's table. A
// second copy of this packing is how the deep chunk header came to be eight
// bytes short in the first place, so there is one.
func (dsw *DeepScanlineWriter) deepChunkBody(chunkY, linesInChunk, width int, channels []Channel, comp Compression) (countTable, pixelData []byte, unpacked uint64, err error) {
	// A caller that passes no channels gets a chunk with a sample count table
	// and no sample data — well formed, entirely empty, and rejected by the
	// reference with a message that names neither the part nor the cause.
	// Refuse instead.
	if len(channels) == 0 {
		return nil, nil, 0, errors.New("exr: deep chunk has no channels")
	}
	yMin := int(dsw.dataWindow.Min.Y)
	numPixels := width * linesInChunk

	// Build the pixel offset (sample count) table. The counts are cumulative
	// along each scanline and start again at zero on the next one: the table
	// entry for pixel (x, y) is the number of samples in scanline y up to and
	// including x. A deep chunk holds exactly one scanline, so this is also
	// the number of samples in the chunk, but the reset is what the format
	// defines and is visible in every deep tile the reference writes.
	sampleCountTable := make([]byte, numPixels*4)
	totalSamples := uint32(0)
	for ly := 0; ly < linesInChunk; ly++ {
		fbY := chunkY + ly - yMin
		cumulative := uint32(0)
		for x := 0; x < width; x++ {
			cumulative += dsw.fb.GetSampleCount(x, fbY)
			offset := (ly*width + x) * 4
			binary.LittleEndian.PutUint32(sampleCountTable[offset:], cumulative)
		}
		totalSamples += cumulative
	}

	// Build the sample data. It is laid out exactly as flat scanline data is:
	// one scanline at a time, and within a scanline one channel at a time, in
	// the channel list's alphabetical order, each channel holding every sample
	// of every pixel of that scanline before the next channel begins.
	//
	// Writing it pixel-major instead (all channels of pixel 0, then all
	// channels of pixel 1) round-trips through this library's own reader and
	// is read as scrambled data by everything else, which is what the deep
	// writers did until the reference was asked.
	bytesPerSample := 0
	for _, ch := range channels {
		bytesPerSample += ch.Type.Size()
	}
	pixelDataSize := int(totalSamples) * bytesPerSample

	writer := xdr.NewBufferWriter(pixelDataSize)

	for ly := 0; ly < linesInChunk; ly++ {
		fbY := chunkY + ly - yMin
		for _, ch := range channels {
			slice := dsw.fb.Slices[ch.Name]
			for x := 0; x < width; x++ {
				sampleCount := dsw.fb.GetSampleCount(x, fbY)
				for s := uint32(0); s < sampleCount; s++ {
					switch ch.Type {
					case PixelTypeHalf:
						var val uint16
						if slice != nil {
							val = slice.GetSampleHalf(x, fbY, int(s))
						}
						writer.WriteUint16(val)
					case PixelTypeFloat:
						var val float32
						if slice != nil {
							val = slice.GetSampleFloat32(x, fbY, int(s))
						}
						writer.WriteFloat32(val)
					case PixelTypeUint:
						var val uint32
						if slice != nil {
							val = slice.GetSampleUint(x, fbY, int(s))
						}
						writer.WriteUint32(val)
					}
				}
			}
		}
	}

	// Compress sample count table and pixel data
	compressedSampleCount, err := dsw.compressData(sampleCountTable, comp)
	if err != nil {
		return nil, nil, 0, err
	}

	uncompressedPixelData := writer.Bytes()
	compressedPixelData, err := dsw.compressData(uncompressedPixelData, comp)
	if err != nil {
		return nil, nil, 0, err
	}

	return compressedSampleCount, compressedPixelData, uint64(len(uncompressedPixelData)), nil
}

func (dsw *DeepScanlineWriter) writeChunk(chunkY, linesInChunk, width int, channels []Channel, comp Compression) error {
	compressedSampleCount, compressedPixelData, unpackedSize, err := dsw.deepChunkBody(
		chunkY, linesInChunk, width, channels, comp)
	if err != nil {
		return err
	}

	// Write chunk header. A deep scanline chunk carries four fields, not
	// three: the reference implementation needs the unpacked size of the
	// sample data to size its decompression buffer, because the packed size
	// says nothing about how many samples the chunk holds. Writing only the
	// first three produced files this library read back perfectly and OpenEXR
	// rejected with "Some scanline chunks were missing or corrupted".
	//
	//   int    y coordinate
	//   uint64 packed size of the pixel offset (sample count) table
	//   uint64 packed size of the sample data
	//   uint64 unpacked size of the sample data
	chunkHeader := make([]byte, deepScanlineChunkHeaderSize)
	xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(chunkY))
	xdr.ByteOrder.PutUint64(chunkHeader[4:12], uint64(len(compressedSampleCount)))
	xdr.ByteOrder.PutUint64(chunkHeader[12:20], uint64(len(compressedPixelData)))
	xdr.ByteOrder.PutUint64(chunkHeader[20:28], unpackedSize)

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
	return compressDeepBlock(data, comp, dsw.header.ZIPLevel())
}

// compressDeepBlock compresses one block of a deep chunk — either the pixel
// offset table or the sample data.
//
// If compressing does not make the block smaller the block is stored as it is.
// That is not an optimisation: it is how the format signals which of the two a
// block is. Nothing in the chunk says "this block is compressed"; a reader
// compares the packed size against the unpacked size and treats a block that
// did not shrink as raw bytes. Small deep chunks reach this constantly — a
// four-pixel sample count table is 16 bytes and zlib makes it bigger — and the
// reference implementation's own files are full of raw blocks.
func compressDeepBlock(data []byte, comp Compression, zipLevel compression.CompressionLevel) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	var out []byte
	switch comp {
	case CompressionNone:
		return data, nil
	case CompressionRLE:
		// Reorder bytes, apply the predictor, then RLE compress
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		out = compression.RLECompress(scratch)
	default:
		// ZIPS, and anything else that reaches here: reorder bytes, apply the
		// predictor, then zlib compress.
		scratch := make([]byte, len(data))
		predictor.DeconstructBytes(scratch, data)
		var err error
		out, err = compression.ZIPCompressLevel(scratch, zipLevel)
		if err != nil {
			return nil, err
		}
	}

	if len(out) >= len(data) {
		return data, nil
	}
	return out, nil
}

// decompressDeepBlock reverses compressDeepBlock. A block whose packed size is
// not smaller than its unpacked size was stored raw; see compressDeepBlock.
func decompressDeepBlock(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if expectedSize > 0 && len(data) >= expectedSize {
		return data[:expectedSize], nil
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
	default:
		decompressed, err := compression.ZIPDecompress(data, expectedSize)
		if err != nil {
			return nil, err
		}
		// Reverse the predictor, then undo the byte reordering.
		output := make([]byte, len(decompressed))
		predictor.ReconstructBytes(output, decompressed)
		return output, nil
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

// IsDeepCompressionSupported returns true if the compression type may be used
// for deep data.
//
// Only NONE, RLE and ZIPS may. Every other codec compresses a fixed-size block
// of several scanlines at once, and a deep chunk holds one scanline of
// variable-length sample data, so there is nothing for them to operate on: a
// deep chunk is always a single scanline. The reference implementation enforces
// this when the file is opened, rejecting anything else with
// EXR_ERR_INVALID_ATTR "Invalid compression for deep data" — measured here for
// ZIP, PIZ, B44 and both HTJ2K variants, all of which this function used to
// allow or the writers used to emit.
func IsDeepCompressionSupported(c Compression) bool {
	switch c {
	case CompressionNone, CompressionRLE, CompressionZIPS:
		return true
	default:
		// ZIP, PIZ, PXR24, B44, B44A, DWAA, DWAB, HTJ2K and anything
		// unrecognised: not permitted for deep data.
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

// chunkIndex calculates the chunk index for a tile at the given coordinates and
// level, through the shared derivation deepTileChunkIndex.
func (r *DeepTiledReader) chunkIndex(tileX, tileY, levelX, levelY int) int {
	return deepTileChunkIndex(r.header, r.tileDesc.Mode, r.tilesX, tileX, tileY, levelX, levelY)
}

// deepTileChunkIndex is where a deep tile's chunk sits in the offset table.
//
// The reader has always derived this per level; the writer used
// tileY*tilesX+tileX and so could only ever produce a single-level file. One
// definition, consulted from both ends, is what keeps a writer and a reader
// from drifting — which is the defect the shallow tiled path had.
func deepTileChunkIndex(header *Header, mode LevelMode, tilesX, tileX, tileY, levelX, levelY int) int {
	if mode == LevelModeOne {
		return tileY*tilesX + tileX
	}

	offset := 0
	switch mode {
	case LevelModeMipmap:
		for l := 0; l < levelX; l++ {
			offset += header.NumXTiles(l) * header.NumYTiles(l)
		}
		offset += tileY*header.NumXTiles(levelX) + tileX
	case LevelModeRipmap:
		for ly := 0; ly < levelY; ly++ {
			numY := header.NumYTiles(ly)
			for lx := 0; lx < header.NumXLevels(); lx++ {
				offset += header.NumXTiles(lx) * numY
			}
		}
		for lx := 0; lx < levelX; lx++ {
			offset += header.NumXTiles(lx) * header.NumYTiles(levelY)
		}
		offset += tileY*header.NumXTiles(levelX) + tileX
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

	// Calculate tile boundaries, clipped to the data window
	tileStartX, tileStartY, tileW, tileH := r.tileExtent(tileX, tileY, levelX, levelY)

	// Decompress sample count table
	numPixelsInTile := tileW * tileH
	expectedSize := numPixelsInTile * 4
	comp := r.header.Compression()
	sampleCounts, err := r.decompressSampleCountTable(sampleCountTable, expectedSize, comp)
	if err != nil {
		return err
	}

	// Unpack sample counts. The table is cumulative along each scanline of the
	// tile and restarts on the next one.
	for ly := 0; ly < tileH; ly++ {
		y := tileStartY + ly
		if y >= r.fb.Height {
			continue
		}
		prevCumulative := uint32(0)
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

// tileExtent returns the origin and the size of a tile after clipping it to the
// data window. A tile that hangs off the right or the bottom edge is stored
// with only the pixels it has inside the window — a 4x4 tile over a 4x3 image
// carries a twelve-entry sample count table, not sixteen — so the tile's stored
// width is also the stride of its sample count table.
//
// Levels are not accounted for: this library writes deep tiled images with a
// single level, and the level size would have to be folded in here for the
// mipmapped case.
func (r *DeepTiledReader) tileExtent(tileX, tileY, levelX, levelY int) (startX, startY, w, h int) {
	tw := int(r.tileDesc.XSize)
	th := int(r.tileDesc.YSize)
	// Clipped to the level's own extent, not the image's.
	//
	// A tile is short wherever it runs off the edge of the level it belongs
	// to, and every level below the tile size is entirely short: level 3 of a
	// 64x64 image tiled at 16x16 is 8x8, one tile of 8x8. Clipping against the
	// data window instead said 16x16, so the reader expected four times the
	// sample-count table the file holds and the codec reported "corrupted ZIP
	// data" — measured on levels 3 through 6 of a 7-level file, while levels 0
	// to 2, all at or above the tile size, read correctly.
	levelW := r.header.LevelWidth(levelX)
	levelH := r.header.LevelHeight(levelY)
	startX = tileX * tw
	startY = tileY * th
	w, h = tw, th
	if startX+w > levelW {
		w = levelW - startX
	}
	if startY+h > levelH {
		h = levelH - startY
	}
	if w < 0 {
		w = 0
	}
	if h < 0 {
		h = 0
	}
	return startX, startY, w, h
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

	// Calculate tile boundaries, clipped to the data window
	tileStartX, tileStartY, tileW, tileH := r.tileExtent(tileX, tileY, levelX, levelY)

	comp := r.header.Compression()
	sortedChannels := r.getSortedChannels()

	// Decompress sample count table
	numPixelsInTile := tileW * tileH
	expectedSampleCountSize := numPixelsInTile * 4
	sampleCounts, err := r.decompressSampleCountTable(sampleCountTable, expectedSampleCountSize, comp)
	if err != nil {
		return err
	}

	// Calculate total samples from the cumulative counts. Each scanline of the
	// tile restarts at zero, so the total is the sum of the last entry of each
	// of the tile's scanlines.
	var totalSamples uint64
	for ly := 0; ly < tileH; ly++ {
		lastIdx := (ly*tileW + tileW - 1) * 4
		if lastIdx >= 0 && lastIdx+4 <= len(sampleCounts) {
			totalSamples += uint64(binary.LittleEndian.Uint32(sampleCounts[lastIdx:]))
		}
	}

	// Allocate samples based on sample counts
	for ly := 0; ly < tileH; ly++ {
		y := tileStartY + ly
		if y >= r.fb.Height {
			continue
		}
		prevCumulative := uint32(0)
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

	// Parse pixel data. Within a tile it is stored one scanline at a time and,
	// within a scanline, one channel at a time: every sample of every pixel of
	// that scanline for one channel, then the next channel.
	if len(decompressedPixelData) > 0 {
		reader := xdr.NewReader(decompressedPixelData)

		for ly := 0; ly < tileH; ly++ {
			y := tileStartY + ly
			if y >= r.fb.Height {
				continue
			}
			for _, ch := range sortedChannels {
				slice := r.fb.Slices[ch.Name]
				prevCumulative := uint32(0)

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
	return decompressDeepBlock(data, expectedSize, comp)
}

// decompressPixelData decompresses the pixel data
func (r *DeepTiledReader) decompressPixelData(data []byte, expectedSize int, comp Compression) ([]byte, error) {
	if len(data) == 0 || expectedSize == 0 {
		return nil, nil
	}
	return decompressDeepBlock(data, expectedSize, comp)
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

	// Take the tile description from the header rather than from the one
	// captured at construction. A caller that asks for a mipmap by calling
	// SetTileDescription got a single-level file and no indication, because
	// the level mode used for the offset table came from the constructor's
	// copy and never changed.
	if td := dtw.header.TileDescription(); td != nil {
		dtw.tileDesc = *td
		dw := dtw.header.DataWindow()
		dtw.tilesX = (int(dw.Width()) + int(td.XSize) - 1) / int(td.XSize)
		dtw.tilesY = (int(dw.Height()) + int(td.YSize) - 1) / int(td.YSize)
	}

	// See DeepScanlineWriter.initialize: the reference implementation refuses
	// a deep file compressed with anything but NONE, RLE or ZIPS.
	if !IsDeepCompressionSupported(dtw.header.Compression()) {
		return ErrDeepNotSupported
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

	// Write the version field with the deep flag and *not* the tiled flag.
	// The tiled flag and the non-image (deep) flag are mutually exclusive in
	// the version field: a single-part deep tiled file is identified by its
	// type attribute, "deeptiled", and OpenEXR rejects the combination with
	// "Invalid combination of version flags: single part flag found, but also
	// marked as deep". Setting both made every deep tiled file this library
	// wrote unopenable by the reference implementation.
	versionField := MakeVersionField(2, false, false, true, false)
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

	// A mipmapped or ripmapped deep image holds a chunk for every tile of
	// every level, not only of level 0.
	numChunks := deepTileChunkCount(dtw.header, dtw.tileDesc.Mode, dtw.tilesX, dtw.tilesY)

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

	// Where this tile's chunk sits in the offset table, derived per level by
	// the same function the reader consults. Using tileY*tilesX+tileX made
	// every level after the first overwrite the first one's slots.
	chunkIndex := deepTileChunkIndex(dtw.header, dtw.tileDesc.Mode, dtw.tilesX,
		tileX, tileY, levelX, levelY)

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

	// Build the pixel offset (sample count) table. Inside a tile the counts
	// are cumulative along each scanline of the tile and start again at zero
	// on the tile's next scanline: a tile the reference wrote for a 4x3 image
	// with two samples in every pixel of rows 1 and 2 has the table
	// 1 3 5 7 | 2 4 6 8 | 2 4 6 8, not one running total.
	sampleCountTable := make([]byte, numPixels*4)
	totalSamples := uint32(0)
	for ly := 0; ly < actualTileH; ly++ {
		y := tileStartY + ly
		cumulative := uint32(0)
		for lx := 0; lx < actualTileW; lx++ {
			x := tileStartX + lx
			cumulative += dtw.fb.GetSampleCount(x, y)
			offset := (ly*actualTileW + lx) * 4
			binary.LittleEndian.PutUint32(sampleCountTable[offset:], cumulative)
		}
		totalSamples += cumulative
	}

	// Build the sample data, one scanline of the tile at a time and, within a
	// scanline, one channel at a time in the channel list's alphabetical
	// order: every sample of every pixel of that scanline for one channel
	// before the next channel begins. See writeChunk for what writing it
	// pixel-major instead costs.
	bytesPerSample := 0
	for _, ch := range sortedChannels {
		bytesPerSample += ch.Type.Size()
	}
	pixelDataSize := int(totalSamples) * bytesPerSample
	writer := xdr.NewBufferWriter(pixelDataSize)

	for ly := 0; ly < actualTileH; ly++ {
		y := tileStartY + ly
		for _, ch := range sortedChannels {
			slice := dtw.fb.Slices[ch.Name]
			for lx := 0; lx < actualTileW; lx++ {
				x := tileStartX + lx
				sampleCount := dtw.fb.GetSampleCount(x, y)
				for s := uint32(0); s < sampleCount; s++ {
					switch ch.Type {
					case PixelTypeHalf:
						var val uint16
						if slice != nil {
							val = slice.GetSampleHalf(x, y, int(s))
						}
						writer.WriteUint16(val)
					case PixelTypeFloat:
						var val float32
						if slice != nil {
							val = slice.GetSampleFloat32(x, y, int(s))
						}
						writer.WriteFloat32(val)
					case PixelTypeUint:
						var val uint32
						if slice != nil {
							val = slice.GetSampleUint(x, y, int(s))
						}
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

	uncompressedPixelData := writer.Bytes()
	compressedPixelData, err := dtw.compressData(uncompressedPixelData, comp)
	if err != nil {
		return err
	}

	// Write deep tile chunk header. Like the deep scanline chunk it ends with
	// the unpacked size of the sample data, which the reference implementation
	// needs to size its buffer; omitting it made every tile unreadable outside
	// this library.
	//
	//   int    tile x, tile y, level x, level y
	//   uint64 packed size of the pixel offset (sample count) table
	//   uint64 packed size of the sample data
	//   uint64 unpacked size of the sample data
	chunkHeader := make([]byte, deepTileChunkHeaderSize)
	xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(tileX))
	xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(tileY))
	xdr.ByteOrder.PutUint32(chunkHeader[8:12], uint32(levelX))
	xdr.ByteOrder.PutUint32(chunkHeader[12:16], uint32(levelY))
	xdr.ByteOrder.PutUint64(chunkHeader[16:24], uint64(len(compressedSampleCount)))
	xdr.ByteOrder.PutUint64(chunkHeader[24:32], uint64(len(compressedPixelData)))
	xdr.ByteOrder.PutUint64(chunkHeader[32:40], uint64(len(uncompressedPixelData)))

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
	return compressDeepBlock(data, comp, dtw.header.ZIPLevel())
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

// deepTileChunkCount is how many chunks a deep tiled image holds: every tile of
// every level its level mode declares.
func deepTileChunkCount(header *Header, mode LevelMode, tilesX, tilesY int) int {
	switch mode {
	case LevelModeMipmap:
		// A mipmap's x and y levels advance together, so it has as many levels
		// as the larger dimension needs.
		levels := header.NumXLevels()
		if n := header.NumYLevels(); n > levels {
			levels = n
		}
		n := 0
		for l := 0; l < levels; l++ {
			n += header.NumXTiles(l) * header.NumYTiles(l)
		}
		return n
	case LevelModeRipmap:
		n := 0
		for ly := 0; ly < header.NumYLevels(); ly++ {
			for lx := 0; lx < header.NumXLevels(); lx++ {
				n += header.NumXTiles(lx) * header.NumYTiles(ly)
			}
		}
		return n
	}
	return tilesX * tilesY
}
