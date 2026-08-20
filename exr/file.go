package exr

import (
	"errors"
	"fmt"
	"io"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Magic number for OpenEXR files
var MagicNumber = []byte{0x76, 0x2f, 0x31, 0x01}

// Version field flags
const (
	VersionFlagTiled     = 1 << 9  // Image is tiled
	VersionFlagLongNames = 1 << 10 // Supports attribute names > 31 bytes
	VersionFlagDeep      = 1 << 11 // Contains deep data
	VersionFlagMultiPart = 1 << 12 // Multi-part file
)

// File format errors
var (
	ErrInvalidMagic       = errors.New("exr: invalid magic number")
	ErrUnsupportedVersion = errors.New("exr: unsupported version")
	ErrIncompleteFile     = errors.New("exr: incomplete file")
	ErrInvalidOffsetTable = errors.New("exr: invalid offset table")
	ErrInvalidFile        = errors.New("exr: invalid file")
	ErrInvalidHeaderSize  = errors.New("exr: invalid header size")
	ErrInvalidChunkSize   = errors.New("exr: invalid chunk size")
	ErrNoHeaders          = errors.New("exr: no headers provided")
	ErrMissingName        = errors.New("exr: multi-part header missing 'name' attribute")
)

// Size limits to prevent DoS attacks from malformed files
const (
	maxHeaderSize = 64 * 1024 * 1024  // 64 MB maximum header size
	maxChunkSize  = 256 * 1024 * 1024 // 256 MB maximum chunk size
	headerBufSize = 1024              // Initial buffer size for header serialization

	// headerPrefixSize is how much of the front of a file Open fetches before
	// it knows how much it needs. It covers the header and the offset table of
	// an ordinary frame in one read; a file needing more grows the prefix
	// rather than falling back to reading everything.
	headerPrefixSize = 64 * 1024
)

// Version extracts the version number from a version field.
func Version(versionField uint32) int {
	return int(versionField & 0xFF)
}

// IsTiled returns true if the version field indicates a tiled image.
func IsTiled(versionField uint32) bool {
	return versionField&VersionFlagTiled != 0
}

// HasLongNames returns true if the version field allows long attribute names.
func HasLongNames(versionField uint32) bool {
	return versionField&VersionFlagLongNames != 0
}

// IsDeep returns true if the version field indicates deep data.
func IsDeep(versionField uint32) bool {
	return versionField&VersionFlagDeep != 0
}

// IsMultiPart returns true if the version field indicates a multi-part file.
func IsMultiPart(versionField uint32) bool {
	return versionField&VersionFlagMultiPart != 0
}

// MakeVersionField creates a version field from flags.
func MakeVersionField(version int, tiled, longNames, deep, multiPart bool) uint32 {
	v := uint32(version)
	if tiled {
		v |= VersionFlagTiled
	}
	if longNames {
		v |= VersionFlagLongNames
	}
	if deep {
		v |= VersionFlagDeep
	}
	if multiPart {
		v |= VersionFlagMultiPart
	}
	return v
}

// SliceReader is an optional interface for zero-copy file access.
// Readers that support direct memory access (like mmap) implement this.
type SliceReader interface {
	io.ReaderAt
	// Slice returns a direct view into the underlying data.
	// The returned slice is only valid while the reader is open.
	Slice(off, length int64) []byte
}

// File represents an open OpenEXR file for reading.
type File struct {
	reader       io.ReaderAt
	sliceReader  SliceReader // Non-nil if reader supports zero-copy
	size         int64
	versionField uint32
	headers      []*Header
	offsets      [][]int64 // Per-part offset tables
	closer       io.Closer // For cleanup if we own the reader
}

// OpenReader opens an OpenEXR file from an io.ReaderAt.
// The size parameter should be the total size of the file in bytes.
func OpenReader(r io.ReaderAt, size int64) (*File, error) {
	f := &File{
		reader: r,
		size:   size,
	}

	// Check if reader supports zero-copy slice access
	if sr, ok := r.(SliceReader); ok {
		f.sliceReader = sr
	}

	// Read and validate magic number
	buf := make([]byte, 8)
	if _, err := r.ReadAt(buf, 0); err != nil {
		return nil, err
	}

	if buf[0] != MagicNumber[0] || buf[1] != MagicNumber[1] ||
		buf[2] != MagicNumber[2] || buf[3] != MagicNumber[3] {
		return nil, ErrInvalidMagic
	}

	// Parse version field
	f.versionField = xdr.ByteOrder.Uint32(buf[4:8])

	// Check version
	version := Version(f.versionField)
	if version != 2 {
		return nil, ErrUnsupportedVersion
	}

	// Fetch the front of the file, growing until the headers and offset tables
	// parse out of it.
	//
	// This used to read size-8 bytes — the whole file — and refuse anything
	// over maxHeaderSize, so a 4096x4096 float frame was pulled entirely into
	// memory to be opened and an 81 MiB one could not be opened at all. Both
	// defeated the point of the byte-range path: File.ReadRegion would go on
	// to fetch 2% of a file that Open had already fetched in full.
	//
	// A prefix is enough because the header and the offset tables sit at the
	// front, before any pixel data, and the tables are what say where the
	// pixel data is. What cannot be known in advance is how long they are —
	// the header is variable-length and the table's size depends on a chunk
	// count inside it — so the prefix grows and the parse is retried. Parsing
	// is pure and reads no I/O, so a retry costs only the fetch.
	limit := size - 8
	if limit <= 0 {
		return nil, ErrInvalidHeaderSize
	}
	prefix := int64(headerPrefixSize)
	if prefix > limit {
		prefix = limit
	}
	var (
		reader   *xdr.Reader
		parseErr error
	)
	for {
		buf := make([]byte, prefix)
		if _, err := r.ReadAt(buf, 8); err != nil && err != io.EOF {
			return nil, err
		}
		reader = xdr.NewReader(buf)
		f.headers = nil
		f.offsets = nil
		var need int64
		need, parseErr = f.parseHeadersAndOffsets(reader, size)
		if parseErr == nil {
			break
		}
		if prefix >= limit {
			// The whole file has been read and it still does not parse, so
			// the error is the file's rather than the prefix's.
			return nil, parseErr
		}
		// Once the headers themselves have parsed, the offset tables' size is
		// known exactly, so the second fetch is the last one — no doubling
		// towards it and no overshoot. need is zero when even the headers did
		// not fit, and then there is nothing to do but ask for more.
		if need > prefix {
			prefix = need
		} else {
			prefix *= 4
		}
		if prefix > limit {
			prefix = limit
		}
	}

	// A writer that never completed leaves the offset table zeroed even though
	// the chunk data is intact. Rebuild it by scanning the chunks, as the
	// reference implementation does, rather than decoding to silent zeroes.
	// This is a no-op for well-formed files.
	f.reconstructOffsetTables(8 + int64(reader.Pos()))

	return f, nil
}

// parseHeadersAndOffsets reads the header or headers and the per-part chunk
// offset tables out of a prefix of the file.
//
// It returns an error when the prefix is too short as well as when the file is
// malformed, and OpenReader cannot tell those apart — which is why it grows the
// prefix and retries rather than trying to distinguish them. The distinction is
// not needed: a parse that still fails once the whole file has been fetched is
// a malformed file by elimination.
//
// The first return is how many bytes past the magic the parse would need, and
// is meaningful only once the headers themselves have parsed — at which point
// the chunk counts are known and the answer is exact. It is what turns growing
// the prefix into at most one further fetch rather than a doubling search.
func (f *File) parseHeadersAndOffsets(reader *xdr.Reader, size int64) (int64, error) {
	if IsMultiPart(f.versionField) {
		// Multi-part file: read multiple headers
		for {
			h, err := ReadHeader(reader)
			if err != nil {
				return 0, err
			}
			if h == nil || len(h.attrs) == 0 {
				// Empty header terminates multi-part header list
				break
			}
			f.headers = append(f.headers, h)
		}
	} else {
		// Single-part file: read one header
		h, err := ReadHeader(reader)
		if err != nil {
			return 0, err
		}
		f.headers = []*Header{h}
	}

	if len(f.headers) == 0 {
		return 0, ErrInvalidHeader
	}

	// The header itself is still bounded, which is what the DoS check was
	// always meant to do. Applying that bound to the file's size instead is
	// what refused every EXR over 64 MiB.
	headerBytes := int64(reader.Pos())
	if headerBytes > maxHeaderSize {
		return 0, ErrInvalidHeaderSize
	}

	// Read offset table(s).
	//
	// The chunk count is derived from header attributes an attacker controls,
	// so it is bounded by what the file could actually contain rather than by
	// a large fixed ceiling: every chunk costs 8 bytes in the offset table
	// alone, so a file of n bytes cannot describe more than n/8 chunks. That
	// keeps the allocation proportional to the input instead of letting a
	// few-hundred-byte file demand hundreds of megabytes.
	const maxChunksPerPart = 16 * 1024 * 1024 // 16M chunks max per part
	maxChunksForSize := size / 8
	var tableBytes int64
	f.offsets = make([][]int64, len(f.headers))
	for i, h := range f.headers {
		numChunks := h.ChunksInFile()
		if numChunks < 0 || numChunks > maxChunksPerPart {
			return 0, fmt.Errorf("invalid chunk count %d (max %d)", numChunks, maxChunksPerPart)
		}
		if int64(numChunks) > maxChunksForSize {
			return 0, fmt.Errorf("chunk count %d exceeds what a %d-byte file can hold", numChunks, size)
		}
		tableBytes += int64(numChunks) * 8
		offsets := make([]int64, numChunks)
		for j := 0; j < numChunks; j++ {
			offset, err := reader.ReadUint64()
			if err != nil {
				return headerBytes + tableBytes, err
			}
			offsets[j] = int64(offset)
		}
		f.offsets[i] = offsets
	}

	return headerBytes + tableBytes, nil
}

// VersionField returns the file's version field.
func (f *File) VersionField() uint32 {
	return f.versionField
}

// Version returns the file format version number (typically 2).
func (f *File) Version() int {
	return Version(f.versionField)
}

// IsTiled returns true if the file contains tiled images.
func (f *File) IsTiled() bool {
	return IsTiled(f.versionField)
}

// IsDeep returns true if the file contains deep data.
func (f *File) IsDeep() bool {
	return IsDeep(f.versionField)
}

// IsMultiPart returns true if the file is a multi-part file.
func (f *File) IsMultiPart() bool {
	return IsMultiPart(f.versionField)
}

// NumParts returns the number of parts in the file.
func (f *File) NumParts() int {
	return len(f.headers)
}

// Header returns the header for the specified part (0-indexed).
func (f *File) Header(part int) *Header {
	if part < 0 || part >= len(f.headers) {
		return nil
	}
	return f.headers[part]
}

// Offsets returns a copy of the chunk offset table for the specified part.
// For performance-critical code, use OffsetsRef instead.
func (f *File) Offsets(part int) []int64 {
	if part < 0 || part >= len(f.offsets) {
		return nil
	}
	result := make([]int64, len(f.offsets[part]))
	copy(result, f.offsets[part])
	return result
}

// OffsetsRef returns a direct reference to the chunk offset table.
// The caller must not modify the returned slice.
func (f *File) OffsetsRef(part int) []int64 {
	if part < 0 || part >= len(f.offsets) {
		return nil
	}
	return f.offsets[part]
}

// SupportsZeroCopy returns true if the file supports zero-copy slice access.
func (f *File) SupportsZeroCopy() bool {
	return f.sliceReader != nil
}

// Close releases any resources associated with the file.
// After Close is called, the File should not be used.
func (f *File) Close() error {
	var err error
	if f.closer != nil {
		// On Windows, sync before close to ensure file handle is properly released
		if syncer, ok := f.closer.(interface{ Sync() error }); ok {
			syncer.Sync() // Best effort, ignore error
		}
		err = f.closer.Close()
	}
	// Clear all internal references to help GC and ensure
	// Windows can release the file handle immediately
	f.closer = nil
	f.reader = nil
	f.sliceReader = nil
	f.headers = nil
	f.offsets = nil
	return err
}

// ReadChunk reads a chunk at the given offset.
// Returns the y-coordinate (for scanline) or tile coordinates (for tiled)
// and the pixel data.
func (f *File) ReadChunk(part int, chunkIndex int) (int32, []byte, error) {
	if part < 0 || part >= len(f.offsets) {
		return 0, nil, errors.New("exr: invalid part index")
	}
	if chunkIndex < 0 || chunkIndex >= len(f.offsets[part]) {
		return 0, nil, errors.New("exr: invalid chunk index")
	}

	offset := f.offsets[part][chunkIndex]

	// Multipart chunks have a 4-byte part number prefix before the header
	headerSize := int64(8)
	headerStart := int64(0)
	if f.IsMultiPart() {
		headerSize = 12
		headerStart = 4 // skip part number
	}

	// Read chunk header (optionally: part number +) y coordinate + packed size
	chunkHeader := make([]byte, headerSize)
	if _, err := f.reader.ReadAt(chunkHeader, offset); err != nil {
		return 0, nil, err
	}

	y := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart : headerStart+4]))
	packedSize := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+4 : headerStart+8]))

	// Validate packedSize to prevent DoS
	if packedSize < 0 || packedSize > maxChunkSize {
		return 0, nil, ErrInvalidChunkSize
	}

	// Read chunk data
	data := make([]byte, packedSize)
	if _, err := f.reader.ReadAt(data, offset+headerSize); err != nil {
		return 0, nil, err
	}

	return y, data, nil
}

// ReadTileChunk reads a tile chunk at the given index.
// Returns the tile coordinates (tileX, tileY, levelX, levelY) and the pixel data.
func (f *File) ReadTileChunk(part int, chunkIndex int) ([4]int32, []byte, error) {
	if part < 0 || part >= len(f.offsets) {
		return [4]int32{}, nil, errors.New("exr: invalid part index")
	}
	if chunkIndex < 0 || chunkIndex >= len(f.offsets[part]) {
		return [4]int32{}, nil, errors.New("exr: invalid chunk index")
	}

	offset := f.offsets[part][chunkIndex]

	// Multipart chunks have a 4-byte part number prefix before the header
	headerSize := int64(20)
	headerStart := int64(0)
	if f.IsMultiPart() {
		headerSize = 24
		headerStart = 4 // skip part number
	}

	// Read tile chunk header (optionally: part number +) tileX, tileY, levelX, levelY, packedSize
	chunkHeader := make([]byte, headerSize)
	if _, err := f.reader.ReadAt(chunkHeader, offset); err != nil {
		return [4]int32{}, nil, err
	}

	tileX := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart : headerStart+4]))
	tileY := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+4 : headerStart+8]))
	levelX := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+8 : headerStart+12]))
	levelY := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+12 : headerStart+16]))
	packedSize := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+16 : headerStart+20]))

	// Validate packedSize to prevent DoS
	if packedSize < 0 || packedSize > maxChunkSize {
		return [4]int32{}, nil, ErrInvalidChunkSize
	}

	// Read chunk data
	data := make([]byte, packedSize)
	if _, err := f.reader.ReadAt(data, offset+headerSize); err != nil {
		return [4]int32{}, nil, err
	}

	return [4]int32{tileX, tileY, levelX, levelY}, data, nil
}

// Deep chunk header sizes, from the OpenEXR file layout. Both carry three
// uint64 sizes after their coordinates — the packed size of the pixel offset
// (sample count) table, the packed size of the sample data, and the *unpacked*
// size of the sample data. The third is not redundant: nothing else in the
// chunk says how many samples it holds, so a reader cannot size its output
// buffer without it, and OpenEXR refuses a chunk that omits it.
const (
	deepScanlineChunkHeaderSize = 28 // int y + 3 x uint64
	deepTileChunkHeaderSize     = 40 // 4 x int tile coords + 3 x uint64
	multiPartChunkPrefixSize    = 4  // uint32 part number, multi-part files only
)

// ReadDeepChunk reads a deep data chunk at the given index.
// Returns the y-coordinate, sample count table, and pixel data.
// Deep scanline chunk format:
//   - 4 bytes: y coordinate
//   - 8 bytes: packed size of the pixel offset (sample count) table
//   - 8 bytes: packed size of the sample data
//   - 8 bytes: unpacked size of the sample data
//   - compressed sample count table (cumulative counts)
//   - compressed sample data
func (f *File) ReadDeepChunk(part int, chunkIndex int) (int32, []byte, []byte, error) {
	if part < 0 || part >= len(f.offsets) {
		return 0, nil, nil, errors.New("exr: invalid part index")
	}
	if chunkIndex < 0 || chunkIndex >= len(f.offsets[part]) {
		return 0, nil, nil, errors.New("exr: invalid chunk index")
	}

	offset := f.offsets[part][chunkIndex]

	// Multipart chunks have a 4-byte part number prefix before the header
	headerSize := int64(deepScanlineChunkHeaderSize)
	headerStart := int64(0)
	if f.IsMultiPart() {
		headerSize += multiPartChunkPrefixSize
		headerStart = multiPartChunkPrefixSize // skip part number
	}

	// Read deep chunk header (optionally: part number +) y + the three sizes
	chunkHeader := make([]byte, headerSize)
	if _, err := f.reader.ReadAt(chunkHeader, offset); err != nil {
		return 0, nil, nil, err
	}

	y := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart : headerStart+4]))
	packedSampleCountSize := int64(xdr.ByteOrder.Uint64(chunkHeader[headerStart+4 : headerStart+12]))
	packedPixelDataSize := int64(xdr.ByteOrder.Uint64(chunkHeader[headerStart+12 : headerStart+20]))

	// These sizes come straight from the file, so validate before allocating.
	if err := f.validateDeepChunkSizes(packedSampleCountSize, packedPixelDataSize, offset+headerSize); err != nil {
		return 0, nil, nil, err
	}

	// Read sample count table
	sampleCountTable := make([]byte, packedSampleCountSize)
	if _, err := f.reader.ReadAt(sampleCountTable, offset+headerSize); err != nil {
		return 0, nil, nil, err
	}

	// Read pixel data
	pixelData := make([]byte, packedPixelDataSize)
	if _, err := f.reader.ReadAt(pixelData, offset+headerSize+packedSampleCountSize); err != nil {
		return 0, nil, nil, err
	}

	return y, sampleCountTable, pixelData, nil
}

// ReadDeepTileChunk reads a deep tiled data chunk at the given index.
// Returns the tile coordinates, sample count table, and pixel data.
// Deep tile chunk format:
//   - 4 bytes: tile X coordinate
//   - 4 bytes: tile Y coordinate
//   - 4 bytes: level X
//   - 4 bytes: level Y
//   - 8 bytes: packed size of the pixel offset (sample count) table
//   - 8 bytes: packed size of the sample data
//   - 8 bytes: unpacked size of the sample data
//   - compressed sample count table (cumulative counts)
//   - compressed sample data
func (f *File) ReadDeepTileChunk(part int, chunkIndex int) ([4]int32, []byte, []byte, error) {
	if part < 0 || part >= len(f.offsets) {
		return [4]int32{}, nil, nil, errors.New("exr: invalid part index")
	}
	if chunkIndex < 0 || chunkIndex >= len(f.offsets[part]) {
		return [4]int32{}, nil, nil, errors.New("exr: invalid chunk index")
	}

	offset := f.offsets[part][chunkIndex]

	// Multipart chunks have a 4-byte part number prefix before the header
	headerSize := int64(deepTileChunkHeaderSize)
	headerStart := int64(0)
	if f.IsMultiPart() {
		headerSize += multiPartChunkPrefixSize
		headerStart = multiPartChunkPrefixSize // skip part number
	}

	// Read deep tile chunk header (optionally: part number +) the four tile
	// coordinates and the three sizes
	chunkHeader := make([]byte, headerSize)
	if _, err := f.reader.ReadAt(chunkHeader, offset); err != nil {
		return [4]int32{}, nil, nil, err
	}

	tileX := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart : headerStart+4]))
	tileY := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+4 : headerStart+8]))
	levelX := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+8 : headerStart+12]))
	levelY := int32(xdr.ByteOrder.Uint32(chunkHeader[headerStart+12 : headerStart+16]))
	packedSampleCountSize := int64(xdr.ByteOrder.Uint64(chunkHeader[headerStart+16 : headerStart+24]))
	packedPixelDataSize := int64(xdr.ByteOrder.Uint64(chunkHeader[headerStart+24 : headerStart+32]))

	coords := [4]int32{tileX, tileY, levelX, levelY}

	// These sizes come straight from the file, so validate before allocating.
	if err := f.validateDeepChunkSizes(packedSampleCountSize, packedPixelDataSize, offset+headerSize); err != nil {
		return coords, nil, nil, err
	}

	// Read sample count table
	sampleCountTable := make([]byte, packedSampleCountSize)
	if _, err := f.reader.ReadAt(sampleCountTable, offset+headerSize); err != nil {
		return coords, nil, nil, err
	}

	// Read pixel data
	pixelData := make([]byte, packedPixelDataSize)
	if _, err := f.reader.ReadAt(pixelData, offset+headerSize+packedSampleCountSize); err != nil {
		return coords, nil, nil, err
	}

	return coords, sampleCountTable, pixelData, nil
}

// validatePackedSize rejects a chunk payload size that the file cannot contain.
// Every path that allocates from a file-supplied size must go through this;
// several fast paths historically did not, which turned a four-byte header
// field into an arbitrary allocation.
func (f *File) validatePackedSize(packedSize int, dataStart int64) error {
	if packedSize < 0 || int64(packedSize) > maxChunkSize {
		return ErrInvalidChunkSize
	}
	if dataStart < 0 || dataStart > f.size {
		return ErrInvalidChunkSize
	}
	if int64(packedSize) > f.size-dataStart {
		return ErrInvalidChunkSize
	}
	return nil
}

// validateDeepChunkSizes rejects deep-chunk payload sizes that a file cannot
// actually contain. The scanline path has always bounded its packed size this
// way; the deep paths did not, so a corrupt or hostile header could drive
// make([]byte, n) with a negative or absurd n and panic the caller.
func (f *File) validateDeepChunkSizes(sampleCountSize, pixelDataSize, dataStart int64) error {
	if sampleCountSize < 0 || pixelDataSize < 0 {
		return ErrInvalidChunkSize
	}
	if sampleCountSize > maxChunkSize || pixelDataSize > maxChunkSize {
		return ErrInvalidChunkSize
	}
	if dataStart < 0 || dataStart > f.size {
		return ErrInvalidChunkSize
	}
	if sampleCountSize+pixelDataSize > f.size-dataStart {
		return ErrInvalidChunkSize
	}
	return nil
}

// Writer represents an OpenEXR file being written.
type Writer struct {
	writer       io.WriteSeeker
	headers      []*Header
	versionField uint32
	offsets      [][]int64
	chunkIndex   []int
	dataStart    int64 // Position where pixel data starts
	finalized    bool
	multiPart    bool
}

// NewWriter creates a new writer for a single-part file.
func NewWriter(w io.WriteSeeker, h *Header) (*Writer, error) {
	if err := h.Validate(); err != nil {
		return nil, err
	}

	writer := &Writer{
		writer:  w,
		headers: []*Header{h},
	}

	// Build version field
	tiled := h.IsTiled()
	writer.versionField = MakeVersionField(2, tiled, false, false, false)

	// Write magic number and version
	if _, err := w.Write(MagicNumber); err != nil {
		return nil, err
	}

	versionBuf := make([]byte, 4)
	xdr.ByteOrder.PutUint32(versionBuf, writer.versionField)
	if _, err := w.Write(versionBuf); err != nil {
		return nil, err
	}

	// Write header
	headerBuf := xdr.NewBufferWriter(headerBufSize)
	if err := WriteHeader(headerBuf, h); err != nil {
		return nil, err
	}
	if _, err := w.Write(headerBuf.Bytes()); err != nil {
		return nil, err
	}

	// Initialize offset table
	numChunks := h.ChunksInFile()
	writer.offsets = [][]int64{make([]int64, numChunks)}
	writer.chunkIndex = []int{0}

	// Write placeholder offset table
	// We'll update this when the file is finalized
	writer.dataStart, _ = w.Seek(0, io.SeekCurrent)
	offsetPlaceholder := make([]byte, numChunks*8)
	if _, err := w.Write(offsetPlaceholder); err != nil {
		return nil, err
	}

	return writer, nil
}

// sharedAttributes are the attributes that describe the file rather than one
// part of it. OpenEXR's MultiPartOutputFile and MultiPartInputFile both walk
// this list and refuse a file whose parts disagree, so a writer that lets them
// disagree produces a file the reference cannot open. The list is the one in
// ImfMultiPartOutputFile.cpp.
var sharedAttributes = []string{
	AttrNameDisplayWindow,
	AttrNamePixelAspectRatio,
	"timeCode",
	"chromaticities",
}

// checkSharedAttributes reports whether part index disagrees with part 0 about
// any attribute every part must share. Part 0 is compared with itself, which
// is free and keeps the caller simple.
func checkSharedAttributes(first, h *Header, index int) error {
	for _, name := range sharedAttributes {
		a, b := first.Get(name), h.Get(name)
		if a == nil && b == nil {
			continue
		}
		if a == nil || b == nil || a.Type != b.Type || a.Value != b.Value {
			return fmt.Errorf("%w: part %d and part 0 disagree about %q",
				ErrConflictingAttributes, index, name)
		}
	}
	return nil
}

// NewMultiPartWriter creates a new writer for a multi-part file.
// Each header should have a unique "name" attribute and a "type" attribute
// indicating whether it is a scanline or tiled part.
func NewMultiPartWriter(w io.WriteSeeker, headers []*Header) (*Writer, error) {
	if len(headers) == 0 {
		return nil, ErrNoHeaders
	}

	// Validate all headers
	for i, h := range headers {
		if err := h.Validate(); err != nil {
			return nil, fmt.Errorf("exr: header %d validation failed: %w", i, err)
		}
		// The same subsampling contract the scanline writer keeps: a channel
		// with XSampling above 1 narrows each row, which the chunk layout can
		// express, and one with YSampling above 1 removes whole rows from a
		// scanline, which it cannot. A tiled part refuses both through
		// Validate, since the format forbids subsampling in a tiled image
		// entirely.
		if !h.IsTiled() {
			if err := checkNoYSubsampling(h.Channels().Channels()); err != nil {
				return nil, fmt.Errorf("exr: header %d: %w", i, err)
			}
		}
		// Multi-part files require "name" and "type" attributes
		if !h.Has(AttrNameName) {
			return nil, ErrMissingName
		}
		if !h.Has(AttrNameType) {
			// Set default type based on whether it's tiled
			if h.IsTiled() {
				h.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeTiled})
			} else {
				h.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeScanline})
			}
		}
		// Every part of a multi-part file has to agree about the attributes
		// that describe the file rather than the part. The reference
		// implementation refuses such a file outright, on writing and on
		// reading ("Conflicting attributes found for header"), so a part list
		// that disagrees here produces a file nothing else can open.
		if err := checkSharedAttributes(headers[0], h, i); err != nil {
			return nil, err
		}
	}

	writer := &Writer{
		writer:    w,
		headers:   headers,
		multiPart: true,
	}

	// The tiled flag in the version field and the multi-part flag are
	// mutually exclusive. A multi-part file records each part's storage in
	// that part's own type attribute, so the file-wide tiled flag has nothing
	// left to say; OpenEXR treats a file with both set as corrupt and refuses
	// it before reading a single pixel:
	//
	//   EXR_ERR_FILE_BAD_HEADER Invalid combination of version flags: single
	//   part flag found, but also marked as deep (0) or multipart (1)
	//
	// (isTiled() in ImfVersion.h is defined as "tiled and not multi-part" for
	// the same reason.) Setting the flag because some part happened to be
	// tiled made every multi-part file with a tiled part unreadable.
	// A multi-part file states each part's storage in that part's own type
	// attribute, so the tiled flag stays clear — the two are mutually
	// exclusive and OpenEXR rejects the combination. The deep flag is
	// different: it is set when any part is deep, which is what the reference
	// writes and what a reader checks before expecting deep chunk headers.
	anyDeep := false
	for _, h := range headers {
		if headerIsDeep(h) {
			anyDeep = true
			break
		}
	}
	writer.versionField = MakeVersionField(2, false, false, anyDeep, true)

	// Write magic number and version
	if _, err := w.Write(MagicNumber); err != nil {
		return nil, err
	}

	versionBuf := make([]byte, 4)
	xdr.ByteOrder.PutUint32(versionBuf, writer.versionField)
	if _, err := w.Write(versionBuf); err != nil {
		return nil, err
	}

	// Size the offset tables before the headers go out, because each part's
	// header has to declare its own chunk count. The format lists chunkCount
	// as required in every part of a multi-part file, and the reference
	// implementation writes it in every part it produces; a reader that meets
	// a part type it does not recognise has nothing else to go on.
	writer.offsets = make([][]int64, len(headers))
	writer.chunkIndex = make([]int, len(headers))

	totalChunks := 0
	for i, h := range headers {
		numChunks := h.ChunksInFile()
		writer.offsets[i] = make([]int64, numChunks)
		totalChunks += numChunks
		h.Set(&Attribute{Name: AttrNameChunkCount, Type: AttrTypeInt, Value: int32(numChunks)})
	}

	// Write all headers
	for _, h := range headers {
		headerBuf := xdr.NewBufferWriter(headerBufSize)
		if err := WriteHeader(headerBuf, h); err != nil {
			return nil, err
		}
		if _, err := w.Write(headerBuf.Bytes()); err != nil {
			return nil, err
		}
	}

	// Write empty header to terminate header list
	if _, err := w.Write([]byte{0}); err != nil {
		return nil, err
	}

	// Write placeholder offset tables
	writer.dataStart, _ = w.Seek(0, io.SeekCurrent)
	offsetPlaceholder := make([]byte, totalChunks*8)
	if _, err := w.Write(offsetPlaceholder); err != nil {
		return nil, err
	}

	return writer, nil
}

// NumParts returns the number of parts in the file.
func (w *Writer) NumParts() int {
	return len(w.headers)
}

// Header returns the header for the specified part.
func (w *Writer) Header(part int) *Header {
	if part < 0 || part >= len(w.headers) {
		return nil
	}
	return w.headers[part]
}

// IsMultiPart returns true if this is a multi-part file.
func (w *Writer) IsMultiPart() bool {
	return w.multiPart
}

// WriteChunk writes a chunk of pixel data to part 0.
// For scanline files, y is the y-coordinate of the first scanline in the chunk.
// data should be the compressed pixel data.
func (w *Writer) WriteChunk(y int32, data []byte) error {
	return w.WriteChunkPart(0, y, data)
}

// WriteChunkPart writes a chunk of pixel data to the specified part.
// For scanline files, y is the y-coordinate of the first scanline in the chunk.
// data should be the compressed pixel data.
//
// The chunk's offset is recorded in the next free slot, so chunks arriving in
// increasing y order fill the table in order. A writer emitting them in another
// order — which DECREASING_Y requires — must say which slot it means; see
// WriteChunkPartAt.
func (w *Writer) WriteChunkPart(part int, y int32, data []byte) error {
	if part < 0 || part >= len(w.offsets) {
		return errors.New("exr: invalid part index")
	}
	return w.WriteChunkPartAt(part, w.chunkIndex[part], y, data)
}

// WriteChunkPartAt writes a chunk and records its offset in a given slot of the
// part's chunk offset table.
//
// The offset table is always ordered by increasing y whatever the file's line
// order is: it is the index a reader seeks with. lineOrder describes the order
// the chunks are laid out in the file, not the order of the table. Writing a
// DECREASING_Y file therefore means emitting the chunks back to front while
// still filling the table front to back, and that needs the slot stated rather
// than inferred from arrival.
func (w *Writer) WriteChunkPartAt(part int, idx int, y int32, data []byte) error {
	if w.finalized {
		return errors.New("exr: cannot write to finalized file")
	}
	if part < 0 || part >= len(w.offsets) {
		return errors.New("exr: invalid part index")
	}
	if idx < 0 || idx >= len(w.offsets[part]) {
		return fmt.Errorf("exr: chunk slot %d is outside the part's %d chunks",
			idx, len(w.offsets[part]))
	}

	// Record the offset
	offset, err := w.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.offsets[part][idx] = offset

	// For multi-part files, chunk header includes part number
	if w.multiPart {
		// Multi-part chunk header: part number (4 bytes) + y (4 bytes) + size (4 bytes)
		chunkHeader := make([]byte, 12)
		xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(part))
		xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(y))
		xdr.ByteOrder.PutUint32(chunkHeader[8:12], uint32(len(data)))
		if _, err := w.writer.Write(chunkHeader); err != nil {
			return err
		}
	} else {
		// Single-part chunk header: y (4 bytes) + size (4 bytes)
		chunkHeader := make([]byte, 8)
		xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(y))
		xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(len(data)))
		if _, err := w.writer.Write(chunkHeader); err != nil {
			return err
		}
	}

	// Write chunk data
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	w.chunkIndex[part]++
	return w.finalizeIfComplete()
}

// WriteTileChunk writes a tile chunk of pixel data to part 0.
// For tiled files, tileX and tileY are the tile coordinates.
// levelX and levelY are the mipmap level coordinates (0,0 for single-level).
// data should be the compressed pixel data.
func (w *Writer) WriteTileChunk(tileX, tileY, levelX, levelY int, data []byte) error {
	return w.WriteTileChunkPart(0, tileX, tileY, levelX, levelY, data)
}

// tileChunkIndex returns the slot a tile's offset occupies in a tiled part's
// chunk offset table.
//
// The format indexes that table by tile coordinate, not by the order the tiles
// were written: level major — for a ripmap, y level major — then tile row, then
// tile column. A reader looks a tile up by coordinate and then checks that the
// chunk it lands on names that tile, so recording offsets in write order
// produces a file that reads back only if the caller happened to write in the
// canonical order, and is otherwise rejected outright:
//
//	(EXR_ERR_BAD_CHUNK_LEADER) Corrupt tile (0, 0), level (0, 0) (chunk 0):
//	bad tile x coordinate (2, expect 0)
//
// The reference implementation lets an application emit tiles in any order for
// exactly this reason, and scripts/validate.sh writes three fixtures in reverse
// order to hold this to it.
func tileChunkIndex(h *Header, tileX, tileY, levelX, levelY int) (int, error) {
	td := h.TileDescription()
	if td == nil {
		return 0, ErrNotTiled
	}
	numXLevels, numYLevels := h.NumXLevels(), h.NumYLevels()
	if levelX < 0 || levelX >= numXLevels || levelY < 0 || levelY >= numYLevels {
		return 0, ErrLevelOutOfRange
	}

	base := 0
	switch td.Mode {
	case LevelModeOne:
		if levelX != 0 || levelY != 0 {
			return 0, ErrLevelOutOfRange
		}
	case LevelModeMipmap:
		if levelX != levelY {
			return 0, ErrLevelOutOfRange
		}
		for l := 0; l < levelX; l++ {
			base += h.NumXTiles(l) * h.NumYTiles(l)
		}
	case LevelModeRipmap:
		// Levels are numbered ly*numXLevels + lx, so every level before this
		// one contributes its own tile count.
		for l := levelY*numXLevels + levelX - 1; l >= 0; l-- {
			base += h.NumXTiles(l%numXLevels) * h.NumYTiles(l/numXLevels)
		}
	default:
		return 0, ErrNotTiled
	}

	numX, numY := h.NumXTiles(levelX), h.NumYTiles(levelY)
	if tileX < 0 || tileX >= numX || tileY < 0 || tileY >= numY {
		return 0, ErrTileOutOfRange
	}
	return base + tileY*numX + tileX, nil
}

// WriteTileChunkPart writes a tile chunk of pixel data to the specified part.
// For tiled files, tileX and tileY are the tile coordinates.
// levelX and levelY are the mipmap level coordinates (0,0 for single-level).
// data should be the compressed pixel data.
//
// Tiles may be written in any order. The offset recorded for a tile is placed
// in the slot the format assigns to its coordinates, not in the slot the write
// happens to arrive in; see tileChunkIndex.
func (w *Writer) WriteTileChunkPart(part, tileX, tileY, levelX, levelY int, data []byte) error {
	if w.finalized {
		return errors.New("exr: cannot write to finalized file")
	}
	if part < 0 || part >= len(w.offsets) {
		return errors.New("exr: invalid part index")
	}

	if w.chunkIndex[part] >= len(w.offsets[part]) {
		return errors.New("exr: too many chunks written")
	}

	idx := w.chunkIndex[part]
	if part < len(w.headers) && w.headers[part] != nil && w.headers[part].IsTiled() {
		var err error
		if idx, err = tileChunkIndex(w.headers[part], tileX, tileY, levelX, levelY); err != nil {
			return err
		}
	}
	if idx < 0 || idx >= len(w.offsets[part]) {
		return errors.New("exr: tile is outside the chunk offset table")
	}

	// Record the offset
	offset, err := w.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.offsets[part][idx] = offset

	// For multi-part files, chunk header includes part number
	if w.multiPart {
		// Multi-part tile chunk header: part number (4 bytes) + tileX, tileY, levelX, levelY, packedSize (20 bytes)
		chunkHeader := make([]byte, 24)
		xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(part))
		xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(tileX))
		xdr.ByteOrder.PutUint32(chunkHeader[8:12], uint32(tileY))
		xdr.ByteOrder.PutUint32(chunkHeader[12:16], uint32(levelX))
		xdr.ByteOrder.PutUint32(chunkHeader[16:20], uint32(levelY))
		xdr.ByteOrder.PutUint32(chunkHeader[20:24], uint32(len(data)))
		if _, err := w.writer.Write(chunkHeader); err != nil {
			return err
		}
	} else {
		// Single-part tile chunk header: tileX, tileY, levelX, levelY, packedSize (20 bytes)
		chunkHeader := make([]byte, 20)
		xdr.ByteOrder.PutUint32(chunkHeader[0:4], uint32(tileX))
		xdr.ByteOrder.PutUint32(chunkHeader[4:8], uint32(tileY))
		xdr.ByteOrder.PutUint32(chunkHeader[8:12], uint32(levelX))
		xdr.ByteOrder.PutUint32(chunkHeader[12:16], uint32(levelY))
		xdr.ByteOrder.PutUint32(chunkHeader[16:20], uint32(len(data)))
		if _, err := w.writer.Write(chunkHeader); err != nil {
			return err
		}
	}

	// Write chunk data
	if _, err := w.writer.Write(data); err != nil {
		return err
	}

	w.chunkIndex[part]++
	return w.finalizeIfComplete()
}

// finalizeIfComplete writes the offset table as soon as every chunk the header
// promised has been written.
//
// The table lives immediately after the header and can only be filled in once
// the chunk offsets are known, so it used to be written by Close alone. A
// caller who never called Close left a file whose table was all zeros: this
// library's own reader recovered by scanning, and the reference implementation
// did not, which is what made issue #4 look like a read bug and cost the
// reporter real time.
//
// Writing it here means a file holding every chunk it declares is complete the
// moment the last one lands, whether or not Close follows. Close still exists,
// and still finalizes a file that is deliberately short.
func (w *Writer) finalizeIfComplete() error {
	if w.finalized {
		return nil
	}
	for part, offsets := range w.offsets {
		if part >= len(w.chunkIndex) || w.chunkIndex[part] < len(offsets) {
			return nil
		}
	}
	pos, err := w.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if err := w.writeOffsetTable(); err != nil {
		return err
	}
	// Writing the table leaves the position inside the header; put it back, so
	// a caller that keeps writing is not silently redirected there. Close is
	// not used for this: it also drops the writer's references, which would
	// break a writer that is still in use.
	_, err = w.writer.Seek(pos, io.SeekStart)
	return err
}

// writeOffsetTable seeks to the table's place after the header and fills in
// every chunk offset recorded so far.
func (w *Writer) writeOffsetTable() error {
	if _, err := w.writer.Seek(w.dataStart, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, 8)
	for _, offsets := range w.offsets {
		for _, offset := range offsets {
			xdr.ByteOrder.PutUint64(buf, uint64(offset))
			if _, err := w.writer.Write(buf); err != nil {
				return err
			}
		}
	}
	w.finalized = true
	return nil
}

// Close finalizes the file by writing the offset table.
// After Close is called, the Writer should not be used.
func (w *Writer) Close() error {
	if w.finalized {
		return nil
	}

	if err := w.writeOffsetTable(); err != nil {
		return err
	}

	// Sync the data to ensure it's flushed to disk
	if syncer, ok := w.writer.(interface{ Sync() error }); ok {
		if err := syncer.Sync(); err != nil {
			// Log but continue - this is a best-effort operation
		}
	}

	// Clear references to help GC and assist Windows file handle release.
	// Note: The caller is responsible for closing the underlying writer.
	// Do not call Close() here as it would close the file prematurely before
	// the caller has a chance to do so (e.g., in ScanlineWriter.Close()).
	w.writer = nil
	w.headers = nil
	w.offsets = nil

	return nil
}
