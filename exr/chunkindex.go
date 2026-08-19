package exr

import (
	"errors"
	"fmt"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// ChunkRange locates one chunk's bytes inside the file, without reading or
// decompressing its pixel data.
//
// Offset is from the start of the file and Length counts the chunk's header as
// well as its data, so the pair is directly a byte range to fetch — an HTTP
// Range header, or one call to ReadAt.
type ChunkRange struct {
	// Offset is where the chunk begins, from the start of the file.
	Offset int64
	// Length is the whole chunk: its header and its compressed data.
	Length int64
	// DataOffset is where the compressed data begins, past the chunk header.
	DataOffset int64
	// DataLength is the compressed size, which is what the header declares.
	DataLength int64
	// Y is the first scanline of a scanline chunk, and is unset for a tile.
	Y int32
	// TileX, TileY, LevelX and LevelY locate a tile chunk, and are unset for a
	// scanline chunk.
	TileX, TileY, LevelX, LevelY int
}

// chunkHeaderLayout returns how many bytes of chunk header precede the
// compressed data, and how far into that header the first field sits.
//
// A multi-part chunk carries a four-byte part number before everything else,
// and a tile chunk carries four int32 coordinates where a scanline chunk
// carries one y.
func (f *File) chunkHeaderLayout(part int) (headerSize, fieldStart int64) {
	fieldStart = 0
	if f.IsMultiPart() {
		fieldStart = 4
	}
	if f.partIsTiled(part) {
		// tileX, tileY, levelX, levelY, then the packed size.
		return fieldStart + 20, fieldStart
	}
	// y, then the packed size.
	return fieldStart + 8, fieldStart
}

// partIsTiled reports whether one part stores tiles, which decides the chunk
// header's shape.
func (f *File) partIsTiled(part int) bool {
	if h := f.Header(part); h != nil {
		if !f.IsMultiPart() {
			return f.IsTiled()
		}
		return h.IsTiled()
	}
	return f.IsTiled()
}

// ChunkRange returns where one chunk sits in the file and how long it is,
// reading only the chunk's header.
//
// This is the outer half of the index a sequence player needs. The offset table
// locates a chunk, and for an HTJ2K chunk go-jpeg2000's packet index locates
// the packets inside it; together they map a frame's byte ranges from a few
// kilobytes read near the front of the file, rather than from a walk over all
// of it. Nothing is decompressed here.
func (f *File) ChunkRange(part, chunkIndex int) (ChunkRange, error) {
	if part < 0 || part >= len(f.offsets) {
		return ChunkRange{}, errors.New("exr: invalid part index")
	}
	if chunkIndex < 0 || chunkIndex >= len(f.offsets[part]) {
		return ChunkRange{}, errors.New("exr: invalid chunk index")
	}

	offset := f.offsets[part][chunkIndex]
	if offset <= 0 {
		return ChunkRange{}, fmt.Errorf("exr: chunk %d of part %d has no offset; "+
			"the file's chunk offset table is incomplete", chunkIndex, part)
	}

	headerSize, fieldStart := f.chunkHeaderLayout(part)
	hdr := make([]byte, headerSize)
	if _, err := f.reader.ReadAt(hdr, offset); err != nil {
		return ChunkRange{}, err
	}

	cr := ChunkRange{Offset: offset, DataOffset: offset + headerSize}
	if f.partIsTiled(part) {
		cr.TileX = int(int32(xdr.ByteOrder.Uint32(hdr[fieldStart : fieldStart+4])))
		cr.TileY = int(int32(xdr.ByteOrder.Uint32(hdr[fieldStart+4 : fieldStart+8])))
		cr.LevelX = int(int32(xdr.ByteOrder.Uint32(hdr[fieldStart+8 : fieldStart+12])))
		cr.LevelY = int(int32(xdr.ByteOrder.Uint32(hdr[fieldStart+12 : fieldStart+16])))
		cr.DataLength = int64(int32(xdr.ByteOrder.Uint32(hdr[fieldStart+16 : fieldStart+20])))
	} else {
		cr.Y = int32(xdr.ByteOrder.Uint32(hdr[fieldStart : fieldStart+4]))
		cr.DataLength = int64(int32(xdr.ByteOrder.Uint32(hdr[fieldStart+4 : fieldStart+8])))
	}

	if cr.DataLength < 0 || cr.DataLength > maxChunkSize {
		return ChunkRange{}, ErrInvalidChunkSize
	}
	cr.Length = headerSize + cr.DataLength
	return cr, nil
}

// NumChunks returns how many chunks a part holds, which is the length of its
// chunk offset table.
func (f *File) NumChunks(part int) int {
	if part < 0 || part >= len(f.offsets) {
		return 0
	}
	return len(f.offsets[part])
}

// ChunkRanges returns the range of every chunk of one part, in chunk order.
//
// A caller planning reads over a network wants the whole table at once: the
// point of the offset table is that a few kilobytes near the front of the file
// describe the layout of all of it.
func (f *File) ChunkRanges(part int) ([]ChunkRange, error) {
	n := f.NumChunks(part)
	if n == 0 {
		return nil, nil
	}
	out := make([]ChunkRange, 0, n)
	for i := 0; i < n; i++ {
		cr, err := f.ChunkRange(part, i)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
		out = append(out, cr)
	}
	return out, nil
}

// ChunksForScanlines returns the chunks of a scanline part that hold any of the
// scanlines in [y0, y1], with their byte ranges.
//
// This is what turns "I need rows 900 to 1000" into a set of reads. A chunk
// holds several scanlines for most compressions, so the answer covers at least
// the requested rows and never fewer.
func (f *File) ChunksForScanlines(part int, y0, y1 int32) ([]ChunkRange, error) {
	if y1 < y0 {
		y0, y1 = y1, y0
	}
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}
	if f.partIsTiled(part) {
		return nil, errors.New("exr: part is tiled; use ChunksForRegion")
	}

	perChunk := int32(h.Compression().ScanlinesPerChunk())
	if perChunk <= 0 {
		perChunk = 1
	}
	dw := h.DataWindow()

	var out []ChunkRange
	for i := 0; i < f.NumChunks(part); i++ {
		cr, err := f.ChunkRange(part, i)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
		// A chunk covers [cr.Y, cr.Y+perChunk), clipped to the data window.
		last := cr.Y + perChunk - 1
		if last > dw.Max.Y {
			last = dw.Max.Y
		}
		if last < y0 || cr.Y > y1 {
			continue
		}
		out = append(out, cr)
	}
	return out, nil
}

// ChunksForRegion returns the chunks of a tiled part whose tiles intersect the
// pixel rectangle [x0, x1) x [y0, y1) at the given resolution level, with their
// byte ranges.
//
// Only tiles at that level are returned: a viewport at a chosen resolution is
// the query a player makes, and the other levels are a different image.
func (f *File) ChunksForRegion(part int, x0, y0, x1, y1 int32, levelX, levelY int) ([]ChunkRange, error) {
	h := f.Header(part)
	if h == nil {
		return nil, errors.New("exr: invalid part index")
	}
	if !f.partIsTiled(part) {
		return nil, errors.New("exr: part is not tiled; use ChunksForScanlines")
	}
	td := h.TileDescription()
	if td == nil || td.XSize == 0 || td.YSize == 0 {
		return nil, errors.New("exr: tiled part has no usable tile description")
	}

	dw := h.DataWindow()
	tw, th := int32(td.XSize), int32(td.YSize)

	var out []ChunkRange
	for i := 0; i < f.NumChunks(part); i++ {
		cr, err := f.ChunkRange(part, i)
		if err != nil {
			return nil, fmt.Errorf("chunk %d: %w", i, err)
		}
		if cr.LevelX != levelX || cr.LevelY != levelY {
			continue
		}
		// The tile's rectangle, in the level's own coordinates, offset by the
		// data window's origin as every reader addresses it.
		tx0 := dw.Min.X + int32(cr.TileX)*tw
		ty0 := dw.Min.Y + int32(cr.TileY)*th
		if tx0 >= x1 || tx0+tw <= x0 || ty0 >= y1 || ty0+th <= y0 {
			continue
		}
		out = append(out, cr)
	}
	return out, nil
}
