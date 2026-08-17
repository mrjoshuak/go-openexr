package exr

import (
	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Chunk offset table reconstruction.
//
// The chunk offset table is written last, because every entry is a file
// position that is only known once the chunk has been written. A writer that
// crashes, is killed, or is simply never closed therefore leaves a table of
// zeroes on an otherwise complete file. This is common enough in practice —
// interrupted renders, aborted transfers, callers who forget to Close — that
// the OpenEXR reference implementation rebuilds the table by walking the chunk
// data (reconstructLineOffsets and reconstructChunkOffsetTable) rather than
// failing.
//
// go-openexr previously trusted the table verbatim, so such a file decoded to
// silent zeroes. Matching the reference here is deliberate: the goal is to
// accept every input a conforming reader accepts, while still writing only
// conforming output. Where the two conflict, correctness of what we produce
// wins; where they do not, being lenient about what we consume costs nothing.

// chunkHeaderKind describes how a part's chunk headers are laid out on disk.
type chunkHeaderKind int

const (
	chunkKindScanline chunkHeaderKind = iota
	chunkKindTiled
	chunkKindDeepScanline
	chunkKindDeepTiled
)

// partChunkKind reports the on-disk chunk layout for a part.
func partChunkKind(h *Header) chunkHeaderKind {
	partType := ""
	if attr := h.Get(AttrNameType); attr != nil {
		if s, ok := attr.Value.(string); ok {
			partType = s
		}
	}

	switch partType {
	case PartTypeDeepScanline:
		return chunkKindDeepScanline
	case PartTypeDeepTiled:
		return chunkKindDeepTiled
	case PartTypeTiled:
		return chunkKindTiled
	case PartTypeScanline:
		return chunkKindScanline
	}

	// No explicit type attribute: single-part files infer it from the presence
	// of a tile description.
	if h.IsTiled() {
		return chunkKindTiled
	}
	return chunkKindScanline
}

// offsetTableIsComplete reports whether every entry of a part's offset table
// points somewhere plausible. The reference treats a single zero entry as
// grounds to rebuild the whole table, and so do we; entries outside the file
// are equally unusable.
func (f *File) offsetTableIsComplete(part int, firstChunkOffset int64) bool {
	for _, off := range f.offsets[part] {
		if off < firstChunkOffset || off >= f.size {
			return false
		}
	}
	return true
}

// reconstructOffsetTables rebuilds any offset table that is missing or
// implausible by scanning the chunks themselves. Tables that already look
// sound are left untouched, so a well-formed file pays nothing for this.
//
// A file that is genuinely truncated mid-chunk keeps whatever entries were
// recovered before the damage; the remaining chunks stay unreadable, but the
// readable prefix becomes available instead of the whole file being lost.
func (f *File) reconstructOffsetTables(firstChunkOffset int64) {
	needed := false
	for part := range f.offsets {
		if len(f.offsets[part]) > 0 && !f.offsetTableIsComplete(part, firstChunkOffset) {
			needed = true
			break
		}
	}
	if !needed {
		return
	}

	// Rebuild into scratch tables rather than in place: if the scan cannot
	// recover a particular entry, the original value is kept. A wrong offset
	// that some other code path copes with is still better than a zero we
	// substituted. The chunk count is bounded against the file size at open
	// time, so this second table is proportional to the input.
	rebuilt := make([][]int64, len(f.offsets))
	filled := make([]int, len(f.offsets))
	for part := range f.offsets {
		if f.offsetTableIsComplete(part, firstChunkOffset) {
			continue
		}
		rebuilt[part] = make([]int64, len(f.offsets[part]))
	}

	multi := f.IsMultiPart()
	pos := firstChunkOffset
	// Large enough for the widest chunk header: deep tiled is 4 int32 tile
	// coordinates plus three uint64 sizes.
	hdr := make([]byte, 40)

	for pos < f.size {
		chunkStart := pos
		part := 0

		if multi {
			if _, err := f.reader.ReadAt(hdr[:4], pos); err != nil {
				break
			}
			part = int(int32(xdr.ByteOrder.Uint32(hdr[:4])))
			if part < 0 || part >= len(f.headers) {
				break
			}
			pos += 4
		}

		var payload int64
		switch partChunkKind(f.headers[part]) {
		case chunkKindScanline:
			if _, err := f.reader.ReadAt(hdr[:8], pos); err != nil {
				return
			}
			y := int32(xdr.ByteOrder.Uint32(hdr[0:4]))
			size := int32(xdr.ByteOrder.Uint32(hdr[4:8]))
			if size < 0 || int64(size) > maxChunkSize {
				return
			}
			pos += 8
			payload = int64(size)
			// Index from the scanline's own y coordinate rather than from file
			// order. That is correct for both line orders and survives chunks
			// written out of order, which sequential counting would not.
			if idx, ok := scanlineChunkIndex(f.headers[part], y); ok {
				recordOffset(rebuilt[part], idx, chunkStart, &filled[part])
			} else {
				recordOffset(rebuilt[part], filled[part], chunkStart, &filled[part])
			}

		case chunkKindTiled:
			if _, err := f.reader.ReadAt(hdr[:20], pos); err != nil {
				return
			}
			size := int32(xdr.ByteOrder.Uint32(hdr[16:20]))
			if size < 0 || int64(size) > maxChunkSize {
				return
			}
			pos += 20
			payload = int64(size)
			recordOffset(rebuilt[part], filled[part], chunkStart, &filled[part])

		case chunkKindDeepScanline:
			// y, then packed offset-table size, packed and unpacked sample sizes.
			if _, err := f.reader.ReadAt(hdr[:28], pos); err != nil {
				return
			}
			packedTable := int64(xdr.ByteOrder.Uint64(hdr[4:12]))
			packedData := int64(xdr.ByteOrder.Uint64(hdr[12:20]))
			if packedTable < 0 || packedData < 0 ||
				packedTable > maxChunkSize || packedData > maxChunkSize {
				return
			}
			pos += 28
			payload = packedTable + packedData
			recordOffset(rebuilt[part], filled[part], chunkStart, &filled[part])

		case chunkKindDeepTiled:
			if _, err := f.reader.ReadAt(hdr[:40], pos); err != nil {
				return
			}
			packedTable := int64(xdr.ByteOrder.Uint64(hdr[16:24]))
			packedData := int64(xdr.ByteOrder.Uint64(hdr[24:32]))
			if packedTable < 0 || packedData < 0 ||
				packedTable > maxChunkSize || packedData > maxChunkSize {
				return
			}
			pos += 40
			payload = packedTable + packedData
			recordOffset(rebuilt[part], filled[part], chunkStart, &filled[part])
		}

		if payload < 0 || pos+payload > f.size {
			break
		}
		pos += payload
	}

	// Adopt only the entries the scan actually recovered.
	for part, table := range rebuilt {
		if table == nil {
			continue
		}
		for i, off := range table {
			if off > 0 {
				f.offsets[part][i] = off
			}
		}
	}
}

// recordOffset stores off at idx when idx is in range, and advances the
// fill counter used as a fallback index.
func recordOffset(table []int64, idx int, off int64, filled *int) {
	if idx >= 0 && idx < len(table) && table[idx] == 0 {
		table[idx] = off
	}
	*filled++
}

// scanlineChunkIndex maps a scanline y coordinate to its chunk index.
func scanlineChunkIndex(h *Header, y int32) (int, bool) {
	dw := h.DataWindow()
	if y < dw.Min.Y || y > dw.Max.Y {
		return 0, false
	}
	linesPerChunk := h.Compression().ScanlinesPerChunk()
	if linesPerChunk <= 0 {
		return 0, false
	}
	return int(y-dw.Min.Y) / linesPerChunk, true
}
