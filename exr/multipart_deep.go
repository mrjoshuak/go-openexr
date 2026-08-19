package exr

import (
	"errors"
	"fmt"
	"io"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Deep parts of a multi-part file.
//
// A deep chunk carries a different header from a flat one — four fields rather
// than three, because nothing else in the chunk says how many samples it holds
// — so it cannot go through WriteChunkPart. The packing is not duplicated: the
// bytes come from DeepScanlineWriter.deepChunkBody, and only the framing
// differs.

// WriteDeepChunkPart writes one deep scanline chunk into a part of a
// multi-part file, recording its offset in that part's chunk offset table.
//
// The chunk is the part number, then the deep scanline chunk header — y, the
// packed size of the sample count table, the packed size of the sample data and
// its unpacked size — then the two payloads. The unpacked size is the field
// this library omitted for its whole life before it was measured: the reference
// needs it to size its decompression buffer, and without it reports "Some
// scanline chunks were missing or corrupted".
func (w *Writer) WriteDeepChunkPart(part int, y int32, countTable, pixelData []byte, unpackedSize uint64) error {
	if w.finalized {
		return errors.New("exr: cannot write to finalized file")
	}
	if part < 0 || part >= len(w.offsets) {
		return errors.New("exr: invalid part index")
	}

	idx := w.chunkIndex[part]
	if idx >= len(w.offsets[part]) {
		return errors.New("exr: too many chunks written")
	}

	offset, err := w.writer.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	w.offsets[part][idx] = offset

	hdrSize := deepScanlineChunkHeaderSize
	prefix := 0
	if w.multiPart {
		prefix = 4
	}
	hdr := make([]byte, prefix+hdrSize)
	if w.multiPart {
		xdr.ByteOrder.PutUint32(hdr[0:4], uint32(part))
	}
	xdr.ByteOrder.PutUint32(hdr[prefix:prefix+4], uint32(y))
	xdr.ByteOrder.PutUint64(hdr[prefix+4:prefix+12], uint64(len(countTable)))
	xdr.ByteOrder.PutUint64(hdr[prefix+12:prefix+20], uint64(len(pixelData)))
	xdr.ByteOrder.PutUint64(hdr[prefix+20:prefix+28], unpackedSize)

	if _, err := w.writer.Write(hdr); err != nil {
		return err
	}
	if _, err := w.writer.Write(countTable); err != nil {
		return err
	}
	if _, err := w.writer.Write(pixelData); err != nil {
		return err
	}

	w.chunkIndex[part]++
	return w.finalizeIfComplete()
}

// SetDeepFrameBuffer attaches a deep frame buffer to one part.
//
// A part is deep or flat according to its header's type attribute, and the two
// take different frame buffers; calling this on a flat part is an error rather
// than a silent no-op.
func (m *MultiPartOutputFile) SetDeepFrameBuffer(part int, fb *DeepFrameBuffer) error {
	if part < 0 || part >= len(m.parts) {
		return errors.New("exr: invalid part index")
	}
	p := m.parts[part]
	if !headerIsDeep(p.header) {
		return fmt.Errorf("exr: part %d is not a deep part", part)
	}
	if fb == nil {
		return errors.New("exr: nil deep frame buffer")
	}
	p.deepFB = fb
	return nil
}

// WriteDeepPixels writes numScanlines of a deep scanline part.
//
// Deep data is limited to NONE, RLE and ZIPS, all of which store one scanline
// per chunk, so each line written is a chunk. Anything else is refused by
// Header.Validate before a byte is written.
func (m *MultiPartOutputFile) WriteDeepPixels(part int, numScanlines int) error {
	if part < 0 || part >= len(m.parts) {
		return errors.New("exr: invalid part index")
	}
	p := m.parts[part]
	if !headerIsDeep(p.header) {
		return fmt.Errorf("exr: part %d is not a deep part", part)
	}
	if p.deepFB == nil {
		return fmt.Errorf("exr: part %d has no deep frame buffer", part)
	}
	if numScanlines <= 0 {
		return nil
	}

	h := p.header
	dw := h.DataWindow()
	width := int(dw.Width())
	comp := h.Compression()
	if !IsDeepCompressionSupported(comp) {
		return fmt.Errorf("%w: %v", ErrDeepNotSupported, comp)
	}

	// A deep scanline part borrows the single-part writer purely for its
	// packing: it never writes through it, so the two cannot disagree about
	// the chunk's contents.
	packer := &DeepScanlineWriter{
		header:     h,
		fb:         p.deepFB,
		dataWindow: dw,
		// The channel list has to be set explicitly: getSortedChannels reads
		// this field and not the header, and returns nil when it is unset —
		// which produced a well-formed chunk holding no samples at all, with
		// its packed and unpacked sizes both zero. The reference reported
		// "Some scanline chunks were missing or corrupted" for every part of
		// the file, not just the deep one.
		channels: h.Channels(),
	}
	channels := packer.getSortedChannels()
	if len(channels) == 0 {
		return fmt.Errorf("exr: part %d declares no channels", part)
	}

	for i := 0; i < numScanlines; i++ {
		y := p.currentY
		if y > int(dw.Max.Y) {
			return errors.New("exr: too many scanlines written")
		}
		countTable, pixelData, unpacked, err := packer.deepChunkBody(y, 1, width, channels, comp)
		if err != nil {
			return fmt.Errorf("exr: part %d scanline %d: %w", part, y, err)
		}
		if err := m.writer.WriteDeepChunkPart(part, int32(y), countTable, pixelData, unpacked); err != nil {
			return err
		}
		p.currentY++
	}
	return nil
}

// headerIsDeep reports whether a part's type attribute names a deep storage.
func headerIsDeep(h *Header) bool {
	if h == nil {
		return false
	}
	attr := h.Get(AttrNameType)
	if attr == nil {
		return false
	}
	switch attr.Value {
	case PartTypeDeepScanline, PartTypeDeepTiled:
		return true
	}
	return false
}
