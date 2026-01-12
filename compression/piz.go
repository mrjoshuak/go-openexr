// Package compression provides compression algorithms for OpenEXR files.
package compression

import (
	"encoding/binary"
	"errors"
	"sync"
)

// PIZ compression for OpenEXR files.
// PIZ uses Huffman coding with wavelet transformation for high compression ratios.
// It's designed specifically for 16-bit half-float image data.

var (
	ErrPIZCorrupted = errors.New("compression: corrupted PIZ data")
	ErrPIZOverflow  = errors.New("compression: PIZ decode overflow")
)

const (
	// Bitmap size for tracking which values are used
	pizBitmapSize = 65536 / 8 // 8KB for 64K possible 16-bit values

	// Huffman table encoding constants (matching OpenEXR spec)
	hufEncBits       = 6                                      // Bits per code length in packed format
	shortZeroCodeRun = 59                                     // Codes 59-62 encode runs of 2-5 zeros
	longZeroCodeRun  = 63                                     // Code 63 followed by 8-bit count for longer runs
	shortestLongRun  = 2 + longZeroCodeRun - shortZeroCodeRun // = 6
	longestLongRun   = 255 + shortestLongRun                  // = 261

	// Huffman encoding size (65536 values + 1 RLE symbol)
	hufEncSize = 65536 + 1
)

// pizWorkBuffer holds reusable buffers for PIZ compression.
type pizWorkBuffer struct {
	bitmap  []byte   // 8KB bitmap
	forward []uint16 // 128KB forward lookup
	inverse []uint16 // 128KB inverse lookup
}

var pizWorkPool = sync.Pool{
	New: func() any {
		return &pizWorkBuffer{
			bitmap:  make([]byte, pizBitmapSize),
			forward: make([]uint16, 65536),
			inverse: make([]uint16, 65536),
		}
	},
}

// pizDecodedBuffer is a growable buffer for decoded PIZ data.
type pizDecodedBuffer struct {
	data []uint16
}

var pizDecodedPool = sync.Pool{
	New: func() any {
		return &pizDecodedBuffer{
			data: nil, // Allocated on demand
		}
	},
}

// applyLut applies a lookup table to the data in-place, processing 8 values at a time.
// Uses a local buffer to break dependency chains, enabling instruction-level parallelism.
// This matches the C++ optimization in internal_piz.c.
func applyLut(lut []uint16, data []uint16) {
	n := len(data)
	i := 0

	// Process 8 values at a time using local buffer to break dependencies
	// The compiler can issue parallel LUT lookups when reads and writes don't alias
	var tmp [8]uint16
	for ; i+8 <= n; i += 8 {
		// Load values into local buffer (breaks dependency chain)
		tmp[0] = data[i+0]
		tmp[1] = data[i+1]
		tmp[2] = data[i+2]
		tmp[3] = data[i+3]
		tmp[4] = data[i+4]
		tmp[5] = data[i+5]
		tmp[6] = data[i+6]
		tmp[7] = data[i+7]

		// Apply LUT (can execute in parallel)
		tmp[0] = lut[tmp[0]]
		tmp[1] = lut[tmp[1]]
		tmp[2] = lut[tmp[2]]
		tmp[3] = lut[tmp[3]]
		tmp[4] = lut[tmp[4]]
		tmp[5] = lut[tmp[5]]
		tmp[6] = lut[tmp[6]]
		tmp[7] = lut[tmp[7]]

		// Store results
		data[i+0] = tmp[0]
		data[i+1] = tmp[1]
		data[i+2] = tmp[2]
		data[i+3] = tmp[3]
		data[i+4] = tmp[4]
		data[i+5] = tmp[5]
		data[i+6] = tmp[6]
		data[i+7] = tmp[7]
	}

	// Handle remaining values
	for ; i < n; i++ {
		data[i] = lut[data[i]]
	}
}

// PIZCompress compresses a block of 16-bit values using PIZ encoding.
// The data should be organized as width*height*numChannels 16-bit values.
// For subsampled channels, numChannels should reflect the actual number of samples.
// Returns compressed data or nil on error.
// Format matches OpenEXR specification for C++ compatibility.
func PIZCompress(data []uint16, width, height, numChannels int) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Validate inputs
	if numChannels <= 0 || width <= 0 || height <= 0 {
		return nil, errors.New("piz: invalid dimensions")
	}

	// Calculate actual pixels per channel from data size
	// This handles cases where len(data) != width*height*numChannels
	numPixels := len(data) / numChannels
	if numPixels <= 0 {
		return nil, errors.New("piz: data too small for channel count")
	}

	// Calculate effective height from actual data size
	// This is more robust than trusting the caller's height
	effectiveHeight := numPixels / width
	if effectiveHeight <= 0 {
		effectiveHeight = 1
	}
	if effectiveHeight > height {
		effectiveHeight = height
	}

	// Make a copy of the data since we'll modify it
	transformed := make([]uint16, len(data))
	copy(transformed, data)

	// Get pooled work buffers
	work := pizWorkPool.Get().(*pizWorkBuffer)
	defer pizWorkPool.Put(work)

	// Clear the bitmap (forward/inverse are overwritten, don't need clearing)
	for i := range work.bitmap {
		work.bitmap[i] = 0
	}

	// Step 1: Build bitmap from ORIGINAL data (before wavelet)
	// Each bit in the bitmap represents whether a value (0-65535) is present
	for _, v := range transformed {
		work.bitmap[v>>3] |= 1 << (v & 7)
	}

	// Step 2: Build forward lookup table and count unique values
	// The forward LUT maps original values to compact indices 0..numUnique-1
	// Zero is always included (implicit)
	numUnique := uint16(0)
	for i := 0; i < 65536; i++ {
		if i == 0 || work.bitmap[i>>3]&(1<<(i&7)) != 0 {
			work.forward[i] = numUnique
			numUnique++
		} else {
			work.forward[i] = 0
		}
	}
	maxValue := numUnique - 1 // Maximum value after LUT remapping

	// Now exclude zero from the bitmap for SERIALIZATION ONLY
	work.bitmap[0] &^= 1

	// Find the byte range with non-zero entries in the bitmap
	// minNonZero and maxNonZero are BYTE INDICES (0-8191), not value indices
	minNonZero := uint16(pizBitmapSize) // Start at max
	maxNonZero := uint16(0)
	for i := uint16(0); i < pizBitmapSize; i++ {
		if work.bitmap[i] != 0 {
			if minNonZero > i {
				minNonZero = i
			}
			if maxNonZero < i {
				maxNonZero = i
			}
		}
	}

	// Step 3: Remap values to compact indices using forward LUT
	applyLut(work.forward[:], transformed)

	// Step 4: Apply wavelet transform AFTER remapping
	// This is the correct order - wavelet operates on remapped values
	for ch := 0; ch < numChannels; ch++ {
		chStart := ch * numPixels
		Wav2DEncode(transformed[chStart:chStart+numPixels], width, effectiveHeight, maxValue)
	}

	// Step 5: Count frequencies for Huffman coding
	// Wavelet output can span the full 16-bit range, so we need hufEncSize entries
	freqs := make([]uint64, hufEncSize)
	for _, v := range transformed {
		freqs[v]++
	}

	// Find min/max symbols with non-zero frequency (im, iM in C++ code)
	im := uint32(0)
	for im < 65536 && freqs[im] == 0 {
		im++
	}
	iM := uint32(65535)
	for iM > im && freqs[iM] == 0 {
		iM--
	}
	// Add RLE symbol at iM+1 (we don't use RLE in encoding, but must reserve the slot)
	iM++
	freqs[iM] = 1

	// Step 6: Build Huffman encoder
	encoder := NewHuffmanEncoder(freqs)

	// Step 7: Serialize output in OpenEXR format
	// Format:
	// - 2 bytes: minNonZero (bitmap byte index)
	// - 2 bytes: maxNonZero (bitmap byte index)
	// - Raw bitmap bytes from minNonZero to maxNonZero (inclusive)
	// - 4 bytes: Huffman block size (includes header + table + data)
	// - 20-byte Huffman header:
	//   - 4 bytes: im (min symbol)
	//   - 4 bytes: iM (max symbol + RLE)
	//   - 4 bytes: tableLength (packed table size)
	//   - 4 bytes: nBits (bits in encoded data)
	//   - 4 bytes: reserved (0)
	// - Packed Huffman table (6-bit code lengths with zero-run encoding)
	// - Huffman encoded data

	result := make([]byte, 0, len(data)*2)

	// Write min/max non-zero values (bitmap byte indices)
	result = binary.LittleEndian.AppendUint16(result, minNonZero)
	result = binary.LittleEndian.AppendUint16(result, maxNonZero)

	// Write raw bitmap bytes (no size prefix, no compression)
	// Bitmap covers byte range from minNonZero to maxNonZero
	if minNonZero <= maxNonZero {
		result = append(result, work.bitmap[minNonZero:maxNonZero+1]...)
	}

	// Reserve space for Huffman block size (will be filled in later)
	huffSizePos := len(result)
	result = append(result, 0, 0, 0, 0)

	huffStartPos := len(result)

	// Reserve space for 20-byte Huffman header (will be filled in later)
	huffHeaderPos := len(result)
	result = append(result, make([]byte, 20)...)

	tableStartPos := len(result)

	// Pack Huffman table with 6-bit code lengths and zero-run encoding
	// Only pack codes from im to iM (inclusive)
	lengths := encoder.GetLengths()
	result = packHufTableRange(result, lengths, int(im), int(iM))

	tableLength := len(result) - tableStartPos

	// Encode data with Huffman
	huffmanData := encoder.Encode(transformed)
	nBits := 0
	for _, v := range transformed {
		if int(v) < len(lengths) {
			nBits += lengths[v]
		}
	}
	result = append(result, huffmanData...)

	// Fill in Huffman header
	binary.LittleEndian.PutUint32(result[huffHeaderPos:], im)
	binary.LittleEndian.PutUint32(result[huffHeaderPos+4:], iM)
	binary.LittleEndian.PutUint32(result[huffHeaderPos+8:], uint32(tableLength))
	binary.LittleEndian.PutUint32(result[huffHeaderPos+12:], uint32(nBits))
	binary.LittleEndian.PutUint32(result[huffHeaderPos+16:], 0) // reserved

	// Fill in Huffman block size
	huffSize := len(result) - huffStartPos
	binary.LittleEndian.PutUint32(result[huffSizePos:], uint32(huffSize))

	return result, nil
}

// packHufTableRange packs Huffman code lengths from im to iM using 6-bit encoding
// with zero-run compression. This matches the OpenEXR huf_pack_enc_table format.
func packHufTableRange(dst []byte, lengths []int, im, iM int) []byte {
	// Bit buffer for 6-bit packing
	var bitBuffer uint64
	var bitsInBuffer int

	flushBits := func() {
		for bitsInBuffer >= 8 {
			bitsInBuffer -= 8
			dst = append(dst, byte(bitBuffer>>(bitsInBuffer)))
		}
	}

	writeBits := func(value uint64, nbits int) {
		bitBuffer = (bitBuffer << nbits) | (value & ((1 << nbits) - 1))
		bitsInBuffer += nbits
		flushBits()
	}

	for i := im; i <= iM; i++ {
		l := 0
		if i < len(lengths) {
			l = lengths[i]
		}

		if l == 0 {
			// Count consecutive zeros
			zerun := 1
			for i+zerun <= iM && zerun < longestLongRun {
				nextL := 0
				if i+zerun < len(lengths) {
					nextL = lengths[i+zerun]
				}
				if nextL != 0 {
					break
				}
				zerun++
			}

			if zerun >= 2 {
				if zerun >= shortestLongRun {
					// Long zero run: code 63 + 8-bit count
					writeBits(longZeroCodeRun, hufEncBits)
					writeBits(uint64(zerun-shortestLongRun), 8)
				} else {
					// Short zero run: codes 59-62 encode 2-5 zeros
					writeBits(uint64(shortZeroCodeRun+zerun-2), hufEncBits)
				}
				i += zerun - 1 // -1 because loop increments
				continue
			}
		}

		// Single code length (including 0)
		writeBits(uint64(l), hufEncBits)
	}

	// Flush remaining bits (pad with zeros)
	if bitsInBuffer > 0 {
		dst = append(dst, byte(bitBuffer<<(8-bitsInBuffer)))
	}

	return dst
}

// debugPIZ enables debug output for PIZ decompression
var debugPIZ = false

// PIZDecompressBytes decompresses PIZ-encoded data directly to bytes.
// This is more efficient than PIZDecompress when you need byte output,
// as it uses a pooled internal buffer and avoids an extra copy.
func PIZDecompressBytes(data []byte, width, height, numChannels int) ([]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Get pooled decoded buffer
	decBuf := pizDecodedPool.Get().(*pizDecodedBuffer)

	decoded, err := pizDecompressInternal(data, width, height, numChannels, decBuf)
	if err != nil {
		pizDecodedPool.Put(decBuf)
		return nil, err
	}

	// Convert uint16 slice to bytes
	result := make([]byte, len(decoded)*2)
	for i, v := range decoded {
		result[i*2] = byte(v)
		result[i*2+1] = byte(v >> 8)
	}

	// Return decoded buffer to pool
	pizDecodedPool.Put(decBuf)
	return result, nil
}

// PIZDecompress decompresses PIZ-encoded data.
// Format matches OpenEXR specification for C++ compatibility.
func PIZDecompress(data []byte, width, height, numChannels int) ([]uint16, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Get pooled decoded buffer
	decBuf := pizDecodedPool.Get().(*pizDecodedBuffer)

	decoded, err := pizDecompressInternal(data, width, height, numChannels, decBuf)
	if err != nil {
		pizDecodedPool.Put(decBuf)
		return nil, err
	}

	// Must copy since decoded is from a pool
	result := make([]uint16, len(decoded))
	copy(result, decoded)

	// Return decoded buffer to pool
	pizDecodedPool.Put(decBuf)
	return result, nil
}

// pizDecompressInternal does the actual decompression using a pooled buffer.
// The returned slice is from decBuf and must not be retained after decBuf is returned to the pool.
func pizDecompressInternal(data []byte, width, height, numChannels int, decBuf *pizDecodedBuffer) ([]uint16, error) {

	if len(data) < 8 {
		return nil, ErrPIZCorrupted
	}

	if debugPIZ {
		// Debug output
		println("PIZDecompress: data len=", len(data), "width=", width, "height=", height, "numChannels=", numChannels)
	}

	numPixels := width * height
	expectedValues := numPixels * numChannels

	pos := 0

	// Read min/max non-zero byte indices in bitmap
	minNonZero := binary.LittleEndian.Uint16(data[pos:])
	pos += 2
	maxNonZero := binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	// Get pooled work buffer for bitmap
	work := pizWorkPool.Get().(*pizWorkBuffer)
	defer pizWorkPool.Put(work)

	// Clear bitmap
	for i := range work.bitmap {
		work.bitmap[i] = 0
	}

	// Read raw bitmap bytes (no size prefix)
	// minNonZero and maxNonZero are byte indices (0-8191)
	if minNonZero <= maxNonZero && maxNonZero < pizBitmapSize {
		bitmapLen := int(maxNonZero - minNonZero + 1)
		if pos+bitmapLen > len(data) {
			return nil, ErrPIZCorrupted
		}
		copy(work.bitmap[minNonZero:minNonZero+uint16(bitmapLen)], data[pos:pos+bitmapLen])
		pos += bitmapLen
	}

	// Zero is always implicitly present (excluded during serialization)
	// We need to set it back for the lookup table
	work.bitmap[0] |= 1

	// Count unique values and build inverse lookup table from bitmap
	// Iterate over all 65536 possible values
	// The inverse LUT maps compact indices back to original values
	numUnique := uint16(0)
	for i := 0; i < 65536; i++ {
		if work.bitmap[i>>3]&(1<<(i&7)) != 0 {
			work.inverse[numUnique] = uint16(i)
			numUnique++
		}
	}
	maxValue := numUnique - 1 // Maximum value for wavelet decoding

	// Read Huffman block size
	if pos+4 > len(data) {
		return nil, ErrPIZCorrupted
	}
	huffSize := binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	if pos+int(huffSize) > len(data) {
		return nil, ErrPIZCorrupted
	}

	huffBlock := data[pos : pos+int(huffSize)]

	// Read 20-byte Huffman header
	if len(huffBlock) < 20 {
		return nil, ErrPIZCorrupted
	}
	im := binary.LittleEndian.Uint32(huffBlock[0:])
	iM := binary.LittleEndian.Uint32(huffBlock[4:])
	tableLength := binary.LittleEndian.Uint32(huffBlock[8:])
	nBits := binary.LittleEndian.Uint32(huffBlock[12:])
	// reserved := binary.LittleEndian.Uint32(huffBlock[16:])  // ignored

	if debugPIZ {
		println("PIZ Huffman header: im=", im, "iM=", iM, "tableLength=", tableLength, "nBits=", nBits)
		println("  huffBlock len=", len(huffBlock))
		// Print first 40 bytes of huffBlock
		print("  First 40 bytes: ")
		for i := 0; i < 40 && i < len(huffBlock); i++ {
			print(huffBlock[i], " ")
		}
		println()
	}

	if im >= hufEncSize || iM >= hufEncSize {
		if debugPIZ {
			println("  ERROR: im or iM out of range")
		}
		return nil, ErrPIZCorrupted
	}

	huffTableData := huffBlock[20:]

	// Unpack Huffman table (6-bit code lengths with zero-run encoding)
	// Table only contains codes from im to iM
	codeLengths, tableBytes, err := unpackHufTableRange(huffTableData, int(im), int(iM))
	if err != nil {
		if debugPIZ {
			println("  ERROR unpacking Huffman table:", err.Error())
		}
		return nil, err
	}

	if debugPIZ {
		println("  Unpacked Huffman table: tableBytes=", tableBytes, "codeLengths len=", len(codeLengths))
		// Count non-zero code lengths
		nonZero := 0
		for _, l := range codeLengths {
			if l > 0 {
				nonZero++
			}
		}
		println("  Non-zero code lengths:", nonZero)
		// Check if RLE symbol has a code
		if int(iM) < len(codeLengths) {
			println("  RLE symbol (", iM, ") code length:", codeLengths[iM])
		}
	}

	// Build pooled Huffman decoder - use bounded version since we know im/iM range
	decoder, err := GetFastHufDecoderWithBounds(codeLengths, int(im), int(iM))
	if err != nil {
		if debugPIZ {
			println("  ERROR building Huffman decoder:", err.Error())
		}
		return nil, err
	}
	defer PutFastHufDecoder(decoder)

	// Decode Huffman data - starts after the table
	huffmanData := huffTableData[tableBytes:]
	if debugPIZ {
		println("  Huffman data offset:", tableBytes, "len:", len(huffmanData))
		println("  expectedValues:", expectedValues, "nBits:", nBits)
	}

	// Use pooled decoded buffer, grow if needed
	if cap(decBuf.data) < expectedValues {
		decBuf.data = make([]uint16, expectedValues)
	} else {
		decBuf.data = decBuf.data[:expectedValues]
	}
	decoded := decBuf.data

	if err := decoder.DecodeIntoWithBits(huffmanData, decoded, int(nBits), int(iM)); err != nil {
		if debugPIZ {
			println("  ERROR decoding Huffman data:", err.Error())
		}
		return nil, err
	}

	// Apply inverse wavelet transform to each channel FIRST
	// (reverse order from compression - wavelet first, then LUT)
	for ch := 0; ch < numChannels; ch++ {
		chStart := ch * numPixels
		Wav2DDecode(decoded[chStart:chStart+numPixels], width, height, maxValue)
	}

	// Then map compact indices back to original values using inverse LUT
	applyLut(work.inverse[:], decoded)

	return decoded, nil
}

// unpackHufTableRange unpacks Huffman code lengths from 6-bit packed format.
// The table contains code lengths from im to iM (inclusive).
// Returns the code lengths (full hufEncSize array), number of bytes consumed, and any error.
func unpackHufTableRange(data []byte, im, iM int) ([]int, int, error) {
	codeLengths := make([]int, hufEncSize)

	// Bit buffer for reading 6-bit values
	var bitBuffer uint64
	var bitsInBuffer int
	dataPos := 0

	readBits := func(nbits int) (uint64, error) {
		// Refill buffer
		for bitsInBuffer < nbits && dataPos < len(data) {
			bitBuffer = (bitBuffer << 8) | uint64(data[dataPos])
			dataPos++
			bitsInBuffer += 8
		}
		if bitsInBuffer < nbits {
			return 0, ErrPIZCorrupted
		}
		bitsInBuffer -= nbits
		return (bitBuffer >> bitsInBuffer) & ((1 << nbits) - 1), nil
	}

	for symbol := im; symbol <= iM; symbol++ {
		code, err := readBits(hufEncBits)
		if err != nil {
			return nil, 0, err
		}

		if code == longZeroCodeRun {
			// Long zero run: read 8-bit count
			count, err := readBits(8)
			if err != nil {
				return nil, 0, err
			}
			runLen := int(count) + shortestLongRun
			if symbol+runLen > iM+1 {
				runLen = iM + 1 - symbol
			}
			// Zeros are already in codeLengths (default value)
			symbol += runLen - 1 // -1 because loop increments
		} else if code >= shortZeroCodeRun {
			// Short zero run: 2-5 zeros
			runLen := int(code) - shortZeroCodeRun + 2
			if symbol+runLen > iM+1 {
				runLen = iM + 1 - symbol
			}
			symbol += runLen - 1 // -1 because loop increments
		} else {
			// Regular code length (0-58)
			codeLengths[symbol] = int(code)
		}
	}

	// Calculate bytes consumed
	bytesUsed := dataPos
	if bitsInBuffer >= 8 {
		bytesUsed -= bitsInBuffer / 8
	}

	return codeLengths, bytesUsed, nil
}
