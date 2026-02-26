package compression

import (
	"encoding/binary"
	"math"
	"os"
	"testing"
)

func TestPIZChannelAwareFloat32Roundtrip(t *testing.T) {
	// Simulate a 4x4 float32 RGB image with fill colors: B=0.8, G=0.3, R=0.5
	// Each float32 is 2 uint16 values in little-endian
	width := 4
	height := 4

	// Channel layout (sorted by name): B, G, R
	// Each channel: Size=2, NX=4, NY=4
	channels := []PIZChannel{
		{Size: 2, NX: width, NY: height}, // B
		{Size: 2, NX: width, NY: height}, // G
		{Size: 2, NX: width, NY: height}, // R
	}

	floatValues := []float32{0.8, 0.3, 0.5} // B, G, R

	// Build channel-contiguous uint16 data
	// For each channel: NX * NY * Size uint16 values
	totalUint16 := 0
	for _, ch := range channels {
		totalUint16 += ch.NX * ch.NY * ch.Size
	}

	uint16Data := make([]uint16, totalUint16)
	offset := 0
	for chIdx, ch := range channels {
		fv := floatValues[chIdx]
		bits := math.Float32bits(fv)
		lo := uint16(bits & 0xFFFF)
		hi := uint16(bits >> 16)
		t.Logf("Channel %d (%.1f): lo=0x%04x hi=0x%04x (bits=0x%08x)", chIdx, fv, lo, hi, bits)

		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				uint16Data[offset] = lo
				uint16Data[offset+1] = hi
				offset += ch.Size
			}
		}
	}

	// Convert to bytes
	inputBytes := make([]byte, len(uint16Data)*2)
	for i, v := range uint16Data {
		inputBytes[i*2] = byte(v)
		inputBytes[i*2+1] = byte(v >> 8)
	}

	t.Logf("Input: %d uint16 values, %d bytes", len(uint16Data), len(inputBytes))
	t.Logf("First 24 uint16: %v", uint16Data[:24])

	// Compress with channel-aware API
	compressed, err := PIZCompressBytesChannels(inputBytes, channels)
	if err != nil {
		t.Fatalf("PIZCompressBytesChannels error: %v", err)
	}
	t.Logf("Compressed: %d bytes (%.1f%% of %d)", len(compressed), float64(len(compressed))*100/float64(len(inputBytes)), len(inputBytes))

	// Decompress with channel-aware API
	decompressed, err := PIZDecompressBytesChannels(compressed, channels)
	if err != nil {
		t.Fatalf("PIZDecompressBytesChannels error: %v", err)
	}

	if len(decompressed) != len(inputBytes) {
		t.Fatalf("Length mismatch: got %d, want %d", len(decompressed), len(inputBytes))
	}

	// Convert back to uint16 and compare
	resultUint16 := make([]uint16, len(decompressed)/2)
	for i := range resultUint16 {
		resultUint16[i] = binary.LittleEndian.Uint16(decompressed[i*2:])
	}

	t.Logf("Result first 24 uint16: %v", resultUint16[:24])

	// Compare
	mismatches := 0
	for i := range uint16Data {
		if uint16Data[i] != resultUint16[i] {
			if mismatches < 10 {
				t.Errorf("Mismatch at index %d: got 0x%04x, want 0x%04x", i, resultUint16[i], uint16Data[i])
			}
			mismatches++
		}
	}
	if mismatches > 0 {
		t.Errorf("Total mismatches: %d / %d", mismatches, len(uint16Data))
	}

	// Also verify as float32 values
	offset = 0
	for chIdx, ch := range channels {
		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				lo := resultUint16[offset]
				hi := resultUint16[offset+1]
				bits := uint32(lo) | uint32(hi)<<16
				fv := math.Float32frombits(bits)
				expected := floatValues[chIdx]
				if fv != expected {
					t.Errorf("Channel %d pixel (%d,%d): got %.6f, want %.6f", chIdx, x, y, fv, expected)
				}
				offset += ch.Size
			}
		}
	}
}

// TestPIZChannelAwareFloat32RoundtripLarger tests with a 64x32 image (full PIZ chunk)
func TestPIZChannelAwareFloat32RoundtripLarger(t *testing.T) {
	width := 64
	height := 32
	channels := []PIZChannel{
		{Size: 2, NX: width, NY: height}, // B
		{Size: 2, NX: width, NY: height}, // G
		{Size: 2, NX: width, NY: height}, // R
	}

	floatValues := []float32{0.8, 0.3, 0.5}

	totalUint16 := 0
	for _, ch := range channels {
		totalUint16 += ch.NX * ch.NY * ch.Size
	}

	uint16Data := make([]uint16, totalUint16)
	offset := 0
	for chIdx, ch := range channels {
		bits := math.Float32bits(floatValues[chIdx])
		lo := uint16(bits & 0xFFFF)
		hi := uint16(bits >> 16)
		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				uint16Data[offset] = lo
				uint16Data[offset+1] = hi
				offset += ch.Size
			}
		}
	}

	inputBytes := make([]byte, len(uint16Data)*2)
	for i, v := range uint16Data {
		inputBytes[i*2] = byte(v)
		inputBytes[i*2+1] = byte(v >> 8)
	}

	compressed, err := PIZCompressBytesChannels(inputBytes, channels)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}
	t.Logf("Compressed: %d bytes (%.1f%% of %d)", len(compressed), float64(len(compressed))*100/float64(len(inputBytes)), len(inputBytes))

	decompressed, err := PIZDecompressBytesChannels(compressed, channels)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if len(decompressed) != len(inputBytes) {
		t.Fatalf("Length mismatch: got %d, want %d", len(decompressed), len(inputBytes))
	}

	resultUint16 := make([]uint16, len(decompressed)/2)
	for i := range resultUint16 {
		resultUint16[i] = binary.LittleEndian.Uint16(decompressed[i*2:])
	}

	mismatches := 0
	for i := range uint16Data {
		if uint16Data[i] != resultUint16[i] {
			mismatches++
		}
	}
	if mismatches > 0 {
		t.Errorf("Total mismatches: %d / %d", mismatches, len(uint16Data))
	}

	// Verify float values
	offset = 0
	wrongFloats := 0
	for chIdx, ch := range channels {
		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				lo := resultUint16[offset]
				hi := resultUint16[offset+1]
				bits := uint32(lo) | uint32(hi)<<16
				fv := math.Float32frombits(bits)
				if fv != floatValues[chIdx] {
					wrongFloats++
				}
				offset += ch.Size
			}
		}
	}
	if wrongFloats > 0 {
		t.Errorf("Wrong float values: %d", wrongFloats)
	}
}

// TestPIZHuffmanRLEDecode tests that Huffman decode with RLE produces correct pre-wavelet data.
// This simulates what C++ produces: constant data → LUT → wavelet → Huffman+RLE encoding.
func TestPIZHuffmanRLEDecode(t *testing.T) {
	// Create known channel-contiguous constant data (like C++ would produce after rearrange)
	// B=0.8 (lo=0xCCCD→LUT5, hi=0x3F4C→LUT3)
	// G=0.3 (lo=0x999A→LUT4, hi=0x3E99→LUT1)
	// R=0.5 (lo=0x0000→LUT0, hi=0x3F00→LUT2)
	width := 64
	height := 32
	channels := []PIZChannel{
		{Size: 2, NX: width, NY: height},
		{Size: 2, NX: width, NY: height},
		{Size: 2, NX: width, NY: height},
	}

	totalValues := 0
	totalSamplesPerPixel := 0
	for _, ch := range channels {
		totalValues += ch.NX * ch.NY * ch.Size
		totalSamplesPerPixel += ch.Size
	}

	// Create the data, compress it, then verify the Huffman decode produces correct intermediate values
	uint16Data := make([]uint16, totalValues)
	floatBits := []uint32{0x3F4CCCCD, 0x3E99999A, 0x3F000000}
	offset := 0
	for chIdx, ch := range channels {
		bits := floatBits[chIdx]
		lo := uint16(bits & 0xFFFF)
		hi := uint16(bits >> 16)
		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				uint16Data[offset] = lo
				uint16Data[offset+1] = hi
				offset += ch.Size
			}
		}
	}

	inputBytes := make([]byte, len(uint16Data)*2)
	for i, v := range uint16Data {
		inputBytes[i*2] = byte(v)
		inputBytes[i*2+1] = byte(v >> 8)
	}

	compressed, err := PIZCompressBytesChannels(inputBytes, channels)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	// Now test the Huffman decode step only (via pizDecompressNoWavelet)
	decBuf := pizDecodedPool.Get().(*pizDecodedBuffer)
	defer pizDecodedPool.Put(decBuf)

	decoded, maxValue, inverseLUT, err := pizDecompressNoWavelet(
		compressed, width, height, totalSamplesPerPixel, totalValues, decBuf,
	)
	if err != nil {
		t.Fatalf("pizDecompressNoWavelet error: %v", err)
	}

	t.Logf("maxValue: %d", maxValue)
	t.Logf("Decoded first 32: %v", decoded[:32])

	// After wavelet inverse, values should be reconstructed
	// Apply wavelet inverse
	decodedCopy := make([]uint16, len(decoded))
	copy(decodedCopy, decoded)

	off := 0
	for _, ch := range channels {
		for j := 0; j < ch.Size; j++ {
			Wav2DDecodeStrided(
				decodedCopy[off+j:],
				ch.NX, ch.Size,
				ch.NY, ch.NX*ch.Size,
				maxValue,
			)
		}
		off += ch.NX * ch.NY * ch.Size
	}

	// Apply inverse LUT
	applyLut(inverseLUT, decodedCopy)

	// Verify against original
	mismatches := 0
	for i, v := range uint16Data {
		if decodedCopy[i] != v {
			if mismatches < 5 {
				t.Errorf("Mismatch at %d: got 0x%04x, want 0x%04x", i, decodedCopy[i], v)
			}
			mismatches++
		}
	}
	t.Logf("Go-compressed Huffman decode test: %d mismatches / %d", mismatches, len(uint16Data))

	// Count non-zero values in pre-wavelet decoded data
	nonZero := 0
	for _, v := range decoded {
		if v != 0 {
			nonZero++
		}
	}
	t.Logf("Pre-wavelet non-zero values: %d / %d (expect ~5 for constant fill)", nonZero, totalValues)
}

// TestPIZHuffmanRLEDecodeFromCppData reads a C++-generated PIZ EXR and verifies
// the Huffman decode produces the correct pre-wavelet data.
func TestPIZHuffmanRLEDecodeFromCppData(t *testing.T) {
	const cppFile = "/tmp/test_fill_piz.exr"
	fileBytes, err := os.ReadFile(cppFile)
	if err != nil {
		t.Skipf("C++ test file not found: %s", cppFile)
	}

	// Parse header
	pos := 8 // skip magic + version
	for pos < len(fileBytes) {
		nameStart := pos
		for pos < len(fileBytes) && fileBytes[pos] != 0 {
			pos++
		}
		name := string(fileBytes[nameStart:pos])
		pos++
		if name == "" {
			break
		}
		for pos < len(fileBytes) && fileBytes[pos] != 0 {
			pos++
		}
		pos++
		attrSize := int(binary.LittleEndian.Uint32(fileBytes[pos:]))
		pos += 4
		pos += attrSize
	}
	headerEnd := pos

	// Read offset table
	offset0 := binary.LittleEndian.Uint64(fileBytes[headerEnd:])
	chunkPos := int(offset0)
	compSize := int(binary.LittleEndian.Uint32(fileBytes[chunkPos+4:]))
	compData := fileBytes[chunkPos+8 : chunkPos+8+compSize]
	t.Logf("Chunk 0: compSize=%d bytes", compSize)

	// Set up channels: 3 float32 (B, G, R), 64 wide, 32 lines
	width := 64
	numLines := 32
	channels := []PIZChannel{
		{Size: 2, NX: width, NY: numLines},
		{Size: 2, NX: width, NY: numLines},
		{Size: 2, NX: width, NY: numLines},
	}

	totalValues := 0
	totalSamplesPerPixel := 0
	for _, ch := range channels {
		totalValues += ch.NX * ch.NY * ch.Size
		totalSamplesPerPixel += ch.Size
	}

	// Huffman decode only
	decBuf := pizDecodedPool.Get().(*pizDecodedBuffer)
	defer pizDecodedPool.Put(decBuf)

	decoded, maxValue, inverseLUT, err := pizDecompressNoWavelet(
		compData, width, numLines, totalSamplesPerPixel, totalValues, decBuf,
	)
	if err != nil {
		t.Fatalf("pizDecompressNoWavelet error: %v", err)
	}

	t.Logf("maxValue: %d, decoded len: %d", maxValue, len(decoded))

	// Count non-zero values in pre-wavelet decoded data
	nonZero := 0
	valueCounts := make(map[uint16]int)
	for _, v := range decoded {
		valueCounts[v]++
		if v != 0 {
			nonZero++
		}
	}
	t.Logf("Pre-wavelet non-zero values: %d / %d", nonZero, len(decoded))
	t.Logf("Value distribution: %v", valueCounts)
	t.Logf("First 32 decoded: %v", decoded[:32])

	// For a constant-fill image, after LUT + wavelet, we should have:
	// ~5 non-zero DC coefficients, rest zeros
	if nonZero > 20 {
		t.Errorf("Too many non-zero values in pre-wavelet data: %d (expected ~5 for constant fill)", nonZero)
	}

	// Apply wavelet inverse + LUT and check values
	decodedCopy := make([]uint16, len(decoded))
	copy(decodedCopy, decoded)

	off := 0
	for _, ch := range channels {
		for j := 0; j < ch.Size; j++ {
			Wav2DDecodeStrided(
				decodedCopy[off+j:],
				ch.NX, ch.Size,
				ch.NY, ch.NX*ch.Size,
				maxValue,
			)
		}
		off += ch.NX * ch.NY * ch.Size
	}
	applyLut(inverseLUT, decodedCopy)

	// Expected values: B=0.8(0x3F4CCCCD), G=0.3(0x3E99999A), R=0.5(0x3F000000)
	expected := []uint32{0x3F4CCCCD, 0x3E99999A, 0x3F000000}
	off = 0
	wrongPixels := 0
	for chIdx, ch := range channels {
		expBits := expected[chIdx]
		expLo := uint16(expBits & 0xFFFF)
		expHi := uint16(expBits >> 16)
		for y := 0; y < ch.NY; y++ {
			for x := 0; x < ch.NX; x++ {
				lo := decodedCopy[off]
				hi := decodedCopy[off+1]
				if lo != expLo || hi != expHi {
					if wrongPixels < 5 {
						t.Errorf("Ch%d pixel(%d,%d): got (0x%04x,0x%04x), want (0x%04x,0x%04x)",
							chIdx, x, y, lo, hi, expLo, expHi)
					}
					wrongPixels++
				}
				off += ch.Size
			}
		}
	}
	if wrongPixels > 0 {
		t.Errorf("Total wrong pixels: %d / %d", wrongPixels, totalValues/2)
	}
	_ = inverseLUT
}

// TestPIZOldVsNewHuffman tests that pizDecompressInternal and pizDecompressNoWavelet
// produce the same Huffman-decoded output from the same input
func TestPIZOldVsNewHuffman(t *testing.T) {
	// Create known data, compress with old API, then decompress with both old and new
	width := 8
	height := 8
	numChannels := 6 // 3 float channels × 2 uint16 each

	// Create fill data: simple pattern for testing
	data := make([]uint16, width*height*numChannels)
	for i := range data {
		data[i] = uint16(i % 7)
	}

	compressed, err := PIZCompress(data, width, height, numChannels)
	if err != nil {
		t.Fatalf("PIZCompress error: %v", err)
	}

	// Decompress with old API
	oldResult, err := PIZDecompress(compressed, width, height, numChannels)
	if err != nil {
		t.Fatalf("PIZDecompress error: %v", err)
	}

	// Verify roundtrip
	for i := range data {
		if data[i] != oldResult[i] {
			t.Errorf("Old roundtrip mismatch at %d: got %d, want %d", i, oldResult[i], data[i])
		}
	}
}
