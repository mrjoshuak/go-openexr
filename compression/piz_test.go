package compression

import (
	"testing"
)

func TestWaveletEncodeDecodeEmpty(t *testing.T) {
	var data []uint16
	WaveletEncode(data, 0, 0)
	WaveletDecode(data, 0, 0)
	// Should not crash
}

func TestWaveletEncodeDecodeSingle(t *testing.T) {
	data := []uint16{42}
	original := make([]uint16, len(data))
	copy(original, data)

	WaveletEncode(data, 1, 1)
	WaveletDecode(data, 1, 1)

	if data[0] != original[0] {
		t.Errorf("Single value: got %d, want %d", data[0], original[0])
	}
}

func TestWaveletEncodeDecodeRow(t *testing.T) {
	data := []uint16{1, 2, 3, 4, 5, 6, 7, 8}
	original := make([]uint16, len(data))
	copy(original, data)

	WaveletEncode(data, 8, 1)
	WaveletDecode(data, 8, 1)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

func TestWaveletEncodeDecodeSquare(t *testing.T) {
	// 4x4 test
	data := []uint16{
		1, 2, 3, 4,
		5, 6, 7, 8,
		9, 10, 11, 12,
		13, 14, 15, 16,
	}
	original := make([]uint16, len(data))
	copy(original, data)

	WaveletEncode(data, 4, 4)
	WaveletDecode(data, 4, 4)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

func TestWaveletEncodeDecodeRectangle(t *testing.T) {
	// 8x4 test
	data := make([]uint16, 32)
	for i := range data {
		data[i] = uint16(i * 100)
	}
	original := make([]uint16, len(data))
	copy(original, data)

	WaveletEncode(data, 8, 4)
	WaveletDecode(data, 8, 4)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

func TestWaveletEncodeDecodeOddSize(t *testing.T) {
	// 5x3 test (odd dimensions)
	data := make([]uint16, 15)
	for i := range data {
		data[i] = uint16(i * 50)
	}
	original := make([]uint16, len(data))
	copy(original, data)

	WaveletEncode(data, 5, 3)
	WaveletDecode(data, 5, 3)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

func TestHuffmanEncodeDecodeEmpty(t *testing.T) {
	encoder := NewHuffmanEncoder(nil)
	result := encoder.Encode(nil)
	if result != nil {
		t.Error("Empty encode should return nil")
	}
}

func TestHuffmanEncodeDecodeSingleSymbol(t *testing.T) {
	freqs := make([]uint64, 256)
	freqs[42] = 100

	encoder := NewHuffmanEncoder(freqs)
	values := []uint16{42, 42, 42, 42, 42}
	encoded := encoder.Encode(values)

	// Create decoder from code lengths
	codes := encoder.GetCodes()
	codeLengths := make([]int, len(codes))
	for i, c := range codes {
		codeLengths[i] = c.length
	}

	decoder := NewHuffmanDecoder(codeLengths)
	decoded, err := decoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestHuffmanEncodeDecodeMultipleSymbols(t *testing.T) {
	freqs := make([]uint64, 256)
	freqs[0] = 50
	freqs[1] = 30
	freqs[2] = 15
	freqs[3] = 5

	encoder := NewHuffmanEncoder(freqs)
	values := []uint16{0, 0, 1, 0, 2, 1, 0, 3, 0, 0}
	encoded := encoder.Encode(values)

	codes := encoder.GetCodes()
	codeLengths := make([]int, len(codes))
	for i, c := range codes {
		codeLengths[i] = c.length
	}

	decoder := NewHuffmanDecoder(codeLengths)
	decoded, err := decoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestPIZCompressDecompressEmpty(t *testing.T) {
	compressed, err := PIZCompress(nil, 0, 0, 0)
	if err != nil || compressed != nil {
		t.Error("Empty compress should return nil, nil")
	}

	decompressed, err := PIZDecompress(nil, 0, 0, 0)
	if err != nil || decompressed != nil {
		t.Error("Empty decompress should return nil, nil")
	}
}

func TestPIZCompressDecompressSimple(t *testing.T) {
	// Simple 4x4 single channel image
	data := make([]uint16, 16)
	for i := range data {
		data[i] = uint16(i * 100)
	}

	// Test wavelet alone first
	waveletData := make([]uint16, len(data))
	copy(waveletData, data)
	WaveletEncode(waveletData, 4, 4)
	WaveletDecode(waveletData, 4, 4)
	for i := range data {
		if waveletData[i] != data[i] {
			t.Errorf("Wavelet round-trip failed at %d: got %d, want %d", i, waveletData[i], data[i])
		}
	}

	compressed, err := PIZCompress(data, 4, 4, 1)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	t.Logf("PIZ compression: %d -> %d bytes (%.1f%%)", len(data)*2, len(compressed), 100.0*float64(len(compressed))/float64(len(data)*2))

	decompressed, err := PIZDecompress(compressed, 4, 4, 1)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	if len(decompressed) != len(data) {
		t.Fatalf("Length mismatch: got %d, want %d", len(decompressed), len(data))
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}
}

func TestPIZCompressDecompressMultiChannel(t *testing.T) {

	// 8x8 image with 3 channels
	width, height, channels := 8, 8, 3
	data := make([]uint16, width*height*channels)
	for ch := 0; ch < channels; ch++ {
		for i := 0; i < width*height; i++ {
			data[ch*width*height+i] = uint16(ch*1000 + i*10)
		}
	}

	compressed, err := PIZCompress(data, width, height, channels)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	t.Logf("PIZ multi-channel: %d -> %d bytes (%.1f%%)", len(data)*2, len(compressed), 100.0*float64(len(compressed))/float64(len(data)*2))

	decompressed, err := PIZDecompress(compressed, width, height, channels)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}
}

func TestPIZCompressDecompressUniform(t *testing.T) {
	// Uniform data (should compress very well)
	data := make([]uint16, 256)
	for i := range data {
		data[i] = 12345
	}

	compressed, err := PIZCompress(data, 16, 16, 1)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	t.Logf("PIZ uniform: %d -> %d bytes (%.1f%%)", len(data)*2, len(compressed), 100.0*float64(len(compressed))/float64(len(data)*2))

	decompressed, err := PIZDecompress(compressed, 16, 16, 1)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}
}

func BenchmarkWaveletEncode(b *testing.B) {
	data := make([]uint16, 256*256)
	for i := range data {
		data[i] = uint16(i)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data) * 2))

	for i := 0; i < b.N; i++ {
		WaveletEncode(data, 256, 256)
	}
}

func BenchmarkWaveletDecode(b *testing.B) {
	data := make([]uint16, 256*256)
	for i := range data {
		data[i] = uint16(i)
	}
	WaveletEncode(data, 256, 256)

	b.ResetTimer()
	b.SetBytes(int64(len(data) * 2))

	for i := 0; i < b.N; i++ {
		WaveletDecode(data, 256, 256)
	}
}

func BenchmarkPIZCompress(b *testing.B) {
	data := make([]uint16, 256*256)
	for i := range data {
		data[i] = uint16(i % 1000)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data) * 2))

	for i := 0; i < b.N; i++ {
		PIZCompress(data, 256, 256, 1)
	}
}

func BenchmarkPIZDecompress(b *testing.B) {
	data := make([]uint16, 256*256)
	for i := range data {
		data[i] = uint16(i % 1000)
	}
	compressed, _ := PIZCompress(data, 256, 256, 1)

	b.ResetTimer()
	b.SetBytes(int64(len(data) * 2))

	for i := 0; i < b.N; i++ {
		PIZDecompress(compressed, 256, 256, 1)
	}
}

// TestWenc16Wdec16RoundTrip tests wenc16/wdec16 functions
func TestWenc16Wdec16RoundTrip(t *testing.T) {
	tests := []struct {
		a, b uint16
	}{
		{0, 0},
		{0, 1},
		{1, 0},
		{0xFFFF, 0},
		{0, 0xFFFF},
		{0xFFFF, 0xFFFF},
		{100, 200},
		{32768, 16384},
		{16384, 32768},
		{0x8000, 0x8000},
		{12345, 54321},
		{0xAAAA, 0x5555},
	}

	for i, tt := range tests {
		l, h := wenc16(tt.a, tt.b)
		a2, b2 := wdec16(l, h)
		if a2 != tt.a || b2 != tt.b {
			t.Errorf("test %d: wenc16(%d, %d) = (%d, %d), wdec16 = (%d, %d), want (%d, %d)",
				i, tt.a, tt.b, l, h, a2, b2, tt.a, tt.b)
		}
	}
}

// TestWdec16_4 tests the 2x2 block decode function
func TestWdec16_4(t *testing.T) {
	// Create test data, encode it, then decode with wdec16_4
	testData := []uint16{100, 200, 300, 400}

	// Manually encode using wenc16
	i00l, i01h := wenc16(testData[0], testData[1])
	i10l, i11h := wenc16(testData[2], testData[3])
	ll, lh := wenc16(i00l, i10l)
	hl, hh := wenc16(i01h, i11h)

	// Put encoded data into array in the positions wdec16_4 expects
	data := []uint16{ll, hl, lh, hh}

	// Decode
	wdec16_4(data, 0, 1, 2, 3)

	// Check results
	if data[0] != testData[0] || data[1] != testData[1] ||
		data[2] != testData[2] || data[3] != testData[3] {
		t.Errorf("wdec16_4 mismatch: got %v, want %v", data, testData)
	}
}

// TestWav2DEncodeDecodeWith16BitData tests wavelet with full 16-bit range
func TestWav2DEncodeDecodeWith16BitData(t *testing.T) {
	// Data with values >= 16384 to trigger wenc16/wdec16 path
	width, height := 8, 8
	data := make([]uint16, width*height)
	for i := range data {
		data[i] = uint16(32768 + i*100) // Values in upper half of uint16 range
	}

	original := make([]uint16, len(data))
	copy(original, data)

	maxValue := uint16(65535) // Trigger 16-bit path
	Wav2DEncode(data, width, height, maxValue)
	Wav2DDecode(data, width, height, maxValue)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

// TestWav2DEncodeDecode14BitData tests wavelet with 14-bit data
func TestWav2DEncodeDecode14BitData(t *testing.T) {
	// Data with values < 16384 to trigger wenc14/wdec14 path
	width, height := 8, 8
	data := make([]uint16, width*height)
	for i := range data {
		data[i] = uint16(i * 100 % 16000) // Values in 14-bit range
	}

	original := make([]uint16, len(data))
	copy(original, data)

	maxValue := uint16(16000) // Trigger 14-bit path
	Wav2DEncode(data, width, height, maxValue)
	Wav2DDecode(data, width, height, maxValue)

	for i := range original {
		if data[i] != original[i] {
			t.Errorf("Index %d: got %d, want %d", i, data[i], original[i])
		}
	}
}

// TestWav2DEncodeDecodeOddDimensions tests wavelet with odd dimensions
func TestWav2DEncodeDecodeOddDimensions(t *testing.T) {
	testCases := []struct {
		width, height int
	}{
		{5, 3},
		{7, 9},
		{11, 11},
		{3, 7},
		{15, 17},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			data := make([]uint16, tc.width*tc.height)
			for i := range data {
				data[i] = uint16(i * 50)
			}

			original := make([]uint16, len(data))
			copy(original, data)

			// Test with 14-bit range
			maxValue := uint16(16000)
			Wav2DEncode(data, tc.width, tc.height, maxValue)
			Wav2DDecode(data, tc.width, tc.height, maxValue)

			for i := range original {
				if data[i] != original[i] {
					t.Errorf("Odd dims %dx%d, index %d: got %d, want %d",
						tc.width, tc.height, i, data[i], original[i])
				}
			}

			// Reset and test with 16-bit range
			copy(data, original)
			maxValue = uint16(65535)
			Wav2DEncode(data, tc.width, tc.height, maxValue)
			Wav2DDecode(data, tc.width, tc.height, maxValue)

			for i := range original {
				if data[i] != original[i] {
					t.Errorf("Odd dims %dx%d (16-bit), index %d: got %d, want %d",
						tc.width, tc.height, i, data[i], original[i])
				}
			}
		})
	}
}

// TestWav2DEncodeDecodeEmpty tests empty/degenerate cases
func TestWav2DEncodeDecodeEmpty(t *testing.T) {
	// Empty data
	var data []uint16
	Wav2DEncode(data, 0, 0, 100)
	Wav2DDecode(data, 0, 0, 100)
	// Should not panic

	// Zero dimensions
	data = []uint16{1, 2, 3, 4}
	Wav2DEncode(data, 0, 4, 100)
	Wav2DDecode(data, 0, 4, 100)

	Wav2DEncode(data, 4, 0, 100)
	Wav2DDecode(data, 4, 0, 100)
}

// TestPIZDecompressBytes tests the byte-oriented decompression function
func TestPIZDecompressBytes(t *testing.T) {
	// Empty input
	result, err := PIZDecompressBytes(nil, 0, 0, 0)
	if err != nil || result != nil {
		t.Error("Empty decompress should return nil, nil")
	}

	// Simple data
	data := make([]uint16, 16)
	for i := range data {
		data[i] = uint16(i * 100)
	}

	compressed, err := PIZCompress(data, 4, 4, 1)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := PIZDecompressBytes(compressed, 4, 4, 1)
	if err != nil {
		t.Fatalf("DecompressBytes error: %v", err)
	}

	// Verify the bytes match the original uint16 data
	if len(decompressed) != len(data)*2 {
		t.Fatalf("Length mismatch: got %d bytes, want %d", len(decompressed), len(data)*2)
	}

	for i, v := range data {
		got := uint16(decompressed[i*2]) | uint16(decompressed[i*2+1])<<8
		if got != v {
			t.Errorf("Index %d: got %d, want %d", i, got, v)
		}
	}
}

// TestPIZDecompressBytesMultiChannel tests PIZDecompressBytes with multiple channels
func TestPIZDecompressBytesMultiChannel(t *testing.T) {
	width, height, channels := 8, 8, 3
	data := make([]uint16, width*height*channels)
	for ch := 0; ch < channels; ch++ {
		for i := 0; i < width*height; i++ {
			data[ch*width*height+i] = uint16(ch*1000 + i*10)
		}
	}

	compressed, err := PIZCompress(data, width, height, channels)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := PIZDecompressBytes(compressed, width, height, channels)
	if err != nil {
		t.Fatalf("DecompressBytes error: %v", err)
	}

	// Verify
	if len(decompressed) != len(data)*2 {
		t.Fatalf("Length mismatch: got %d bytes, want %d", len(decompressed), len(data)*2)
	}

	for i, v := range data {
		got := uint16(decompressed[i*2]) | uint16(decompressed[i*2+1])<<8
		if got != v {
			t.Errorf("Index %d: got %d, want %d", i, got, v)
		}
	}
}

// TestPIZCompressErrors tests error cases in PIZ compression
func TestPIZCompressErrors(t *testing.T) {
	// Invalid dimensions
	data := make([]uint16, 16)
	_, err := PIZCompress(data, 0, 4, 1)
	if err == nil {
		t.Error("Expected error for zero width")
	}

	_, err = PIZCompress(data, 4, 0, 1)
	if err == nil {
		t.Error("Expected error for zero height")
	}

	_, err = PIZCompress(data, 4, 4, 0)
	if err == nil {
		t.Error("Expected error for zero channels")
	}

	// Data too small for channel count
	_, err = PIZCompress(data, 4, 4, 100)
	if err == nil {
		t.Error("Expected error for too many channels")
	}
}

// TestPIZDecompressErrors tests error cases in PIZ decompression
func TestPIZDecompressErrors(t *testing.T) {
	// Data too short
	_, err := PIZDecompress([]byte{1, 2, 3}, 4, 4, 1)
	if err == nil {
		t.Error("Expected error for short data")
	}

	// Corrupted bitmap range
	badData := make([]byte, 100)
	badData[0] = 0xFF // minNonZero high
	badData[1] = 0xFF
	badData[2] = 0x00 // maxNonZero low
	badData[3] = 0x00
	_, err = PIZDecompress(badData, 4, 4, 1)
	if err == nil {
		t.Error("Expected error for invalid bitmap range")
	}
}

// TestPIZLargerImages tests with larger image sizes
func TestPIZLargerImages(t *testing.T) {
	testCases := []struct {
		width, height, channels int
	}{
		{32, 32, 1},
		{64, 64, 1},
		{100, 100, 1},
		{32, 32, 3},
		{64, 64, 4},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			data := make([]uint16, tc.width*tc.height*tc.channels)
			for i := range data {
				data[i] = uint16(i % 65536)
			}

			compressed, err := PIZCompress(data, tc.width, tc.height, tc.channels)
			if err != nil {
				t.Fatalf("Compress error: %v", err)
			}

			decompressed, err := PIZDecompress(compressed, tc.width, tc.height, tc.channels)
			if err != nil {
				t.Fatalf("Decompress error: %v", err)
			}

			if len(decompressed) != len(data) {
				t.Fatalf("Length mismatch: got %d, want %d", len(decompressed), len(data))
			}

			for i := range data {
				if decompressed[i] != data[i] {
					t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
					break
				}
			}

			t.Logf("%dx%dx%d: %d -> %d bytes (%.1f%%)",
				tc.width, tc.height, tc.channels,
				len(data)*2, len(compressed),
				100.0*float64(len(compressed))/float64(len(data)*2))
		})
	}
}

// TestPIZHighValueData tests with data containing high values (triggers wenc16 path)
func TestPIZHighValueData(t *testing.T) {
	width, height, channels := 16, 16, 1
	data := make([]uint16, width*height*channels)
	for i := range data {
		// Use values >= 16384 to trigger wenc16/wdec16 path
		data[i] = uint16(32768 + i*100)
	}

	compressed, err := PIZCompress(data, width, height, channels)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := PIZDecompress(compressed, width, height, channels)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}
}

// TestPIZGradientData tests with gradient data patterns
func TestPIZGradientData(t *testing.T) {
	width, height := 64, 64

	// Horizontal gradient
	data := make([]uint16, width*height)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			data[y*width+x] = uint16(x * 1000)
		}
	}

	compressed, err := PIZCompress(data, width, height, 1)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := PIZDecompress(compressed, width, height, 1)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}

	t.Logf("Gradient data: %d -> %d bytes (%.1f%%)",
		len(data)*2, len(compressed),
		100.0*float64(len(compressed))/float64(len(data)*2))
}

// TestPIZOddDimensions tests with odd dimensions
func TestPIZOddDimensions(t *testing.T) {
	testCases := []struct {
		width, height int
	}{
		{7, 7},
		{15, 15},
		{17, 19},
		{31, 33},
		{63, 65},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			data := make([]uint16, tc.width*tc.height)
			for i := range data {
				data[i] = uint16(i * 100)
			}

			compressed, err := PIZCompress(data, tc.width, tc.height, 1)
			if err != nil {
				t.Fatalf("Compress error for %dx%d: %v", tc.width, tc.height, err)
			}

			decompressed, err := PIZDecompress(compressed, tc.width, tc.height, 1)
			if err != nil {
				t.Fatalf("Decompress error for %dx%d: %v", tc.width, tc.height, err)
			}

			for i := range data {
				if decompressed[i] != data[i] {
					t.Errorf("%dx%d Index %d: got %d, want %d", tc.width, tc.height, i, decompressed[i], data[i])
					break
				}
			}
		})
	}
}

// TestPIZSparseData tests with sparse data (few unique values)
func TestPIZSparseData(t *testing.T) {
	width, height := 32, 32
	data := make([]uint16, width*height)

	// Only use a few unique values
	for i := range data {
		data[i] = uint16((i % 5) * 1000)
	}

	compressed, err := PIZCompress(data, width, height, 1)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	decompressed, err := PIZDecompress(compressed, width, height, 1)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	for i := range data {
		if decompressed[i] != data[i] {
			t.Errorf("Index %d: got %d, want %d", i, decompressed[i], data[i])
		}
	}

	t.Logf("Sparse data: %d -> %d bytes (%.1f%%)",
		len(data)*2, len(compressed),
		100.0*float64(len(compressed))/float64(len(data)*2))
}

// TestGetFastHufDecoder tests the pooled decoder function
func TestGetFastHufDecoder(t *testing.T) {
	// Create test frequencies
	freqs := make([]uint64, 256)
	freqs[0] = 50
	freqs[1] = 30
	freqs[2] = 15
	freqs[3] = 5

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	// Get decoder from pool
	decoder, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	if decoder == nil {
		t.Fatal("GetFastHufDecoder returned nil")
	}

	// Use it
	values := []uint16{0, 0, 1, 0, 2, 1, 0, 3, 0, 0}
	encoded := encoder.Encode(values)

	decoded, err := decoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}

	// Return to pool
	PutFastHufDecoder(decoder)

	// Get another one (should reuse)
	decoder2, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed on reuse: %v", err)
	}
	if decoder2 == nil {
		t.Fatal("GetFastHufDecoder returned nil on reuse")
	}

	decoded2, err := decoder2.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error on reuse: %v", err)
	}

	for i, v := range decoded2 {
		if v != values[i] {
			t.Errorf("Reuse Index %d: got %d, want %d", i, v, values[i])
		}
	}

	PutFastHufDecoder(decoder2)
}

// TestPutFastHufDecoderNil tests that PutFastHufDecoder handles nil
func TestPutFastHufDecoderNil(t *testing.T) {
	// Should not panic
	PutFastHufDecoder(nil)
}

// TestFastHufDecoderReset tests the Reset method
func TestFastHufDecoderReset(t *testing.T) {
	// Create test frequencies
	freqs := make([]uint64, 256)
	freqs[0] = 50
	freqs[1] = 30
	freqs[2] = 15
	freqs[3] = 5

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	// Get decoder and use it
	decoder, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	values := []uint16{0, 0, 1, 0, 2, 1, 0, 3, 0, 0}
	encoded := encoder.Encode(values)

	_, err = decoder.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("First decode error: %v", err)
	}

	// Reset with different code lengths
	freqs2 := make([]uint64, 256)
	freqs2[10] = 100
	freqs2[20] = 50
	freqs2[30] = 25

	encoder2 := NewHuffmanEncoder(freqs2)
	lengths2 := encoder2.GetLengths()

	if err := decoder.Reset(lengths2); err != nil {
		t.Fatalf("Reset failed: %v", err)
	}

	values2 := []uint16{10, 20, 30, 10, 10, 20}
	encoded2 := encoder2.Encode(values2)

	decoded2, err := decoder.Decode(encoded2, len(values2))
	if err != nil {
		t.Fatalf("Second decode error: %v", err)
	}

	for i, v := range decoded2 {
		if v != values2[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values2[i])
		}
	}

	PutFastHufDecoder(decoder)
}

// BenchmarkHuffmanDecoders compares old HuffmanDecoder vs new FastHufDecoder
func BenchmarkHuffmanDecoders(b *testing.B) {
	// Create test data with realistic distribution
	numValues := 64 * 1024
	data := make([]uint16, numValues)
	for i := range data {
		// Mix of values to create a realistic Huffman tree
		data[i] = uint16(i % 500)
	}

	// Build frequency table
	freqs := make([]uint64, 500)
	for _, v := range data {
		freqs[v]++
	}

	// Create encoder and encode data
	encoder := NewHuffmanEncoder(freqs)
	encoded := encoder.Encode(data)
	codeLengths := encoder.GetLengths()

	b.Run("HuffmanDecoder", func(b *testing.B) {
		decoder := NewHuffmanDecoder(codeLengths)
		b.ResetTimer()
		b.SetBytes(int64(len(encoded)))

		for i := 0; i < b.N; i++ {
			decoder.Decode(encoded, numValues)
		}
	})

	b.Run("FastHufDecoder", func(b *testing.B) {
		decoder := NewFastHufDecoder(codeLengths)
		b.ResetTimer()
		b.SetBytes(int64(len(encoded)))

		for i := 0; i < b.N; i++ {
			decoder.Decode(encoded, numValues)
		}
	})
}
