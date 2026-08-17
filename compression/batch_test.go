package compression

import (
	"math"
	"testing"
)

func TestBatchWaveletEncodeDecodeRow(t *testing.T) {
	// Test with various sizes
	sizes := []int{8, 16, 32, 64, 100, 128, 255, 256}

	for _, n := range sizes {
		data := make([]uint16, n)
		for i := range data {
			data[i] = uint16(i * 100)
		}
		original := make([]uint16, n)
		copy(original, data)

		temp := make([]uint16, n)

		// Encode
		WaveletEncodeRow(data, temp, n)

		// Decode
		WaveletDecodeRow(data, temp, n)

		// Verify round-trip
		for i := range data {
			if data[i] != original[i] {
				t.Errorf("n=%d: round-trip failed at index %d: got %d, want %d",
					n, i, data[i], original[i])
			}
		}
	}
}

func TestWaveletEncodeDecode2D(t *testing.T) {
	// Test 2D wavelet transform
	widths := []int{8, 16, 32, 64}
	heights := []int{8, 16, 32, 64}

	for _, width := range widths {
		for _, height := range heights {
			data := make([]uint16, width*height)
			for i := range data {
				data[i] = uint16(i % 65536)
			}
			original := make([]uint16, width*height)
			copy(original, data)

			// Encode
			WaveletEncode2D(data, width, height)

			// Decode
			WaveletDecode2D(data, width, height)

			// Verify round-trip
			for i := range data {
				if data[i] != original[i] {
					t.Errorf("%dx%d: round-trip failed at index %d: got %d, want %d",
						width, height, i, data[i], original[i])
				}
			}
		}
	}
}

func TestWavelet2DMatchesOriginal(t *testing.T) {
	// Compare optimized 2D wavelet with original implementation
	width, height := 64, 64
	data1 := make([]uint16, width*height)
	data2 := make([]uint16, width*height)

	for i := range data1 {
		data1[i] = uint16(i * 17 % 65536)
		data2[i] = data1[i]
	}

	// Use original implementation
	WaveletEncode(data1, width, height)

	// Use optimized implementation
	WaveletEncode2D(data2, width, height)

	// Both should produce the same result
	for i := range data1 {
		if data1[i] != data2[i] {
			t.Errorf("index %d: original=%d, optimized=%d", i, data1[i], data2[i])
		}
	}
}

func TestDCT8x8RoundTrip(t *testing.T) {
	var data [64]float32
	var original [64]float32

	// Initialize with test data
	for i := 0; i < 64; i++ {
		data[i] = float32(i) / 64.0
		original[i] = data[i]
	}

	// Forward DCT
	DCT8x8Forward(&data)

	// Inverse DCT
	DCT8x8Inverse(&data)

	// Verify round-trip (with some tolerance for floating point)
	const epsilon = 1e-5
	for i := 0; i < 64; i++ {
		if math.Abs(float64(data[i]-original[i])) > epsilon {
			t.Errorf("DCT round-trip failed at index %d: got %f, want %f",
				i, data[i], original[i])
		}
	}
}

func TestTranspose8x8(t *testing.T) {
	var src, dst [64]float32

	// Initialize with known values
	for i := 0; i < 64; i++ {
		src[i] = float32(i)
	}

	transpose8x8(&src, &dst)

	// Verify transpose
	for row := 0; row < 8; row++ {
		for col := 0; col < 8; col++ {
			srcIdx := row*8 + col
			dstIdx := col*8 + row
			if dst[dstIdx] != src[srcIdx] {
				t.Errorf("transpose failed at (%d,%d): got %f, want %f",
					row, col, dst[dstIdx], src[srcIdx])
			}
		}
	}
}

func TestCSC709RoundTrip(t *testing.T) {
	// Test RGB -> YCbCr -> RGB conversion
	// Note: Color space conversion has inherent floating point precision loss
	n := 64
	r := make([]float32, n)
	g := make([]float32, n)
	b := make([]float32, n)
	origR := make([]float32, n)
	origG := make([]float32, n)
	origB := make([]float32, n)

	// Initialize with test data
	for i := 0; i < n; i++ {
		r[i] = float32(i) / float32(n)
		g[i] = float32(n-i) / float32(n)
		b[i] = 0.5
		origR[i] = r[i]
		origG[i] = g[i]
		origB[i] = b[i]
	}

	// Forward (RGB -> YCbCr)
	CSC709ForwardBatch(r, g, b)

	// Inverse (YCbCr -> RGB)
	CSC709InverseBatch(r, g, b)

	// Verify round-trip with tolerance for floating point precision
	// Color space conversions have inherent precision loss in matrix operations
	const epsilon = 1e-3
	for i := 0; i < n; i++ {
		if math.Abs(float64(r[i]-origR[i])) > epsilon {
			t.Errorf("R round-trip failed at index %d: got %f, want %f", i, r[i], origR[i])
		}
		if math.Abs(float64(g[i]-origG[i])) > epsilon {
			t.Errorf("G round-trip failed at index %d: got %f, want %f", i, g[i], origG[i])
		}
		if math.Abs(float64(b[i]-origB[i])) > epsilon {
			t.Errorf("B round-trip failed at index %d: got %f, want %f", i, b[i], origB[i])
		}
	}
}

func TestZigzagRoundTrip(t *testing.T) {
	var src, zigzagged, result [64]float32

	// Initialize with test data
	for i := 0; i < 64; i++ {
		src[i] = float32(i)
	}

	// Forward zigzag
	ZigzagReorderBatch(&zigzagged, &src)

	// Inverse zigzag
	ZigzagUnreorderBatch(&result, &zigzagged)

	// Verify round-trip
	for i := 0; i < 64; i++ {
		if result[i] != src[i] {
			t.Errorf("Zigzag round-trip failed at index %d: got %f, want %f",
				i, result[i], src[i])
		}
	}
}

func TestCopyBatch16(t *testing.T) {
	sizes := []int{1, 7, 8, 15, 16, 100, 128}

	for _, n := range sizes {
		src := make([]uint16, n)
		dst := make([]uint16, n)

		for i := range src {
			src[i] = uint16(i)
		}

		copyBatch16(dst, src)

		for i := range src {
			if dst[i] != src[i] {
				t.Errorf("n=%d: copy failed at index %d: got %d, want %d",
					n, i, dst[i], src[i])
			}
		}
	}
}

func TestExtractStoreColumn(t *testing.T) {
	width, height := 16, 16
	data := make([]uint16, width*height)

	// Initialize with test data
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			data[y*width+x] = uint16(y*100 + x)
		}
	}

	col := make([]uint16, height)

	// Test each column
	for x := 0; x < width; x++ {
		extractColumn(data, col, x, width, height)

		// Verify extracted values
		for y := 0; y < height; y++ {
			expected := uint16(y*100 + x)
			if col[y] != expected {
				t.Errorf("column %d, row %d: got %d, want %d", x, y, col[y], expected)
			}
		}

		// Modify column
		for y := 0; y < height; y++ {
			col[y] = col[y] + 1000
		}

		// Store back
		storeColumn(data, col, x, width, height)

		// Verify stored values
		for y := 0; y < height; y++ {
			expected := uint16(y*100 + x + 1000)
			if data[y*width+x] != expected {
				t.Errorf("stored column %d, row %d: got %d, want %d",
					x, y, data[y*width+x], expected)
			}
		}
	}
}

func TestInterleaveDeinterleave(t *testing.T) {
	numChannels := 4
	pixelsPerChannel := 64
	bytesPerSample := 2

	// Create channel data
	channels := make([][]byte, numChannels)
	for ch := 0; ch < numChannels; ch++ {
		channels[ch] = make([]byte, pixelsPerChannel*bytesPerSample)
		for p := 0; p < pixelsPerChannel; p++ {
			channels[ch][p*2] = byte(ch*64 + p)
			channels[ch][p*2+1] = byte((ch*64 + p) >> 8)
		}
	}

	// Interleave
	interleaved := make([]byte, numChannels*pixelsPerChannel*bytesPerSample)
	InterleaveChannelsBatch(interleaved, channels, bytesPerSample)

	// Deinterleave
	resultChannels := make([][]byte, numChannels)
	for ch := 0; ch < numChannels; ch++ {
		resultChannels[ch] = make([]byte, pixelsPerChannel*bytesPerSample)
	}
	DeinterleaveChannelsBatch(interleaved, resultChannels, bytesPerSample)

	// Verify round-trip
	for ch := 0; ch < numChannels; ch++ {
		for i := 0; i < len(channels[ch]); i++ {
			if resultChannels[ch][i] != channels[ch][i] {
				t.Errorf("channel %d, byte %d: got %d, want %d",
					ch, i, resultChannels[ch][i], channels[ch][i])
			}
		}
	}
}

// Benchmarks

func BenchmarkWaveletEncodeRow(b *testing.B) {
	data := make([]uint16, 1024)
	temp := make([]uint16, 1024)
	for i := range data {
		data[i] = uint16(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WaveletEncodeRow(data, temp, len(data))
	}
}

func BenchmarkWaveletEncode2D(b *testing.B) {
	data := make([]uint16, 64*64)
	for i := range data {
		data[i] = uint16(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WaveletEncode2D(data, 64, 64)
	}
}

func BenchmarkWaveletEncodeOriginal(b *testing.B) {
	data := make([]uint16, 64*64)
	for i := range data {
		data[i] = uint16(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		WaveletEncode(data, 64, 64)
	}
}

func BenchmarkDCT8x8Forward(b *testing.B) {
	var data [64]float32
	for i := range data {
		data[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		DCT8x8Forward(&data)
	}
}

func BenchmarkCSC709Forward(b *testing.B) {
	n := 1024
	r := make([]float32, n)
	g := make([]float32, n)
	bb := make([]float32, n)
	for i := 0; i < n; i++ {
		r[i] = float32(i) / float32(n)
		g[i] = float32(n-i) / float32(n)
		bb[i] = 0.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CSC709ForwardBatch(r, g, bb)
	}
}

func BenchmarkTranspose8x8(b *testing.B) {
	var src, dst [64]float32
	for i := range src {
		src[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		transpose8x8(&src, &dst)
	}
}

func BenchmarkZigzagReorder(b *testing.B) {
	var src, dst [64]float32
	for i := range src {
		src[i] = float32(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ZigzagReorderBatch(&dst, &src)
	}
}

func BenchmarkCopyBatch16(b *testing.B) {
	src := make([]uint16, 1024)
	dst := make([]uint16, 1024)
	for i := range src {
		src[i] = uint16(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copyBatch16(dst, src)
	}
}

func TestCSC709ForwardBatchShortSlice(t *testing.T) {
	// Test with slices of different lengths (g shorter than r)
	r := make([]float32, 100)
	g := make([]float32, 50) // Shorter than r
	b := make([]float32, 100)

	for i := range r {
		r[i] = 0.5
		b[i] = 0.5
	}
	for i := range g {
		g[i] = 0.5
	}

	// Save originals to verify guard returns early without modifying data
	origR := make([]float32, len(r))
	copy(origR, r)

	CSC709ForwardBatch(r, g, b)

	// Function should return early when g is shorter — data unchanged
	for i := range r {
		if r[i] != origR[i] {
			t.Errorf("r[%d] was modified despite short g slice: got %v, want %v", i, r[i], origR[i])
			break
		}
	}
}

func TestCSC709InverseBatchShortSlice(t *testing.T) {
	// Test with slices of different lengths (cr shorter than y)
	y := make([]float32, 100)
	cb := make([]float32, 100)
	cr := make([]float32, 50) // Shorter than y

	for i := range y {
		y[i] = 0.5
		cb[i] = 0.0
	}
	for i := range cr {
		cr[i] = 0.0
	}

	// Save originals to verify guard returns early without modifying data
	origY := make([]float32, len(y))
	copy(origY, y)

	CSC709InverseBatch(y, cb, cr)

	// Function should return early when cr is shorter — data unchanged
	for i := range y {
		if y[i] != origY[i] {
			t.Errorf("y[%d] was modified despite short cr slice: got %v, want %v", i, y[i], origY[i])
			break
		}
	}
}

func TestCSC709ForwardBatchSmallRemainder(t *testing.T) {
	// Test with a size that has a remainder after batch processing
	// batchSize is 8, so test with 11 elements (8 + 3 remainder)
	r := make([]float32, 11)
	g := make([]float32, 11)
	b := make([]float32, 11)

	for i := 0; i < 11; i++ {
		r[i] = 0.5
		g[i] = 0.3
		b[i] = 0.2
	}

	origR := make([]float32, 11)
	origG := make([]float32, 11)
	origB := make([]float32, 11)
	copy(origR, r)
	copy(origG, g)
	copy(origB, b)

	CSC709ForwardBatch(r, g, b)

	// Verify the result is different from input
	changed := false
	for i := 0; i < 11; i++ {
		if r[i] != origR[i] || g[i] != origG[i] || b[i] != origB[i] {
			changed = true
			break
		}
	}
	if !changed {
		t.Error("CSC709ForwardBatch should modify the data")
	}

	// Now inverse
	CSC709InverseBatch(r, g, b)

	// Should approximately match original (with some floating point error)
	const tolerance = 1e-3
	for i := 0; i < 11; i++ {
		diff := r[i] - origR[i]
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("R[%d]: got %v, want %v", i, r[i], origR[i])
		}
	}
}

func TestCSC709InverseBatchSmallRemainder(t *testing.T) {
	// Test inverse with remainder: 13 = 8 batch + 5 remainder
	// Use gray input (cb=0, cr=0) — inverse should yield R=G=B=Y
	y := make([]float32, 13)
	cb := make([]float32, 13)
	cr := make([]float32, 13)

	for i := 0; i < 13; i++ {
		y[i] = 0.5
		cb[i] = 0.0
		cr[i] = 0.0
	}

	CSC709InverseBatch(y, cb, cr)

	// With cb=0, cr=0: R = Y + 1.5747*0 = Y, G = Y - 0 - 0 = Y, B = Y + 1.8556*0 = Y
	// All channels should remain 0.5
	const tolerance = 1e-5
	for i := 0; i < 13; i++ {
		if diff := y[i] - 0.5; diff < -tolerance || diff > tolerance {
			t.Errorf("y[%d] = %v, want ~0.5 for gray input", i, y[i])
		}
		if diff := cb[i] - 0.5; diff < -tolerance || diff > tolerance {
			t.Errorf("cb[%d] = %v, want ~0.5 for gray input", i, cb[i])
		}
		if diff := cr[i] - 0.5; diff < -tolerance || diff > tolerance {
			t.Errorf("cr[%d] = %v, want ~0.5 for gray input", i, cr[i])
		}
	}
}

func TestInterleaveChannelsBatchEmpty(t *testing.T) {
	// Test with empty channels — dst should remain unchanged
	dst := make([]byte, 100)
	for i := range dst {
		dst[i] = 0xFF
	}

	InterleaveChannelsBatch(dst, [][]byte{}, 2)

	// Verify dst was not modified
	for i := range dst {
		if dst[i] != 0xFF {
			t.Errorf("dst[%d] = 0x%02X, want 0xFF (should be unchanged)", i, dst[i])
			break
		}
	}
}

func TestDeinterleaveChannelsBatchEmpty(t *testing.T) {
	// Test with empty channels — src should remain unchanged
	src := make([]byte, 100)
	for i := range src {
		src[i] = byte(i)
	}

	DeinterleaveChannelsBatch(src, [][]byte{}, 2)

	// Verify src was not modified
	for i := range src {
		if src[i] != byte(i) {
			t.Errorf("src[%d] = %d, want %d (should be unchanged)", i, src[i], byte(i))
			break
		}
	}
}

func TestWaveletEncode2DEmpty(t *testing.T) {
	// Test with empty/zero dimensions — verify guard returns without error
	// Use a non-nil slice with data to verify it isn't modified
	data := []uint16{100, 200, 300}
	origData := make([]uint16, len(data))
	copy(origData, data)

	WaveletEncode2D(nil, 0, 0)
	WaveletEncode2D(data, 0, 10)

	// data should be unchanged since width=0 triggers early return
	for i := range data {
		if data[i] != origData[i] {
			t.Errorf("data[%d] = %d, want %d (should be unchanged with zero width)", i, data[i], origData[i])
		}
	}

	WaveletEncode2D(data, 10, 0)

	for i := range data {
		if data[i] != origData[i] {
			t.Errorf("data[%d] = %d, want %d (should be unchanged with zero height)", i, data[i], origData[i])
		}
	}
}

func TestWaveletDecode2DEmpty(t *testing.T) {
	// Test with empty/zero dimensions — verify guard returns without error
	data := []uint16{100, 200, 300}
	origData := make([]uint16, len(data))
	copy(origData, data)

	WaveletDecode2D(nil, 0, 0)
	WaveletDecode2D(data, 0, 10)

	for i := range data {
		if data[i] != origData[i] {
			t.Errorf("data[%d] = %d, want %d (should be unchanged with zero width)", i, data[i], origData[i])
		}
	}

	WaveletDecode2D(data, 10, 0)

	for i := range data {
		if data[i] != origData[i] {
			t.Errorf("data[%d] = %d, want %d (should be unchanged with zero height)", i, data[i], origData[i])
		}
	}
}

func TestWaveletRowSmallData(t *testing.T) {
	// Test with n < 2 (should return early)
	data := []uint16{42}
	temp := make([]uint16, 1)

	WaveletEncodeRow(data, temp, 1)
	if data[0] != 42 {
		t.Errorf("Data should be unchanged for n=1: got %d", data[0])
	}

	WaveletDecodeRow(data, temp, 1)
	if data[0] != 42 {
		t.Errorf("Data should be unchanged for n=1: got %d", data[0])
	}
}
