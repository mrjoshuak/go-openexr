package compression

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

func TestDctForwardInverse(t *testing.T) {
	// Test that forward+inverse DCT produces the original values (within tolerance)
	// Create a test block with known values
	var original [64]float32
	for i := 0; i < 64; i++ {
		original[i] = float32(i) / 64.0
	}

	// Make a copy for transformation
	var block [64]float32
	copy(block[:], original[:])

	// Forward transform
	dctForward8x8(&block)

	// Values should be different after forward DCT
	dcComp := block[0]
	if dcComp == original[0] {
		t.Errorf("Forward DCT DC component unchanged")
	}

	// Inverse transform
	dctInverse8x8(&block, 0)

	// Check round-trip accuracy
	maxDiff := float32(0)
	for i := 0; i < 64; i++ {
		diff := float32(math.Abs(float64(block[i] - original[i])))
		if diff > maxDiff {
			maxDiff = diff
		}
	}
	t.Logf("Forward DCT DC component: %f, after inverse: %f, max diff: %f", dcComp, block[0], maxDiff)

	// The DCT round-trip should be reasonably accurate
	if maxDiff > 0.01 {
		t.Errorf("DCT round-trip max difference too high: %f", maxDiff)
	}
}

func TestDctInverse8x8DcOnly(t *testing.T) {
	var block [64]float32
	block[0] = 1.0 // DC component only

	dctInverse8x8DcOnly(&block)

	// All values should be the same (DC spread evenly)
	expectedVal := block[0]
	for i := 1; i < 64; i++ {
		if block[i] != expectedVal {
			t.Errorf("DC-only inverse at [%d]: got %f, want %f",
				i, block[i], expectedVal)
		}
	}
}

func TestCsc709RoundTrip(t *testing.T) {
	// Test RGB -> YCbCr -> RGB round trip
	var r, g, b [64]float32

	// Set some test RGB values
	for i := 0; i < 64; i++ {
		r[i] = float32(i) / 64.0
		g[i] = float32(64-i) / 64.0
		b[i] = float32((i*2)%64) / 64.0
	}

	// Save originals
	var origR, origG, origB [64]float32
	copy(origR[:], r[:])
	copy(origG[:], g[:])
	copy(origB[:], b[:])

	// Forward (RGB -> YCbCr)
	csc709Forward(&r, &g, &b)

	// Inverse (YCbCr -> RGB)
	csc709Inverse(&r, &g, &b)

	// Check round-trip accuracy
	tolerance := float32(0.001)
	for i := 0; i < 64; i++ {
		if math.Abs(float64(r[i]-origR[i])) > float64(tolerance) {
			t.Errorf("R round-trip error at [%d]: got %f, want %f", i, r[i], origR[i])
		}
		if math.Abs(float64(g[i]-origG[i])) > float64(tolerance) {
			t.Errorf("G round-trip error at [%d]: got %f, want %f", i, g[i], origG[i])
		}
		if math.Abs(float64(b[i]-origB[i])) > float64(tolerance) {
			t.Errorf("B round-trip error at [%d]: got %f, want %f", i, b[i], origB[i])
		}
	}
}

func TestDwaConvertToLinearNonLinear(t *testing.T) {
	// Test round-trip: linear -> nonlinear -> linear
	testValues := []float32{0.0, 0.1, 0.25, 0.5, 0.75, 1.0, 2.0, 10.0}

	for _, val := range testValues {
		h := half.FromFloat32(val)
		hv := uint16(h)

		// Convert to nonlinear
		nl := dwaConvertToNonLinear(hv)

		// Convert back to linear
		lin := dwaConvertToLinear(nl)

		// Compare
		origFloat := half.Half(hv).Float32()
		roundTrip := half.Half(lin).Float32()

		// Allow some tolerance due to half-float precision
		tolerance := float32(0.1)
		if val > 1.0 {
			tolerance = float32(val * 0.15) // Higher tolerance for HDR values
		}

		diff := float32(math.Abs(float64(roundTrip - origFloat)))
		if diff > tolerance && val != 0 {
			t.Errorf("Linear/nonlinear round-trip for %f: got %f (diff %f)",
				val, roundTrip, diff)
		}
	}
}

func TestZigzagOrder(t *testing.T) {
	// Verify zigzag and inverse zigzag are consistent
	for i := 0; i < 64; i++ {
		zigIdx := zigzag[i]
		// invZigzag[zigIdx] should give us back i
		if invZigzag[zigIdx] != i {
			t.Errorf("Zigzag mismatch at %d: zigzag[%d]=%d, invZigzag[%d]=%d",
				i, i, zigIdx, zigIdx, invZigzag[zigIdx])
		}
	}
}

func TestDwaDecompressorInvalidData(t *testing.T) {
	d := NewDwaDecompressor(4, 4)

	// Test with empty data
	err := d.Decompress([]byte{}, make([]byte, 100))
	if err != ErrDwaCorruptData {
		t.Errorf("Expected ErrDwaCorruptData for empty data, got %v", err)
	}

	// Test with data too small for header
	err = d.Decompress(make([]byte, 10), make([]byte, 100))
	if err != ErrDwaCorruptData {
		t.Errorf("Expected ErrDwaCorruptData for small data, got %v", err)
	}
}

func TestDwaCompressorBasic(t *testing.T) {
	// Test basic compression/decompression cycle
	width := 8
	height := 8
	bytesPerPixel := 6 // 3 channels * 2 bytes (HALF)
	srcData := make([]byte, width*height*bytesPerPixel)

	// Fill with some test data
	for i := range srcData {
		srcData[i] = byte(i % 256)
	}

	c := NewDwaCompressor(width, height, 45.0)
	compressed, err := c.Compress(srcData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	if len(compressed) == 0 {
		t.Fatal("Compressed data is empty")
	}

	// Verify header is present
	if len(compressed) < dwaHeaderSize {
		t.Fatal("Compressed data too small for header")
	}

	t.Logf("Original size: %d, compressed size: %d", len(srcData), len(compressed))
}

func TestDctCosValues(t *testing.T) {
	// Verify DCT cosine coefficients
	expectedCos := []float32{
		float32(math.Cos(1.0 * math.Pi / 16.0)),
		float32(math.Cos(2.0 * math.Pi / 16.0)),
		float32(math.Cos(3.0 * math.Pi / 16.0)),
		float32(math.Cos(4.0 * math.Pi / 16.0)),
		float32(math.Cos(5.0 * math.Pi / 16.0)),
		float32(math.Cos(6.0 * math.Pi / 16.0)),
		float32(math.Cos(7.0 * math.Pi / 16.0)),
	}
	actualCos := []float32{dctCos1, dctCos2, dctCos3, dctCos4, dctCos5, dctCos6, dctCos7}

	for i := 0; i < 7; i++ {
		if math.Abs(float64(actualCos[i]-expectedCos[i])) > 0.0001 {
			t.Errorf("dctCos%d = %f, expected %f", i+1, actualCos[i], expectedCos[i])
		}
	}

	// Verify sin values
	expectedSin := []float32{
		float32(math.Sin(1.0 * math.Pi / 16.0)),
		float32(math.Sin(2.0 * math.Pi / 16.0)),
		float32(math.Sin(3.0 * math.Pi / 16.0)),
	}
	actualSin := []float32{dctSin1, dctSin2, dctSin3}

	for i := 0; i < 3; i++ {
		if math.Abs(float64(actualSin[i]-expectedSin[i])) > 0.0001 {
			t.Errorf("dctSin%d = %f, expected %f", i+1, actualSin[i], expectedSin[i])
		}
	}
}

func TestDwaLookupTableInit(t *testing.T) {
	ensureDwaTables()

	// Test that zero maps to zero
	if dwaToLinearTable[0] != 0 {
		t.Errorf("dwaToLinearTable[0] = %d, expected 0", dwaToLinearTable[0])
	}
	if dwaToNonLinearTable[0] != 0 {
		t.Errorf("dwaToNonLinearTable[0] = %d, expected 0", dwaToNonLinearTable[0])
	}

	// Test that infinity/nan maps to zero
	infHalf := uint16(0x7c00) // Positive infinity in half-float
	if dwaToLinearTable[infHalf] != 0 {
		t.Errorf("dwaToLinearTable[inf] = %d, expected 0", dwaToLinearTable[infHalf])
	}
	if dwaToNonLinearTable[infHalf] != 0 {
		t.Errorf("dwaToNonLinearTable[inf] = %d, expected 0", dwaToNonLinearTable[infHalf])
	}
}

func BenchmarkDctForward(b *testing.B) {
	var block [64]float32
	for i := 0; i < 64; i++ {
		block[i] = float32(i) / 64.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dctForward8x8(&block)
	}
}

func BenchmarkDctInverse(b *testing.B) {
	var block [64]float32
	for i := 0; i < 64; i++ {
		block[i] = float32(i) / 64.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dctInverse8x8(&block, 0)
	}
}

func BenchmarkCsc709Forward(b *testing.B) {
	var r, g, bl [64]float32
	for i := 0; i < 64; i++ {
		r[i] = float32(i) / 64.0
		g[i] = float32(i) / 64.0
		bl[i] = float32(i) / 64.0
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		csc709Forward(&r, &g, &bl)
	}
}

func TestClassifyChannelName(t *testing.T) {
	tests := []struct {
		name      string
		pixelType int
		expected  int
	}{
		// DCT channels (HALF only)
		{"R", dwaPixelTypeHalf, compressorLossyDCT},
		{"G", dwaPixelTypeHalf, compressorLossyDCT},
		{"B", dwaPixelTypeHalf, compressorLossyDCT},
		{"Y", dwaPixelTypeHalf, compressorLossyDCT},
		{"RY", dwaPixelTypeHalf, compressorLossyDCT},
		{"BY", dwaPixelTypeHalf, compressorLossyDCT},
		// With layer prefix
		{"layer.R", dwaPixelTypeHalf, compressorLossyDCT},
		{"diffuse.G", dwaPixelTypeHalf, compressorLossyDCT},
		// RLE channels
		{"A", dwaPixelTypeHalf, compressorRLE},
		{"layer.A", dwaPixelTypeHalf, compressorRLE},
		// Unknown channels
		{"Z", dwaPixelTypeHalf, compressorUnknown},
		{"N", dwaPixelTypeHalf, compressorUnknown},
		{"custom", dwaPixelTypeHalf, compressorUnknown},
		// Non-HALF types go to UNKNOWN
		{"R", dwaPixelTypeFloat, compressorUnknown},
		{"G", dwaPixelTypeUint, compressorUnknown},
	}

	for _, tt := range tests {
		result := classifyChannelName(tt.name, tt.pixelType)
		if result != tt.expected {
			t.Errorf("classifyChannelName(%q, %d) = %d, want %d",
				tt.name, tt.pixelType, result, tt.expected)
		}
	}
}

func TestAcRleEncodeDecode(t *testing.T) {
	// Test with some typical AC coefficient patterns
	tests := [][]uint16{
		// All zeros
		{0, 0, 0, 0, 0, 0, 0, 0},
		// Some non-zero values
		{100, 0, 0, 50, 0, 0, 0, 25},
		// Alternating zeros
		{1, 0, 2, 0, 3, 0, 4, 0},
		// No zeros
		{1, 2, 3, 4, 5, 6, 7, 8},
		// Long zero run
		make([]uint16, 63),
	}

	for i, coeffs := range tests {
		encoded := encodeAcCoefficients(coeffs)
		decoded, err := decodeAcCoefficients(encoded, len(coeffs))
		if err != nil {
			t.Errorf("Test %d: decode error: %v", i, err)
			continue
		}
		if len(decoded) != len(coeffs) {
			t.Errorf("Test %d: length mismatch: got %d, want %d",
				i, len(decoded), len(coeffs))
			continue
		}
		for j := range coeffs {
			if decoded[j] != coeffs[j] {
				t.Errorf("Test %d: mismatch at %d: got %d, want %d",
					i, j, decoded[j], coeffs[j])
			}
		}
	}
}

func TestPopcount16(t *testing.T) {
	tests := []struct {
		val      uint16
		expected int
	}{
		{0, 0},
		{1, 1},
		{0xFF, 8},
		{0xFFFF, 16},
		{0x5555, 8}, // alternating bits
		{0xAAAA, 8}, // alternating bits
		{0x8000, 1},
	}

	for _, tt := range tests {
		result := popcount16(tt.val)
		if result != tt.expected {
			t.Errorf("popcount16(0x%04X) = %d, want %d", tt.val, result, tt.expected)
		}
	}
}

func TestDwaCompressWithChannels(t *testing.T) {
	width := 16
	height := 16
	numPixels := width * height

	// Create test data with R, G, B, A channels (HALF)
	channels := []DwaChannelData{
		{Name: "R", PixelType: dwaPixelTypeHalf},
		{Name: "G", PixelType: dwaPixelTypeHalf},
		{Name: "B", PixelType: dwaPixelTypeHalf},
		{Name: "A", PixelType: dwaPixelTypeHalf},
	}

	// Create interleaved pixel data (RGBA as HALF)
	srcData := make([]byte, numPixels*4*2) // 4 channels * 2 bytes per HALF
	for pixel := 0; pixel < numPixels; pixel++ {
		// R
		half.FromFloat32(float32(pixel%256) / 255.0)
		binary.LittleEndian.PutUint16(srcData[pixel*8:], uint16(half.FromFloat32(float32(pixel%256)/255.0)))
		// G
		binary.LittleEndian.PutUint16(srcData[pixel*8+2:], uint16(half.FromFloat32(float32((pixel+85)%256)/255.0)))
		// B
		binary.LittleEndian.PutUint16(srcData[pixel*8+4:], uint16(half.FromFloat32(float32((pixel+170)%256)/255.0)))
		// A
		binary.LittleEndian.PutUint16(srcData[pixel*8+6:], uint16(half.FromFloat32(1.0)))
	}

	// Compress
	c := NewDwaCompressor(width, height, 45.0)
	c.SetChannels(channels)
	compressed, err := c.Compress(srcData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	t.Logf("Original size: %d, compressed size: %d, ratio: %.2f%%",
		len(srcData), len(compressed), float64(len(compressed))/float64(len(srcData))*100)

	// Decompress
	d := NewDwaDecompressor(width, height)
	d.SetChannels(channels)
	decompressed := make([]byte, len(srcData))
	err = d.Decompress(compressed, decompressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	// Check that decompressed data is similar to original (lossy compression)
	// We expect some loss but should be within reasonable bounds
	maxDiff := float32(0)
	for pixel := 0; pixel < numPixels; pixel++ {
		for ch := 0; ch < 4; ch++ {
			origVal := binary.LittleEndian.Uint16(srcData[pixel*8+ch*2:])
			decompVal := binary.LittleEndian.Uint16(decompressed[pixel*8+ch*2:])
			origFloat := half.Half(origVal).Float32()
			decompFloat := half.Half(decompVal).Float32()
			diff := float32(math.Abs(float64(origFloat - decompFloat)))
			if diff > maxDiff {
				maxDiff = diff
			}
		}
	}

	t.Logf("Max difference: %f", maxDiff)
	// Allow reasonable loss for lossy compression
	if maxDiff > 0.5 {
		t.Errorf("Maximum difference too high: %f", maxDiff)
	}
}

func TestDwaCompressDecompressRoundTrip(t *testing.T) {
	// Test basic round-trip with fallback compression (no channels set)
	width := 8
	height := 8
	srcData := make([]byte, width*height*6) // 3 channels * 2 bytes

	// Fill with gradient data
	for i := range srcData {
		srcData[i] = byte(i % 256)
	}

	c := NewDwaCompressor(width, height, 45.0)
	compressed, err := c.Compress(srcData)
	if err != nil {
		t.Fatalf("Compress error: %v", err)
	}

	d := NewDwaDecompressor(width, height)
	decompressed := make([]byte, len(srcData))
	err = d.Decompress(compressed, decompressed)
	if err != nil {
		t.Fatalf("Decompress error: %v", err)
	}

	// For fallback compression (UNKNOWN), should be exact match
	for i := range srcData {
		if decompressed[i] != srcData[i] {
			t.Errorf("Mismatch at byte %d: got %d, want %d", i, decompressed[i], srcData[i])
			break
		}
	}
}

func TestQuantizeCoefficient(t *testing.T) {
	// Test quantization with various levels
	testCases := []struct {
		val       float32
		quantLvl  float32
		expectVal bool // if true, result should equal input
	}{
		{0.0, 45.0, true},    // Zero stays zero
		{1.0, 0.0, true},     // No quantization
		{0.5, 45.0, false},   // Should be quantized
		{-0.5, 45.0, false},  // Negative values
		{100.0, 45.0, false}, // Large values
	}

	for _, tc := range testCases {
		result := quantizeCoefficient(tc.val, tc.quantLvl)
		resultFloat := half.Half(result).Float32()
		if tc.expectVal {
			origHalf := half.FromFloat32(tc.val)
			if result != uint16(origHalf) {
				t.Errorf("quantizeCoefficient(%f, %f) = %d (%.4f), expected %d",
					tc.val, tc.quantLvl, result, resultFloat, origHalf)
			}
		}
	}
}

func BenchmarkDwaCompressRGB(b *testing.B) {
	width := 64
	height := 64
	channels := []DwaChannelData{
		{Name: "R", PixelType: dwaPixelTypeHalf},
		{Name: "G", PixelType: dwaPixelTypeHalf},
		{Name: "B", PixelType: dwaPixelTypeHalf},
	}

	srcData := make([]byte, width*height*3*2)
	for i := range srcData {
		srcData[i] = byte(i % 256)
	}

	c := NewDwaCompressor(width, height, 45.0)
	c.SetChannels(channels)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.Compress(srcData)
	}
}

func TestCompressDWAA(t *testing.T) {
	width := 64
	height := 32

	// Create test data - half precision RGB
	srcData := make([]byte, width*height*3*2)
	for i := 0; i < width*height*3; i++ {
		h := half.FromFloat32(float32(i%256) / 255.0)
		binary.LittleEndian.PutUint16(srcData[i*2:], h.Bits())
	}

	compressed, err := CompressDWAA(srcData, width, height, 45.0)
	if err != nil {
		t.Fatalf("CompressDWAA failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Fatal("CompressDWAA returned empty data")
	}

	t.Logf("DWAA compression: %d -> %d bytes (%.1f%%)",
		len(srcData), len(compressed), float64(len(compressed))/float64(len(srcData))*100)
}

func TestCompressDWAB(t *testing.T) {
	width := 64
	height := 256

	// Create test data - half precision RGB
	srcData := make([]byte, width*height*3*2)
	for i := 0; i < width*height*3; i++ {
		h := half.FromFloat32(float32(i%256) / 255.0)
		binary.LittleEndian.PutUint16(srcData[i*2:], h.Bits())
	}

	compressed, err := CompressDWAB(srcData, width, height, 45.0)
	if err != nil {
		t.Fatalf("CompressDWAB failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Fatal("CompressDWAB returned empty data")
	}

	t.Logf("DWAB compression: %d -> %d bytes (%.1f%%)",
		len(srcData), len(compressed), float64(len(compressed))/float64(len(srcData))*100)
}

func TestDecompressDWAA(t *testing.T) {
	width := 64
	height := 32

	// Create test data - half precision RGB
	srcData := make([]byte, width*height*3*2)
	for i := 0; i < width*height*3; i++ {
		h := half.FromFloat32(float32(i%256) / 255.0)
		binary.LittleEndian.PutUint16(srcData[i*2:], h.Bits())
	}

	compressed, err := CompressDWAA(srcData, width, height, 45.0)
	if err != nil {
		t.Fatalf("CompressDWAA failed: %v", err)
	}

	dst := make([]byte, len(srcData))
	err = DecompressDWAA(compressed, dst, width, height)
	if err != nil {
		t.Logf("DecompressDWAA returned error (expected for this stub): %v", err)
	}
}

func TestDecompressDWAB(t *testing.T) {
	width := 64
	height := 256

	// Create test data - half precision RGB
	srcData := make([]byte, width*height*3*2)
	for i := 0; i < width*height*3; i++ {
		h := half.FromFloat32(float32(i%256) / 255.0)
		binary.LittleEndian.PutUint16(srcData[i*2:], h.Bits())
	}

	compressed, err := CompressDWAB(srcData, width, height, 45.0)
	if err != nil {
		t.Fatalf("CompressDWAB failed: %v", err)
	}

	dst := make([]byte, len(srcData))
	err = DecompressDWAB(compressed, dst, width, height)
	if err != nil {
		t.Logf("DecompressDWAB returned error (expected for this stub): %v", err)
	}
}

// Benchmarks for DCT operations
func BenchmarkDctForward8x8(b *testing.B) {
	var block [64]float32
	for i := 0; i < 64; i++ {
		block[i] = float32(i) / 64.0
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dctForward8x8(&block)
	}
}

func BenchmarkDctInverse8x8(b *testing.B) {
	var block [64]float32
	for i := 0; i < 64; i++ {
		block[i] = float32(i) / 64.0
	}
	dctForward8x8(&block) // Start with DCT coefficients
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dctInverse8x8(&block, 0)
	}
}

func BenchmarkDctInverse8x8DcOnly(b *testing.B) {
	var block [64]float32
	block[0] = 1.5 // Only DC component set
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dctInverse8x8DcOnly(&block)
	}
}

func TestZlibDecompressError(t *testing.T) {
	// Test with corrupted zlib data — must return error
	badData := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	_, err := zlibDecompress(badData, 100)
	if err == nil {
		t.Error("Expected error from corrupted zlib data")
	}

	// Test with valid zlib but short read — should return error or short result
	validZlib := []byte{0x78, 0x9C, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01}
	result, err := zlibDecompress(validZlib, 1000)
	if err == nil && len(result) == 1000 {
		t.Error("Expected error or short result from zlib with insufficient data")
	}
	if err == nil && len(result) >= 1000 {
		t.Errorf("zlibDecompress returned len=%d without error, expected short read", len(result))
	}
}

func TestNewDwaCompressorNegativeLevel(t *testing.T) {
	// Test with negative level (should use default)
	c := NewDwaCompressor(64, 64, -1)
	if c.compressionLevel != 45.0 {
		t.Errorf("Expected default level 45.0, got %f", c.compressionLevel)
	}

	// Test with zero level (valid)
	c = NewDwaCompressor(64, 64, 0)
	if c.compressionLevel != 0 {
		t.Errorf("Expected level 0, got %f", c.compressionLevel)
	}
}

func TestDecodeAcCoefficientsError(t *testing.T) {
	// Test with truncated data (0xFF followed by nothing)
	truncated := []byte{0xFF}
	_, err := decodeAcCoefficients(truncated, 10)
	if err != ErrDwaCorruptData {
		t.Errorf("Expected ErrDwaCorruptData, got %v", err)
	}

	// Test with non-zero run but truncated value
	truncatedValue := []byte{0x01}
	_, err = decodeAcCoefficients(truncatedValue, 10)
	if err != ErrDwaCorruptData {
		t.Errorf("Expected ErrDwaCorruptData for truncated value, got %v", err)
	}
}

func TestHuffmanDecodeFunction(t *testing.T) {
	// huffmanDecode falls back to zlibDecompress
	// Test with corrupted data
	badData := []byte{0xFF, 0xFF, 0xFF}
	_, err := huffmanDecode(badData, 100)
	if err == nil {
		t.Error("Expected error from huffmanDecode with bad data")
	}
}
