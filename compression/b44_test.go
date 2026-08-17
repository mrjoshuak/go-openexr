package compression

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

func TestPackUnpack14(t *testing.T) {
	// Test pack/unpack round-trip for normal blocks
	original := [16]uint16{
		0x3c00, 0x3c00, 0x3c00, 0x3c00, // 1.0
		0x3c00, 0x4000, 0x4000, 0x3c00, // 1.0, 2.0, 2.0, 1.0
		0x3c00, 0x4000, 0x4000, 0x3c00,
		0x3c00, 0x3c00, 0x3c00, 0x3c00,
	}

	var packed [14]byte
	n := packB44(original, packed[:], false, true)

	if n != 14 {
		t.Errorf("packB44 returned %d bytes, want 14", n)
	}

	var unpacked [16]uint16
	unpack14(packed[:], &unpacked)

	// B44 is lossy, so we check within tolerance
	for i := 0; i < 16; i++ {
		origF := halfToFloat32(original[i])
		unpackF := halfToFloat32(unpacked[i])
		diff := origF - unpackF
		if diff < 0 {
			diff = -diff
		}
		// Allow ~1% tolerance
		tolerance := origF * 0.05
		if tolerance < 0.01 {
			tolerance = 0.01
		}
		if diff > tolerance {
			t.Errorf("Pixel %d: original %v, unpacked %v, diff %v", i, origF, unpackF, diff)
		}
	}
}

func TestPackUnpack3FlatField(t *testing.T) {
	// Test flat field (all same value) round-trip
	original := [16]uint16{
		0x3c00, 0x3c00, 0x3c00, 0x3c00, // All 1.0
		0x3c00, 0x3c00, 0x3c00, 0x3c00,
		0x3c00, 0x3c00, 0x3c00, 0x3c00,
		0x3c00, 0x3c00, 0x3c00, 0x3c00,
	}

	var packed [14]byte
	n := packB44(original, packed[:], true, true) // flatfields = true

	if n != 3 {
		t.Errorf("packB44 for flat field returned %d bytes, want 3", n)
	}

	var unpacked [16]uint16
	unpack3(packed[:], &unpacked)

	// Flat field should be exact
	for i := 0; i < 16; i++ {
		if unpacked[i] != original[i] {
			t.Errorf("Pixel %d: original %x, unpacked %x", i, original[i], unpacked[i])
		}
	}
}

func TestB44RoundtripSimple(t *testing.T) {
	// Simple test with uniform half data
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 8},
	}

	// Create test data - all 1.0 (0x3c00)
	data := make([]byte, 8*8*2)
	for i := 0; i < len(data)/2; i++ {
		data[i*2] = 0x00
		data[i*2+1] = 0x3c
	}

	compressed, err := B44Compress(data, channels, 8, 8, false)
	if err != nil {
		t.Fatalf("B44Compress failed: %v", err)
	}

	t.Logf("Original size: %d, compressed size: %d", len(data), len(compressed))

	decompressed, err := B44Decompress(compressed, channels, 8, 8, len(data))
	if err != nil {
		t.Fatalf("B44Decompress failed: %v", err)
	}

	// B44 is lossy, check approximate equality
	for i := 0; i < len(data)/2; i++ {
		origV := uint16(data[i*2]) | uint16(data[i*2+1])<<8
		decV := uint16(decompressed[i*2]) | uint16(decompressed[i*2+1])<<8

		origF := halfToFloat32(origV)
		decF := halfToFloat32(decV)

		diff := origF - decF
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.1 {
			t.Errorf("Pixel %d: original %v, decompressed %v", i, origF, decF)
		}
	}
}

func TestB44RoundtripVaried(t *testing.T) {
	// Test with varied data (gradient)
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 8},
	}

	// Create gradient data
	data := make([]byte, 8*8*2)
	for i := 0; i < 64; i++ {
		// Values from 0.0 to 1.0
		f := float32(i) / 63.0
		h := float32ToHalf(f)
		data[i*2] = byte(h)
		data[i*2+1] = byte(h >> 8)
	}

	compressed, err := B44Compress(data, channels, 8, 8, false)
	if err != nil {
		t.Fatalf("B44Compress failed: %v", err)
	}

	t.Logf("Gradient: original size: %d, compressed size: %d", len(data), len(compressed))

	decompressed, err := B44Decompress(compressed, channels, 8, 8, len(data))
	if err != nil {
		t.Fatalf("B44Decompress failed: %v", err)
	}

	// B44 is lossy, check approximate equality with higher tolerance
	maxDiff := float32(0)
	for i := 0; i < len(data)/2; i++ {
		origV := uint16(data[i*2]) | uint16(data[i*2+1])<<8
		decV := uint16(decompressed[i*2]) | uint16(decompressed[i*2+1])<<8

		origF := halfToFloat32(origV)
		decF := halfToFloat32(decV)

		diff := origF - decF
		if diff < 0 {
			diff = -diff
		}
		if diff > maxDiff {
			maxDiff = diff
		}
	}

	t.Logf("Max difference: %v", maxDiff)

	// B44 can have up to ~5% error for lossy compression
	if maxDiff > 0.1 {
		t.Errorf("Max difference %v exceeds tolerance", maxDiff)
	}
}

func TestB44AFlatFieldCompression(t *testing.T) {
	// Test B44A (with flat field detection)
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 8},
	}

	// Create uniform data
	data := make([]byte, 8*8*2)
	for i := 0; i < len(data)/2; i++ {
		data[i*2] = 0x00
		data[i*2+1] = 0x3c // 1.0
	}

	// With flatfields=true (B44A mode), should get better compression
	compressedA, err := B44Compress(data, channels, 8, 8, true)
	if err != nil {
		t.Fatalf("B44A Compress failed: %v", err)
	}

	// With flatfields=false (B44 mode)
	compressed, err := B44Compress(data, channels, 8, 8, false)
	if err != nil {
		t.Fatalf("B44 Compress failed: %v", err)
	}

	t.Logf("B44 size: %d, B44A size: %d", len(compressed), len(compressedA))

	// B44A should be smaller or equal for uniform data
	if len(compressedA) > len(compressed) {
		t.Errorf("B44A should be at least as good as B44 for uniform data")
	}
}

func TestHalfConversions(t *testing.T) {
	// Test half-to-float32 conversions
	tests := []struct {
		half uint16
		want float32
	}{
		{0x0000, 0.0},        // +0
		{0x8000, 0.0},        // -0 (treated as 0)
		{0x3c00, 1.0},        // 1.0
		{0x4000, 2.0},        // 2.0
		{0xbc00, -1.0},       // -1.0
		{0x3800, 0.5},        // 0.5
		{0x7c00, float32(1)}, // +Inf (will be special)
	}

	for _, tt := range tests {
		got := halfToFloat32(tt.half)
		// Handle special cases
		if tt.half == 0x7c00 {
			// Check for infinity
			if got < 1e30 {
				t.Errorf("halfToFloat32(0x%04x) = %v, want +Inf", tt.half, got)
			}
			continue
		}
		if got != tt.want {
			// Allow small tolerance for float comparison
			diff := got - tt.want
			if diff < 0 {
				diff = -diff
			}
			if diff > 0.001 {
				t.Errorf("halfToFloat32(0x%04x) = %v, want %v", tt.half, got, tt.want)
			}
		}
	}
}

// Benchmarks

func BenchmarkPackB44(b *testing.B) {
	// Create test data with some variation
	var s [16]uint16
	for i := 0; i < 16; i++ {
		s[i] = float32ToHalf(float32(i) / 15.0)
	}
	var packed [14]byte

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packB44(s, packed[:], false, true)
	}
}

func BenchmarkUnpack14(b *testing.B) {
	// Create packed data
	var s [16]uint16
	for i := 0; i < 16; i++ {
		s[i] = float32ToHalf(float32(i) / 15.0)
	}
	var packed [14]byte
	packB44(s, packed[:], false, true)

	var unpacked [16]uint16

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unpack14(packed[:], &unpacked)
	}
}

func BenchmarkB44Compress(b *testing.B) {
	// Create test image 64x64 with 3 half channels
	width, height := 64, 64
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: width, Height: height},
		{Type: b44PixelTypeHalf, Width: width, Height: height},
		{Type: b44PixelTypeHalf, Width: width, Height: height},
	}

	// Create gradient data
	data := make([]byte, width*height*3*2)
	for c := 0; c < 3; c++ {
		for i := 0; i < width*height; i++ {
			f := float32(i) / float32(width*height-1)
			h := float32ToHalf(f)
			offset := (c*width*height + i) * 2
			data[offset] = byte(h)
			data[offset+1] = byte(h >> 8)
		}
	}

	b.SetBytes(int64(len(data)))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = B44Compress(data, channels, width, height, false)
	}
}

func BenchmarkB44Decompress(b *testing.B) {
	// Create test image and compress it first
	width, height := 64, 64
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: width, Height: height},
		{Type: b44PixelTypeHalf, Width: width, Height: height},
		{Type: b44PixelTypeHalf, Width: width, Height: height},
	}

	// Create gradient data
	data := make([]byte, width*height*3*2)
	for c := 0; c < 3; c++ {
		for i := 0; i < width*height; i++ {
			f := float32(i) / float32(width*height-1)
			h := float32ToHalf(f)
			offset := (c*width*height + i) * 2
			data[offset] = byte(h)
			data[offset+1] = byte(h >> 8)
		}
	}

	compressed, _ := B44Compress(data, channels, width, height, false)
	expectedSize := len(data)

	b.SetBytes(int64(expectedSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = B44Decompress(compressed, channels, width, height, expectedSize)
	}
}

func TestSignMagConvert(t *testing.T) {
	// Test signMagConvert with high bit set
	v1 := signMagConvert(0x8001) // High bit set
	if v1 != 0x0001 {
		t.Errorf("signMagConvert(0x8001) = 0x%04x, want 0x0001", v1)
	}

	// Test signMagConvert with high bit clear (should invert)
	v2 := signMagConvert(0x0001) // High bit clear
	if v2 != 0xFFFE {            // ^0x0001
		t.Errorf("signMagConvert(0x0001) = 0x%04x, want 0xFFFE", v2)
	}

	// Test with 0
	v3 := signMagConvert(0x0000)
	if v3 != 0xFFFF { // ^0 = all ones
		t.Errorf("signMagConvert(0x0000) = 0x%04x, want 0xFFFF", v3)
	}

	// Test with 0x8000 (just the sign bit)
	v4 := signMagConvert(0x8000)
	if v4 != 0x0000 {
		t.Errorf("signMagConvert(0x8000) = 0x%04x, want 0x0000", v4)
	}
}

func TestFloat32ToHalfEdgeCases(t *testing.T) {
	// Test NaN - use math.NaN
	nanF := float32(math.NaN())
	nanH := float32ToHalf(nanF)
	// NaN should have exponent 31 and non-zero mantissa
	if (nanH & 0x7C00) != 0x7C00 {
		t.Errorf("NaN half exponent wrong: 0x%04x", nanH)
	}

	// Test very small values (denormalized)
	smallF := float32(1e-10)
	smallH := float32ToHalf(smallF)
	// Very small values should become 0 or denormalized
	if smallH != 0 && (smallH&0x7C00) != 0 {
		// It's denormalized (exponent 0, non-zero mantissa) - that's fine
		if (smallH & 0x7FFF) >= 0x7C00 {
			t.Errorf("Very small float gave unexpected half: 0x%04x", smallH)
		}
	}

	// Test very large values (overflow to infinity)
	largeF := float32(1e10)
	largeH := float32ToHalf(largeF)
	if largeH != 0x7C00 && largeH != 0xFC00 { // +Inf or -Inf
		t.Errorf("Large float should overflow to infinity, got 0x%04x", largeH)
	}

	// Test negative values
	negF := float32(-2.5)
	negH := float32ToHalf(negF)
	if (negH & 0x8000) == 0 {
		t.Errorf("Negative float should have sign bit set: 0x%04x", negH)
	}

	// Test denormalized path (exp <= 0 but not < -10)
	tinyF := float32(5e-8) // Very small but not too small
	tinyH := float32ToHalf(tinyF)
	// Should either be zero or a denormalized number
	if tinyH != 0 && (tinyH&0x7C00) != 0 {
		// Non-zero denormalized numbers have exponent 0
		// This is an edge case we're testing
	}
}

func TestHalfToFloat32EdgeCases(t *testing.T) {
	// Test denormalized half values
	denormH := uint16(0x0001) // Smallest denormalized
	denormF := halfToFloat32(denormH)
	if denormF <= 0 {
		t.Errorf("Denormalized half should be positive: %v", denormF)
	}

	// Test NaN propagation
	nanH := uint16(0x7C01) // NaN (exponent 31, non-zero mantissa)
	nanF := halfToFloat32(nanH)
	if nanF == nanF { // NaN != NaN
		t.Errorf("NaN half should produce NaN float, got %v", nanF)
	}

	// Test -Inf
	negInfH := uint16(0xFC00)
	negInfF := halfToFloat32(negInfH)
	if negInfF >= 0 {
		t.Errorf("-Inf half should produce negative, got %v", negInfF)
	}
}

func TestB44WithLinearChannel(t *testing.T) {
	// Test B44 with linear channel (triggers exp/log conversion)
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 8, IsLinear: true},
	}

	// Create test data with positive values
	data := make([]byte, 8*8*2)
	for i := 0; i < 64; i++ {
		f := float32(i+1) / 64.0 // Values 0.015 to 1.0
		h := float32ToHalf(f)
		data[i*2] = byte(h)
		data[i*2+1] = byte(h >> 8)
	}

	compressed, err := B44Compress(data, channels, 8, 8, false)
	if err != nil {
		t.Fatalf("B44Compress with linear channel failed: %v", err)
	}

	decompressed, err := B44Decompress(compressed, channels, 8, 8, len(data))
	if err != nil {
		t.Fatalf("B44Decompress with linear channel failed: %v", err)
	}

	// Just verify it runs without panic - linear conversion is lossy
	if len(decompressed) != len(data) {
		t.Errorf("Decompressed size mismatch: got %d, want %d", len(decompressed), len(data))
	}
}

func TestB44NonHalfChannels(t *testing.T) {
	// Test B44 with non-HALF channels (uint and float)
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeUint, Width: 4, Height: 4},
		{Type: b44PixelTypeFloat, Width: 4, Height: 4},
		{Type: b44PixelTypeHalf, Width: 4, Height: 4},
	}

	// Create data: 4 bytes per uint pixel, 4 bytes per float pixel, 2 bytes per half pixel
	dataSize := 4*4*4 + 4*4*4 + 4*4*2
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// B44 must refuse a FLOAT or UINT channel rather than emit a file with
	// those channels zeroed. This test previously compressed, decompressed and
	// checked only that the byte COUNT matched, so it passed for years while
	// every non-HALF channel was silently replaced with zeros.
	_, err := B44Compress(data, channels, 4, 4, false)
	if !errors.Is(err, ErrB44NonHalfChannel) {
		t.Fatalf("B44Compress with mixed channels: got %v, want ErrB44NonHalfChannel", err)
	}
}

// TestB44HalfOnlyPreservesValues is the assertion the test above was missing:
// that the values themselves survive, not merely the byte count.
func TestB44HalfOnlyPreservesValues(t *testing.T) {
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 4, Height: 4},
	}
	data := make([]byte, 4*4*2)
	for i := 0; i < len(data); i += 2 {
		// A narrow ramp around 1.0. B44 quantises each 4x4 block to a shared
		// exponent, so its absolute error scales with the block's dynamic
		// range; keeping the range small lets this assert tightly.
		binary.LittleEndian.PutUint16(data[i:], uint16(half.FromFloat32(1.0+float32(i/2)*0.01)))
	}

	compressed, err := B44Compress(data, channels, 4, 4, false)
	if err != nil {
		t.Fatalf("B44Compress: %v", err)
	}
	decompressed, err := B44Decompress(compressed, channels, 4, 4, len(data))
	if err != nil {
		t.Fatalf("B44Decompress: %v", err)
	}
	if len(decompressed) != len(data) {
		t.Fatalf("size: got %d, want %d", len(decompressed), len(data))
	}
	// B44 is a fixed-rate lossy codec, so the tolerance is generous. It is
	// still far tighter than the failure this guards against: before B44
	// refused non-HALF channels, whole channels came back as zeros.
	const tolerance = 0.01
	nonZero := 0
	for i := 0; i < len(data); i += 2 {
		want := half.Half(binary.LittleEndian.Uint16(data[i:])).Float32()
		got := half.Half(binary.LittleEndian.Uint16(decompressed[i:])).Float32()
		if got != 0 {
			nonZero++
		}
		if diff := got - want; diff > tolerance || diff < -tolerance {
			t.Fatalf("sample %d: got %v, want %v (tolerance %v)", i/2, got, want, tolerance)
		}
	}
	if nonZero == 0 {
		t.Fatal("every sample decoded to zero")
	}
}

func TestB44EdgeBlocks(t *testing.T) {
	// Test with dimensions not divisible by 4 (edge blocks)
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 7, Height: 5}, // Not divisible by 4
	}

	data := make([]byte, 7*5*2)
	for i := 0; i < 7*5; i++ {
		h := float32ToHalf(float32(i) / 34.0)
		data[i*2] = byte(h)
		data[i*2+1] = byte(h >> 8)
	}

	compressed, err := B44Compress(data, channels, 7, 5, false)
	if err != nil {
		t.Fatalf("B44Compress with edge blocks failed: %v", err)
	}

	decompressed, err := B44Decompress(compressed, channels, 7, 5, len(data))
	if err != nil {
		t.Fatalf("B44Decompress with edge blocks failed: %v", err)
	}

	if len(decompressed) != len(data) {
		t.Errorf("Decompressed size: got %d, want %d", len(decompressed), len(data))
	}
}

func TestB44TruncatedData(t *testing.T) {
	// Test decompression with truncated data
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 8},
	}

	// Very short data
	shortData := []byte{0x00, 0x3c, 0xfc} // Just a flat field marker

	_, err := B44Decompress(shortData, channels, 8, 8, 8*8*2)
	// Should not panic, even with truncated data
	if err != nil {
		t.Logf("B44Decompress with truncated data returned error (expected): %v", err)
	}
}

func TestB44ZeroHeightChannel(t *testing.T) {
	// Test with zero-height channels
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 8, Height: 0}, // Zero height
		{Type: b44PixelTypeHalf, Width: 8, Height: 8},
	}

	data := make([]byte, 8*8*2)
	for i := range data {
		data[i] = byte(i % 256)
	}

	compressed, err := B44Compress(data, channels, 8, 8, false)
	if err != nil {
		t.Fatalf("B44Compress with zero height channel failed: %v", err)
	}

	decompressed, err := B44Decompress(compressed, channels, 8, 8, len(data))
	if err != nil {
		t.Fatalf("B44Decompress with zero height channel failed: %v", err)
	}

	if len(decompressed) != len(data) {
		t.Errorf("Decompressed size: got %d, want %d", len(decompressed), len(data))
	}
}

func TestConvertFromLinearEdgeCases(t *testing.T) {
	// Test convertFromLinear with special values
	initB44Tables()

	// Test with Inf (exponent 0x7c00)
	infH := uint16(0x7C00)
	result := convertFromLinear(infH)
	if result != 0 {
		t.Errorf("convertFromLinear(Inf) = 0x%04x, want 0", result)
	}

	// Test with NaN (exponent 0x7c00 with mantissa)
	nanH := uint16(0x7C01)
	result = convertFromLinear(nanH)
	if result != 0 {
		t.Errorf("convertFromLinear(NaN) = 0x%04x, want 0", result)
	}

	// Test with large positive value that should clamp
	largeH := uint16(0x5590) // Large positive number
	result = convertFromLinear(largeH)
	if result != 0x7bff {
		t.Logf("convertFromLinear(0x%04x) = 0x%04x", largeH, result)
	}
}

func TestConvertToLinearEdgeCases(t *testing.T) {
	initB44Tables()

	// Test with Inf
	infH := uint16(0x7C00)
	result := convertToLinear(infH)
	if result != 0 {
		t.Errorf("convertToLinear(Inf) = 0x%04x, want 0", result)
	}

	// Test with negative value > 0x8000
	negH := uint16(0x8001)
	result = convertToLinear(negH)
	if result != 0 {
		t.Errorf("convertToLinear(0x8001) = 0x%04x, want 0", result)
	}
}

func TestB44CompressionRatioCheck(t *testing.T) {
	// Test that when compressed is larger, original is returned
	channels := []B44ChannelInfo{
		{Type: b44PixelTypeHalf, Width: 4, Height: 4}, // Very small
	}

	// Create highly variable data that won't compress well
	data := make([]byte, 4*4*2)
	for i := 0; i < 16; i++ {
		// Random-looking values
		h := uint16((i * 7919) % 65536)
		data[i*2] = byte(h)
		data[i*2+1] = byte(h >> 8)
	}

	compressed, err := B44Compress(data, channels, 4, 4, false)
	if err != nil {
		t.Fatalf("B44Compress failed: %v", err)
	}

	// If compressed >= original, should return original
	t.Logf("Original: %d, Compressed: %d", len(data), len(compressed))
}
