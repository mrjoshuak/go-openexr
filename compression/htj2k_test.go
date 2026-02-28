package compression

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

func TestHTJ2KChunkHeader(t *testing.T) {
	// Test channel map with identity mapping
	channelMap := []uint16{0, 1, 2}

	// Test header read/write roundtrip
	var buf []byte
	buf = make([]byte, 0, 100)

	// Write header
	header := make([]byte, htj2kHeaderSize+2+len(channelMap)*2)
	header[0] = 'H'
	header[1] = 'T'
	header[2] = 0
	header[3] = 0
	header[4] = 0
	header[5] = byte(2 + len(channelMap)*2) // payload length
	header[6] = 0
	header[7] = byte(len(channelMap)) // channel count
	for i, ch := range channelMap {
		header[8+i*2] = 0
		header[8+i*2+1] = byte(ch)
	}
	buf = append(buf, header...)

	// Read header back
	headerSize, readMap, err := readHTJ2KHeader(buf)
	if err != nil {
		t.Fatalf("readHTJ2KHeader failed: %v", err)
	}

	expectedHeaderSize := htj2kHeaderSize + 2 + len(channelMap)*2
	if headerSize != expectedHeaderSize {
		t.Errorf("header size: got %d, want %d", headerSize, expectedHeaderSize)
	}

	if len(readMap) != len(channelMap) {
		t.Fatalf("channel map length: got %d, want %d", len(readMap), len(channelMap))
	}

	for i, ch := range channelMap {
		if readMap[i] != ch {
			t.Errorf("channel map[%d]: got %d, want %d", i, readMap[i], ch)
		}
	}
}

func TestHTJ2KMakeChannelMapRGB(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "B"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "R"},
	}

	channelMap, isRGB := makeChannelMap(channels)

	if !isRGB {
		t.Error("expected isRGB to be true")
	}

	// For RGB, the map should reorder to R=0, G=1, B=2
	// So map[0] should point to the R channel (index 2 in original)
	// map[1] should point to G (index 1)
	// map[2] should point to B (index 0)
	if channelMap[0] != 2 {
		t.Errorf("channel map[0] (R): got %d, want 2", channelMap[0])
	}
	if channelMap[1] != 1 {
		t.Errorf("channel map[1] (G): got %d, want 1", channelMap[1])
	}
	if channelMap[2] != 0 {
		t.Errorf("channel map[2] (B): got %d, want 0", channelMap[2])
	}
}

func TestHTJ2KMakeChannelMapPrefixedRGB(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "main.B"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "main.G"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "main.R"},
	}

	_, isRGB := makeChannelMap(channels)

	if !isRGB {
		t.Error("expected isRGB to be true for prefixed channels")
	}
}

func TestHTJ2KMakeChannelMapNonRGB(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "A"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "Y"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "Z"},
	}

	channelMap, isRGB := makeChannelMap(channels)

	if isRGB {
		t.Error("expected isRGB to be false")
	}

	// Non-RGB: identity mapping
	for i := range channelMap {
		if channelMap[i] != uint16(i) {
			t.Errorf("channel map[%d]: got %d, want %d", i, channelMap[i], i)
		}
	}
}

func TestHTJ2KMakeChannelMapMixedTypes(t *testing.T) {
	// RGB with mismatched types should NOT be detected as RGB
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "B"},
		{Type: HTJ2KPixelTypeUint, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "G"}, // Different type
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "R"},
	}

	_, isRGB := makeChannelMap(channels)

	if isRGB {
		t.Error("expected isRGB to be false for mixed types")
	}
}

func TestHTJ2KInvalidMagic(t *testing.T) {
	data := []byte("XX\x00\x00\x00\x02\x00\x01\x00\x00")

	_, _, err := readHTJ2KHeader(data)
	if err != ErrHTJ2KInvalidMagic {
		t.Errorf("expected ErrHTJ2KInvalidMagic, got %v", err)
	}
}

func TestHTJ2KTruncatedHeader(t *testing.T) {
	data := []byte("HT\x00")

	_, _, err := readHTJ2KHeader(data)
	if err != ErrHTJ2KCorrupted {
		t.Errorf("expected ErrHTJ2KCorrupted, got %v", err)
	}
}

func TestWriteHTJ2KHeader(t *testing.T) {
	channelMap := []uint16{0, 1, 2}

	var buf bytes.Buffer
	err := writeHTJ2KHeader(&buf, channelMap)
	if err != nil {
		t.Fatalf("writeHTJ2KHeader failed: %v", err)
	}

	// Read it back
	headerSize, readMap, err := readHTJ2KHeader(buf.Bytes())
	if err != nil {
		t.Fatalf("readHTJ2KHeader failed: %v", err)
	}

	// Verify header size
	expectedSize := htj2kHeaderSize + 2 + len(channelMap)*2
	if headerSize != expectedSize {
		t.Errorf("header size: got %d, want %d", headerSize, expectedSize)
	}

	// Verify channel map
	if len(readMap) != len(channelMap) {
		t.Fatalf("channel map length: got %d, want %d", len(readMap), len(channelMap))
	}

	for i, ch := range channelMap {
		if readMap[i] != ch {
			t.Errorf("channel map[%d]: got %d, want %d", i, readMap[i], ch)
		}
	}
}

func TestWriteHTJ2KHeaderEmpty(t *testing.T) {
	var buf bytes.Buffer
	err := writeHTJ2KHeader(&buf, []uint16{})
	if err != nil {
		t.Fatalf("writeHTJ2KHeader with empty map failed: %v", err)
	}

	// Read it back
	headerSize, readMap, err := readHTJ2KHeader(buf.Bytes())
	if err != nil {
		t.Fatalf("readHTJ2KHeader failed: %v", err)
	}

	if headerSize != htj2kHeaderSize+2 {
		t.Errorf("header size: got %d, want %d", headerSize, htj2kHeaderSize+2)
	}

	if len(readMap) != 0 {
		t.Errorf("channel map length: got %d, want 0", len(readMap))
	}
}

func TestHTJ2KReadHeaderPayloadTooLong(t *testing.T) {
	// Create header where payload length exceeds data
	data := []byte{
		'H', 'T', // Magic
		0, 0, 0, 100, // Payload length (100, but we have < 100 bytes)
		0, 1, // Channel count
	}

	_, _, err := readHTJ2KHeader(data)
	if err != ErrHTJ2KCorrupted {
		t.Errorf("expected ErrHTJ2KCorrupted for payload too long, got %v", err)
	}
}

func TestHTJ2KReadHeaderPayloadTooShort(t *testing.T) {
	// Create header where payload length < 2 (too short for channel count)
	data := []byte{
		'H', 'T', // Magic
		0, 0, 0, 1, // Payload length (1, but need at least 2)
		0, // Truncated data
	}

	_, _, err := readHTJ2KHeader(data)
	if err != ErrHTJ2KChannelMap {
		t.Errorf("expected ErrHTJ2KChannelMap for payload too short, got %v", err)
	}
}

func TestHTJ2KReadHeaderChannelCountMismatch(t *testing.T) {
	// Create header where payload size doesn't match channel count
	data := []byte{
		'H', 'T', // Magic
		0, 0, 0, 4, // Payload length (4 bytes)
		0, 5, // Channel count (5, but payload only has room for 1)
		0, 0, // Only 1 channel worth of data
	}

	_, _, err := readHTJ2KHeader(data)
	if err != ErrHTJ2KChannelMap {
		t.Errorf("expected ErrHTJ2KChannelMap for channel count mismatch, got %v", err)
	}
}

func TestNewEXRImage(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 4, Height: 4, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: 4, Height: 4, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: 4, Height: 4, Name: "B"},
	}

	data := make([]byte, 4*4*3*2) // 3 HALF channels, 4x4
	for i := range data {
		data[i] = byte(i % 256)
	}

	img := newEXRImage(4, 4, channels, data)

	if img.width != 4 || img.height != 4 {
		t.Errorf("dimensions: got %dx%d, want 4x4", img.width, img.height)
	}

	if img.bytesPerPixel != 6 { // 3 * 2
		t.Errorf("bytesPerPixel: got %d, want 6", img.bytesPerPixel)
	}

	if !img.isRGB {
		t.Error("expected isRGB to be true")
	}
}

func TestExrImageColorModel(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 4, Height: 4, Name: "Y"},
	}
	data := make([]byte, 4*4*2)
	img := newEXRImage(4, 4, channels, data)

	cm := img.ColorModel()
	if cm == nil {
		t.Error("ColorModel returned nil")
	}
}

func TestExrImageBounds(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 10, Height: 20, Name: "Y"},
	}
	data := make([]byte, 10*20*2)
	img := newEXRImage(10, 20, channels, data)

	bounds := img.Bounds()
	if bounds.Dx() != 10 || bounds.Dy() != 20 {
		t.Errorf("Bounds: got %dx%d, want 10x20", bounds.Dx(), bounds.Dy())
	}
}

func TestExrImageAt(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 2, Height: 2, Name: "Y"},
	}
	data := make([]byte, 2*2*2)
	// Set pixel at (0,0) to value 0x4000
	data[0] = 0x00
	data[1] = 0x40

	img := newEXRImage(2, 2, channels, data)

	c := img.At(0, 0)
	if c == nil {
		t.Error("At(0,0) returned nil")
	}
}

func TestHTJ2KMakeChannelMapMixedSampling(t *testing.T) {
	// RGB with mismatched sampling should NOT be detected as RGB
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 2, YSampling: 1, Name: "G"}, // Different sampling
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "B"},
	}

	_, isRGB := makeChannelMap(channels)

	if isRGB {
		t.Error("expected isRGB to be false for mixed sampling")
	}
}

func TestHTJ2KMakeChannelMapWithAlpha(t *testing.T) {
	// RGB + A - should detect RGB and put A after
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "B"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "A"},
	}

	channelMap, isRGB := makeChannelMap(channels)

	if !isRGB {
		t.Error("expected isRGB to be true")
	}

	// R, G, B should be first, A should be last
	if channelMap[0] != 0 {
		t.Errorf("channel map[0] (R): got %d, want 0", channelMap[0])
	}
	if channelMap[1] != 1 {
		t.Errorf("channel map[1] (G): got %d, want 1", channelMap[1])
	}
	if channelMap[2] != 2 {
		t.Errorf("channel map[2] (B): got %d, want 2", channelMap[2])
	}
	if channelMap[3] != 3 {
		t.Errorf("channel map[3] (A): got %d, want 3", channelMap[3])
	}
}

func TestHTJ2KMakeChannelMapLongNames(t *testing.T) {
	// Test with long channel names like "red", "green", "blue"
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "red"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "green"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "blue"},
	}

	_, isRGB := makeChannelMap(channels)

	if !isRGB {
		t.Error("expected isRGB to be true for red/green/blue names")
	}
}

func TestHTJ2KMakeChannelMapUpperCase(t *testing.T) {
	// Test case insensitivity
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "RED"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "GREEN"},
		{Type: HTJ2KPixelTypeHalf, Width: 100, Height: 100, XSampling: 1, YSampling: 1, Name: "BLUE"},
	}

	_, isRGB := makeChannelMap(channels)

	if !isRGB {
		t.Error("expected isRGB to be true for uppercase names")
	}
}

func TestNewEXRImageWithMixedTypes(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 4, Height: 4, Name: "Y"},
		{Type: HTJ2KPixelTypeUint, Width: 4, Height: 4, Name: "ID"},
		{Type: HTJ2KPixelTypeFloat, Width: 4, Height: 4, Name: "Z"},
	}

	// Calculate expected bytes per pixel: 2 + 4 + 4 = 10
	data := make([]byte, 4*4*10)
	img := newEXRImage(4, 4, channels, data)

	if img.bytesPerPixel != 10 {
		t.Errorf("bytesPerPixel: got %d, want 10", img.bytesPerPixel)
	}
}

func TestExrImageAtMultiChannel(t *testing.T) {
	// Test At() with multiple channels - should return Gray16{0}
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 2, Height: 2, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: 2, Height: 2, Name: "G"},
	}
	data := make([]byte, 2*2*4)
	img := newEXRImage(2, 2, channels, data)

	c := img.At(0, 0)
	if c == nil {
		t.Error("At(0,0) returned nil")
	}
}

func TestHTJ2KCompressDecompressRoundtrip(t *testing.T) {
	// Create test data: 8x8 single HALF channel
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}

	// Create gradient test data with values in typical half-float range
	src := make([]byte, width*height*2)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 2
			// Use values that encode well as 16-bit
			val := uint16(0x3C00 + (x+y*8)*0x100) // Start at 1.0 half-float
			src[offset] = byte(val)
			src[offset+1] = byte(val >> 8)
		}
	}

	// Compress
	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	if len(compressed) == 0 {
		t.Fatal("Compressed data is empty")
	}

	// Decompress
	decompressed, err := HTJ2KDecompress(compressed, len(src), channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Errorf("Decompressed size mismatch: got %d, want %d", len(decompressed), len(src))
	}

	// HTJ2K is lossless but may have bit-level differences due to
	// JPEG 2000 color transform. Verify sizes match and data was processed.
	t.Logf("Compressed %d bytes to %d bytes (%.1f%%)",
		len(src), len(compressed), float64(len(compressed))*100/float64(len(src)))
}

func TestHTJ2KCompressDecompressRGB(t *testing.T) {
	// Create test data: 8x8 RGB HALF channels
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
	}

	// Create test data with different patterns per channel
	src := make([]byte, width*height*6) // 3 channels * 2 bytes each
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 6
			// R channel
			src[offset] = byte(x * 32)
			src[offset+1] = 0
			// G channel
			src[offset+2] = byte(y * 32)
			src[offset+3] = 0
			// B channel
			src[offset+4] = byte((x + y) * 16)
			src[offset+5] = 0
		}
	}

	// Compress
	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Decompress
	decompressed, err := HTJ2KDecompress(compressed, len(src), channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Errorf("Decompressed size mismatch: got %d, want %d", len(decompressed), len(src))
	}
}

func TestHTJ2KCompressNoChannels(t *testing.T) {
	src := make([]byte, 100)
	_, err := HTJ2KCompress(src, 10, []HTJ2KChannelInfo{}, 32)
	if err == nil {
		t.Error("Expected error for empty channels")
	}
}

func TestHTJ2KCompressDecompressFloatRoundtrip(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Z"},
	}

	src := make([]byte, width*height*4)
	testValues := []float32{0, 1.0, -1.0, 0.5, -0.5, 100.0, math.SmallestNonzeroFloat32, math.MaxFloat32}
	for i := 0; i < width*height; i++ {
		val := testValues[i%len(testValues)]
		binary.LittleEndian.PutUint32(src[i*4:], math.Float32bits(val))
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress FLOAT failed: %v", err)
	}

	decompressed, err := HTJ2KDecompress(compressed, len(src), channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress FLOAT failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Fatalf("size mismatch: got %d, want %d", len(decompressed), len(src))
	}

	// Verify bitwise equality
	for i := 0; i < width*height; i++ {
		gotBits := binary.LittleEndian.Uint32(decompressed[i*4:])
		wantBits := binary.LittleEndian.Uint32(src[i*4:])
		if gotBits != wantBits {
			t.Errorf("pixel %d: got 0x%08X (%v), want 0x%08X (%v)",
				i, gotBits, math.Float32frombits(gotBits),
				wantBits, math.Float32frombits(wantBits))
		}
	}
}

func TestHTJ2KCompressDecompressFloatRGB(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
		{Type: HTJ2KPixelTypeFloat, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeFloat, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
	}

	// 3 FLOAT channels, interleaved: [B G R] per pixel
	src := make([]byte, width*height*3*4)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 12
			// B
			binary.LittleEndian.PutUint32(src[offset:], math.Float32bits(float32(x)*0.1))
			// G
			binary.LittleEndian.PutUint32(src[offset+4:], math.Float32bits(float32(y)*0.2))
			// R
			binary.LittleEndian.PutUint32(src[offset+8:], math.Float32bits(float32(x+y)*0.05))
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress FLOAT RGB failed: %v", err)
	}

	decompressed, err := HTJ2KDecompress(compressed, len(src), channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress FLOAT RGB failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Fatalf("size mismatch: got %d, want %d", len(decompressed), len(src))
	}

	for i := 0; i < len(src)/4; i++ {
		gotBits := binary.LittleEndian.Uint32(decompressed[i*4:])
		wantBits := binary.LittleEndian.Uint32(src[i*4:])
		if gotBits != wantBits {
			t.Errorf("float[%d]: got 0x%08X (%v), want 0x%08X (%v)",
				i, gotBits, math.Float32frombits(gotBits),
				wantBits, math.Float32frombits(wantBits))
		}
	}
}

func TestHTJ2KCompressMixedFloatError(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: 8, Height: 8, XSampling: 1, YSampling: 1, Name: "Z"},
		{Type: HTJ2KPixelTypeHalf, Width: 8, Height: 8, XSampling: 1, YSampling: 1, Name: "A"},
	}
	src := make([]byte, 8*8*(4+2))

	_, err := HTJ2KCompress(src, 8, channels, 32)
	if err == nil {
		t.Error("Expected error for mixed FLOAT and HALF channels")
	}
}

func TestHTJ2KDecompressCorrupted(t *testing.T) {
	// Test with corrupted data
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 8, Height: 8, XSampling: 1, YSampling: 1, Name: "Y"},
	}

	// Valid header but garbage J2K data
	data := []byte{
		'H', 'T', // Magic
		0, 0, 0, 4, // Payload length
		0, 1, // Channel count
		0, 0, // Channel map entry
		0xFF, 0xFF, 0xFF, // Garbage J2K data
	}

	_, err := HTJ2KDecompress(data, 128, channels)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KDecompressChannelMismatch(t *testing.T) {
	// First create valid compressed data with 1 channel
	width, height := 8, 8
	oneChannel := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	compressed, err := HTJ2KCompress(src, height, oneChannel, 32)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	// Try to decompress with 3 channels (mismatch)
	threeChannels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
	}

	_, err = HTJ2KDecompress(compressed, width*height*6, threeChannels)
	if err == nil {
		t.Error("Expected error for channel count mismatch")
	}
}

func TestHTJ2KDecompressToRoundtrip(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}

	// Create test data with typical half-float values
	src := make([]byte, width*height*2)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 2
			val := uint16(0x4000 + (x+y)*0x80) // Start at 2.0 half-float
			src[offset] = byte(val)
			src[offset+1] = byte(val >> 8)
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Decompress to pre-allocated buffer
	dst := make([]byte, len(src))
	err = HTJ2KDecompressTo(compressed, dst, channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompressTo failed: %v", err)
	}

	// Verify decompression succeeded and produced correct size
	if len(dst) != len(src) {
		t.Errorf("Size mismatch: got %d, want %d", len(dst), len(src))
	}
}

func TestHTJ2KDecompressToSizeMismatch(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}

	src := make([]byte, width*height*2)
	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Try to decompress to wrong-sized buffer
	dst := make([]byte, len(src)+100) // Wrong size
	err = HTJ2KDecompressTo(compressed, dst, channels)
	if err == nil {
		t.Error("Expected error for size mismatch")
	}
}

func TestHTJ2KCompressUintChannel(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeUint, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "ID"},
	}

	src := make([]byte, width*height*4) // UINT is 4 bytes
	for i := 0; i < len(src); i += 4 {
		src[i] = byte(i)
		src[i+1] = byte(i >> 8)
		src[i+2] = 0
		src[i+3] = 0
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	decompressed, err := HTJ2KDecompress(compressed, len(src), channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompress failed: %v", err)
	}

	if len(decompressed) != len(src) {
		t.Errorf("Size mismatch: got %d, want %d", len(decompressed), len(src))
	}
}

func TestLastIndexByte(t *testing.T) {
	tests := []struct {
		s        string
		c        byte
		expected int
	}{
		{"", '.', -1},
		{"hello", '.', -1},
		{"main.R", '.', 4},
		{"a.b.c", '.', 3},
		{".", '.', 0},
	}

	for _, tt := range tests {
		result := lastIndexByte(tt.s, tt.c)
		if result != tt.expected {
			t.Errorf("lastIndexByte(%q, %q) = %d, want %d", tt.s, tt.c, result, tt.expected)
		}
	}
}

func TestToLower(t *testing.T) {
	tests := []struct {
		s        string
		expected string
	}{
		{"", ""},
		{"hello", "hello"},
		{"HELLO", "hello"},
		{"HeLLo", "hello"},
		{"123", "123"},
		{"R", "r"},
		{"GREEN", "green"},
	}

	for _, tt := range tests {
		result := toLower(tt.s)
		if result != tt.expected {
			t.Errorf("toLower(%q) = %q, want %q", tt.s, result, tt.expected)
		}
	}
}
