package compression

import (
	"testing"
)

func TestHuffmanDecoderMaxLenZero(t *testing.T) {
	// Test decoder with maxLen = 0 (no valid codes)
	d := NewHuffmanDecoder([]int{})
	_, err := d.Decode([]byte{0xFF, 0xFF}, 5)
	if err != ErrHuffmanCorrupted {
		t.Errorf("Expected ErrHuffmanCorrupted for maxLen=0, got %v", err)
	}
}

func TestHuffmanDecoderLongCodes(t *testing.T) {
	// Create a frequency distribution that produces codes longer than tableBits (12)
	// We need many symbols with similar frequencies to create a deep tree
	freqs := make([]uint64, 8192)
	for i := 0; i < 8192; i++ {
		freqs[i] = 1 // All same frequency = deep tree
	}

	encoder := NewHuffmanEncoder(freqs)
	codeLengths := encoder.GetLengths()

	// Check if any codes are longer than 12 bits
	maxLen := 0
	for _, l := range codeLengths {
		if l > maxLen {
			maxLen = l
		}
	}

	if maxLen <= 12 {
		t.Skipf("Max code length %d not > 12, cannot test long codes path", maxLen)
	}

	// Test encode/decode round-trip with long codes
	values := make([]uint16, 1000)
	for i := range values {
		values[i] = uint16(i % 8192)
	}

	encoded := encoder.Encode(values)
	if len(encoded) == 0 {
		t.Fatal("Encoded data is empty")
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

func TestHuffmanDecoderCorruptedData(t *testing.T) {
	// Create a valid encoder
	freqs := make([]uint64, 256)
	freqs[0] = 50
	freqs[1] = 30
	freqs[2] = 15
	freqs[3] = 5

	encoder := NewHuffmanEncoder(freqs)
	codeLengths := encoder.GetLengths()
	decoder := NewHuffmanDecoder(codeLengths)

	// Try to decode random garbage
	_, err := decoder.Decode([]byte{0xFF, 0x00, 0xAA, 0x55}, 1000)
	if err == nil {
		t.Error("Expected error decoding garbage data")
	}
}

func TestHuffmanDecoderBitBufferExhaustion(t *testing.T) {
	// Create a simple encoding
	freqs := make([]uint64, 16)
	freqs[0] = 100
	freqs[1] = 50
	freqs[2] = 25

	encoder := NewHuffmanEncoder(freqs)
	codeLengths := encoder.GetLengths()
	decoder := NewHuffmanDecoder(codeLengths)

	// Encode a small amount of data
	values := []uint16{0, 1, 0, 2}
	encoded := encoder.Encode(values)

	// Try to decode more values than encoded
	_, err := decoder.Decode(encoded, 100)
	if err == nil {
		t.Error("Expected error when trying to decode more values than encoded")
	}
}

func TestFastHufDecoderConstructor(t *testing.T) {
	// Test NewFastHufDecoder with empty lengths
	d := NewFastHufDecoder([]int{})
	if d == nil {
		t.Fatal("NewFastHufDecoder returned nil for empty lengths")
	}
	if d.maxLen != 0 {
		t.Errorf("Expected maxLen=0, got %d", d.maxLen)
	}

	// Test with all zero lengths
	d = NewFastHufDecoder(make([]int, 100))
	if d.maxLen != 0 {
		t.Errorf("Expected maxLen=0 for all zero lengths, got %d", d.maxLen)
	}

	// Test with valid lengths
	freqs := make([]uint64, 256)
	freqs[0] = 50
	freqs[1] = 30
	freqs[2] = 15
	freqs[3] = 5

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	d = NewFastHufDecoder(lengths)
	if d.maxLen == 0 {
		t.Error("Expected non-zero maxLen for valid lengths")
	}
}

func TestFastHufDecoderLongCodes(t *testing.T) {
	// Create frequency distribution that produces long codes
	freqs := make([]uint64, 32768)
	for i := 0; i < 32768; i++ {
		freqs[i] = 1 // All same frequency = very deep tree
	}

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	// Check if we have codes > 14 bits (fastHufTableBits)
	maxLen := 0
	for _, l := range lengths {
		if l > maxLen {
			maxLen = l
		}
	}

	if maxLen <= 14 {
		t.Skipf("Max code length %d not > 14, cannot test long codes path", maxLen)
	}

	d := NewFastHufDecoder(lengths)

	// Encode some values and decode
	values := make([]uint16, 1000)
	for i := range values {
		values[i] = uint16(i % 32768)
	}

	encoded := encoder.Encode(values)
	decoded, err := d.Decode(encoded, len(values))
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	for i, v := range decoded {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestFastHufDecoderDecodeInto(t *testing.T) {
	// Test DecodeInto with various scenarios
	freqs := make([]uint64, 256)
	freqs[0] = 100
	freqs[1] = 50
	freqs[2] = 25
	freqs[3] = 12
	freqs[4] = 6

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Test with empty data
	err = d.DecodeInto(nil, []uint16{})
	if err != nil {
		t.Errorf("DecodeInto with nil data should not error: %v", err)
	}

	// Test with empty result
	err = d.DecodeInto([]byte{0xFF}, nil)
	if err != nil {
		t.Errorf("DecodeInto with nil result should not error: %v", err)
	}

	// Test valid decode
	values := []uint16{0, 1, 2, 3, 4, 0, 1, 2}
	encoded := encoder.Encode(values)

	result := make([]uint16, len(values))
	err = d.DecodeInto(encoded, result)
	if err != nil {
		t.Fatalf("DecodeInto error: %v", err)
	}

	for i, v := range result {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestFastHufDecoderDecodeIntoMaxLenZero(t *testing.T) {
	// Test DecodeInto with maxLen = 0
	d, err := GetFastHufDecoder([]int{}) // All zero lengths
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	result := make([]uint16, 10)
	err = d.DecodeInto([]byte{0xFF}, result)
	if err != ErrHuffmanCorrupted {
		t.Errorf("Expected ErrHuffmanCorrupted, got %v", err)
	}
}

func TestFastHufDecoderDecodeIntoWithBits(t *testing.T) {
	// Test DecodeIntoWithBits with valid data
	freqs := make([]uint64, 256)
	freqs[0] = 100
	freqs[1] = 50
	freqs[2] = 25

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	values := []uint16{0, 1, 2, 0, 1, 0}
	encoded := encoder.Encode(values)

	// Calculate total bits
	nBits := 0
	for _, v := range values {
		if int(v) < len(lengths) {
			nBits += lengths[v]
		}
	}

	result := make([]uint16, len(values))
	err = d.DecodeIntoWithBits(encoded, result, nBits, 65536)
	if err != nil {
		t.Fatalf("DecodeIntoWithBits error: %v", err)
	}

	for i, v := range result {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestFastHufDecoderDecodeIntoWithBitsEmpty(t *testing.T) {
	freqs := make([]uint64, 256)
	freqs[0] = 100

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Empty data
	err = d.DecodeIntoWithBits(nil, []uint16{}, 0, 65536)
	if err != nil {
		t.Errorf("Empty data should not error: %v", err)
	}

	// Empty result
	err = d.DecodeIntoWithBits([]byte{0xFF}, nil, 8, 65536)
	if err != nil {
		t.Errorf("Empty result should not error: %v", err)
	}
}

func TestFastHufDecoderDecodeIntoWithBitsMaxLenZero(t *testing.T) {
	d, err := GetFastHufDecoder([]int{})
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	result := make([]uint16, 10)
	err = d.DecodeIntoWithBits([]byte{0xFF}, result, 8, 65536)
	if err != ErrHuffmanCorrupted {
		t.Errorf("Expected ErrHuffmanCorrupted, got %v", err)
	}
}

func TestFastHufDecoderResetWithBoundsEdgeCases(t *testing.T) {
	d, err := GetFastHufDecoder(nil)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Test with minIdx > maxIdx
	if err := d.ResetWithBounds([]int{1, 2, 3}, 5, 2); err != nil {
		t.Errorf("ResetWithBounds failed: %v", err)
	}
	if d.maxLen != 0 {
		t.Errorf("Expected maxLen=0 for invalid bounds, got %d", d.maxLen)
	}

	// Test with negative minIdx
	lengths := []int{0, 0, 3, 0, 2}
	if err := d.ResetWithBounds(lengths, -1, 4); err != nil {
		t.Errorf("ResetWithBounds failed: %v", err)
	}
	if d.maxLen == 0 {
		t.Error("Expected non-zero maxLen after ResetWithBounds")
	}

	// Test with maxIdx > len
	if err := d.ResetWithBounds(lengths, 0, 100); err != nil {
		t.Errorf("ResetWithBounds failed: %v", err)
	}
	if d.maxLen == 0 {
		t.Error("Expected non-zero maxLen after ResetWithBounds with large maxIdx")
	}
}

func TestFastHufDecoderResetClearsTable(t *testing.T) {
	// Create decoder with some data
	freqs := make([]uint64, 256)
	freqs[0] = 100
	freqs[1] = 50

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}

	// Reset with empty lengths
	if err := d.Reset([]int{}); err != nil {
		t.Errorf("Reset failed: %v", err)
	}

	// Verify cleared
	if d.maxLen != 0 {
		t.Errorf("Expected maxLen=0 after Reset, got %d", d.maxLen)
	}

	PutFastHufDecoder(d)
}

func TestFastHufDecoderDecodeIntoWithBitsLongCodes(t *testing.T) {
	// Create frequency distribution that produces long codes
	freqs := make([]uint64, 32768)
	for i := 0; i < 32768; i++ {
		freqs[i] = 1 // All same frequency = very deep tree
	}

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	// Check if we have codes > 14 bits (fastHufTableBits)
	maxLen := 0
	for _, l := range lengths {
		if l > maxLen {
			maxLen = l
		}
	}

	if maxLen <= 14 {
		t.Skipf("Max code length %d not > 14, cannot test long codes path", maxLen)
	}

	d := NewFastHufDecoder(lengths)

	// Encode some values
	values := make([]uint16, 100)
	for i := range values {
		values[i] = uint16(i % 32768)
	}

	encoded := encoder.Encode(values)

	// Calculate total bits
	nBits := 0
	for _, v := range values {
		if int(v) < len(lengths) {
			nBits += lengths[v]
		}
	}

	result := make([]uint16, len(values))
	err := d.DecodeIntoWithBits(encoded, result, nBits, 65536)
	if err != nil {
		t.Fatalf("DecodeIntoWithBits error with long codes: %v", err)
	}

	for i, v := range result {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
			break
		}
	}
}

func TestFastHufDecoderTailBytesRead(t *testing.T) {
	// Test the read64 function with various tail sizes (1-7 bytes)
	freqs := make([]uint64, 16)
	freqs[0] = 100
	freqs[1] = 50

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Create short data that will have tail bytes
	values := []uint16{0, 1, 0}
	encoded := encoder.Encode(values)

	// Ensure we have fewer than 8 bytes
	if len(encoded) > 8 {
		t.Skipf("Encoded data %d bytes, need <8 for tail test", len(encoded))
	}

	result := make([]uint16, len(values))
	err = d.DecodeInto(encoded, result)
	if err != nil {
		t.Fatalf("DecodeInto error: %v", err)
	}

	for i, v := range result {
		if v != values[i] {
			t.Errorf("Index %d: got %d, want %d", i, v, values[i])
		}
	}
}

func TestHuffmanArenaFallback(t *testing.T) {
	// Create a very large frequency table to potentially exhaust arena
	// This tests the fallback heap allocation path
	freqs := make([]uint64, 65537) // Max PIZ symbols + 1
	for i := range freqs {
		freqs[i] = uint64(i + 1)
	}

	encoder := NewHuffmanEncoder(freqs)
	if encoder == nil {
		t.Fatal("NewHuffmanEncoder returned nil")
	}

	lengths := encoder.GetLengths()
	if len(lengths) != len(freqs) {
		t.Errorf("Lengths count mismatch: got %d, want %d", len(lengths), len(freqs))
	}
}

func TestHuffmanDecoderFewBitsRemaining(t *testing.T) {
	// Test the fallback path in HuffmanDecoder.Decode when bitsInBuffer < tableBits
	freqs := make([]uint64, 16)
	freqs[0] = 100
	freqs[1] = 50
	freqs[2] = 25
	freqs[3] = 12

	encoder := NewHuffmanEncoder(freqs)
	codeLengths := encoder.GetLengths()
	decoder := NewHuffmanDecoder(codeLengths)

	// Encode a small sequence that will decode with few bits at the end
	values := []uint16{0, 1, 2, 3, 0, 1}
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
}

func TestFastHufDecoderBufferRefill(t *testing.T) {
	// Test with enough data to require buffer refills
	freqs := make([]uint64, 256)
	for i := 0; i < 256; i++ {
		freqs[i] = uint64(256 - i) // Varying frequencies
	}

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Create enough values to require multiple buffer refills
	values := make([]uint16, 10000)
	for i := range values {
		values[i] = uint16(i % 256)
	}

	encoded := encoder.Encode(values)

	result := make([]uint16, len(values))
	err = d.DecodeInto(encoded, result)
	if err != nil {
		t.Fatalf("DecodeInto error: %v", err)
	}

	mismatch := 0
	for i, v := range result {
		if v != values[i] {
			mismatch++
			if mismatch <= 5 {
				t.Errorf("Index %d: got %d, want %d", i, v, values[i])
			}
		}
	}
	if mismatch > 5 {
		t.Errorf("Total mismatches: %d", mismatch)
	}
}

func TestFastHufDecoderDecodeIntoNotFound(t *testing.T) {
	// Test the error path when no valid code is found
	freqs := make([]uint64, 4)
	freqs[0] = 100
	freqs[1] = 50

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Manually create data that won't decode properly
	// Use all zeros which likely won't form valid codes for a long time
	badData := []byte{0x00, 0x00, 0x00, 0x00}

	result := make([]uint16, 1000)
	err = d.DecodeInto(badData, result)
	if err == nil {
		t.Error("Expected error for bad data, got none")
	}
}

func TestHuffmanEncoderEmptyFreqs(t *testing.T) {
	encoder := NewHuffmanEncoder(nil)
	if encoder == nil {
		t.Fatal("NewHuffmanEncoder(nil) returned nil")
	}

	result := encoder.Encode(nil)
	if result != nil {
		t.Error("Encode(nil) should return nil")
	}

	codes := encoder.GetCodes()
	if len(codes) != 0 {
		t.Errorf("GetCodes() should return empty slice, got %d elements", len(codes))
	}
}

func TestHuffmanEncoderAllZeroFreqs(t *testing.T) {
	freqs := make([]uint64, 100)
	encoder := NewHuffmanEncoder(freqs)
	if encoder == nil {
		t.Fatal("NewHuffmanEncoder returned nil")
	}

	// No symbols have non-zero frequency, so encoding should fail gracefully
	values := []uint16{0, 1, 2}
	result := encoder.Encode(values)
	if result != nil {
		t.Error("Encode should return nil for all-zero frequencies")
	}
}

func TestFastHufDecoderEndOfStreamLookup(t *testing.T) {
	// Test the fallback table lookup at end of stream
	freqs := make([]uint64, 8)
	freqs[0] = 100
	freqs[1] = 50
	freqs[2] = 25

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()
	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		t.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	defer PutFastHufDecoder(d)

	// Create very short encoded data
	values := []uint16{0}
	encoded := encoder.Encode(values)

	result := make([]uint16, len(values))
	err = d.DecodeInto(encoded, result)
	if err != nil {
		t.Fatalf("DecodeInto error: %v", err)
	}

	if result[0] != values[0] {
		t.Errorf("Got %d, want %d", result[0], values[0])
	}
}

func BenchmarkFastHufDecoderDecodeInto(b *testing.B) {
	// Create realistic frequency distribution
	freqs := make([]uint64, 1000)
	for i := range freqs {
		freqs[i] = uint64(1000 - i)
	}

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	values := make([]uint16, 64*1024)
	for i := range values {
		values[i] = uint16(i % 1000)
	}
	encoded := encoder.Encode(values)

	d, err := GetFastHufDecoder(lengths)
	if err != nil {
		b.Fatalf("GetFastHufDecoder failed: %v", err)
	}
	result := make([]uint16, len(values))

	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))

	for i := 0; i < b.N; i++ {
		d.DecodeInto(encoded, result)
	}

	PutFastHufDecoder(d)
}

func BenchmarkFastHufDecoderPooled(b *testing.B) {
	freqs := make([]uint64, 500)
	for i := range freqs {
		freqs[i] = uint64(500 - i)
	}

	encoder := NewHuffmanEncoder(freqs)
	lengths := encoder.GetLengths()

	values := make([]uint16, 16*1024)
	for i := range values {
		values[i] = uint16(i % 500)
	}
	encoded := encoder.Encode(values)

	b.ResetTimer()
	b.SetBytes(int64(len(encoded)))

	for i := 0; i < b.N; i++ {
		d, err := GetFastHufDecoder(lengths)
		if err != nil {
			b.Fatalf("GetFastHufDecoder failed: %v", err)
		}
		result := make([]uint16, len(values))
		d.DecodeInto(encoded, result)
		PutFastHufDecoder(d)
	}
}
