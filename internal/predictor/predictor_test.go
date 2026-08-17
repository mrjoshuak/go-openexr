package predictor

import (
	"bytes"
	"testing"
)

func TestEncodeEmpty(t *testing.T) {
	// Empty and single-byte data should be unchanged
	data := []byte{}
	Encode(data)
	if len(data) != 0 {
		t.Error("Empty slice should remain empty")
	}

	data = []byte{42}
	Encode(data)
	if data[0] != 42 {
		t.Errorf("Single byte = %d, want 42", data[0])
	}
}

func TestDecodeEmpty(t *testing.T) {
	data := []byte{}
	Decode(data)
	if len(data) != 0 {
		t.Error("Empty slice should remain empty")
	}

	data = []byte{42}
	Decode(data)
	if data[0] != 42 {
		t.Errorf("Single byte = %d, want 42", data[0])
	}
}

func TestEncodeSimple(t *testing.T) {
	// Constant values encode to the first value followed by the bias, since
	// each delta is 0 and OpenEXR stores deltas biased by +128.
	data := []byte{5, 5, 5, 5}
	Encode(data)
	expected := []byte{5, 128, 128, 128}
	if !bytes.Equal(data, expected) {
		t.Errorf("Encode constant = %v, want %v", data, expected)
	}
}

func TestDecodeSimple(t *testing.T) {
	// Reverse of encode
	data := []byte{5, 128, 128, 128}
	Decode(data)
	expected := []byte{5, 5, 5, 5}
	if !bytes.Equal(data, expected) {
		t.Errorf("Decode constant = %v, want %v", data, expected)
	}
}

func TestEncodeIncreasing(t *testing.T) {
	// Increasing by 1 each time encodes to [first, 129, 129, ...]: a delta of
	// 1 plus the +128 bias.
	data := []byte{10, 11, 12, 13, 14}
	Encode(data)
	expected := []byte{10, 129, 129, 129, 129}
	if !bytes.Equal(data, expected) {
		t.Errorf("Encode increasing = %v, want %v", data, expected)
	}
}

func TestDecodeIncreasing(t *testing.T) {
	data := []byte{10, 129, 129, 129, 129}
	Decode(data)
	expected := []byte{10, 11, 12, 13, 14}
	if !bytes.Equal(data, expected) {
		t.Errorf("Decode increasing = %v, want %v", data, expected)
	}
}

func TestRoundTrip(t *testing.T) {
	original := []byte{100, 50, 25, 200, 150, 75, 255, 0, 128}
	data := make([]byte, len(original))
	copy(data, original)

	Encode(data)
	Decode(data)

	if !bytes.Equal(data, original) {
		t.Errorf("Round-trip failed: got %v, want %v", data, original)
	}
}

func TestEncodeRowEmpty(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	original := make([]byte, len(data))
	copy(original, data)

	// Zero width/channels/bytesPerPixel should do nothing
	EncodeRow(data, 0, 1, 1)
	if !bytes.Equal(data, original) {
		t.Error("EncodeRow with width=0 should not modify data")
	}

	copy(data, original)
	EncodeRow(data, 1, 0, 1)
	if !bytes.Equal(data, original) {
		t.Error("EncodeRow with numChannels=0 should not modify data")
	}

	copy(data, original)
	EncodeRow(data, 1, 1, 0)
	if !bytes.Equal(data, original) {
		t.Error("EncodeRow with bytesPerPixel=0 should not modify data")
	}
}

func TestDecodeRowEmpty(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	original := make([]byte, len(data))
	copy(original, data)

	DecodeRow(data, 0, 1, 1)
	if !bytes.Equal(data, original) {
		t.Error("DecodeRow with width=0 should not modify data")
	}

	copy(data, original)
	DecodeRow(data, 1, 0, 1)
	if !bytes.Equal(data, original) {
		t.Error("DecodeRow with numChannels=0 should not modify data")
	}

	copy(data, original)
	DecodeRow(data, 1, 1, 0)
	if !bytes.Equal(data, original) {
		t.Error("DecodeRow with bytesPerPixel=0 should not modify data")
	}
}

func TestEncodeDecodeRowRoundTrip(t *testing.T) {
	// Simulate a 4-pixel row with 3 channels, 2 bytes per channel
	original := []byte{
		// Pixel 0: R(2), G(2), B(2)
		0x10, 0x20, 0x30, 0x40, 0x50, 0x60,
		// Pixel 1
		0x11, 0x21, 0x31, 0x41, 0x51, 0x61,
		// Pixel 2
		0x12, 0x22, 0x32, 0x42, 0x52, 0x62,
		// Pixel 3
		0x13, 0x23, 0x33, 0x43, 0x53, 0x63,
	}
	data := make([]byte, len(original))
	copy(data, original)

	EncodeRow(data, 4, 3, 2)
	DecodeRow(data, 4, 3, 2)

	if !bytes.Equal(data, original) {
		t.Errorf("Row round-trip failed:\ngot  %v\nwant %v", data, original)
	}
}

func TestEncodeUnderflow(t *testing.T) {
	// Test that underflow wraps correctly (using unsigned byte arithmetic)
	data := []byte{10, 5, 2}
	Encode(data)
	// 10 stays as-is (first byte unchanged)
	// 5 - 10 + 128 = 123
	// 2 - 5 + 128 = 125
	expected := []byte{10, 123, 125}
	if !bytes.Equal(data, expected) {
		t.Errorf("Encode underflow = %v, want %v", data, expected)
	}

	// Decode should restore original
	Decode(data)
	if data[0] != 10 || data[1] != 5 || data[2] != 2 {
		t.Errorf("Decode after underflow = %v, want [10, 5, 2]", data)
	}
}

func BenchmarkEncode(b *testing.B) {
	// 1920x1080 image with 3 channels at 2 bytes each = ~12MB
	data := make([]byte, 1920*1080*3*2)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		Encode(data)
	}
}

func BenchmarkDecode(b *testing.B) {
	data := make([]byte, 1920*1080*3*2)
	for i := range data {
		data[i] = byte(i)
	}

	b.ResetTimer()
	b.SetBytes(int64(len(data)))

	for i := 0; i < b.N; i++ {
		Decode(data)
	}
}
