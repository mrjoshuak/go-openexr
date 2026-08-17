package compression

import (
	"bytes"
	"testing"
)

// FuzzRLEDecompress tests RLE decompression with arbitrary data.
func FuzzRLEDecompress(f *testing.F) {
	// Valid RLE data seeds
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x01, 0x41})                   // Single byte literal
	f.Add([]byte{0x7f, 0x41})                   // Max literal run
	f.Add([]byte{0x80, 0x41})                   // Run of 1
	f.Add([]byte{0xff, 0x41})                   // Max run of 127
	f.Add([]byte{0x03, 0x41, 0x42, 0x43, 0x44}) // 4-byte literal

	// Malicious seeds
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})         // All max run codes
	f.Add(bytes.Repeat([]byte{0x7f}, 1000))       // Many literal codes without data
	f.Add(bytes.Repeat([]byte{0xff, 0x00}, 1000)) // Many runs

	f.Fuzz(func(t *testing.T, data []byte) {
		// Try decompression - should not panic or hang
		_, _ = RLEDecompress(data, 1024*1024) // 1MB max output
	})
}

// FuzzRLERoundtrip tests RLE compress/decompress roundtrip.
func FuzzRLERoundtrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x41, 0x41, 0x41, 0x41})
	f.Add([]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05})
	f.Add(bytes.Repeat([]byte{0x42}, 1000))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 100000 {
			return // Limit input size
		}

		compressed := RLECompress(data)
		if compressed == nil && len(data) > 0 {
			return
		}

		decompressed, err := RLEDecompress(compressed, len(data))
		if err != nil {
			t.Errorf("roundtrip failed: compress succeeded but decompress failed: %v", err)
			return
		}

		if !bytes.Equal(data, decompressed) {
			t.Errorf("roundtrip data mismatch")
		}
	})
}

// FuzzZIPDecompress tests ZIP (zlib) decompression.
func FuzzZIPDecompress(f *testing.F) {
	// Valid zlib headers
	f.Add([]byte{0x78, 0x9c}) // Default compression
	f.Add([]byte{0x78, 0x01}) // No compression
	f.Add([]byte{0x78, 0xda}) // Best compression

	// Compressed empty data
	f.Add([]byte{0x78, 0x9c, 0x03, 0x00, 0x00, 0x00, 0x00, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 {
			return
		}

		_, _ = ZIPDecompress(data, 1024*1024) // 1MB max
	})
}

// FuzzZIPRoundtrip tests ZIP compress/decompress roundtrip.
func FuzzZIPRoundtrip(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add(bytes.Repeat([]byte{0x42}, 1000))
	f.Add([]byte{0x01, 0x02, 0x03, 0x04, 0x05})

	f.Fuzz(func(t *testing.T, data []byte) {
		// Skip empty or very small data (ZIPCompress returns nil for empty)
		if len(data) < 1 {
			return
		}
		if len(data) > 100000 {
			return
		}

		compressed, err := ZIPCompress(data)
		if err != nil {
			return
		}
		if compressed == nil {
			// ZIPCompress returns nil for empty input - skip
			return
		}

		decompressed, err := ZIPDecompress(compressed, len(data)) // Must be exact size
		if err != nil {
			t.Errorf("roundtrip failed for %d bytes: %v", len(data), err)
			return
		}

		if !bytes.Equal(data, decompressed) {
			t.Errorf("data mismatch")
		}
	})
}

// FuzzPIZDecompress tests PIZ decompression.
func FuzzPIZDecompress(f *testing.F) {
	// PIZ uses huffman + wavelet, hard to craft valid seeds
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add(bytes.Repeat([]byte{0x00}, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		// PIZ needs dimensions
		width := 64
		height := 64
		numChannels := 3

		_, _ = PIZDecompress(data, width, height, numChannels)
	})
}

// FuzzPIZRoundtrip tests PIZ compress/decompress roundtrip.
func FuzzPIZRoundtrip(f *testing.F) {
	// Add some valid half-float data as bytes (2 bytes per uint16)
	f.Add(make([]byte, 64*64*3*2))

	f.Fuzz(func(t *testing.T, rawData []byte) {
		// Must have even length for uint16 conversion
		if len(rawData) < 128 || len(rawData)%2 != 0 {
			return
		}
		if len(rawData) > 20000 {
			return
		}

		// Convert bytes to uint16 slice
		numValues := len(rawData) / 2
		data := make([]uint16, numValues)
		for i := 0; i < numValues; i++ {
			data[i] = uint16(rawData[i*2]) | uint16(rawData[i*2+1])<<8
		}

		// Calculate dimensions
		width := 64
		height := len(data) / (width * 3)
		if height < 1 {
			height = 1
		}
		numChannels := 3

		// Trim to exact size
		expectedLen := width * height * numChannels
		if len(data) < expectedLen {
			return
		}
		data = data[:expectedLen]

		compressed, err := PIZCompress(data, width, height, numChannels)
		if err != nil {
			return
		}

		decompressed, err := PIZDecompress(compressed, width, height, numChannels)
		if err != nil {
			t.Errorf("roundtrip failed: %v", err)
			return
		}

		if len(decompressed) != len(data) {
			t.Errorf("length mismatch: got %d, want %d", len(decompressed), len(data))
		}
	})
}

// FuzzB44Decompress tests B44 decompression.
func FuzzB44Decompress(f *testing.F) {
	// B44 block size is 14 bytes
	f.Add(make([]byte, 14))   // Single block
	f.Add(make([]byte, 14*4)) // 2x2 blocks
	f.Add(bytes.Repeat([]byte{0xff}, 14*16))

	f.Fuzz(func(t *testing.T, data []byte) {
		width := 64
		height := 64

		channels := []B44ChannelInfo{
			{Type: 1, XSampling: 1, YSampling: 1}, // Half float
		}

		expectedSize := width * height * 2 // Half-float output
		_, _ = B44Decompress(data, channels, width, height, expectedSize)
	})
}

// FuzzPXR24Decompress tests PXR24 decompression.
func FuzzPXR24Decompress(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add(bytes.Repeat([]byte{0xff}, 100))

	f.Fuzz(func(t *testing.T, data []byte) {
		width := 64
		height := 64

		channels := []ChannelInfo{
			{Type: 2, Width: width, Height: height}, // Float
			{Type: 2, Width: width, Height: height}, // Float
			{Type: 2, Width: width, Height: height}, // Float
		}

		expectedSize := width * height * 3 * 4 // 3 float channels
		_, _ = PXR24Decompress(data, channels, width, height, expectedSize)
	})
}

// FuzzDWADecompress tests DWA decompression.
func FuzzDWADecompress(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00, 0x00, 0x00})
	f.Add(bytes.Repeat([]byte{0x00}, 256))

	f.Fuzz(func(t *testing.T, data []byte) {
		width := 64
		height := 64

		channels := []DwaChannel{
			{Name: "A", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
			{Name: "B", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
			{Name: "G", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
			{Name: "R", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
			{Name: "Z", PixelType: DwaPixelTypeFloat, XSampling: 1, YSampling: 1},
		}

		dst := make([]byte, width*height*(4*2+4))
		_ = DWADecompress(data, channels, 0, width-1, 0, height-1, dst)
	})
}

// FuzzInterleave tests the interleave/deinterleave functions.
func FuzzInterleave(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x01, 0x02, 0x03, 0x04})
	f.Add(bytes.Repeat([]byte{0xaa, 0x55}, 500))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data) > 100000 {
			return
		}

		// Ensure even length for interleave
		if len(data)%2 != 0 {
			data = data[:len(data)-1]
		}

		// Interleave then deinterleave
		interleaved := Interleave(data)
		deinterleaved := Deinterleave(interleaved)

		if !bytes.Equal(data, deinterleaved) {
			t.Errorf("interleave roundtrip failed")
		}
	})
}
