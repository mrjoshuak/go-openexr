package exr

import (
	"bytes"
	"os"
	"testing"
)

// FuzzOpenReader tests the main file parsing entry point.
// This is the primary attack surface for malformed EXR files.
func FuzzOpenReader(f *testing.F) {
	// Add seed corpus from valid EXR files
	seeds := []string{
		"testdata/sample.exr",
		"testdata/Flowers.exr",
		"testdata/tiled.exr",
		"testdata/comp_none.exr",
		"testdata/comp_rle.exr",
		"testdata/comp_zip.exr",
		"testdata/comp_zips.exr",
		"testdata/comp_piz.exr",
		"testdata/comp_b44.exr",
		"testdata/comp_dwaa_v2.exr",
		"testdata/comp_dwab_v2.exr",
		"testdata/multipart.0001.exr",
		"testdata/11.deep.exr",
	}

	for _, path := range seeds {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			f.Add(data)
		}
	}

	// Add crafted malicious inputs
	addMaliciousSeeds(f)

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 8 {
			return // Too short to be valid
		}

		reader := bytes.NewReader(data)
		file, err := OpenReader(reader, int64(len(data)))
		if err != nil {
			return // Expected for malformed input
		}
		defer file.Close()

		// Try to access headers - this exercises more parsing
		for i := 0; i < file.NumParts(); i++ {
			h := file.Header(i)
			if h == nil {
				continue
			}

			// Exercise header methods
			_ = h.DataWindow()
			_ = h.DisplayWindow()
			_ = h.Compression()
			_ = h.Channels()
			_ = h.IsTiled()
			_ = h.LineOrder()

			// Try to read attributes
			for _, attr := range h.Attributes() {
				_ = attr.Name
				_ = attr.Type
				_ = attr.Value
			}
		}
	})
}

// FuzzScanlineReader tests scanline reading with arbitrary data.
func FuzzScanlineReader(f *testing.F) {
	// Add valid scanline files as seeds
	seeds := []string{
		"testdata/sample.exr",
		"testdata/Flowers.exr",
		"testdata/comp_none.exr",
		"testdata/comp_zip.exr",
	}

	for _, path := range seeds {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 8 {
			return
		}

		reader := bytes.NewReader(data)
		file, err := OpenReader(reader, int64(len(data)))
		if err != nil {
			return
		}
		defer file.Close()

		// Skip tiled files
		if file.IsTiled() {
			return
		}

		sr, err := NewScanlineReader(file)
		if err != nil {
			return
		}

		h := sr.Header()
		if h == nil {
			return
		}

		// Create a minimal framebuffer
		dw := h.DataWindow()
		width := int(dw.Width())
		height := int(dw.Height())

		// No geometry guard here on purpose. Bounding the input in the harness
		// would stop the fuzzer ever reaching the allocation path with hostile
		// dimensions, which is the path that has to be safe. The ceiling below
		// is enforced by the library.
		_ = width
		_ = height

		// Allocate through the library's own API rather than hand-rolling it.
		// The bound belongs in the implementation, where a real caller handed a
		// hostile header gets it too; a limit applied only here would stop the
		// fuzzer exercising the very code that needs to be safe.
		fb, _, err := AllocateChannelsLimit(h.Channels(), dw, 64<<20)
		if err != nil {
			return
		}

		sr.SetFrameBuffer(fb)

		// Try to read a few scanlines (not all, to keep tests fast)
		maxLines := height
		if maxLines > 10 {
			maxLines = 10
		}
		for y := 0; y < maxLines; y++ {
			_ = sr.ReadPixels(int(dw.Min.Y)+y, int(dw.Min.Y)+y)
		}
	})
}

// FuzzTiledReader tests tiled reading with arbitrary data.
func FuzzTiledReader(f *testing.F) {
	seeds := []string{
		"testdata/tiled.exr",
	}

	for _, path := range seeds {
		data, err := os.ReadFile(path)
		if err == nil && len(data) > 0 {
			f.Add(data)
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 8 {
			return
		}

		reader := bytes.NewReader(data)
		file, err := OpenReader(reader, int64(len(data)))
		if err != nil {
			return
		}
		defer file.Close()

		// Skip non-tiled files
		if !file.IsTiled() {
			return
		}

		tr, err := NewTiledReader(file)
		if err != nil {
			return
		}

		h := tr.Header()
		if h == nil {
			return
		}

		// Get tile description
		td := h.TileDescription()
		if td == nil {
			return
		}

		dw := h.DataWindow()
		width := int(dw.Width())
		height := int(dw.Height())

		// No geometry guard here on purpose; see FuzzScanlineReader.
		_ = width
		_ = height

		// Allocate through the library's own API rather than hand-rolling it.
		// The bound belongs in the implementation, where a real caller handed a
		// hostile header gets it too; a limit applied only here would stop the
		// fuzzer exercising the very code that needs to be safe.
		fb, _, err := AllocateChannelsLimit(h.Channels(), dw, 64<<20)
		if err != nil {
			return
		}

		tr.SetFrameBuffer(fb)

		// Try to read first tile only
		_ = tr.ReadTile(0, 0)
	})
}

// FuzzReadHeader tests header parsing in isolation.
func FuzzReadHeader(f *testing.F) {
	// Add some crafted header data
	f.Add([]byte{})
	f.Add([]byte{0x00}) // Empty header marker
	f.Add([]byte("channels\x00chlist\x00\x00\x00\x00\x00"))
	f.Add([]byte("dataWindow\x00box2i\x00\x10\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\x00\x00\x00\xff\x00\x00\x00"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Create minimal valid EXR structure
		magic := []byte{0x76, 0x2f, 0x31, 0x01}
		version := []byte{0x02, 0x00, 0x00, 0x00}

		fullData := append(magic, version...)
		fullData = append(fullData, data...)

		file, err := OpenReader(bytes.NewReader(fullData), int64(len(fullData)))
		if err != nil {
			return
		}
		file.Close()
	})
}

// FuzzAttributeValue tests attribute value parsing.
func FuzzAttributeValue(f *testing.F) {
	// Attribute types to test
	attrTypes := []struct {
		name string
		typ  string
	}{
		{"test", "int"},
		{"test", "float"},
		{"test", "double"},
		{"test", "string"},
		{"test", "stringvector"},
		{"test", "box2i"},
		{"test", "box2f"},
		{"test", "v2i"},
		{"test", "v2f"},
		{"test", "v3i"},
		{"test", "v3f"},
		{"test", "m33f"},
		{"test", "m44f"},
		{"test", "chlist"},
		{"test", "compression"},
		{"test", "lineOrder"},
		{"test", "tiledesc"},
		{"test", "chromaticities"},
		{"test", "keycode"},
		{"test", "timecode"},
		{"test", "rational"},
		{"test", "preview"},
	}

	for _, at := range attrTypes {
		// Create attribute header
		header := at.name + "\x00" + at.typ + "\x00"
		for size := 0; size <= 64; size++ {
			data := header
			// Add size as 4-byte little-endian
			data += string([]byte{byte(size), byte(size >> 8), byte(size >> 16), byte(size >> 24)})
			// Add random-ish data
			for i := 0; i < size; i++ {
				data += string([]byte{byte(i)})
			}
			f.Add([]byte(data))
		}
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Wrap in valid EXR structure
		magic := []byte{0x76, 0x2f, 0x31, 0x01}
		version := []byte{0x02, 0x00, 0x00, 0x00}

		fullData := append(magic, version...)
		fullData = append(fullData, data...)
		// Add null terminator for header
		fullData = append(fullData, 0x00)

		file, err := OpenReader(bytes.NewReader(fullData), int64(len(fullData)))
		if err != nil {
			return
		}
		defer file.Close()

		// Try to read all attributes
		for i := 0; i < file.NumParts(); i++ {
			h := file.Header(i)
			if h != nil {
				for _, attr := range h.Attributes() {
					_ = attr.Value
				}
			}
		}
	})
}

// addMaliciousSeeds adds crafted inputs designed to trigger edge cases.
func addMaliciousSeeds(f *testing.F) {
	// Valid magic + version but truncated
	f.Add([]byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00})

	// Invalid magic
	f.Add([]byte{0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00})

	// Version 1 (unsupported)
	f.Add([]byte{0x76, 0x2f, 0x31, 0x01, 0x01, 0x00, 0x00, 0x00})

	// Version with all flags set
	f.Add([]byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x1e, 0x00, 0x00})

	// Extremely large claimed size in header
	largeSize := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00}
	largeSize = append(largeSize, []byte("dataWindow\x00box2i\x00")...)
	largeSize = append(largeSize, []byte{0xff, 0xff, 0xff, 0x7f}...) // Max int32 size
	f.Add(largeSize)

	// Negative coordinates in box2i
	negCoords := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00}
	negCoords = append(negCoords, []byte("dataWindow\x00box2i\x00\x10\x00\x00\x00")...)
	negCoords = append(negCoords, []byte{0x00, 0x00, 0x00, 0x80, 0x00, 0x00, 0x00, 0x80}...) // Min: -2147483648
	negCoords = append(negCoords, []byte{0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0x7f}...) // Max: 2147483647
	f.Add(negCoords)

	// Zero-sized data window
	zeroWindow := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00}
	zeroWindow = append(zeroWindow, []byte("dataWindow\x00box2i\x00\x10\x00\x00\x00")...)
	zeroWindow = append(zeroWindow, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...)
	zeroWindow = append(zeroWindow, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...)
	f.Add(zeroWindow)

	// Channel list with invalid pixel type
	invalidChannel := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00}
	invalidChannel = append(invalidChannel, []byte("channels\x00chlist\x00\x20\x00\x00\x00")...)
	invalidChannel = append(invalidChannel, []byte("R\x00")...)                                        // Channel name
	invalidChannel = append(invalidChannel, []byte{0xff, 0xff, 0xff, 0xff}...)                         // Invalid pixel type
	invalidChannel = append(invalidChannel, []byte{0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00}...) // plinear, xSampling, ySampling
	f.Add(invalidChannel)

	// Deeply nested / recursive structure simulation
	nested := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x10, 0x00, 0x00} // Multi-part flag
	for i := 0; i < 100; i++ {
		nested = append(nested, []byte("attr\x00string\x00\x04\x00\x00\x00test")...)
	}
	f.Add(nested)

	// Integer overflow in offset table
	overflow := []byte{0x76, 0x2f, 0x31, 0x01, 0x02, 0x00, 0x00, 0x00}
	overflow = append(overflow, []byte("dataWindow\x00box2i\x00\x10\x00\x00\x00")...)
	overflow = append(overflow, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}...)
	overflow = append(overflow, []byte{0xff, 0xff, 0x00, 0x00, 0xff, 0xff, 0x00, 0x00}...) // 65535x65535
	f.Add(overflow)
}
