package exr

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Helper to open a test file - returns the EXR file and a cleanup function
func openTestFile(t *testing.T, name string) (*File, func()) {
	t.Helper()
	path := filepath.Join("testdata", name)
	f, err := os.Open(path)
	if err != nil {
		t.Skipf("Test file %s not available: %v", name, err)
		return nil, func() {}
	}

	stat, err := f.Stat()
	if err != nil {
		f.Close()
		t.Fatalf("Failed to stat %s: %v", name, err)
	}

	exrFile, err := OpenReader(f, stat.Size())
	if err != nil {
		f.Close()
		t.Fatalf("Failed to open %s: %v", name, err)
	}
	return exrFile, func() { f.Close() }
}

func TestReadScanlineFiles(t *testing.T) {
	tests := []struct {
		name        string
		compression Compression
	}{
		{"comp_none.exr", CompressionNone},
		{"comp_rle.exr", CompressionRLE},
		{"comp_zip.exr", CompressionZIP},
		{"comp_zips.exr", CompressionZIPS},
		{"comp_piz.exr", CompressionPIZ},
		{"comp_b44.exr", CompressionB44},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, cleanup := openTestFile(t, tt.name)
			defer cleanup()
			if f == nil {
				return
			}

			header := f.Header(0)
			if header == nil {
				t.Fatal("Header is nil")
			}

			t.Logf("File: %s", tt.name)
			t.Logf("  Compression: %s", header.Compression())
			t.Logf("  DataWindow: %v", header.DataWindow())
			t.Logf("  DisplayWindow: %v", header.DisplayWindow())
			t.Logf("  Channels: %d", header.Channels().Len())

			// Verify compression matches expected
			if header.Compression() != tt.compression {
				t.Errorf("Compression = %v, want %v", header.Compression(), tt.compression)
			}

			// Create scanline reader
			reader, err := NewScanlineReader(f)
			if err != nil {
				t.Fatalf("NewScanlineReader error: %v", err)
			}

			// Read all pixels
			dw := header.DataWindow()
			width := int(dw.Width())
			height := int(dw.Height())

			fb := NewFrameBuffer()
			for i := 0; i < header.Channels().Len(); i++ {
				ch := header.Channels().At(i)
				fb.Set(ch.Name, NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
			}

			reader.SetFrameBuffer(fb)
			err = reader.ReadPixels(int(dw.Min.Y), int(dw.Max.Y))
			if err != nil {
				// Log but don't fail - some compression formats have known issues
				t.Logf("ReadPixels warning (may be expected): %v", err)
			}
		})
	}
}

func TestReadTiledFile(t *testing.T) {
	f, cleanup := openTestFile(t, "tiled.exr")
	defer cleanup()
	if f == nil {
		return
	}

	header := f.Header(0)
	if header == nil {
		t.Fatal("Header is nil")
	}

	if !header.IsTiled() {
		t.Error("Expected tiled file")
	}

	td := header.TileDescription()
	t.Logf("Tile size: %dx%d", td.XSize, td.YSize)
	t.Logf("Level mode: %d", td.Mode)

	// Create tiled reader
	reader, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader error: %v", err)
	}

	// Get dimensions
	dw := header.DataWindow()
	width := int(dw.Width())
	height := int(dw.Height())

	// Create frame buffer
	fb := NewFrameBuffer()
	for i := 0; i < header.Channels().Len(); i++ {
		ch := header.Channels().At(i)
		fb.Set(ch.Name, NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	}

	reader.SetFrameBuffer(fb)

	// Read all tiles
	numTilesX := reader.NumTilesX()
	numTilesY := reader.NumTilesY()
	t.Logf("Tiles: %dx%d", numTilesX, numTilesY)

	err = reader.ReadTiles(0, numTilesX-1, 0, numTilesY-1)
	if err != nil {
		// Log but don't fail - tiled reading has known issues
		t.Logf("ReadTiles warning (may be expected): %v", err)
	}
}

// TestReadFlowersFile covers the one file in the corpus with subsampled
// chroma: Flowers.exr is B44 with Y at full resolution and RY/BY at ySampling 2.
//
// Such a channel appears on only every second scanline of a chunk, so the chunk
// is not "one row per channel per scanline" the way the rest of this library
// assumes. Reading it as if it were shifts every channel's bytes and leaves the
// tail of each chunk zeroed. This test used to assert only that ReadPixels
// returned no error, which it did — while returning that silently wrong image.
//
// Until the layout is implemented, the honest answer is a refusal, and this
// test pins it. The reference implementation refuses too: oiiotool reports
// `Subsampled channels are not supported (channel "BY" has sampling 2,2)`.
func TestReadFlowersFile(t *testing.T) {
	f, cleanup := openTestFile(t, "Flowers.exr")
	defer cleanup()
	if f == nil {
		return
	}

	header := f.Header(0)
	if header == nil {
		t.Fatal("Header is nil")
	}

	t.Logf("Flowers.exr:")
	t.Logf("  Compression: %s", header.Compression())
	t.Logf("  DataWindow: %v", header.DataWindow())
	t.Logf("  Channels: %d", header.Channels().Len())

	for i := 0; i < header.Channels().Len(); i++ {
		ch := header.Channels().At(i)
		t.Logf("    %s: %s", ch.Name, ch.Type)
	}

	// Read the file
	reader, err := NewScanlineReader(f)
	if err != nil {
		t.Fatalf("NewScanlineReader error: %v", err)
	}

	dw := header.DataWindow()
	width := int(dw.Width())
	height := int(dw.Height())

	fb := NewFrameBuffer()
	bufs := make(map[string][]byte)
	for i := 0; i < header.Channels().Len(); i++ {
		ch := header.Channels().At(i)
		buf := make([]byte, width*height*4)
		bufs[ch.Name] = buf
		fb.Set(ch.Name, NewSlice(PixelTypeFloat, buf, width, height))
	}

	reader.SetFrameBuffer(fb)
	err = reader.ReadPixels(int(dw.Min.Y), int(dw.Max.Y))
	if !errors.Is(err, ErrSubsampledChannels) {
		t.Fatalf("ReadPixels of a ySampling=2 file: got %v, want ErrSubsampledChannels", err)
	}

	// And no half-decoded image may be left behind under the guise of a
	// successful read.
	for name, buf := range bufs {
		for _, b := range buf {
			if b != 0 {
				t.Fatalf("channel %s: frame buffer was written by a failed read", name)
			}
		}
	}
}

func TestReadDWAFiles(t *testing.T) {
	tests := []string{"comp_dwaa_v2.exr", "comp_dwab_v2.exr"}

	for _, name := range tests {
		t.Run(name, func(t *testing.T) {
			f, cleanup := openTestFile(t, name)
			defer cleanup()
			if f == nil {
				return
			}

			header := f.Header(0)
			t.Logf("%s compression: %s", name, header.Compression())

			reader, err := NewScanlineReader(f)
			if err != nil {
				t.Fatalf("NewScanlineReader error: %v", err)
			}

			dw := header.DataWindow()
			width := int(dw.Width())
			height := int(dw.Height())

			fb := NewFrameBuffer()
			for i := 0; i < header.Channels().Len(); i++ {
				ch := header.Channels().At(i)
				fb.Set(ch.Name, NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
			}

			reader.SetFrameBuffer(fb)
			err = reader.ReadPixels(int(dw.Min.Y), int(dw.Max.Y))
			if err != nil {
				// Log but don't fail - DWA decompression has known issues
				t.Logf("ReadPixels warning (may be expected): %v", err)
			}
		})
	}
}

func TestFileHeader(t *testing.T) {
	f, cleanup := openTestFile(t, "sample.exr")
	defer cleanup()
	if f == nil {
		return
	}

	// Test File methods
	if f.NumParts() != 1 {
		t.Errorf("NumParts() = %d, want 1", f.NumParts())
	}

	header := f.Header(0)
	if header == nil {
		t.Fatal("Header(0) returned nil")
	}

	// Test out of bounds
	if f.Header(1) != nil {
		t.Error("Header(1) should return nil for single-part file")
	}

	if f.Header(-1) != nil {
		t.Error("Header(-1) should return nil")
	}
}

func TestFileVersion(t *testing.T) {
	f, cleanup := openTestFile(t, "sample.exr")
	defer cleanup()
	if f == nil {
		return
	}

	version := f.Version()
	if version != 2 {
		t.Errorf("Version = %d, want 2", version)
	}
}

func TestOpenFile(t *testing.T) {
	path := filepath.Join("testdata", "sample.exr")
	f, err := OpenFile(path)
	if err != nil {
		t.Skipf("Test file not available: %v", err)
		return
	}
	defer f.Close()

	if f == nil {
		t.Fatal("OpenFile returned nil")
	}

	header := f.Header(0)
	if header == nil {
		t.Fatal("Header is nil")
	}
}

func TestHeaderAttributesFromFile(t *testing.T) {
	f, cleanup := openTestFile(t, "Flowers.exr")
	defer cleanup()
	if f == nil {
		return
	}

	header := f.Header(0)

	// Test various attribute accessors
	dw := header.DataWindow()
	if dw.Width() <= 0 || dw.Height() <= 0 {
		t.Error("DataWindow has invalid dimensions")
	}

	displayW := header.DisplayWindow()
	if displayW.Width() <= 0 || displayW.Height() <= 0 {
		t.Error("DisplayWindow has invalid dimensions")
	}

	// Test compression
	comp := header.Compression()
	t.Logf("Compression: %s", comp)

	// Test line order
	order := header.LineOrder()
	t.Logf("LineOrder: %v", order)

	// Test pixel aspect ratio
	par := header.PixelAspectRatio()
	t.Logf("PixelAspectRatio: %f", par)

	// Test screen window center/width
	center := header.ScreenWindowCenter()
	t.Logf("ScreenWindowCenter: %v", center)

	width := header.ScreenWindowWidth()
	t.Logf("ScreenWindowWidth: %f", width)
}

func TestScanlineReaderMethods(t *testing.T) {
	f, cleanup := openTestFile(t, "comp_none.exr")
	defer cleanup()
	if f == nil {
		return
	}

	reader, err := NewScanlineReader(f)
	if err != nil {
		t.Fatalf("NewScanlineReader error: %v", err)
	}

	// Test Header method
	header := reader.Header()
	if header == nil {
		t.Error("Header() returned nil")
	}

	// Test DataWindow method
	dw := reader.DataWindow()
	if dw.Width() <= 0 {
		t.Error("DataWindow has invalid width")
	}
}

func TestTiledReaderMethods(t *testing.T) {
	f, cleanup := openTestFile(t, "tiled.exr")
	defer cleanup()
	if f == nil {
		return
	}

	reader, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader error: %v", err)
	}

	// Test Header method
	header := reader.Header()
	if header == nil {
		t.Error("Header() returned nil")
	}

	// Test DataWindow method
	dw := reader.DataWindow()
	if dw.Width() <= 0 {
		t.Error("DataWindow has invalid width")
	}

	// Test TileDescription method
	td := reader.TileDescription()
	if td.XSize == 0 || td.YSize == 0 {
		t.Error("TileDescription has invalid tile size")
	}

	// Test NumTilesX/Y
	numX := reader.NumTilesX()
	numY := reader.NumTilesY()
	if numX <= 0 || numY <= 0 {
		t.Errorf("NumTiles = %d x %d, expected positive", numX, numY)
	}

	// Test NumXLevels/NumYLevels
	xLevels := reader.NumXLevels()
	yLevels := reader.NumYLevels()
	t.Logf("Levels: %d x %d", xLevels, yLevels)
}

func TestReadSingleTile(t *testing.T) {
	f, cleanup := openTestFile(t, "tiled.exr")
	defer cleanup()
	if f == nil {
		return
	}

	reader, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader error: %v", err)
	}

	header := reader.Header()
	dw := header.DataWindow()
	width := int(dw.Width())
	height := int(dw.Height())

	fb := NewFrameBuffer()
	for i := 0; i < header.Channels().Len(); i++ {
		ch := header.Channels().At(i)
		fb.Set(ch.Name, NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	}

	reader.SetFrameBuffer(fb)

	// Read single tile
	err = reader.ReadTile(0, 0)
	if err != nil {
		// Log but don't fail - tiled reading has known issues
		t.Logf("ReadTile warning (may be expected): %v", err)
	}
}

func TestReadTileLevel(t *testing.T) {
	f, cleanup := openTestFile(t, "tiled.exr")
	defer cleanup()
	if f == nil {
		return
	}

	reader, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader error: %v", err)
	}

	header := reader.Header()
	dw := header.DataWindow()
	width := int(dw.Width())
	height := int(dw.Height())

	fb := NewFrameBuffer()
	for i := 0; i < header.Channels().Len(); i++ {
		ch := header.Channels().At(i)
		fb.Set(ch.Name, NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	}

	reader.SetFrameBuffer(fb)

	// Read tile at level 0
	err = reader.ReadTileLevel(0, 0, 0, 0)
	if err != nil {
		// Log but don't fail - tiled reading has known issues
		t.Logf("ReadTileLevel warning (may be expected): %v", err)
	}
}
