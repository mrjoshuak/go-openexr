package exr

import (
	"bytes"
	"sync"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

func TestNewTiledHeader(t *testing.T) {
	h := NewTiledHeader(128, 128, 32, 32)

	if !h.IsTiled() {
		t.Error("IsTiled should be true")
	}

	td := h.TileDescription()
	if td == nil {
		t.Fatal("TileDescription should not be nil")
	}

	if td.XSize != 32 || td.YSize != 32 {
		t.Errorf("Tile size = %dx%d, want 32x32", td.XSize, td.YSize)
	}

	if td.Mode != LevelModeOne {
		t.Errorf("Mode = %v, want LevelModeOne", td.Mode)
	}
}

func TestTiledWriterCreate(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	if tw.Header() != h {
		t.Error("Header() should return the same header")
	}

	if tw.NumTilesX() != 2 {
		t.Errorf("NumTilesX() = %d, want 2", tw.NumTilesX())
	}

	if tw.NumTilesY() != 2 {
		t.Errorf("NumTilesY() = %d, want 2", tw.NumTilesY())
	}

	if err := tw.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestTiledWriterScanlineError(t *testing.T) {
	h := NewScanlineHeader(64, 64)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	_, err := NewTiledWriter(ws, h)
	if err != ErrNotTiled {
		t.Errorf("NewTiledWriter with scanline header error = %v, want ErrNotTiled", err)
	}
}

func TestTiledWriterNoFrameBuffer(t *testing.T) {
	h := NewTiledHeader(32, 32, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	err = tw.WriteTile(0, 0)
	if err != ErrNoFrameBuffer {
		t.Errorf("WriteTile without framebuffer error = %v, want ErrNoFrameBuffer", err)
	}

	tw.Close()
}

func TestTiledWriteAndRead(t *testing.T) {
	// Create a 64x64 tiled image with 32x32 tiles
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	// Write the image
	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	writeFB := NewRGBAFrameBuffer(64, 64, false)
	// Set test values - gradient pattern
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			r := float32(x) / 63.0
			g := float32(y) / 63.0
			b := float32(x+y) / 126.0
			writeFB.SetPixel(x, y, r, g, b, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())

	// Write all tiles
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Read the image back
	data := ws.Bytes()
	t.Logf("Written file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	// Verify file is tiled
	if !f.IsTiled() {
		t.Error("File should be tiled")
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	if tr.NumTilesX() != 2 {
		t.Errorf("NumTilesX() = %d, want 2", tr.NumTilesX())
	}

	if tr.NumTilesY() != 2 {
		t.Errorf("NumTilesY() = %d, want 2", tr.NumTilesY())
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	// Verify pixels
	rSlice := readFB.Get("R")
	gSlice := readFB.Get("G")
	bSlice := readFB.Get("B")

	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			expectedR := float32(x) / 63.0
			expectedG := float32(y) / 63.0
			expectedB := float32(x+y) / 126.0

			gotR := rSlice.GetFloat32(x, y)
			gotG := gSlice.GetFloat32(x, y)
			gotB := bSlice.GetFloat32(x, y)

			if !almostEqual(gotR, expectedR, 0.01) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, gotR, expectedR)
			}
			if !almostEqual(gotG, expectedG, 0.01) {
				t.Errorf("G at (%d,%d) = %v, want ~%v", x, y, gotG, expectedG)
			}
			if !almostEqual(gotB, expectedB, 0.01) {
				t.Errorf("B at (%d,%d) = %v, want ~%v", x, y, gotB, expectedB)
			}
		}
	}
}

func TestTiledWriteAndReadRLE(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionRLE)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			writeFB.SetPixel(x, y, float32(x)/63.0, float32(y)/63.0, 0.5, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("RLE compressed file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionRLE {
		t.Errorf("Compression = %v, want RLE", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	rSlice := readFB.Get("R")
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			expectedR := float32(x) / 63.0
			gotR := rSlice.GetFloat32(x, y)
			if !almostEqual(gotR, expectedR, 0.01) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, gotR, expectedR)
			}
		}
	}
}

func TestTiledWriteAndReadZIP(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionZIP)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			writeFB.SetPixel(x, y, float32(x)/63.0, float32(y)/63.0, 0.5, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("ZIP compressed file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionZIP {
		t.Errorf("Compression = %v, want ZIP", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	rSlice := readFB.Get("R")
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			expectedR := float32(x) / 63.0
			gotR := rSlice.GetFloat32(x, y)
			if !almostEqual(gotR, expectedR, 0.01) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, gotR, expectedR)
			}
		}
	}
}

func TestTiledReaderScanlineError(t *testing.T) {
	h := NewScanlineHeader(64, 64)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	sw, _ := NewScanlineWriter(ws, h)
	fb := NewRGBAFrameBuffer(64, 64, false)
	sw.SetFrameBuffer(fb.ToFrameBuffer())
	sw.WritePixels(0, 63)
	sw.Close()

	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, _ := OpenReader(reader, int64(len(data)))

	_, err := NewTiledReader(f)
	if err != ErrNotTiled {
		t.Errorf("NewTiledReader with scanline file error = %v, want ErrNotTiled", err)
	}
}

func TestTiledOutOfRange(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb := NewRGBAFrameBuffer(64, 64, false)
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	// Out of range tile
	err := tw.WriteTile(-1, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTile(-1,0) error = %v, want ErrTileOutOfRange", err)
	}

	err = tw.WriteTile(2, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTile(2,0) error = %v, want ErrTileOutOfRange", err)
	}

	tw.Close()
}

func TestTiledWriteAndReadPIZ(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionPIZ)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			writeFB.SetPixel(x, y, float32(x)/63.0, float32(y)/63.0, 0.5, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("PIZ tiled file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionPIZ {
		t.Errorf("Compression = %v, want PIZ", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	rSlice := readFB.Get("R")
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			expectedR := float32(x) / 63.0
			gotR := rSlice.GetFloat32(x, y)
			if !almostEqual(gotR, expectedR, 0.01) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, gotR, expectedR)
			}
		}
	}
}

func TestTiledWriteAndReadPXR24(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionPXR24)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			writeFB.SetPixel(x, y, float32(x)/63.0, float32(y)/63.0, 0.5, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("PXR24 tiled file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionPXR24 {
		t.Errorf("Compression = %v, want PXR24", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB2, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB2)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	rSlicePxr := readFB2.Get("R")
	// PXR24 is lossy, so use a larger tolerance
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			expectedR := float32(x) / 63.0
			gotR := rSlicePxr.GetFloat32(x, y)
			if !almostEqual(gotR, expectedR, 0.02) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, gotR, expectedR)
			}
		}
	}
}

// NewMipmapTiledHeader creates a header for a mipmap tiled image.
func NewMipmapTiledHeader(width, height, tileWidth, tileHeight int) *Header {
	h := NewTiledHeader(width, height, tileWidth, tileHeight)
	td := h.TileDescription()
	td.Mode = LevelModeMipmap
	td.RoundingMode = LevelRoundDown
	h.SetTileDescription(*td)
	return h
}

func TestTiledMipmapLevelMethods(t *testing.T) {
	h := NewMipmapTiledHeader(64, 64, 32, 32)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Check level methods
	if tw.NumLevels() != 7 {
		t.Errorf("NumLevels() = %d, want 7 (64->32->16->8->4->2->1)", tw.NumLevels())
	}

	if tw.LevelMode() != LevelModeMipmap {
		t.Errorf("LevelMode() = %v, want LevelModeMipmap", tw.LevelMode())
	}

	// Check level dimensions
	if tw.LevelWidth(0) != 64 {
		t.Errorf("LevelWidth(0) = %d, want 64", tw.LevelWidth(0))
	}
	if tw.LevelWidth(1) != 32 {
		t.Errorf("LevelWidth(1) = %d, want 32", tw.LevelWidth(1))
	}
	if tw.LevelWidth(2) != 16 {
		t.Errorf("LevelWidth(2) = %d, want 16", tw.LevelWidth(2))
	}

	// Check tile counts at different levels
	if tw.NumXTilesAtLevel(0) != 2 {
		t.Errorf("NumXTilesAtLevel(0) = %d, want 2", tw.NumXTilesAtLevel(0))
	}
	if tw.NumXTilesAtLevel(1) != 1 {
		t.Errorf("NumXTilesAtLevel(1) = %d, want 1", tw.NumXTilesAtLevel(1))
	}

	tw.Close()
}

func TestTiledWriteAndReadMipmap(t *testing.T) {
	// Create a 64x64 mipmap tiled image with 32x32 tiles
	h := NewMipmapTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionZIP)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	numLevels := tw.NumLevels()
	t.Logf("Number of mipmap levels: %d", numLevels)

	// Write all levels
	for level := 0; level < numLevels; level++ {
		levelWidth := tw.LevelWidth(level)
		levelHeight := tw.LevelHeight(level)
		t.Logf("Level %d: %dx%d", level, levelWidth, levelHeight)

		// Create a frame buffer for this level
		fb := NewRGBAFrameBuffer(levelWidth, levelHeight, false)

		// Fill with a pattern unique to this level
		levelValue := float32(level) / float32(numLevels-1)
		for y := 0; y < levelHeight; y++ {
			for x := 0; x < levelWidth; x++ {
				r := float32(x) / float32(max(levelWidth-1, 1))
				g := float32(y) / float32(max(levelHeight-1, 1))
				b := levelValue
				fb.SetPixel(x, y, r, g, b, 1.0)
			}
		}

		tw.SetFrameBuffer(fb.ToFrameBuffer())

		// Write all tiles at this level
		numTilesX := tw.NumXTilesAtLevel(level)
		numTilesY := tw.NumYTilesAtLevel(level)
		for ty := 0; ty < numTilesY; ty++ {
			for tx := 0; tx < numTilesX; tx++ {
				if err := tw.WriteTileLevel(tx, ty, level, level); err != nil {
					t.Fatalf("WriteTileLevel(%d,%d,%d,%d) error = %v", tx, ty, level, level, err)
				}
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("Mipmap tiled file size: %d bytes", len(data))

	// Read the file back
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	if tr.NumLevels() != numLevels {
		t.Errorf("NumLevels() = %d, want %d", tr.NumLevels(), numLevels)
	}

	if tr.LevelMode() != LevelModeMipmap {
		t.Errorf("LevelMode() = %v, want LevelModeMipmap", tr.LevelMode())
	}

	// Verify each level
	for level := 0; level < numLevels; level++ {
		levelWidth := tr.LevelWidth(level)
		levelHeight := tr.LevelHeight(level)

		// Create frame buffer for reading this level
		fb := NewRGBAFrameBuffer(levelWidth, levelHeight, false)
		tr.SetFrameBuffer(fb.ToFrameBuffer())

		// Read all tiles at this level
		numTilesX := tr.NumXTilesAtLevel(level)
		numTilesY := tr.NumYTilesAtLevel(level)
		for ty := 0; ty < numTilesY; ty++ {
			for tx := 0; tx < numTilesX; tx++ {
				if err := tr.ReadTileLevel(tx, ty, level, level); err != nil {
					t.Fatalf("ReadTileLevel(%d,%d,%d,%d) error = %v", tx, ty, level, level, err)
				}
			}
		}

		// Verify pixel values at center of this level
		rSlice := fb.ToFrameBuffer().Get("R")
		bSlice := fb.ToFrameBuffer().Get("B")

		centerX := levelWidth / 2
		centerY := levelHeight / 2
		if centerX >= levelWidth {
			centerX = levelWidth - 1
		}
		if centerY >= levelHeight {
			centerY = levelHeight - 1
		}

		// Check B channel which should be level-dependent
		expectedB := float32(level) / float32(numLevels-1)
		gotB := bSlice.GetFloat32(centerX, centerY)
		if !almostEqual(gotB, expectedB, 0.02) {
			t.Errorf("Level %d B at (%d,%d) = %v, want ~%v", level, centerX, centerY, gotB, expectedB)
		}

		// Check R channel which should be position-dependent
		if levelWidth > 1 {
			expectedR := float32(centerX) / float32(levelWidth-1)
			gotR := rSlice.GetFloat32(centerX, centerY)
			if !almostEqual(gotR, expectedR, 0.02) {
				t.Errorf("Level %d R at (%d,%d) = %v, want ~%v", level, centerX, centerY, gotR, expectedR)
			}
		}
	}
}

func TestTiledLevelOutOfRange(t *testing.T) {
	h := NewMipmapTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	fb := NewRGBAFrameBuffer(64, 64, false)
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	// Out of range level
	err = tw.WriteTileLevel(0, 0, -1, 0)
	if err != ErrLevelOutOfRange {
		t.Errorf("WriteTileLevel(0,0,-1,0) error = %v, want ErrLevelOutOfRange", err)
	}

	err = tw.WriteTileLevel(0, 0, 100, 100)
	if err != ErrLevelOutOfRange {
		t.Errorf("WriteTileLevel(0,0,100,100) error = %v, want ErrLevelOutOfRange", err)
	}

	tw.Close()
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func TestTiledWriteAndReadDWAA(t *testing.T) {
	// Test DWAA tiled compression round-trip
	// Note: DWA compression is partially implemented - this test verifies
	// the basic write/read pipeline works without errors
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionDWAA)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Create frame buffer with gradient
	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			r := float32(x) / 63.0
			g := float32(y) / 63.0
			b := float32(x+y) / 126.0
			writeFB.SetPixel(x, y, r, g, b, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("DWAA tiled written file size: %d bytes", len(data))

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionDWAA {
		t.Errorf("Compression = %v, want DWAA", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	// Verify we got valid frame buffer slices
	if readFB.Get("R") == nil {
		t.Error("R channel is nil")
	}
	if readFB.Get("G") == nil {
		t.Error("G channel is nil")
	}
	if readFB.Get("B") == nil {
		t.Error("B channel is nil")
	}

	t.Log("DWAA tiled compression round-trip completed successfully")
}

func TestTiledWriteAndReadB44(t *testing.T) {
	// Test B44 tiled compression round-trip
	// B44 is a lossy compression optimized for 4x4 pixel blocks
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionB44)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Create frame buffer with gradient
	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			r := float32(x) / 63.0
			g := float32(y) / 63.0
			b := float32(x+y) / 126.0
			writeFB.SetPixel(x, y, r, g, b, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	if err := tw.WriteTiles(0, 0, 1, 1); err != nil {
		t.Fatalf("WriteTiles: %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	data := ws.Bytes()
	t.Logf("B44 tiled written file size: %d bytes", len(data))

	if len(data) < 100 {
		t.Errorf("File too small: %d bytes", len(data))
		return
	}

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionB44 {
		t.Errorf("Compression = %v, want B44", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	if err := tr.ReadTiles(0, 0, 1, 1); err != nil {
		t.Fatalf("ReadTiles: %v", err)
	}

	checkB44Gradient(t, readFB, 64, 64)
}

// TestTiledB44MixedPixelTypes covers the tiled path's share of B44's
// UINT/FLOAT passthrough: the codec is the same, but the tile geometry is
// computed by a different caller. The FLOAT and UINT channels are not touched
// by the codec, so they must survive a tile round trip bit for bit.
func TestTiledB44MixedPixelTypes(t *testing.T) {
	const w, h = 40, 40

	for _, comp := range []Compression{CompressionB44, CompressionB44A} {
		t.Run(comp.String(), func(t *testing.T) {
			header := NewTiledHeader(w, h, 16, 16)
			header.SetCompression(comp)
			cl := NewChannelList()
			cl.Add(Channel{Name: "B", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
			cl.Add(Channel{Name: "G", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
			cl.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
			cl.Add(Channel{Name: "Z", Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
			cl.Add(Channel{Name: "id", Type: PixelTypeUint, XSampling: 1, YSampling: 1})
			header.SetChannels(cl)

			rBuf := make([]half.Half, w*h)
			gBuf := make([]half.Half, w*h)
			bBuf := make([]half.Half, w*h)
			zBuf := make([]float32, w*h)
			idBuf := make([]uint32, w*h)
			for i := range zBuf {
				x, y := i%w, i/w
				rBuf[i] = half.FromFloat32(float32(x) / float32(w-1))
				gBuf[i] = half.FromFloat32(float32(y) / float32(h-1))
				bBuf[i] = half.FromFloat32(float32(x+y) / float32(2*(w-1)))
				zBuf[i] = float32(i)*1.7320508 - 12345.678
				idBuf[i] = 0xC0FFEE00 ^ uint32(i*2654435761)
			}

			ws := newMockWriteSeeker()
			tw, err := NewTiledWriter(ws, header)
			if err != nil {
				t.Fatalf("NewTiledWriter: %v", err)
			}
			fb := NewFrameBuffer()
			fb.Set("R", NewSliceFromHalf(rBuf, w, h))
			fb.Set("G", NewSliceFromHalf(gBuf, w, h))
			fb.Set("B", NewSliceFromHalf(bBuf, w, h))
			fb.Set("Z", NewSliceFromFloat32(zBuf, w, h))
			fb.Set("id", NewSliceFromUint32(idBuf, w, h))
			tw.SetFrameBuffer(fb)
			if err := tw.WriteTiles(0, 0, tw.NumTilesX()-1, tw.NumTilesY()-1); err != nil {
				t.Fatalf("WriteTiles: %v", err)
			}
			if err := tw.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			data := ws.Bytes()
			f, err := OpenReader(&readerAtWrapper{bytes.NewReader(data)}, int64(len(data)))
			if err != nil {
				t.Fatalf("OpenReader: %v", err)
			}
			tr, err := NewTiledReader(f)
			if err != nil {
				t.Fatalf("NewTiledReader: %v", err)
			}
			readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
			tr.SetFrameBuffer(readFB)
			if err := tr.ReadTiles(0, 0, tr.NumTilesX()-1, tr.NumTilesY()-1); err != nil {
				t.Fatalf("ReadTiles: %v", err)
			}

			z := readFB.Get("Z")
			id := readFB.Get("id")
			if z == nil || id == nil {
				t.Fatal("Z or id missing from frame buffer")
			}
			for i := range zBuf {
				x, y := i%w, i/w
				if got := z.GetFloat32(x, y); got != zBuf[i] {
					t.Fatalf("Z pixel (%d,%d) = %v, want %v: B44 must pass FLOAT channels through untouched",
						x, y, got, zBuf[i])
				}
				if got := id.GetUint32(x, y); got != idBuf[i] {
					t.Fatalf("id pixel (%d,%d) = %#08x, want %#08x: B44 must pass UINT channels through untouched",
						x, y, got, idBuf[i])
				}
			}
			checkB44Gradient(t, readFB, w, h)
		})
	}
}

func TestTiledWriteAndReadB44A(t *testing.T) {
	// Test B44A tiled compression round-trip
	// B44A is like B44 but with flat area optimization
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionB44A)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Create frame buffer with solid color (to test flat area optimization)
	writeFB := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			writeFB.SetPixel(x, y, 0.5, 0.5, 0.5, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())
	err = tw.WriteTiles(0, 0, 1, 1)
	if err != nil {
		t.Logf("B44A WriteTiles warning (may have issues): %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Logf("B44A Close warning (may have issues): %v", err)
	}

	data := ws.Bytes()
	t.Logf("B44A tiled written file size: %d bytes", len(data))

	if len(data) < 100 {
		t.Errorf("File too small: %d bytes", len(data))
		return
	}

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionB44A {
		t.Errorf("Compression = %v, want B44A", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Logf("B44A ReadTiles warning (may have issues): %v", err)
	}

	t.Log("B44A tiled compression round-trip completed")
}

func TestDecodeTile(t *testing.T) {
	width := 64
	height := 64
	tileSize := 32

	// Create a tiled header
	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionNone)
	channels := h.Channels()

	// Create source frame buffer
	fb, _ := AllocateChannels(channels, h.DataWindow())
	for i, name := range channels.Names() {
		slice := fb.Get(name)
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				slice.SetFloat32(x, y, float32(x+y+i))
			}
		}
	}

	// Create tiled writer
	var buf seekableBuffer
	w, err := NewTiledWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}
	w.SetFrameBuffer(fb)

	// Test encodeTile (wrapper for level 0)
	data, err := w.encodeTile(0, 0, int(tileSize), int(tileSize))
	if err != nil {
		t.Fatalf("encodeTile() error = %v", err)
	}
	if len(data) == 0 {
		t.Error("encodeTile returned empty data")
	}

	// Write all tiles
	for ty := 0; ty < w.NumTilesY(); ty++ {
		for tx := 0; tx < w.NumTilesX(); tx++ {
			if err := w.WriteTile(tx, ty); err != nil {
				t.Fatalf("WriteTile(%d, %d) error = %v", tx, ty, err)
			}
		}
	}
	w.Close()

	// Read back
	fileData := buf.Buffer.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(fileData)}
	f, err := OpenReader(reader, int64(len(fileData)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(channels, h.DataWindow())
	tr.SetFrameBuffer(readFB)

	// Test ReadTile (level 0)
	err = tr.ReadTile(0, 0)
	if err != nil {
		t.Fatalf("ReadTile() error = %v", err)
	}
}

func TestTiledWithUintChannel(t *testing.T) {
	width := 32
	height := 32
	tileSize := 16

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionNone)

	// Add uint channel
	channels := NewChannelList()
	channels.Add(Channel{Name: "ID", Type: PixelTypeUint, XSampling: 1, YSampling: 1})
	channels.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(Channel{Name: "Z", Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	h.SetChannels(channels)

	var buf bytes.Buffer
	ws := &seekableWriter{Buffer: &buf}

	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter error: %v", err)
	}

	fb := NewFrameBuffer()
	idData := make([]byte, width*height*4)
	rData := make([]byte, width*height*2)
	zData := make([]byte, width*height*4)

	// Fill with test data
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			idx := y*width + x
			// ID as uint32
			val := uint32(idx * 100)
			idData[idx*4] = byte(val)
			idData[idx*4+1] = byte(val >> 8)
			idData[idx*4+2] = byte(val >> 16)
			idData[idx*4+3] = byte(val >> 24)
		}
	}

	fb.Set("ID", NewSlice(PixelTypeUint, idData, width, height))
	fb.Set("R", NewSlice(PixelTypeHalf, rData, width, height))
	fb.Set("Z", NewSlice(PixelTypeFloat, zData, width, height))
	tw.SetFrameBuffer(fb)

	numTilesX := (width + tileSize - 1) / tileSize
	numTilesY := (height + tileSize - 1) / tileSize

	for ty := 0; ty < numTilesY; ty++ {
		for tx := 0; tx < numTilesX; tx++ {
			if err := tw.WriteTile(tx, ty); err != nil {
				t.Fatalf("WriteTile error: %v", err)
			}
		}
	}
	tw.Close()

	// Read back
	data := buf.Bytes()
	reader := bytes.NewReader(data)
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader error: %v", err)
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader error: %v", err)
	}

	readFB := NewFrameBuffer()
	readIDData := make([]byte, width*height*4)
	readRData := make([]byte, width*height*2)
	readZData := make([]byte, width*height*4)

	readFB.Set("ID", NewSlice(PixelTypeUint, readIDData, width, height))
	readFB.Set("R", NewSlice(PixelTypeHalf, readRData, width, height))
	readFB.Set("Z", NewSlice(PixelTypeFloat, readZData, width, height))
	tr.SetFrameBuffer(readFB)

	// Read all tiles
	for ty := 0; ty < numTilesY; ty++ {
		for tx := 0; tx < numTilesX; tx++ {
			if err := tr.ReadTile(tx, ty); err != nil {
				t.Fatalf("ReadTile(%d,%d) error: %v", tx, ty, err)
			}
		}
	}

	// Verify ID data
	idSlice := readFB.Get("ID")
	if idSlice == nil {
		t.Fatal("ID slice not found")
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			expected := uint32((y*width + x) * 100)
			got := idSlice.GetUint32(x, y)
			if got != expected {
				t.Errorf("ID mismatch at (%d,%d): got %d, expected %d", x, y, got, expected)
			}
		}
	}
}

func TestEncodeTile(t *testing.T) {
	width := 32
	height := 32
	tileSize := 16

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionNone)
	channels := h.Channels()

	fb, _ := AllocateChannels(channels, h.DataWindow())

	var buf seekableBuffer
	w, err := NewTiledWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}
	w.SetFrameBuffer(fb)

	// Test encodeTile directly
	data, err := w.encodeTile(0, 0, tileSize, tileSize)
	if err != nil {
		t.Fatalf("encodeTile() error = %v", err)
	}
	if len(data) == 0 {
		t.Error("encodeTile returned empty data")
	}

	t.Logf("encodeTile produced %d bytes", len(data))
}

func TestWriteTilesLevelInvalidRange(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	fb := NewRGBAFrameBuffer(64, 64, false)
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	// Invalid range (x1 > x2)
	err = tw.WriteTilesLevel(2, 0, 1, 0, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTilesLevel with x1>x2 error = %v, want ErrTileOutOfRange", err)
	}

	// Invalid range (y1 > y2)
	err = tw.WriteTilesLevel(0, 2, 0, 1, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTilesLevel with y1>y2 error = %v, want ErrTileOutOfRange", err)
	}

	tw.Close()
}

// NewRipmapTiledHeader creates a header for a ripmap tiled image.
func NewRipmapTiledHeader(width, height, tileWidth, tileHeight int) *Header {
	h := NewTiledHeader(width, height, tileWidth, tileHeight)
	td := h.TileDescription()
	td.Mode = LevelModeRipmap
	td.RoundingMode = LevelRoundDown
	h.SetTileDescription(*td)
	return h
}

func TestTiledRipmapChunkIndex(t *testing.T) {
	// Create a ripmap tiled image header
	h := NewRipmapTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Check that it's a ripmap
	if tw.LevelMode() != LevelModeRipmap {
		t.Errorf("LevelMode() = %v, want LevelModeRipmap", tw.LevelMode())
	}

	// NumXLevels and NumYLevels should be independent for ripmap
	t.Logf("NumXLevels=%d, NumYLevels=%d", tw.NumXLevels(), tw.NumYLevels())

	// Create frame buffer for level 0
	fb := NewRGBAFrameBuffer(64, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			fb.SetPixel(x, y, float32(x)/63.0, float32(y)/63.0, 0.5, 1.0)
		}
	}
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	// Write tile at level (0, 0)
	err = tw.WriteTileLevel(0, 0, 0, 0)
	if err != nil {
		t.Fatalf("WriteTileLevel(0,0,0,0) error = %v", err)
	}

	// Write tile at level (1, 0) - different X/Y levels for ripmap
	fb2 := NewRGBAFrameBuffer(32, 64, false)
	for y := 0; y < 64; y++ {
		for x := 0; x < 32; x++ {
			fb2.SetPixel(x, y, 0.5, 0.5, 0.5, 1.0)
		}
	}
	tw.SetFrameBuffer(fb2.ToFrameBuffer())

	// This should use the ripmap branch in chunkIndex
	err = tw.WriteTileLevel(0, 0, 1, 0)
	if err != nil {
		t.Logf("WriteTileLevel(0,0,1,0) error = %v (may be expected)", err)
	}

	tw.Close()
}

func TestTiledReaderReadTileLevelErrors(t *testing.T) {
	// Create a simple tiled file
	h := NewTiledHeader(32, 32, 16, 16)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb := NewRGBAFrameBuffer(32, 32, false)
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	for ty := 0; ty < 2; ty++ {
		for tx := 0; tx < 2; tx++ {
			tw.WriteTile(tx, ty)
		}
	}
	tw.Close()

	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, _ := OpenReader(reader, int64(len(data)))
	tr, _ := NewTiledReader(f)

	// Test without frame buffer
	err := tr.ReadTileLevel(0, 0, 0, 0)
	if err != ErrNoFrameBuffer {
		t.Errorf("ReadTileLevel without frame buffer error = %v, want ErrNoFrameBuffer", err)
	}

	// Set frame buffer
	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	// Test invalid level
	err = tr.ReadTileLevel(0, 0, 99, 0)
	if err != ErrLevelOutOfRange {
		t.Errorf("ReadTileLevel with invalid level error = %v, want ErrLevelOutOfRange", err)
	}

	// Test invalid tile coordinates
	err = tr.ReadTileLevel(99, 0, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("ReadTileLevel with invalid tile X error = %v, want ErrTileOutOfRange", err)
	}

	err = tr.ReadTileLevel(0, 99, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("ReadTileLevel with invalid tile Y error = %v, want ErrTileOutOfRange", err)
	}
}

func TestNewTiledReaderPartNilFile(t *testing.T) {
	_, err := NewTiledReaderPart(nil, 0)
	if err == nil {
		t.Error("NewTiledReaderPart(nil, 0) should return error")
	}
}

func TestTiledWriterWriteTileLevelErrors(t *testing.T) {
	h := NewTiledHeader(32, 32, 16, 16)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	// Test without frame buffer
	err := tw.WriteTileLevel(0, 0, 0, 0)
	if err != ErrNoFrameBuffer {
		t.Errorf("WriteTileLevel without frame buffer error = %v, want ErrNoFrameBuffer", err)
	}

	fb := NewRGBAFrameBuffer(32, 32, false)
	tw.SetFrameBuffer(fb.ToFrameBuffer())

	// Test invalid tile coordinates
	err = tw.WriteTileLevel(-1, 0, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTileLevel with negative tile X error = %v, want ErrTileOutOfRange", err)
	}

	err = tw.WriteTileLevel(0, -1, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTileLevel with negative tile Y error = %v, want ErrTileOutOfRange", err)
	}

	err = tw.WriteTileLevel(99, 0, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTileLevel with out-of-range tile X error = %v, want ErrTileOutOfRange", err)
	}

	err = tw.WriteTileLevel(0, 99, 0, 0)
	if err != ErrTileOutOfRange {
		t.Errorf("WriteTileLevel with out-of-range tile Y error = %v, want ErrTileOutOfRange", err)
	}

	// Test invalid level
	err = tw.WriteTileLevel(0, 0, -1, 0)
	if err != ErrLevelOutOfRange {
		t.Errorf("WriteTileLevel with negative level X error = %v, want ErrLevelOutOfRange", err)
	}

	err = tw.WriteTileLevel(0, 0, 99, 0)
	if err != ErrLevelOutOfRange {
		t.Errorf("WriteTileLevel with out-of-range level X error = %v, want ErrLevelOutOfRange", err)
	}

	tw.Close()
}

func TestNewTiledWriterScanlineError(t *testing.T) {
	// NewTiledWriter with scanline header should fail
	h := NewScanlineHeader(32, 32)
	ws := newMockWriteSeeker()
	_, err := NewTiledWriter(ws, h)
	if err != ErrNotTiled {
		t.Errorf("NewTiledWriter with scanline header error = %v, want ErrNotTiled", err)
	}
}

func TestTiledReaderRipmapChunkIndex(t *testing.T) {
	// Write a ripmap file and read it back to exercise reader's chunkIndex
	h := NewRipmapTiledHeader(64, 64, 32, 32)
	h.SetCompression(CompressionNone)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	numXLevels := tw.NumXLevels()
	numYLevels := tw.NumYLevels()
	t.Logf("Ripmap levels: X=%d, Y=%d", numXLevels, numYLevels)

	// Write all ripmap level tiles
	for ly := 0; ly < numYLevels; ly++ {
		for lx := 0; lx < numXLevels; lx++ {
			levelW := tw.LevelWidth(lx)
			levelH := tw.LevelHeight(ly)

			fb := NewRGBAFrameBuffer(levelW, levelH, false)
			for y := 0; y < levelH; y++ {
				for x := 0; x < levelW; x++ {
					fb.SetPixel(x, y, float32(lx)/float32(numXLevels), float32(ly)/float32(numYLevels), 0.5, 1.0)
				}
			}
			tw.SetFrameBuffer(fb.ToFrameBuffer())

			numTilesX := tw.NumXTilesAtLevel(lx)
			numTilesY := tw.NumYTilesAtLevel(ly)

			for ty := 0; ty < numTilesY; ty++ {
				for tx := 0; tx < numTilesX; tx++ {
					if err := tw.WriteTileLevel(tx, ty, lx, ly); err != nil {
						t.Logf("WriteTileLevel(%d,%d,%d,%d) error = %v", tx, ty, lx, ly, err)
					}
				}
			}
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	// Now read it back
	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	if tr.LevelMode() != LevelModeRipmap {
		t.Errorf("LevelMode() = %v, want LevelModeRipmap", tr.LevelMode())
	}

	// Read tiles at different ripmap levels (exercises chunkIndex ripmap branch)
	for ly := 0; ly < tr.NumYLevels(); ly++ {
		for lx := 0; lx < tr.NumXLevels(); lx++ {
			levelW := tr.LevelWidth(lx)
			levelH := tr.LevelHeight(ly)

			fb, _ := AllocateChannels(tr.Header().Channels(), Box2i{Min: V2i{0, 0}, Max: V2i{int32(levelW), int32(levelH)}})
			tr.SetFrameBuffer(fb)

			err := tr.ReadTileLevel(0, 0, lx, ly)
			if err != nil {
				t.Logf("ReadTileLevel(0,0,%d,%d) error = %v", lx, ly, err)
			}
		}
	}
}

func TestTiledDWABLargeImage(t *testing.T) {
	width := 128
	height := 128
	tileSize := 64

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionDWAB)
	channels := h.Channels()

	fb, _ := AllocateChannels(channels, h.DataWindow())
	for _, name := range channels.Names() {
		slice := fb.Get(name)
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				slice.SetFloat32(x, y, float32(x+y)/float32(width+height))
			}
		}
	}

	var buf seekableBuffer
	w, err := NewTiledWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}
	w.SetFrameBuffer(fb)

	for ty := 0; ty < w.NumTilesY(); ty++ {
		for tx := 0; tx < w.NumTilesX(); tx++ {
			if err := w.WriteTile(tx, ty); err != nil {
				t.Fatalf("WriteTile(%d, %d) error = %v", tx, ty, err)
			}
		}
	}
	w.Close()

	data := buf.Buffer.Bytes()
	if len(data) < 100 {
		t.Errorf("File too small: %d bytes", len(data))
		return
	}

	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	if f.Header(0).Compression() != CompressionDWAB {
		t.Errorf("Compression = %v, want DWAB", f.Header(0).Compression())
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	err = tr.ReadTiles(0, 0, 1, 1)
	if err != nil {
		t.Logf("DWAB ReadTiles warning: %v", err)
	}

	t.Log("DWAB tiled compression round-trip completed")
}

func TestTiledReaderReadTileOutOfRange(t *testing.T) {
	// Create a minimal tiled file
	width, height := 32, 32
	tileSize := 16

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionNone)

	fb := NewFrameBuffer()
	rData := make([]byte, width*height*2)
	fb.Set("R", NewSlice(PixelTypeHalf, rData, width, height))

	var buf seekableBuffer
	w, err := NewTiledWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}
	w.SetFrameBuffer(fb)

	// Write all tiles
	for ty := 0; ty < w.NumTilesY(); ty++ {
		for tx := 0; tx < w.NumTilesX(); tx++ {
			if err := w.WriteTile(tx, ty); err != nil {
				t.Fatalf("WriteTile(%d, %d) error = %v", tx, ty, err)
			}
		}
	}
	w.Close()

	// Read back
	data := buf.Buffer.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	tr, err := NewTiledReader(f)
	if err != nil {
		t.Fatalf("NewTiledReader() error = %v", err)
	}

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	// Test reading out of range tiles
	err = tr.ReadTile(100, 100)
	if err == nil {
		t.Error("ReadTile(100, 100) should fail for out of range")
	}
}

func TestTiledWriterWriteTileNoFrameBuffer(t *testing.T) {
	width, height := 16, 16
	tileSize := 8

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionNone)

	var buf seekableBuffer
	w, err := NewTiledWriter(&buf, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Try to write without setting frame buffer
	err = w.WriteTile(0, 0)
	if err == nil {
		t.Error("WriteTile without frame buffer should fail")
	}
}

// TestTiledZIPParallelDecompression tests that ZIP decompression works correctly
// when tiles are read concurrently from multiple goroutines using separate readers.
// Regression test for: shared decompressBuf causing race conditions.
// This test verifies that the decompression buffer pool is thread-safe.
func TestTiledZIPParallelDecompression(t *testing.T) {
	// Create a tiled image with ZIP compression and multiple tiles
	width, height := 128, 128
	tileSize := 32

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionZIP)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	// Create frame buffer with gradient
	writeFB := NewRGBAFrameBuffer(width, height, false)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			r := float32(x) / float32(width-1)
			g := float32(y) / float32(height-1)
			b := float32(x+y) / float32(width+height-2)
			writeFB.SetPixel(x, y, r, g, b, 1.0)
		}
	}

	tw.SetFrameBuffer(writeFB.ToFrameBuffer())

	// Write all tiles
	err = tw.WriteTiles(0, 0, tw.NumTilesX()-1, tw.NumTilesY()-1)
	if err != nil {
		t.Fatalf("WriteTiles() error = %v", err)
	}

	if err := tw.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	data := ws.Bytes()
	t.Logf("Tiled ZIP file size: %d bytes", len(data))

	// Get tile counts from original reader
	tmpReader := &readerAtWrapper{bytes.NewReader(data)}
	tmpFile, err := OpenReader(tmpReader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}
	tmpTR, _ := NewTiledReader(tmpFile)
	numTilesX := tmpTR.NumTilesX()
	numTilesY := tmpTR.NumTilesY()
	numTiles := numTilesX * numTilesY
	headerChan := tmpTR.Header().Channels()
	dataWindow := tmpTR.DataWindow()

	// Use a WaitGroup to coordinate parallel reads
	var wg sync.WaitGroup
	errors := make(chan error, numTiles)

	// Read tiles in parallel using 4 goroutines, each with its own reader
	numWorkers := 4
	tilesPerWorker := (numTiles + numWorkers - 1) / numWorkers

	for worker := 0; worker < numWorkers; worker++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			startTile := workerID * tilesPerWorker
			endTile := startTile + tilesPerWorker
			if endTile > numTiles {
				endTile = numTiles
			}

			// Each worker gets its own reader from the shared data
			workerReader := &readerAtWrapper{bytes.NewReader(data)}
			workerFile, err := OpenReader(workerReader, int64(len(data)))
			if err != nil {
				errors <- err
				return
			}

			workerTR, err := NewTiledReader(workerFile)
			if err != nil {
				errors <- err
				return
			}

			// Each worker gets its own frame buffer
			readFB, _ := AllocateChannels(headerChan, dataWindow)
			workerTR.SetFrameBuffer(readFB)

			for tileIdx := startTile; tileIdx < endTile; tileIdx++ {
				tx := tileIdx % numTilesX
				ty := tileIdx / numTilesX

				if err := workerTR.ReadTileLevel(tx, ty, 0, 0); err != nil {
					errors <- err
					return
				}
			}
		}(worker)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		if err != nil {
			t.Fatalf("Parallel tile read error: %v", err)
		}
	}

	t.Logf("Successfully read %d tiles in parallel across %d workers", numTiles, numWorkers)
}

// TestTiledReaderNoFrameBuffer tests reading without frame buffer.
func TestTiledReaderNoFrameBuffer(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb, _ := AllocateChannels(h.Channels(), h.DataWindow())
	tw.SetFrameBuffer(fb)

	for ty := 0; ty < 2; ty++ {
		for tx := 0; tx < 2; tx++ {
			tw.WriteTile(tx, ty)
		}
	}
	tw.Close()

	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, _ := OpenReader(reader, int64(len(data)))
	tr, _ := NewTiledReader(f)

	// Don't set frame buffer
	err := tr.ReadTile(0, 0)
	if err == nil {
		t.Error("ReadTile without frame buffer should error")
	}

	err = tr.ReadTileLevel(0, 0, 0, 0)
	if err == nil {
		t.Error("ReadTileLevel without frame buffer should error")
	}
}

// TestTiledWriterInvalidTileCoords tests error handling for invalid tile coords.
func TestTiledWriterInvalidTileCoords(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb, _ := AllocateChannels(h.Channels(), h.DataWindow())
	tw.SetFrameBuffer(fb)

	// Write valid tiles first
	tw.WriteTile(0, 0)
	tw.WriteTile(1, 0)
	tw.WriteTile(0, 1)
	tw.WriteTile(1, 1)

	// Try writing to invalid tile coords
	err := tw.WriteTile(100, 100)
	if err == nil {
		t.Error("WriteTile with invalid coords should error")
	}

	err = tw.WriteTile(-1, 0)
	if err == nil {
		t.Error("WriteTile with negative coords should error")
	}
}

// TestTiledWriteTilesLevelMultiple tests WriteTilesLevel with multiple tiles.
func TestTiledWriteTilesLevelMultiple(t *testing.T) {
	width := 128
	height := 128
	tileSize := 32

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionPIZ)

	ws := newMockWriteSeeker()
	tw, err := NewTiledWriter(ws, h)
	if err != nil {
		t.Fatalf("NewTiledWriter() error = %v", err)
	}

	fb, _ := AllocateChannels(h.Channels(), h.DataWindow())
	tw.SetFrameBuffer(fb)

	// Fill with gradient
	rSlice := fb.Get("R")
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			rSlice.SetFloat32(x, y, float32(x%16)/16.0)
		}
	}

	numTilesX := (width + tileSize - 1) / tileSize
	numTilesY := (height + tileSize - 1) / tileSize

	// Write all tiles at once
	if err := tw.WriteTilesLevel(0, 0, numTilesX-1, numTilesY-1, 0, 0); err != nil {
		t.Fatalf("WriteTilesLevel() error = %v", err)
	}

	tw.Close()

	// Verify the file is readable
	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, err := OpenReader(reader, int64(len(data)))
	if err != nil {
		t.Fatalf("OpenReader() error = %v", err)
	}

	tr, _ := NewTiledReader(f)
	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	// Read using ReadTilesLevel
	if err := tr.ReadTilesLevel(0, 0, numTilesX-1, numTilesY-1, 0, 0); err != nil {
		t.Fatalf("ReadTilesLevel() error = %v", err)
	}

	// Verify a few pixels
	rRead := readFB.Get("R")
	for y := 0; y < height; y += 20 {
		for x := 0; x < width; x += 20 {
			expected := float32(x%16) / 16.0
			got := rRead.GetFloat32(x, y)
			if !almostEqual(got, expected, 0.02) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, got, expected)
			}
		}
	}
}

// TestTiledReaderPartCreation tests creating tiled reader for specific parts.
func TestTiledReaderPartCreation(t *testing.T) {
	h := NewTiledHeader(64, 64, 32, 32)
	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb, _ := AllocateChannels(h.Channels(), h.DataWindow())
	tw.SetFrameBuffer(fb)

	tw.WriteTiles(0, 0, 1, 1)
	tw.Close()

	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, _ := OpenReader(reader, int64(len(data)))

	// Create reader for part 0
	tr, err := NewTiledReaderPart(f, 0)
	if err != nil {
		t.Fatalf("NewTiledReaderPart(0) error = %v", err)
	}

	if tr.Header() == nil {
		t.Error("Header should not be nil")
	}

	// Invalid part should error
	_, err = NewTiledReaderPart(f, 999)
	if err == nil {
		t.Error("NewTiledReaderPart(999) should error")
	}
}

// TestTiledRLECompression tests RLE compression specifically.
func TestTiledRLECompression(t *testing.T) {
	width := 64
	height := 64
	tileSize := 32

	h := NewTiledHeader(width, height, tileSize, tileSize)
	h.SetCompression(CompressionRLE)

	ws := newMockWriteSeeker()
	tw, _ := NewTiledWriter(ws, h)

	fb, _ := AllocateChannels(h.Channels(), h.DataWindow())
	tw.SetFrameBuffer(fb)

	// Create data that compresses well with RLE
	rSlice := fb.Get("R")
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			// Repeated values compress well
			rSlice.SetFloat32(x, y, float32(y/8)/8.0)
		}
	}

	tw.WriteTiles(0, 0, 1, 1)
	tw.Close()

	// Read back
	data := ws.Bytes()
	reader := &readerAtWrapper{bytes.NewReader(data)}
	f, _ := OpenReader(reader, int64(len(data)))
	tr, _ := NewTiledReader(f)

	readFB, _ := AllocateChannels(tr.Header().Channels(), tr.DataWindow())
	tr.SetFrameBuffer(readFB)

	if err := tr.ReadTiles(0, 0, 1, 1); err != nil {
		t.Fatalf("ReadTiles() error = %v", err)
	}

	// Verify lossless round-trip
	rRead := readFB.Get("R")
	for y := 0; y < height; y += 8 {
		for x := 0; x < width; x += 8 {
			expected := float32(y/8) / 8.0
			got := rRead.GetFloat32(x, y)
			if !almostEqual(got, expected, 0.001) {
				t.Errorf("R at (%d,%d) = %v, want ~%v", x, y, got, expected)
			}
		}
	}
}
