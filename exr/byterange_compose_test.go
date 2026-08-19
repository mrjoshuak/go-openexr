package exr

import (
	"os"
	"testing"

	"github.com/mrjoshuak/go-openexr/compression"
)

// TestByteRangeIndexesCompose is the property the roadmap asks for: a viewport
// resolves to a set of byte ranges before any pixel data is read.
//
// Two indexes have to meet for that. The EXR chunk offset table locates the
// chunks a region touches, and inside an HTJ2K chunk the JPEG 2000 packet index
// locates the packets. Neither is enough alone: the chunk table cannot see
// inside a chunk, and the packet index cannot find the chunk.
//
// What is asserted is that the composition narrows. A "viewport" that resolves
// to the whole file is not an index, and every step here is measured against
// the alternative of reading everything.
func TestByteRangeIndexesCompose(t *testing.T) {
	dir := t.TempDir()
	const w, h, tw, th = 128, 128, 32, 32
	path := writeTiledTestFile(t, dir, w, h, tw, th, CompressionHTJ2K256)

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	fileSize := info.Size()

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	all, err := f.ChunkRanges(0)
	if err != nil {
		t.Fatalf("ChunkRanges: %v", err)
	}

	// Step one: which chunks does a 32x32 viewport touch?
	region, err := f.ChunksForRegion(0, 0, 0, 32, 32, 0, 0)
	if err != nil {
		t.Fatalf("ChunksForRegion: %v", err)
	}
	if len(region) == 0 {
		t.Fatal("a viewport inside the image selected no chunks")
	}
	if len(region) >= len(all) {
		t.Fatalf("a %dx%d viewport of a %dx%d image selected %d of %d chunks; "+
			"the chunk table must narrow", 32, 32, w, h, len(region), len(all))
	}

	var chunkBytes int64
	for _, cr := range region {
		chunkBytes += cr.Length
	}
	if chunkBytes >= fileSize {
		t.Errorf("the selected chunks are %d bytes of a %d-byte file", chunkBytes, fileSize)
	}

	// Step two: inside one of those chunks, which packets does the region
	// touch? The chunk's bytes are read through its range alone — no decode.
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cr := region[0]
	chunk := raw[cr.DataOffset : cr.DataOffset+cr.DataLength]

	idx, _, err := compression.HTJ2KBuildPacketIndex(chunk)
	if err != nil {
		t.Fatalf("HTJ2KBuildPacketIndex on a chunk located by its range alone: %v", err)
	}
	if idx.Len() == 0 {
		t.Fatal("the chunk's codestream holds no packets")
	}

	// Every packet must name real bytes inside the chunk, or the composition
	// produces ranges that fetch the wrong thing.
	located := 0
	for _, addr := range idx.AllAddresses() {
		r, ok := idx.Range(addr)
		if !ok {
			continue
		}
		if r.Offset < 0 || r.Offset+r.Length > len(chunk) {
			t.Fatalf("packet %v names %d+%d, outside the %d-byte chunk",
				addr, r.Offset, r.Length, len(chunk))
		}
		located++
	}
	if located == 0 {
		t.Fatal("no packet in the chunk had a byte range")
	}

	t.Logf("viewport 32x32 of %dx%d: %d/%d chunks, %d/%d bytes; "+
		"first chunk holds %d packets, %d with byte ranges",
		w, h, len(region), len(all), chunkBytes, fileSize, idx.Len(), located)
}
