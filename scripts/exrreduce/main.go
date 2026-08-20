// Command exrreduce decodes one tile of a tiled HTJ2K EXR at a reduced
// resolution, and writes out the tile's raw codestream so the reference can be
// asked what that decode should have produced.
//
// It exists because libOpenEXR has no reduced-resolution decode to compare
// against — a chunk decompresses whole there, which is the whole point of this
// being an extension. The oracle is one layer down: an HTJ2K chunk is a JPEG
// 2000 codestream, and ojph_expand -skip_res reconstructs exactly the
// resolution this library is asked for. Comparing the two is comparing two
// decoders on one codestream, which is the claim worth making.
//
//	exrreduce <file.exr> <tileX> <tileY> <reduce> <out.pfm> <out.j2c>
//	exrreduce cmp <a.pfm> <b.pfm>
//
// The cmp mode compares raw sample bits rather than numeric values, and lives
// here so this repository's gate needs nothing from go-jpeg2000's scripts.
//
// The part is 0 and the channel count must be one: PFM carries one or three
// components, and ojph_expand writes PFM for a 32-bit component.
//
// What it does NOT do is assert that the result looks like a downsample. It is
// not one — see the note in compression/htj2k_partial_test.go. This compares
// against the reference, not against an expectation of what reduced content
// ought to look like, which is the mistake that had this capability refused.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/exr"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func main() {
	if len(os.Args) == 4 && os.Args[1] == "cmp" {
		cmpPFM(os.Args[2], os.Args[3])
		return
	}
	if len(os.Args) != 7 {
		fail("usage: exrreduce <file.exr> <tileX> <tileY> <reduce> <out.pfm> <out.j2c>")
	}
	path := os.Args[1]
	nums := make([]int, 3)
	for i := 0; i < 3; i++ {
		v, err := strconv.Atoi(os.Args[i+2])
		if err != nil {
			fail("argument %d: %v", i+2, err)
		}
		nums[i] = v
	}
	tileX, tileY, reduce := nums[0], nums[1], nums[2]

	f, err := exr.OpenFile(path)
	if err != nil {
		fail("open: %v", err)
	}
	defer f.Close()

	h := f.Header(0)
	if h == nil {
		fail("no part 0")
	}
	chans := h.Channels().SortedByName()
	if len(chans) != 1 {
		fail("this needs a single-channel part; %s has %d", path, len(chans))
	}
	td := h.TileDescription()
	if td == nil {
		fail("%s is not tiled", path)
	}

	// Locate the tile's chunk by its byte range and read only that.
	ranges, err := f.ChunkRanges(0)
	if err != nil {
		fail("ChunkRanges: %v", err)
	}
	var found *exr.ChunkRange
	for i := range ranges {
		if ranges[i].TileX == tileX && ranges[i].TileY == tileY &&
			ranges[i].LevelX == 0 && ranges[i].LevelY == 0 {
			found = &ranges[i]
			break
		}
	}
	if found == nil {
		fail("no chunk for tile (%d,%d)", tileX, tileY)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		fail("%v", err)
	}
	data := raw[found.DataOffset : found.DataOffset+found.DataLength]

	// The codestream, for the oracle. HTJ2KExtractCodestream is the supported
	// way to reach it; the chunk header before it is this format's, not JPEG
	// 2000's.
	cs, _, err := compression.HTJ2KExtractCodestream(data)
	if err != nil {
		fail("HTJ2KExtractCodestream: %v", err)
	}
	if err := os.WriteFile(os.Args[6], cs, 0o644); err != nil {
		fail("%v", err)
	}

	dw := h.DataWindow()
	tw := min(int(td.XSize), int(dw.Max.X-dw.Min.X)+1-tileX*int(td.XSize))
	th := min(int(td.YSize), int(dw.Max.Y-dw.Min.Y)+1-tileY*int(td.YSize))
	channels := []compression.HTJ2KChannelInfo{{
		Type: compression.HTJ2KPixelTypeFloat, Width: tw, Height: th,
		XSampling: 1, YSampling: 1, Name: chans[0].Name,
	}}

	var opts *compression.HTJ2KDecodeOptions
	if reduce > 0 {
		opts = &compression.HTJ2KDecodeOptions{ReduceResolution: reduce}
	}
	res, err := compression.HTJ2KDecompressPartial(data, channels, opts)
	if err != nil {
		fail("HTJ2KDecompressPartial: %v", err)
	}

	out, err := os.Create(os.Args[5])
	if err != nil {
		fail("%v", err)
	}
	defer out.Close()
	// PFM stores rows bottom to top.
	fmt.Fprintf(out, "Pf\n%d %d\n-1.0\n", res.Width, res.Height)
	// The samples are already little-endian float32, which is what a PFM with
	// a negative scale holds, so the row copies straight out.
	for y := res.Height - 1; y >= 0; y-- {
		line := res.Data[y*res.BytesPerLine:]
		if _, err := out.Write(line[:res.Width*4]); err != nil {
			fail("%v", err)
		}
	}

	fmt.Printf("tile (%d,%d) of %dx%d, reduce %d: %dx%d samples, decoded %d of %d code-block bytes, skipped %d\n",
		tileX, tileY, tw, th, reduce, res.Width, res.Height,
		res.DecodedBytes, res.DecodedBytes+res.SkippedBytes, res.SkippedBytes)
}

// readPFM reads a single-component PFM and returns its dimensions and raw
// sample bits, top row first.
func readPFM(path string) (int, int, []uint32) {
	f, err := os.Open(path)
	if err != nil {
		fail("%v", err)
	}
	defer f.Close()
	r := bufio.NewReader(f)
	magic, _ := r.ReadString('\n')
	if strings.TrimSpace(magic) != "Pf" {
		fail("%s is not a single-component PFM", path)
	}
	dims, _ := r.ReadString('\n')
	var w, h int
	if _, err := fmt.Sscanf(strings.TrimSpace(dims), "%d %d", &w, &h); err != nil {
		fail("%s: dimensions: %v", path, err)
	}
	if _, err := r.ReadString('\n'); err != nil {
		fail("%s: scale: %v", path, err)
	}
	raw := make([]byte, w*h*4)
	if _, err := io.ReadFull(r, raw); err != nil {
		fail("%s: samples: %v", path, err)
	}
	// PFM stores rows bottom to top.
	out := make([]uint32, w*h)
	for y := 0; y < h; y++ {
		src := raw[(h-1-y)*w*4:]
		for x := 0; x < w; x++ {
			out[y*w+x] = binary.LittleEndian.Uint32(src[x*4:])
		}
	}
	return w, h, out
}

// cmpPFM compares two PFMs by raw sample bits. Bits rather than values because
// a decoder has to return exactly what the other one did: two NaNs with
// different payloads are different samples.
func cmpPFM(a, b string) {
	aw, ah, av := readPFM(a)
	bw, bh, bv := readPFM(b)
	if aw != bw || ah != bh {
		fmt.Printf("dimensions differ: %dx%d vs %dx%d\n", aw, ah, bw, bh)
		os.Exit(1)
	}
	bad := 0
	first := -1
	for i := range av {
		if av[i] != bv[i] {
			if first < 0 {
				first = i
			}
			bad++
		}
	}
	if bad != 0 {
		fmt.Printf("%d/%d samples differ; first at %d: %08x vs %08x\n",
			bad, len(av), first, av[first], bv[first])
		os.Exit(1)
	}
	fmt.Println("0")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
