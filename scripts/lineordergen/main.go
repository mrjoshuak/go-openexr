// Command lineordergen writes a scanline EXR in each line order and reports the
// order the chunks actually sit in the file.
//
//	lineordergen <outdir>
//
// lineOrder was stored in the header and ignored: every file was written
// ascending whatever it declared. Nothing noticed, because the reference reads
// through the chunk offset table and does not care about the physical order —
// so a round trip, and the reference itself, both pass on a file whose header
// says the opposite of what the file does. Only reading the layout can see it,
// which is what this does.
//
// The offset table is always ordered by increasing y whatever the line order
// is: it is the index a reader seeks with. lineOrder describes where the chunks
// sit, so the offsets are sorted and the chunk's y read at each.
//
// RANDOM_Y is tiled-only (ImfLineOrder.h: "only for tiled files; tiles are
// written in random order") and must be refused on a scanline part.
package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/mrjoshuak/go-openexr/exr"
)

const w, h = 16, 12

func write(path string, order exr.LineOrder) error {
	hdr := exr.NewScanlineHeader(w, h)
	hdr.SetCompression(exr.CompressionZIPS)
	hdr.SetLineOrder(order)
	cl := exr.NewChannelList()
	cl.Add(exr.Channel{Name: "Y", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	hdr.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	wr, err := exr.NewScanlineWriter(f, hdr)
	if err != nil {
		return err
	}
	fb, _ := exr.AllocateChannels(hdr.Channels(), hdr.DataWindow())
	s := fb.Get("Y")
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			s.SetFloat32(x, y, float32(y*100+x))
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		return err
	}
	return wr.Close()
}

// chunkOrder reads the y of each chunk in the order the offset table lists them.
func chunkOrder(path string) ([]int32, error) {
	d, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// The offset table follows the header's terminating null byte.
	i := 8
	for {
		// name
		start := i
		for d[i] != 0 {
			i++
		}
		if i == start { // empty name ends the header
			i++
			break
		}
		i++
		for d[i] != 0 { // type
			i++
		}
		i++
		size := int32(binary.LittleEndian.Uint32(d[i : i+4]))
		i += 4 + int(size)
	}
	// The offset table is always ordered by increasing y — it is the index a
	// reader seeks with. What lineOrder describes is the order the chunks sit
	// in the file, so the offsets are sorted and the chunk y read at each.
	offs := make([]int64, 0, h)
	for c := 0; c < h; c++ {
		offs = append(offs, int64(binary.LittleEndian.Uint64(d[i:i+8])))
		i += 8
	}
	sorted := append([]int64(nil), offs...)
	sort.Slice(sorted, func(a, b int) bool { return sorted[a] < sorted[b] })
	var ys []int32
	for _, off := range sorted {
		ys = append(ys, int32(binary.LittleEndian.Uint32(d[off:off+4])))
	}
	return ys, nil
}

func main() {
	dir := os.Args[1]
	os.MkdirAll(dir, 0o755)

	for _, c := range []struct {
		name  string
		order exr.LineOrder
	}{
		{"increasing", exr.LineOrderIncreasing},
		{"decreasing", exr.LineOrderDecreasing},
		{"random", exr.LineOrderRandom},
	} {
		path := filepath.Join(dir, c.name+".exr")
		if err := write(path, c.order); err != nil {
			fmt.Printf("  %-11s refused: %v\n", c.name, err)
			continue
		}
		ys, err := chunkOrder(path)
		if err != nil {
			fmt.Printf("  %-11s written, could not read the offset table: %v\n", c.name, err)
			continue
		}
		fmt.Printf("  %-11s written; chunk y in file order: %v\n", c.name, ys)
	}
}
