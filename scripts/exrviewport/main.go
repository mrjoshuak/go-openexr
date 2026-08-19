// Command exrviewport reads a rectangle of a tiled EXR through this library's
// byte-range path and prints the samples it produced.
//
// It exists so the claim "a viewport costs a viewport" can be checked against
// the reference rather than against this library's own idea of the file. The
// samples it prints come from File.ReadRegion, which resolves the rectangle to
// chunks by reading chunk headers, fetches only those chunks, and for HTJ2K
// decodes only the code-blocks the rectangle can reach. scripts/exrtiledump
// prints every sample of the same file using libOpenEXR, so the two dumps
// compare directly with scripts/tilecmp.awk.
//
//	exrviewport [-part N] <file.exr> <x0> <y0> <x1> <y1>
//
// The rectangle is inclusive, in the image's own coordinates. Output is one
// line per sample:
//
//	0 0 <x> <y> <channel> <value>
//
// with x and y relative to the data window's origin, which is what exrtiledump
// prints. The leading "0 0" is the level, which is always the full-resolution
// one here: this reads the file's own pixels, not a pyramid. Lines beginning
// with '#' report what the read cost.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/mrjoshuak/go-openexr/exr"
)

func main() {
	part := flag.Int("part", 0, "part index")
	flag.Parse()
	if flag.NArg() != 5 {
		fmt.Fprintln(os.Stderr, "usage: exrviewport [-part N] <file.exr> <x0> <y0> <x1> <y1>")
		os.Exit(2)
	}

	coords := make([]int32, 4)
	for i := 0; i < 4; i++ {
		v, err := strconv.Atoi(flag.Arg(i + 1))
		if err != nil {
			fmt.Fprintf(os.Stderr, "coordinate %q: %v\n", flag.Arg(i+1), err)
			os.Exit(2)
		}
		coords[i] = int32(v)
	}

	path := flag.Arg(0)
	st, err := os.Stat(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	f, err := exr.OpenFile(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "open:", err)
		os.Exit(1)
	}
	defer f.Close()

	region := exr.Box2i{
		Min: exr.V2i{X: coords[0], Y: coords[1]},
		Max: exr.V2i{X: coords[2], Y: coords[3]},
	}
	got, err := f.ReadRegion(*part, region)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ReadRegion:", err)
		os.Exit(1)
	}

	h := f.Header(*part)
	dw := h.DataWindow()
	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	fmt.Fprintf(w, "# region %d %d %d %d\n",
		got.Region.Min.X, got.Region.Min.Y, got.Region.Max.X, got.Region.Max.Y)
	fmt.Fprintf(w, "# chunks %d %d\n", got.ChunksRead, got.ChunksTotal)
	fmt.Fprintf(w, "# filebytes %d %d\n", got.FileBytes, st.Size())
	fmt.Fprintf(w, "# codeblocks %d %d\n", got.DecodedBytes, got.SkippedBytes)

	rw := int(got.Region.Max.X-got.Region.Min.X) + 1
	for _, name := range got.Channels {
		plane := got.Planes[name]
		for y := int(got.Region.Min.Y); y <= int(got.Region.Max.Y); y++ {
			for x := int(got.Region.Min.X); x <= int(got.Region.Max.X); x++ {
				v := plane[(y-int(got.Region.Min.Y))*rw+(x-int(got.Region.Min.X))]
				// exrtiledump prints coordinates relative to the level's data
				// window; match it so the two dumps share a key space.
				fmt.Fprintf(w, "0 0 %d %d %s %.9g\n",
					x-int(dw.Min.X), y-int(dw.Min.Y), name, v)
			}
		}
	}
}
