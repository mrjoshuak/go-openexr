// Command mpdeepgen writes a multi-part EXR containing a deep scanline part
// beside a flat one, together with the samples the deep part is meant to hold.
//
// It exists because this library could not write one at all: MultiPartOutputFile
// exposed only WritePixels and WriteTile, so "deep parts in a multi-part file"
// was a gap in the writer rather than in the fixtures.
//
//	mpdeepgen <outdir>
//
// Writes mp_deep.exr and mp_deep.txt, the latter one line per pixel:
//
//	<x> <y> <count> <channel>:<value> ...
//
// which scripts/deepdiff.awk compares against what the reference reads back.
package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/mrjoshuak/go-openexr/exr"
)

const (
	flatW, flatH = 24, 12
	deepW, deepH = 13, 9
)

// sampleCount varies per pixel, including pixels with none, because a deep
// chunk whose every pixel holds the same number of samples cannot distinguish a
// correct pixel offset table from one that is merely monotonic.
func sampleCount(x, y int) int {
	if (x+y)%7 == 0 {
		return 0
	}
	return 1 + (x*3+y*5)%4
}

func depthOf(x, y, s int) float32 {
	return float32(x)*0.5 + float32(y)*0.25 + float32(s)*0.125
}

func alphaOf(x, y, s int) float32 {
	return float32((x*7+y*3+s*11)%100) / 100
}

func header(w, h int, display exr.Box2i, name, typ string, comp exr.Compression) *exr.Header {
	hdr := exr.NewHeader()
	hdr.SetDataWindow(exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: int32(w - 1), Y: int32(h - 1)}})
	hdr.SetDisplayWindow(display)
	hdr.SetCompression(comp)
	hdr.SetLineOrder(exr.LineOrderIncreasing)
	hdr.SetPixelAspectRatio(1)
	hdr.SetScreenWindowCenter(exr.V2f{X: 0, Y: 0})
	hdr.SetScreenWindowWidth(1)
	hdr.Set(&exr.Attribute{Name: exr.AttrNameName, Type: exr.AttrTypeString, Value: name})
	hdr.Set(&exr.Attribute{Name: exr.AttrNameType, Type: exr.AttrTypeString, Value: typ})
	return hdr
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: mpdeepgen <outdir>")
		os.Exit(2)
	}
	dir := os.Args[1]
	if err := os.MkdirAll(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	display := exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: flatW - 1, Y: flatH - 1}}

	flat := header(flatW, flatH, display, "flat", exr.PartTypeScanline, exr.CompressionZIP)
	fcl := exr.NewChannelList()
	for _, n := range []string{"B", "G", "R"} {
		fcl.Add(exr.Channel{Name: n, Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	}
	flat.SetChannels(fcl)

	deep := header(deepW, deepH, display, "deep", exr.PartTypeDeepScanline, exr.CompressionZIPS)
	dcl := exr.NewChannelList()
	dcl.Add(exr.Channel{Name: "A", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	dcl.Add(exr.Channel{Name: "Z", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	deep.SetChannels(dcl)

	path := filepath.Join(dir, "mp_deep.exr")
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer f.Close()

	mp, err := exr.NewMultiPartOutputFile(f, []*exr.Header{flat, deep})
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewMultiPartOutputFile:", err)
		os.Exit(1)
	}

	// Part 0: the flat part, so the file is genuinely mixed and the deep
	// chunk's part-number prefix has to be right.
	ffb, _ := exr.AllocateChannels(flat.Channels(), flat.DataWindow())
	for ci, n := range []string{"B", "G", "R"} {
		s := ffb.Get(n)
		for y := 0; y < flatH; y++ {
			for x := 0; x < flatW; x++ {
				s.SetFloat32(x, y, float32(ci)+float32(x)/flatW+float32(y)/flatH)
			}
		}
	}
	if err := mp.SetFrameBuffer(0, ffb); err != nil {
		fmt.Fprintln(os.Stderr, "SetFrameBuffer(0):", err)
		os.Exit(1)
	}
	if err := mp.WritePixels(0, flatH); err != nil {
		fmt.Fprintln(os.Stderr, "WritePixels:", err)
		os.Exit(1)
	}

	// Part 1: the deep part.
	dfb := exr.NewDeepFrameBuffer(deepW, deepH)
	dfb.Insert("A", exr.PixelTypeFloat)
	dfb.Insert("Z", exr.PixelTypeFloat)
	total := 0
	for y := 0; y < deepH; y++ {
		for x := 0; x < deepW; x++ {
			n := sampleCount(x, y)
			dfb.SetSampleCount(x, y, uint32(n))
			dfb.AllocateSamples(x, y)
			for s := 0; s < n; s++ {
				dfb.Slices["A"].SetSampleFloat32(x, y, s, alphaOf(x, y, s))
				dfb.Slices["Z"].SetSampleFloat32(x, y, s, depthOf(x, y, s))
			}
			total += n
		}
	}

	if err := mp.SetDeepFrameBuffer(1, dfb); err != nil {
		fmt.Fprintln(os.Stderr, "SetDeepFrameBuffer:", err)
		os.Exit(1)
	}
	if err := mp.WriteDeepPixels(1, deepH); err != nil {
		fmt.Fprintln(os.Stderr, "WriteDeepPixels:", err)
		os.Exit(1)
	}

	if err := mp.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "Close:", err)
		os.Exit(1)
	}

	// The truth file, in the form scripts/deepdiff.awk compares.
	tf, err := os.Create(filepath.Join(dir, "mp_deep.txt"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer tf.Close()
	tw := bufio.NewWriter(tf)
	defer tw.Flush()
	// The shape scripts/deepdiff.awk parses, which is oiiotool --dumpdata's
	// own output with its punctuation turned into separators.
	for y := 0; y < deepH; y++ {
		for x := 0; x < deepW; x++ {
			n := sampleCount(x, y)
			fmt.Fprintf(tw, "Pixel (%d, %d): %d samples ", x, y, n)
			for s := 0; s < n; s++ {
				if s > 0 {
					tw.WriteString(" / ")
				} else {
					tw.WriteString(": ")
				}
				fmt.Fprintf(tw, "A=%s Z=%s",
					strconv.FormatFloat(float64(alphaOf(x, y, s)), 'g', -1, 32),
					strconv.FormatFloat(float64(depthOf(x, y, s)), 'g', -1, 32))
			}
			tw.WriteString("\n")
		}
	}

	// The same deep content as a single-part file, which the deep section
	// already gates. Any difference between the two is this path's framing
	// rather than the fixture's content, which is the distinction worth having
	// when one of them is refused.
	single := filepath.Join(dir, "mp_deep_single.exr")
	sf, err := os.Create(single)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer sf.Close()
	dsw, err := exr.NewDeepScanlineWriter(sf, deepW, deepH)
	if err != nil {
		fmt.Fprintln(os.Stderr, "NewDeepScanlineWriter:", err)
		os.Exit(1)
	}
	dsw.Header().SetCompression(exr.CompressionZIPS)
	scl := exr.NewChannelList()
	scl.Add(exr.Channel{Name: "A", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	scl.Add(exr.Channel{Name: "Z", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	dsw.Header().SetChannels(scl)
	dsw.SetFrameBuffer(dfb)
	if err := dsw.WritePixels(deepH); err != nil {
		fmt.Fprintln(os.Stderr, "single-part WritePixels:", err)
		os.Exit(1)
	}
	if err := dsw.Finalize(); err != nil {
		fmt.Fprintln(os.Stderr, "single-part Finalize:", err)
		os.Exit(1)
	}

	fmt.Printf("wrote %s: a %dx%d flat part and a %dx%d deep part holding %d samples\n",
		path, flatW, flatH, deepW, deepH, total)
}
