// Command interopgen writes one EXR per (pixel type x compression) from a
// known gradient, so an external tool can independently confirm the OpenEXR
// reference implementation reads each one correctly.
//
// This exists so validation does not depend on any test in this repository:
// the oracle is the reference implementation, invoked separately on the
// resulting files. A defect applied identically to our encoder and our decoder
// is invisible to a round-trip test but fails here.
//
// Alongside the files it writes a manifest naming, for each combination,
// whether the codec is lossless for that pixel type *by specification*. The
// expectation is declared from the format definition, never fitted to what we
// happen to measure — a combination declared lossless that does not compare
// bit-exact is a real defect, and is meant to fail.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

const w, h = 71, 40

type pixelType struct {
	name string
	pt   exr.PixelType
}

type codec struct {
	name string
	c    exr.Compression
	// lossless lists the pixel types this codec reproduces exactly, per the
	// OpenEXR format specification:
	//
	//   none/rle/zips/zip/piz  entirely lossless
	//   pxr24                  keeps 24 of 32 float bits; HALF and UINT are
	//                          stored exactly
	//   b44/b44a               4x4 block quantisation of HALF only; FLOAT and
	//                          UINT are passed through uncompressed
	//   dwaa/dwab              lossy for HALF and FLOAT; UINT is passed through
	//   htj2k                  entirely lossless: reversible 5/3 wavelet over
	//                          the raw sample bit patterns. OpenEXR's own
	//                          compression table marks htj2k256 and htj2k32
	//                          lossy=false for every pixel type
	//                          (ImfCompression.cpp)
	lossless []string
}

var (
	types = []pixelType{
		{"half", exr.PixelTypeHalf},
		{"float", exr.PixelTypeFloat},
		{"uint", exr.PixelTypeUint},
	}
	all = []string{"half", "float", "uint"}

	codecs = []codec{
		{"none", exr.CompressionNone, all},
		{"rle", exr.CompressionRLE, all},
		{"zips", exr.CompressionZIPS, all},
		{"zip", exr.CompressionZIP, all},
		{"piz", exr.CompressionPIZ, all},
		{"pxr24", exr.CompressionPXR24, []string{"half", "uint"}},
		{"b44", exr.CompressionB44, []string{"float", "uint"}},
		{"b44a", exr.CompressionB44A, []string{"float", "uint"}},
		{"dwaa", exr.CompressionDWAA, []string{"uint"}},
		{"dwab", exr.CompressionDWAB, []string{"uint"}},
		{"htj2k256", exr.CompressionHTJ2K256, all},
		{"htj2k32", exr.CompressionHTJ2K32, all},
	}
)

func gradient() map[string][]float32 {
	vals := map[string][]float32{}
	for _, n := range []string{"R", "G", "B", "A"} {
		v := make([]float32, w*h)
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				fx, fy := float32(x)/float32(w-1), float32(y)/float32(h-1)
				switch n {
				case "R":
					v[y*w+x] = fx
				case "G":
					v[y*w+x] = fy
				case "B":
					v[y*w+x] = fx * fy
				case "A":
					v[y*w+x] = 1
				}
			}
		}
		vals[n] = v
	}
	return vals
}

func write(path string, t pixelType, c codec, vals map[string][]float32) error {
	header := exr.NewScanlineHeader(w, h)
	header.SetCompression(c.c)
	cl := exr.NewChannelList()
	for _, n := range []string{"A", "B", "G", "R"} {
		cl.Add(exr.Channel{Name: n, Type: t.pt, XSampling: 1, YSampling: 1})
	}
	header.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	wr, err := exr.NewScanlineWriter(f, header)
	if err != nil {
		return err
	}
	fb := exr.NewFrameBuffer()
	for _, n := range []string{"R", "G", "B", "A"} {
		switch t.pt {
		case exr.PixelTypeHalf:
			hv := make([]half.Half, w*h)
			for i, v := range vals[n] {
				hv[i] = half.FromFloat32(v)
			}
			fb.Set(n, exr.NewSliceFromHalf(hv, w, h))
		case exr.PixelTypeFloat:
			fb.Set(n, exr.NewSliceFromFloat32(vals[n], w, h))
		case exr.PixelTypeUint:
			uv := make([]uint32, w*h)
			for i, v := range vals[n] {
				uv[i] = uint32(v * 1000)
			}
			fb.Set(n, exr.NewSliceFromUint32(uv, w, h))
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		return err
	}
	// Close is what flushes the chunk offset table. Skipping it silently
	// truncates the file, which is how issue #4 first presented.
	return wr.Close()
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: interopgen <outdir>")
		os.Exit(2)
	}
	outDir := os.Args[1]
	vals := gradient()

	var manifest strings.Builder
	manifest.WriteString("# file\ttype\tcodec\texpect\n")

	written, failed := 0, 0
	for _, t := range types {
		for _, c := range codecs {
			name := fmt.Sprintf("wr_%s_%s.exr", t.name, c.name)
			if err := write(filepath.Join(outDir, name), t, c, vals); err != nil {
				fmt.Printf("FAIL write %-6s %-9s: %v\n", t.name, c.name, err)
				failed++
				continue
			}
			expect := "lossy"
			for _, l := range c.lossless {
				if l == t.name {
					expect = "exact"
				}
			}
			fmt.Fprintf(&manifest, "%s\t%s\t%s\t%s\n", name, t.name, c.name, expect)
			written++
		}
	}

	if err := os.WriteFile(filepath.Join(outDir, "manifest.tsv"), []byte(manifest.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "manifest: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("wrote %d files, %d write failures\n", written, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
