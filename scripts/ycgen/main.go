// Command ycgen writes a luminance/chroma EXR and reports what a round trip
// through this library costs, so the reference can be asked whether the file's
// chroma means what the format says it means.
//
//	ycgen <outdir>
//
// It exists because a round trip cannot check a colour encoding. The format
// stores (R-Y)/Y and (B-Y)/Y; this library stored the plain differences, which
// is self-consistent — its own reader undid its own writer exactly — and means
// something different to every other reader. Only comparing the written planes
// against the format's own definition can see that, which is what
// scripts/yccheck.py does with libOpenEXR's reading of the same file.
//
// The content is smooth in every channel and nowhere near black. Both matter:
// 2x2 chroma subsampling destroys high-frequency chroma, and the (R-Y)/Y
// encoding is badly conditioned as Y approaches zero, so either would measure
// the format rather than the implementation.
package main

import (
	"fmt"
	"image"
	"os"
	"path/filepath"

	"github.com/mrjoshuak/go-openexr/exr"
)

const w, h = 32, 24

func src() *exr.RGBAImage {
	img := &exr.RGBAImage{
		Pix:    make([]float32, w*h*4),
		Stride: 4,
		Rect:   image.Rect(0, 0, w, h),
	}
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			i := (y*w + x) * 4
			// Smooth in every channel: 2x2 chroma subsampling is lossy by
			// construction, so high-frequency chroma would measure the format
			// rather than the implementation.
			img.Pix[i+0] = 0.2 + 0.6*float32(x)/float32(w-1)
			img.Pix[i+1] = 0.3 + 0.5*float32(y)/float32(h-1)
			img.Pix[i+2] = 0.25 + 0.4*float32(x+y)/float32(w+h-2)
			img.Pix[i+3] = 1
		}
	}
	return img
}

func main() {
	dir := os.Args[1]
	os.MkdirAll(dir, 0o755)
	path := filepath.Join(dir, "yc.exr")

	out, err := exr.NewYCOutputFile(path, w, h, exr.WriteYC)
	if err != nil {
		fmt.Println("NewYCOutputFile:", err)
		return
	}
	original := src()
	if err := out.WriteRGBA(original); err != nil {
		fmt.Println("WriteRGBA:", err)
		return
	}
	fmt.Println("wrote", path)

	in, err := exr.OpenYCInputFile(path)
	if err != nil {
		fmt.Println("OpenYCInputFile:", err)
		return
	}
	defer in.Close()
	fmt.Printf("read back: IsYC=%v %dx%d\n", in.IsYC(), in.Width(), in.Height())
	got, err := in.ReadRGBA()
	if err != nil {
		fmt.Println("ReadRGBA:", err)
		return
	}

	worst, at := 0.0, ""
	interior, iat := 0.0, ""
	for i := 0; i < w*h; i++ {
		px, py := i%w, i/w
		for c, name := range []string{"R", "G", "B"} {
			a := original.Pix[i*4+c]
			b := got.Pix[i*4+c]
			d := float64(a - b)
			if d < 0 {
				d = -d
			}
			if d > worst {
				worst, at = d, fmt.Sprintf("%s at pixel %d: %v vs %v", name, i, a, b)
			}
			if px >= 2 && px < w-2 && py >= 2 && py < h-2 && d > interior {
				interior, iat = d, fmt.Sprintf("%s at (%d,%d): %v vs %v", name, px, py, a, b)
			}
		}
	}
	fmt.Printf("round trip: worst %.5g (%s)\n", worst, at)
	fmt.Printf("round trip, interior only: worst %.5g (%s)\n", interior, iat)
}
