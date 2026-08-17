package openexr_test

import (
	"image"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

// The README's code blocks used to drift out of step with the API: as of
// v1.2.1 all three of them referenced symbols that did not exist
// (exr.NewHeader(w, h), exr.NewWriter(path, ...), exr.HalfFromFloat32,
// exr.WithCompression, RGBAInputFile.ReadPixels), so the first thing a new
// user copied could not compile.
//
// These tests are those blocks, kept compiling. They are deliberately near
// verbatim: if the API changes under them the build breaks here rather than in
// somebody else's editor. Keep them in step with README.md by hand — the point
// is that the compiler notices when you forget.

// readmeQuickStartWrite mirrors the "Writing an EXR File" quick start.
func TestREADMEQuickStartWrite(t *testing.T) {
	width, height := 64, 48
	path := filepath.Join(t.TempDir(), "output.exr")

	out, err := exr.NewRGBAOutputFile(path, width, height)
	if err != nil {
		t.Fatal(err)
	}
	out.Header().SetCompression(exr.CompressionPIZ)

	img := &exr.RGBAImage{
		Pix:    make([]float32, width*height*4),
		Stride: 4,
		Rect:   image.Rect(0, 0, width, height),
	}
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			i := (y*width + x) * 4
			img.Pix[i+0] = float32(x) / float32(width)
			img.Pix[i+1] = float32(y) / float32(height)
			img.Pix[i+2] = 0.5
			img.Pix[i+3] = 1.0
		}
	}

	if err := out.WriteRGBA(img); err != nil {
		t.Fatal(err)
	}
}

// TestREADMEQuickStartRead mirrors the "Reading an EXR File" quick start.
func TestREADMEQuickStartRead(t *testing.T) {
	path := writeREADMEFixture(t)

	file, err := exr.OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	header := file.Header(0)
	dataWindow := header.DataWindow()
	width := dataWindow.Max.X - dataWindow.Min.X + 1
	height := dataWindow.Max.Y - dataWindow.Min.Y + 1
	if width <= 0 || height <= 0 {
		t.Fatalf("bad dimensions %dx%d", width, height)
	}

	channels := header.Channels()
	for i := 0; i < channels.Len(); i++ {
		ch := channels.At(i)
		_ = ch.Name
		_ = ch.Type
	}

	rgbaFile, err := exr.OpenRGBAInputFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer rgbaFile.Close()

	img, err := rgbaFile.ReadRGBA()
	if err != nil {
		t.Fatal(err)
	}
	r, g, b, a := img.RGBA(0, 0)
	_, _, _, _ = r, g, b, a
}

// TestREADMELowLevelAPI mirrors the "Using the Low-Level API" example.
func TestREADMELowLevelAPI(t *testing.T) {
	width, height := 32, 16
	path := filepath.Join(t.TempDir(), "output.exr")

	header := exr.NewScanlineHeader(width, height)
	header.SetCompression(exr.CompressionZIP)

	header.Channels().Add(exr.Channel{Name: "R", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	header.Channels().Add(exr.Channel{Name: "G", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	header.Channels().Add(exr.Channel{Name: "B", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	header.Channels().Add(exr.Channel{Name: "Z", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})

	rPixels := make([]half.Half, width*height)
	gPixels := make([]half.Half, width*height)
	bPixels := make([]half.Half, width*height)
	zPixels := make([]float32, width*height)

	fb := exr.NewFrameBuffer()
	fb.Insert("R", exr.NewSliceFromHalf(rPixels, width, height))
	fb.Insert("G", exr.NewSliceFromHalf(gPixels, width, height))
	fb.Insert("B", exr.NewSliceFromHalf(bPixels, width, height))
	fb.Insert("Z", exr.NewSliceFromFloat32(zPixels, width, height))

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	writer, err := exr.NewScanlineWriter(f, header)
	if err != nil {
		t.Fatal(err)
	}

	writer.SetFrameBuffer(fb)
	if err := writer.WritePixels(0, height-1); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestREADMEConfiguration mirrors the "Configuration" section.
func TestREADMEConfiguration(t *testing.T) {
	header := exr.NewScanlineHeader(16, 16)
	header.SetCompression(exr.CompressionPIZ)
	header.SetLineOrder(exr.LineOrderIncreasing)

	original := exr.GetParallelConfig()
	defer exr.SetParallelConfig(original)
	exr.SetParallelConfig(exr.ParallelConfig{NumWorkers: 4})
}

func writeREADMEFixture(t *testing.T) string {
	t.Helper()

	const width, height = 16, 8
	path := filepath.Join(t.TempDir(), "image.exr")

	out, err := exr.NewRGBAOutputFile(path, width, height)
	if err != nil {
		t.Fatal(err)
	}
	img := &exr.RGBAImage{
		Pix:    make([]float32, width*height*4),
		Stride: 4,
		Rect:   image.Rect(0, 0, width, height),
	}
	for i := range img.Pix {
		img.Pix[i] = 0.5
	}
	if err := out.WriteRGBA(img); err != nil {
		t.Fatal(err)
	}
	return path
}
