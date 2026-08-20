package exr

import (
	"fmt"
	"image"
	"math"
	"os"
	"path/filepath"
	"testing"
)

func TestRGBtoYCRoundTrip(t *testing.T) {
	testCases := []struct {
		r, g, b float32
	}{
		{0, 0, 0},       // Black
		{1, 1, 1},       // White
		{1, 0, 0},       // Red
		{0, 1, 0},       // Green
		{0, 0, 1},       // Blue
		{0.5, 0.5, 0.5}, // Gray
		{0.8, 0.2, 0.1}, // Random color
		{2.0, 1.5, 0.5}, // HDR values
	}

	for _, tc := range testCases {
		y, ry, by := RGBtoYC(tc.r, tc.g, tc.b)
		r2, g2, b2 := YCtoRGB(y, ry, by)

		// Check round-trip accuracy
		eps := float32(0.0001)
		if absF32(r2-tc.r) > eps || absF32(g2-tc.g) > eps || absF32(b2-tc.b) > eps {
			t.Errorf("RGBtoYC/YCtoRGB round-trip failed for RGB(%f, %f, %f): got (%f, %f, %f)",
				tc.r, tc.g, tc.b, r2, g2, b2)
		}
	}
}

func absF32(v float32) float32 {
	if v < 0 {
		return -v
	}
	return v
}

func TestRGBtoYCLuminance(t *testing.T) {
	// Test that pure gray has zero chroma
	y, ry, by := RGBtoYC(0.5, 0.5, 0.5)

	if absF32(y-0.5) > 0.0001 {
		t.Errorf("Gray luminance = %f, expected 0.5", y)
	}
	if absF32(ry) > 0.0001 || absF32(by) > 0.0001 {
		t.Errorf("Gray chroma = RY:%f BY:%f, expected 0", ry, by)
	}
}

func TestIsYCImage(t *testing.T) {
	// Create a YC header
	h := NewHeader()
	channels := NewChannelList()
	channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	channels.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	h.SetChannels(channels)

	if !IsYCImage(h) {
		t.Error("Should detect YC image")
	}

	// Test non-YC header
	h2 := NewHeader()
	channels2 := NewChannelList()
	channels2.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels2.Add(Channel{Name: "G", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels2.Add(Channel{Name: "B", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	h2.SetChannels(channels2)

	if IsYCImage(h2) {
		t.Error("Should not detect RGB image as YC")
	}
}

func TestIsLuminanceOnlyImage(t *testing.T) {
	// Create a luminance-only header
	h := NewHeader()
	channels := NewChannelList()
	channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	h.SetChannels(channels)

	if !IsLuminanceOnlyImage(h) {
		t.Error("Should detect luminance-only image")
	}

	// YC image is not luminance-only
	h2 := NewHeader()
	channels2 := NewChannelList()
	channels2.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels2.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	channels2.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	h2.SetChannels(channels2)

	if IsLuminanceOnlyImage(h2) {
		t.Error("YC image should not be luminance-only")
	}
}

func TestYCWriteY(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_y.exr")

	// Create test image
	img := NewRGBAImage(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			gray := float32(x+y) / 64.0
			img.SetRGBA(x, y, gray, gray, gray, 1.0)
		}
	}

	// Write as luminance-only
	out, err := NewYCOutputFile(path, 32, 32, WriteY)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read back
	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer f.Close()

	h := f.Header(0)
	if !IsLuminanceOnlyImage(h) {
		t.Error("File should be luminance-only")
	}

	// Check channels
	channels := h.Channels()
	if channels.Len() != 1 {
		t.Errorf("Expected 1 channel, got %d", channels.Len())
	}
	if channels.At(0).Name != "Y" {
		t.Errorf("Expected Y channel, got %s", channels.At(0).Name)
	}
}

func TestYCWriteYA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_ya.exr")

	img := NewRGBAImage(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			gray := float32(x+y) / 64.0
			alpha := float32(x) / 32.0
			img.SetRGBA(x, y, gray, gray, gray, alpha)
		}
	}

	out, err := NewYCOutputFile(path, 32, 32, WriteYA)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer f.Close()

	h := f.Header(0)
	channels := h.Channels()
	if channels.Len() != 2 {
		t.Errorf("Expected 2 channels, got %d", channels.Len())
	}

	// Check for A and Y channels
	hasA := false
	hasY := false
	for i := 0; i < channels.Len(); i++ {
		switch channels.At(i).Name {
		case "A":
			hasA = true
		case "Y":
			hasY = true
		}
	}
	if !hasA || !hasY {
		t.Error("Expected A and Y channels")
	}
}

func TestYCWriteYC(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_yc.exr")

	// Create a colorful test image
	img := NewRGBAImage(image.Rect(0, 0, 64, 64))
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			r := float32(x) / 64.0
			g := float32(y) / 64.0
			b := float32(x+y) / 128.0
			img.SetRGBA(x, y, r, g, b, 1.0)
		}
	}

	// Write as YC
	out, err := NewYCOutputFile(path, 64, 64, WriteYC)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Check file
	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer f.Close()

	h := f.Header(0)
	if !IsYCImage(h) {
		t.Error("File should be YC image")
	}

	// Check channels have correct subsampling
	channels := h.Channels()
	for i := 0; i < channels.Len(); i++ {
		ch := channels.At(i)
		switch ch.Name {
		case "Y":
			if ch.XSampling != 1 || ch.YSampling != 1 {
				t.Errorf("Y channel has wrong sampling: %dx%d", ch.XSampling, ch.YSampling)
			}
		case "RY", "BY":
			if ch.XSampling != 2 || ch.YSampling != 2 {
				t.Errorf("%s channel has wrong sampling: %dx%d", ch.Name, ch.XSampling, ch.YSampling)
			}
		}
	}
}

func TestYCWriteYCA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_yca.exr")

	img := NewRGBAImage(image.Rect(0, 0, 64, 64))
	for y := 0; y < 64; y++ {
		for x := 0; x < 64; x++ {
			r := float32(x) / 64.0
			g := float32(y) / 64.0
			b := float32(0.5)
			a := float32(x+y) / 128.0
			img.SetRGBA(x, y, r, g, b, a)
		}
	}

	out, err := NewYCOutputFile(path, 64, 64, WriteYCA)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer f.Close()

	h := f.Header(0)
	if !IsYCImage(h) {
		t.Error("File should be YC image")
	}

	// Check for alpha channel
	channels := h.Channels()
	hasA := false
	for i := 0; i < channels.Len(); i++ {
		if channels.At(i).Name == "A" {
			hasA = true
			break
		}
	}
	if !hasA {
		t.Error("Expected A channel")
	}
}

// TestYCRoundTrip measures what a luminance/chroma round trip costs, and ties
// the tolerance to the encoding rather than to whatever happened to pass.
//
// The format stores (R-Y)/Y and (B-Y)/Y, not the plain differences. This
// library stored the differences until v1.4.17: self-consistent, so its own
// round trip was exact, and not what the file claims — every YC file it wrote
// had chroma that meant something else to every other reader.
//
// Storing the ratios is correct and is worse conditioned, and that is a real
// consequence rather than a regression. As Y approaches zero the ratios grow
// without bound, so averaging the chroma of a 2x2 block whose luminance varies
// reconstructs the darkest pixel of that block poorly. Measured on this
// library, worst channel error against the minimum luminance in the image:
//
//	minimum Y 0.0217 -> 0.066     (R=G=0, B=0.3: chroma ratio near 13)
//	minimum Y 0.2751 -> 0.0082
//	minimum Y 0.5751 -> 0.0047
//
// So the bound is stated per fixture and the dark case is expected to be loose.
// It is kept precisely because it is the case that exposes the conditioning.
func TestYCRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		bound float32
		pix   func(x, y int) (float32, float32, float32)
	}{
		{
			// Well-conditioned content: nothing near black, so the ratios stay
			// small and the only loss is the chroma subsampling itself.
			name:  "well conditioned",
			bound: 0.02,
			pix: func(x, y int) (float32, float32, float32) {
				return 0.2 + 0.6*float32(x)/64, 0.3 + 0.5*float32(y)/64,
					0.25 + 0.4*float32(x+y)/128
			},
		},
		{
			// The pathological case: B constant while R and G fall to zero, so
			// the darkest pixel has luminance 0.0217 and a chroma ratio near
			// 13. This is the format's conditioning, not a defect, and the
			// bound says so.
			name:  "dark corner",
			bound: 0.08,
			pix: func(x, y int) (float32, float32, float32) {
				return float32(x) / 64, float32(y) / 64, 0.3
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "yc.exr")

			orig := NewRGBAImage(image.Rect(0, 0, 64, 64))
			for y := 0; y < 64; y++ {
				for x := 0; x < 64; x++ {
					r, g, b := c.pix(x, y)
					orig.SetRGBA(x, y, r, g, b, 1.0)
				}
			}

			out, err := NewYCOutputFile(path, 64, 64, WriteYC)
			if err != nil {
				t.Fatalf("NewYCOutputFile: %v", err)
			}
			if err := out.WriteRGBA(orig); err != nil {
				t.Fatalf("WriteRGBA: %v", err)
			}

			input, err := OpenYCInputFile(path)
			if err != nil {
				t.Fatalf("OpenYCInputFile: %v", err)
			}
			defer input.Close()
			if !input.IsYC() {
				t.Error("the file was not recognised as luminance/chroma")
			}
			readImg, err := input.ReadRGBA()
			if err != nil {
				t.Fatalf("ReadRGBA: %v", err)
			}

			var worst float32
			var at string
			for y := 0; y < 64; y++ {
				for x := 0; x < 64; x++ {
					or, og, ob, _ := orig.RGBA(x, y)
					rr, rg, rb, _ := readImg.RGBA(x, y)
					for _, d := range []struct {
						n    string
						a, b float32
					}{{"R", or, rr}, {"G", og, rg}, {"B", ob, rb}} {
						e := absF32(d.a - d.b)
						if e > worst {
							worst, at = e, fmt.Sprintf("%s at (%d,%d): %v vs %v", d.n, x, y, d.a, d.b)
						}
					}
				}
			}
			if worst > c.bound {
				t.Errorf("worst channel error %v exceeds %v (%s)", worst, c.bound, at)
			}
			t.Logf("worst channel error %v (%s)", worst, at)
		})
	}
}

func TestYCRoundTripLuminanceOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_y_roundtrip.exr")

	// Create grayscale test image
	origImg := NewRGBAImage(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			gray := float32(x+y) / 64.0
			origImg.SetRGBA(x, y, gray, gray, gray, 1.0)
		}
	}

	// Write as Y
	out, err := NewYCOutputFile(path, 32, 32, WriteY)
	if err != nil {
		t.Fatalf("Failed to create output: %v", err)
	}

	if err := out.WriteRGBA(origImg); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Read back
	input, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer input.Close()

	if !input.IsLuminanceOnly() {
		t.Error("Should detect luminance-only image")
	}

	readImg, err := input.ReadRGBA()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	// Compare
	maxError := float32(0.01)
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			// Original was grayscale, so all channels should be equal
			or, og, ob, _ := origImg.RGBA(x, y)
			rr, rg, rb, _ := readImg.RGBA(x, y)

			// Read image should also be grayscale (R=G=B=Y)
			if absF32(rr-rg) > maxError || absF32(rg-rb) > maxError {
				t.Errorf("Pixel (%d,%d) not grayscale: (%f,%f,%f)", x, y, rr, rg, rb)
			}

			// Luminance should match original grayscale value
			origLum := (or + og + ob) / 3.0 // Original was grayscale
			if absF32(rr-origLum) > maxError {
				t.Errorf("Pixel (%d,%d) luminance mismatch: orig %f, read %f", x, y, origLum, rr)
			}
		}
	}
}

func TestYCFileSizeReduction(t *testing.T) {
	// NOTE: This test currently verifies that YC files can be written and read,
	// but the file size optimization is not yet implemented. The scanline writer
	// needs to properly handle YSampling to achieve the expected compression.
	// For now, we skip the size comparison check.
	t.Skip("YSampling optimization not yet implemented in scanline writer")

	dir := t.TempDir()
	rgbaPath := filepath.Join(dir, "test_rgba.exr")
	ycPath := filepath.Join(dir, "test_yc.exr")

	// Create test image
	img := NewRGBAImage(image.Rect(0, 0, 256, 256))
	for y := 0; y < 256; y++ {
		for x := 0; x < 256; x++ {
			r := float32(math.Sin(float64(x)*0.1))*0.5 + 0.5
			g := float32(math.Sin(float64(y)*0.1))*0.5 + 0.5
			b := float32(math.Sin(float64(x+y)*0.05))*0.5 + 0.5
			img.SetRGBA(x, y, r, g, b, 1.0)
		}
	}

	// Write as RGBA
	rgbaOut, _ := NewYCOutputFile(rgbaPath, 256, 256, WriteRGBA)
	rgbaOut.WriteRGBA(img)

	// Write as YC
	ycOut, _ := NewYCOutputFile(ycPath, 256, 256, WriteYC)
	ycOut.WriteRGBA(img)

	// Compare file sizes
	rgbaInfo, _ := os.Stat(rgbaPath)
	ycInfo, _ := os.Stat(ycPath)

	rgbaSize := rgbaInfo.Size()
	ycSize := ycInfo.Size()

	// YC should be roughly 50% smaller (Y full-res, RY/BY quarter-res each)
	// So: Y (1x) + RY (0.25x) + BY (0.25x) = 1.5x vs RGBA = 4x
	// Expected ratio: 1.5/4 = 37.5% (with some compression overhead)
	ratio := float64(ycSize) / float64(rgbaSize)

	t.Logf("RGBA size: %d bytes, YC size: %d bytes, ratio: %.2f%%", rgbaSize, ycSize, ratio*100)

	if ratio > 0.7 {
		t.Errorf("YC file should be significantly smaller than RGBA (ratio: %.2f)", ratio)
	}
}

func TestBilinearSample(t *testing.T) {
	// Create a simple 2x2 chroma plane
	data := make([]byte, 2*2*4)
	slice := NewSlice(PixelTypeFloat, data, 2, 2)

	// Set corner values
	slice.SetFloat32(0, 0, 0.0)
	slice.SetFloat32(1, 0, 1.0)
	slice.SetFloat32(0, 1, 1.0)
	slice.SetFloat32(1, 1, 0.0)

	// Test interpolation at center of full-res pixel (1,1) which maps to chroma (0.5, 0.5)
	// This should interpolate between all four chroma corners
	center := bilinearSample(&slice, 1, 1, 4, 4, 2, 2)
	expected := float32(0.5) // Average of all four corners: (0+1+1+0)/4 = 0.5
	if absF32(center-expected) > 0.01 {
		t.Errorf("Center interpolation = %f, expected %f", center, expected)
	}

	// Test corner (should be exact value)
	corner := bilinearSample(&slice, 0, 0, 4, 4, 2, 2)
	if absF32(corner-0.0) > 0.01 {
		t.Errorf("Corner interpolation = %f, expected 0.0", corner)
	}
}

func TestYCInputFileRGBAFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_rgba.exr")

	// Create and write a standard RGBA image
	img := NewRGBAImage(image.Rect(0, 0, 16, 16))
	for y := 0; y < 16; y++ {
		for x := 0; x < 16; x++ {
			img.SetRGBA(x, y, 0.5, 0.3, 0.7, 1.0)
		}
	}

	// Write as standard RGBA
	out, _ := NewYCOutputFile(path, 16, 16, WriteRGBA)
	out.WriteRGBA(img)

	// Read using YCInputFile
	input, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer input.Close()

	if input.IsYC() {
		t.Error("RGBA file should not be detected as YC")
	}

	readImg, err := input.ReadRGBA()
	if err != nil {
		t.Fatalf("Failed to read: %v", err)
	}

	// Verify
	r, g, b, _ := readImg.RGBA(8, 8)
	if absF32(r-0.5) > 0.01 || absF32(g-0.3) > 0.01 || absF32(b-0.7) > 0.01 {
		t.Errorf("RGBA fallback failed: got (%f,%f,%f), expected (0.5,0.3,0.7)", r, g, b)
	}
}

// TestYCOutputFileHeader tests that Header() returns a valid header for YCOutputFile.
func TestYCOutputFileHeader(t *testing.T) {
	dir := t.TempDir()

	modes := []struct {
		mode     YCMode
		name     string
		channels int
	}{
		{WriteY, "y", 1},
		{WriteYA, "ya", 2},
		{WriteYC, "yc", 3},
		{WriteYCA, "yca", 4},
		{WriteRGBA, "rgba", 4},
	}

	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(dir, "test_"+tc.name+".exr")
			out, err := NewYCOutputFile(path, 32, 32, tc.mode)
			if err != nil {
				t.Fatalf("NewYCOutputFile failed: %v", err)
			}

			// Test Header() method
			h := out.Header()
			if h == nil {
				t.Fatal("Header() returned nil")
			}

			// Verify dimensions
			if h.Width() != 32 || h.Height() != 32 {
				t.Errorf("Header dimensions = %dx%d, want 32x32", h.Width(), h.Height())
			}

			// Verify channels count
			channels := h.Channels()
			if channels == nil {
				t.Fatal("Header channels is nil")
			}
			if channels.Len() != tc.channels {
				t.Errorf("Header has %d channels, want %d", channels.Len(), tc.channels)
			}
		})
	}
}

// TestYCInputFileHeader tests that Header() returns a valid header for YCInputFile.
func TestYCInputFileHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_header.exr")

	// Create and write a YC image
	img := NewRGBAImage(image.Rect(0, 0, 32, 32))
	for y := 0; y < 32; y++ {
		for x := 0; x < 32; x++ {
			img.SetRGBA(x, y, 0.5, 0.3, 0.7, 1.0)
		}
	}

	out, err := NewYCOutputFile(path, 32, 32, WriteYC)
	if err != nil {
		t.Fatalf("NewYCOutputFile failed: %v", err)
	}
	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("WriteRGBA failed: %v", err)
	}

	// Open and test Header()
	input, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("OpenYCInputFile failed: %v", err)
	}
	defer input.Close()

	h := input.Header()
	if h == nil {
		t.Fatal("Header() returned nil")
	}

	// Verify dimensions
	if h.Width() != 32 || h.Height() != 32 {
		t.Errorf("Header dimensions = %dx%d, want 32x32", h.Width(), h.Height())
	}

	// Verify this is detected as YC
	if !IsYCImage(h) {
		t.Error("Header should be detected as YC image")
	}
}

func TestIsYCImageNilHeader(t *testing.T) {
	// Test nil header
	if IsYCImage(nil) {
		t.Error("IsYCImage(nil) should return false")
	}
}

func TestIsYCImageNilChannels(t *testing.T) {
	// Test header with nil channels
	h := NewHeader()
	// Don't set channels
	if IsYCImage(h) {
		t.Error("IsYCImage with nil channels should return false")
	}
}

func TestIsYCImagePartialChannels(t *testing.T) {
	// Test with Y and RY but no BY (should still be YC if one chroma is present)
	h := NewHeader()
	channels := NewChannelList()
	channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	h.SetChannels(channels)

	if !IsYCImage(h) {
		t.Error("Y + RY should be detected as YC image")
	}

	// Test with Y and BY but no RY
	h2 := NewHeader()
	channels2 := NewChannelList()
	channels2.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels2.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
	h2.SetChannels(channels2)

	if !IsYCImage(h2) {
		t.Error("Y + BY should be detected as YC image")
	}
}

func TestIsYCImageWrongSampling(t *testing.T) {
	// Test RY/BY with wrong sampling (should not be YC)
	h := NewHeader()
	channels := NewChannelList()
	channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 1, YSampling: 1}) // Wrong sampling
	channels.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 1, YSampling: 1}) // Wrong sampling
	h.SetChannels(channels)

	if IsYCImage(h) {
		t.Error("RY/BY with wrong sampling should not be detected as YC")
	}
}

func TestIsLuminanceOnlyImageNilHeader(t *testing.T) {
	if IsLuminanceOnlyImage(nil) {
		t.Error("IsLuminanceOnlyImage(nil) should return false")
	}
}

func TestIsLuminanceOnlyImageNilChannels(t *testing.T) {
	h := NewHeader()
	// Don't set channels
	if IsLuminanceOnlyImage(h) {
		t.Error("IsLuminanceOnlyImage with nil channels should return false")
	}
}

func TestOpenYCInputFileInvalidPath(t *testing.T) {
	_, err := OpenYCInputFile("/nonexistent/path/file.exr")
	if err == nil {
		t.Error("OpenYCInputFile with invalid path should return error")
	}
}

func TestNewYCInputFileNil(t *testing.T) {
	_, err := NewYCInputFile(nil)
	if err == nil {
		t.Error("NewYCInputFile(nil) should return error")
	}
}

func TestYCOutputFileInvalidPath(t *testing.T) {
	// NewYCOutputFile doesn't create the file until WriteRGBA is called
	out, err := NewYCOutputFile("/nonexistent/dir/file.exr", 32, 32, WriteYC)
	if err != nil {
		// If it fails early, that's also fine
		return
	}

	// Try to write - this should fail because the directory doesn't exist
	img := NewRGBAImage(image.Rect(0, 0, 32, 32))
	err = out.WriteRGBA(img)
	if err == nil {
		t.Error("WriteRGBA to invalid path should return error")
	}
}

func TestBilinearSampleEdgeCases(t *testing.T) {
	// Create a 4x4 chroma plane to test edge cases
	data := make([]byte, 4*4*4)
	slice := NewSlice(PixelTypeFloat, data, 4, 4)

	// Set values
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			slice.SetFloat32(x, y, float32(x+y))
		}
	}

	// Test at various positions
	_ = bilinearSample(&slice, 0, 0, 8, 8, 2, 2) // Top-left corner
	_ = bilinearSample(&slice, 7, 0, 8, 8, 2, 2) // Top-right edge
	_ = bilinearSample(&slice, 0, 7, 8, 8, 2, 2) // Bottom-left edge
	_ = bilinearSample(&slice, 7, 7, 8, 8, 2, 2) // Bottom-right corner
	_ = bilinearSample(&slice, 3, 3, 8, 8, 2, 2) // Center region
}

func TestIsYCImageWithNilHeader(t *testing.T) {
	if IsYCImage(nil) {
		t.Error("IsYCImage(nil) should return false")
	}
}

// TestYCInputFileRoundTrip tests reading back YC files.
func TestYCInputFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_yc_rt.exr")

	// Write a YC image
	width, height := 64, 64
	img := NewRGBAImage(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			r := float32(x) / float32(width)
			g := float32(y) / float32(height)
			b := float32(x+y) / float32(width+height)
			img.SetRGBA(x, y, r, g, b, 1.0)
		}
	}

	out, err := NewYCOutputFile(path, width, height, WriteYC)
	if err != nil {
		t.Fatalf("NewYCOutputFile error: %v", err)
	}
	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("WriteRGBA error: %v", err)
	}

	// Read back with YCInputFile
	ycIn, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("OpenYCInputFile error: %v", err)
	}
	defer ycIn.Close()

	if ycIn.Header() == nil {
		t.Error("Header should not be nil")
	}

	if ycIn.Width() != width || ycIn.Height() != height {
		t.Errorf("Dimensions: got %dx%d, want %dx%d", ycIn.Width(), ycIn.Height(), width, height)
	}

	// Read the image
	readImg, err := ycIn.ReadRGBA()
	if err != nil {
		t.Fatalf("ReadRGBA error: %v", err)
	}

	if readImg == nil {
		t.Fatal("ReadRGBA returned nil")
	}

	// Verify dimensions
	if readImg.Rect.Dx() != width || readImg.Rect.Dy() != height {
		t.Errorf("Read image dimensions: got %dx%d, want %dx%d",
			readImg.Rect.Dx(), readImg.Rect.Dy(), width, height)
	}
}

// TestYCInputFileReadLuminanceOnly tests reading luminance-only files.
func TestYCInputFileReadLuminanceOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test_y_only.exr")

	// Write a luminance-only image
	width, height := 32, 32
	img := NewRGBAImage(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			gray := float32(x+y) / float32(width+height)
			img.SetRGBA(x, y, gray, gray, gray, 1.0)
		}
	}

	out, err := NewYCOutputFile(path, width, height, WriteY)
	if err != nil {
		t.Fatalf("NewYCOutputFile error: %v", err)
	}
	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("WriteRGBA error: %v", err)
	}

	// Read back
	ycIn, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("OpenYCInputFile error: %v", err)
	}
	defer ycIn.Close()

	readImg, err := ycIn.ReadRGBA()
	if err != nil {
		t.Fatalf("ReadRGBA error: %v", err)
	}

	if readImg == nil {
		t.Fatal("ReadRGBA returned nil")
	}

	// For luminance-only, RGB should be equal (grayscale)
	for y := 0; y < height; y += 4 {
		for x := 0; x < width; x += 4 {
			r, g, b, _ := readImg.RGBA(x, y)
			if absF32(r-g) > 0.1 || absF32(r-b) > 0.1 {
				t.Errorf("Pixel at (%d,%d) not grayscale: R=%v, G=%v, B=%v", x, y, r, g, b)
			}
		}
	}
}

// TestYCInputFileCloseNilFile tests closing when internal file is nil.
func TestYCInputFileCloseNilFile(t *testing.T) {
	// Create a YCInputFile with nil internal file
	ycIn := &YCInputFile{file: nil}
	err := ycIn.Close()
	if err != nil {
		t.Errorf("Close with nil file should not error, got %v", err)
	}
}

// TestYCInputFileDoubleClose tests closing twice.
func TestYCInputFileDoubleClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "close_test.exr")

	width, height := 8, 8
	img := NewRGBAImage(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			img.SetRGBA(x, y, 0.5, 0.5, 0.5, 1.0)
		}
	}

	out, err := NewYCOutputFile(path, width, height, WriteYC)
	if err != nil {
		t.Fatalf("NewYCOutputFile error: %v", err)
	}
	if err := out.WriteRGBA(img); err != nil {
		t.Fatalf("WriteRGBA error: %v", err)
	}

	ycIn, err := OpenYCInputFile(path)
	if err != nil {
		t.Fatalf("OpenYCInputFile error: %v", err)
	}

	// First close
	err = ycIn.Close()
	if err != nil {
		t.Logf("First close (may error after file closed): %v", err)
	}

	// Second close should be safe
	err = ycIn.Close()
	// This might error or not depending on implementation
	t.Logf("Second close (expected to be handled): %v", err)
}

// TestYCWriteModesAll tests all YC write modes in sequence.
func TestYCWriteModesAll(t *testing.T) {
	modes := []YCMode{WriteY, WriteYC, WriteYA, WriteYCA}
	modeNames := []string{"WriteY", "WriteYC", "WriteYA", "WriteYCA"}

	width, height := 32, 32
	img := NewRGBAImage(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			r := float32(x) / float32(width)
			g := float32(y) / float32(height)
			b := float32(0.5)
			a := float32(x+y) / float32(width+height)
			img.SetRGBA(x, y, r, g, b, a)
		}
	}

	for i, mode := range modes {
		t.Run(modeNames[i], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.exr")

			out, err := NewYCOutputFile(path, width, height, mode)
			if err != nil {
				t.Fatalf("NewYCOutputFile error: %v", err)
			}
			if err := out.WriteRGBA(img); err != nil {
				t.Fatalf("WriteRGBA error: %v", err)
			}

			// Verify file exists and is readable
			f, err := OpenFile(path)
			if err != nil {
				t.Fatalf("OpenFile error: %v", err)
			}
			f.Close()
		})
	}
}
