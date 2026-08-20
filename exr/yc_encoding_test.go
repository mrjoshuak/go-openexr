package exr

import (
	"image"
	"math"
	"path/filepath"
	"testing"
)

// TestYCEncodingMatchesTheFormat checks the chroma encoding against the
// format's definition rather than against a round trip.
//
// This is the check the area never had. The format stores (R-Y)/Y and (B-Y)/Y
// (ImfRgbaYca RGBAtoYCA), and this library stored the plain differences. That
// is perfectly self-consistent — its own reader undid its own writer exactly,
// so every round-trip test passed — and it means something different from what
// the file declares. The chroma of every YC file written here was wrong for
// every other reader, and every YC file written elsewhere was read wrongly
// here.
//
// The gate makes the same comparison through libOpenEXR on a real file. This
// one needs no oracle, because the definition is arithmetic.
func TestYCEncodingMatchesTheFormat(t *testing.T) {
	// Luminance weights for Rec.709, which is what RGBtoYC uses when the file
	// carries no chromaticities.
	const kr, kg, kb = 0.2126, 0.7152, 0.0722

	cases := []struct{ r, g, b float32 }{
		{0.5, 0.5, 0.5},
		{0.8, 0.2, 0.1},
		{0.2, 0.9, 0.4},
		{1.0, 0.0, 0.0},
		{0.05, 0.06, 0.07},
	}

	for _, c := range cases {
		y, ry, by := RGBtoYC(c.r, c.g, c.b)

		wantY := kr*float64(c.r) + kg*float64(c.g) + kb*float64(c.b)
		if math.Abs(float64(y)-wantY) > 1e-6 {
			t.Errorf("Y for (%v,%v,%v) = %v, want %v", c.r, c.g, c.b, y, wantY)
		}
		wantRY := (float64(c.r) - wantY) / wantY
		wantBY := (float64(c.b) - wantY) / wantY
		if math.Abs(float64(ry)-wantRY) > 1e-5 {
			t.Errorf("RY for (%v,%v,%v) = %v, want (R-Y)/Y = %v; the format stores the "+
				"ratio, not the difference", c.r, c.g, c.b, ry, wantRY)
		}
		if math.Abs(float64(by)-wantBY) > 1e-5 {
			t.Errorf("BY for (%v,%v,%v) = %v, want (B-Y)/Y = %v", c.r, c.g, c.b, by, wantBY)
		}

		// And the inverse must undo it exactly, since nothing is subsampled
		// here.
		gr, gg, gb := YCtoRGB(y, ry, by)
		for _, d := range []struct {
			n    string
			a, b float32
		}{{"R", gr, c.r}, {"G", gg, c.g}, {"B", gb, c.b}} {
			if math.Abs(float64(d.a-d.b)) > 1e-5 {
				t.Errorf("YCtoRGB round trip: %s = %v, want %v", d.n, d.a, d.b)
			}
		}
	}

	// Y of zero has no chroma to express; the reference writes zero rather than
	// dividing, and so must this.
	y, ry, by := RGBtoYC(0, 0, 0)
	if y != 0 || ry != 0 || by != 0 {
		t.Errorf("black gave Y=%v RY=%v BY=%v, want zeros rather than a division", y, ry, by)
	}
}

// TestYCChromaLandsWhereItBelongs pins the plane-indexing half, and does it by
// going through the actual conversion rather than by exercising a Slice.
//
// The chroma writer passed plane coordinates to an accessor that divides by the
// channel's sampling, so each value landed at a quarter of its position and
// three quarters of the plane was never written. The reference reads such a
// file without complaint — it is a well-formed file with the wrong pixels — so
// the check has to look at where the samples ended up. Reading the RY channel
// back as an ordinary EXR channel, rather than through the YC reader, is what
// makes that visible: the YC reader's upsampler would smear the missing three
// quarters away.
func TestYCChromaLandsWhereItBelongs(t *testing.T) {
	const w, h = 16, 16
	dir := t.TempDir()
	path := filepath.Join(dir, "yc.exr")

	// Chroma that varies across the whole image, so an untouched cell is
	// unmistakable.
	orig := NewRGBAImage(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			orig.SetRGBA(x, y,
				0.3+0.5*float32(x)/float32(w-1),
				0.4,
				0.3+0.5*float32(y)/float32(h-1), 1)
		}
	}
	out, err := NewYCOutputFile(path, w, h, WriteYC)
	if err != nil {
		t.Fatalf("NewYCOutputFile: %v", err)
	}
	if err := out.WriteRGBA(orig); err != nil {
		t.Fatalf("WriteRGBA: %v", err)
	}

	// Read RY as an ordinary channel.
	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()
	r, err := NewScanlineReader(f)
	if err != nil {
		t.Fatalf("NewScanlineReader: %v", err)
	}
	fb, _ := AllocateChannels(f.Header(0).Channels(), f.Header(0).DataWindow())
	r.SetFrameBuffer(fb)
	if err := r.ReadPixels(0, h-1); err != nil {
		t.Fatalf("ReadPixels: %v", err)
	}

	ry := fb.Get("RY")
	if ry == nil {
		t.Fatal("the file has no RY channel")
	}
	// Every chroma cell must carry a value. With the plane index passed
	// straight in, only the top-left quarter is written and the rest is zero.
	zero := 0
	total := 0
	for cy := 0; cy < h/2; cy++ {
		for cx := 0; cx < w/2; cx++ {
			total++
			if ry.GetFloat32(cx*ry.XSampling, cy*ry.YSampling) == 0 {
				zero++
			}
		}
	}
	if zero > 0 {
		t.Errorf("%d of %d chroma cells are zero; the conversion wrote each value at "+
			"a fraction of its position and left the rest untouched", zero, total)
	}

	// And the values must vary along x, since the source red does.
	a := ry.GetFloat32(0, 0)
	b := ry.GetFloat32((w/2-1)*ry.XSampling, 0)
	if a == b {
		t.Errorf("the chroma plane does not vary across x: both ends are %v", a)
	}
}
