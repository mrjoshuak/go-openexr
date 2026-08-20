package exr

import (
	"math"
	"testing"
)

// This file exists because the ACES conversion was wrong for a long time and
// nothing noticed. It produced a well-formed file with the wrong colours: no
// error, no failed round trip, and no test that would have caught it, because
// every check it had went out through this library and came back through it.
//
// The cause was three places mixing two matrix conventions. Imath composes row
// vectors — v*M — so its matrices are stored transposed and its products read
// left to right. This file multiplies M*v. The Bradford constants had been
// copied in Imath's form, the adaptation product was composed in Imath's order,
// and so was the final RGB->XYZ->adapt->ACES chain, while RGBtoXYZ and the
// per-pixel application were M*v. The mixture is self-consistent enough to look
// right and is off by up to 58% per channel.
//
// The checks below are the ones that would have caught it. The external
// comparison against the reference exr2aces lives in the gate.

// acesConvert applies the same conversion AcesInputFile does, through the
// exported matrix builders, so a test can exercise it without a file.
func acesConvert(t *testing.T, fileChr Chromaticities, r, g, b float32) (float32, float32, float32) {
	t.Helper()
	acesChr := ACESChromaticities()
	m := multiply44(
		multiply44(XYZtoRGB(acesChr),
			ChromaticAdaptation(
				V2f{X: fileChr.WhiteX, Y: fileChr.WhiteY},
				V2f{X: acesChr.WhiteX, Y: acesChr.WhiteY})),
		RGBtoXYZ(fileChr))
	return m[0]*r + m[1]*g + m[2]*b,
		m[4]*r + m[5]*g + m[6]*b,
		m[8]*r + m[9]*g + m[10]*b
}

// TestACESConversionMatchesTheReference pins the conversion to what libOpenEXR's
// exr2aces produces for a known input.
//
// The expected values are the reference's own answer, read off its output for a
// constant Rec.709 image of (0.8, 0.2, 0.1). Anchoring on the reference rather
// than on a matrix derived here is the point: the previous implementation was
// self-consistent, and any expectation computed the same wrong way would have
// agreed with it.
//
// The tolerance is one half-float ULP, which is 2^-10 relative for the
// magnitudes involved and comes from the format rather than from what happened
// to pass: the file's samples are half, and this library and the reference round
// their float32 matrix products independently.
func TestACESConversionMatchesTheReference(t *testing.T) {
	const wantR, wantG, wantB = 0.446045, 0.244141, 0.123413
	const tol = 1.0 / 1024 // 2^-10, one half ULP at these magnitudes

	gotR, gotG, gotB := acesConvert(t, DefaultChromaticities(), 0.8, 0.2, 0.1)

	for _, c := range []struct {
		name      string
		got, want float32
	}{{"R", gotR, wantR}, {"G", gotG, wantG}, {"B", gotB, wantB}} {
		rel := math.Abs(float64(c.got-c.want)) / math.Abs(float64(c.want))
		if rel > tol {
			t.Errorf("channel %s = %v, the reference exr2aces produces %v (%.1f%% off)",
				c.name, c.got, c.want, 100*rel)
		}
	}
	t.Logf("Rec.709 (0.8, 0.2, 0.1) -> ACES (%v, %v, %v); the reference gives (%v, %v, %v)",
		gotR, gotG, gotB, float32(wantR), float32(wantG), float32(wantB))
}

// TestACESConversionOfACESIsIdentity is the invariant that costs nothing and
// catches a whole class of error — but not, on its own, the one that was here.
//
// A file already in ACES needs no conversion, so the matrix must be the
// identity. That is true whatever convention the Bradford constants are written
// in, because the white points are equal and the adaptation ratios are all one:
// the transposition cancels. It is recorded as insufficient rather than left
// out, so that nobody adds it later and believes the area is covered.
func TestACESConversionOfACESIsIdentity(t *testing.T) {
	r, g, b := acesConvert(t, ACESChromaticities(), 0.8, 0.2, 0.1)
	for _, c := range []struct {
		name      string
		got, want float32
	}{{"R", r, 0.8}, {"G", g, 0.2}, {"B", b, 0.1}} {
		if math.Abs(float64(c.got-c.want)) > 1e-4 {
			t.Errorf("ACES to ACES changed %s: %v became %v", c.name, c.want, c.got)
		}
	}
}

// TestChromaticAdaptationMovesTheWhitePoint is the direct check on the piece
// that was wrong, and it does not depend on the reference.
//
// Adapting from a source white to a destination white must take the source
// white point to the destination white point in XYZ — that is the definition of
// the transform. A transposed Bradford matrix does not satisfy it, which is what
// makes this test able to see the defect on its own.
func TestChromaticAdaptationMovesTheWhitePoint(t *testing.T) {
	src := V2f{X: 0.3127, Y: 0.3290} // D65, Rec.709's white
	dst := V2f{X: 0.32168, Y: 0.33767}

	adapt := ChromaticAdaptation(src, dst)

	// The source white in XYZ, normalised to Y = 1.
	sx := float64(src.X) / float64(src.Y)
	sz := (1 - float64(src.X) - float64(src.Y)) / float64(src.Y)
	got := transformV3(adapt, sx, 1, sz)

	wx := float64(dst.X) / float64(dst.Y)
	wz := (1 - float64(dst.X) - float64(dst.Y)) / float64(dst.Y)

	for _, c := range []struct {
		name      string
		got, want float64
	}{{"X", got[0], wx}, {"Y", got[1], 1}, {"Z", got[2], wz}} {
		if math.Abs(c.got-c.want) > 1e-3 {
			t.Errorf("adapting the source white gave %s = %v, want %v; "+
				"a chromatic adaptation must map one white point onto the other",
				c.name, c.got, c.want)
		}
	}
}
