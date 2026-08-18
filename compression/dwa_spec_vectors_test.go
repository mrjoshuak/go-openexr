package compression

import (
	"math"
	"testing"
)

// Mutation testing (scripts/mutation/run.py) showed two DWA defects that no
// test in this package could see:
//
//	dwa-pi           the DCT constants rebuilt from full-precision pi instead
//	                 of the truncated 3.14159 literal the reference uses
//	dwa-quant-table  one entry of the JPEG luminance quantisation table changed
//
// The first survives because TestDwaDctInverseMatchesDefinition and
// TestDwaDctForwardMatchesDefinition compare against the mathematically ideal
// DCT with a 2e-4 tolerance — moving towards true pi moves *towards* what they
// assert. The second survives because every other DWA test is a lossy round
// trip judged against a tolerance, and the quantiser is inside the loop.
//
// Both are wire-visible: DWA is decoded by multiplying the same constants back
// in, so a file this library writes with different constants is a file the
// reference reads as different pixels. The tests below pin the constants
// themselves.

// TestDwaDctConstantsAreTheReferenceLiterals pins the DCT butterfly constants
// to the exact float32 values OpenEXR's internal_dwa_simd.h computes.
//
// The reference writes them as .5f * cosf (3.14159f * k / 16.f) — a truncated
// decimal literal, not M_PI — and the truncation is part of the format. The
// expected bit patterns below were computed outside Go, in double precision,
// by rounding 3.14159 to float32, dividing in float32, taking the cosine and
// rounding the result to float32, exactly as cosf does.
func TestDwaDctConstantsAreTheReferenceLiterals(t *testing.T) {
	tests := []struct {
		name string
		got  float32
		bits uint32
		expr string
	}{
		{"dwaIDctA", dwaIDctA, 0x3EB504FB, ".5f * cosf(3.14159f / 4.f)"},
		{"dwaIDctB", dwaIDctB, 0x3EFB14BF, ".5f * cosf(3.14159f / 16.f)"},
		{"dwaIDctC", dwaIDctC, 0x3EEC8361, ".5f * cosf(3.14159f / 8.f)"},
		{"dwaIDctD", dwaIDctD, 0x3ED4DB36, ".5f * cosf(3.f * 3.14159f / 16.f)"},
		{"dwaIDctE", dwaIDctE, 0x3E8E39E5, ".5f * cosf(5.f * 3.14159f / 16.f)"},
		{"dwaIDctF", dwaIDctF, 0x3E43EF33, ".5f * cosf(3.f * 3.14159f / 8.f)"},
		{"dwaIDctG", dwaIDctG, 0x3DC7C60B, ".5f * cosf(7.f * 3.14159f / 16.f)"},
		{"dwaFDctC1", dwaFDctC1, 0x3F7B14BF, "cosf(3.14159f * 1.f / 16.f)"},
		{"dwaFDctC2", dwaFDctC2, 0x3F6C8361, "cosf(3.14159f * 2.f / 16.f)"},
		{"dwaFDctC3", dwaFDctC3, 0x3F54DB36, "cosf(3.14159f * 3.f / 16.f)"},
		{"dwaFDctC4", dwaFDctC4, 0x3F3504FB, "cosf(3.14159f * 4.f / 16.f)"},
		{"dwaFDctC5", dwaFDctC5, 0x3F0E39E5, "cosf(3.14159f * 5.f / 16.f)"},
		{"dwaFDctC6", dwaFDctC6, 0x3EC3EF33, "cosf(3.14159f * 6.f / 16.f)"},
		{"dwaFDctC7", dwaFDctC7, 0x3E47C60B, "cosf(3.14159f * 7.f / 16.f)"},
	}

	for _, tt := range tests {
		want := math.Float32frombits(tt.bits)
		if tt.got != want {
			t.Errorf("%s = %v (0x%08X), want %v (0x%08X), the float32 value of %s",
				tt.name, tt.got, math.Float32bits(tt.got), want, tt.bits, tt.expr)
		}
	}

	// The literal itself, stated separately: substituting pi here is the
	// mutation this test exists to kill, and it is one ulp-scale change away
	// from every constant above.
	if dwaPi != math.Float32frombits(0x40490FD0) {
		t.Errorf("dwaPi = %v (0x%08X), want the float32 value of the literal 3.14159 (0x40490FD0)",
			dwaPi, math.Float32bits(dwaPi))
	}
	if float64(dwaPi) == math.Pi {
		t.Error("dwaPi is pi; the format is defined by the reference's truncated literal 3.14159")
	}
}

// TestDwaQuantTablesAreJpegAnnexK pins DWA's per-coefficient error tolerances
// to the JPEG quantisation tables they are taken from.
//
// ISO/IEC 10918-1 Annex K, Table K.1 (luminance) and Table K.2 (chrominance),
// in natural (row-major, not zig-zag) order. OpenEXR's
// LossyDctEncoder_base_construct reproduces them verbatim and divides each by
// the table's smallest entry, so an altered entry changes which DCT
// coefficients survive quantisation and therefore the bytes on the wire.
func TestDwaQuantTablesAreJpegAnnexK(t *testing.T) {
	annexKLuminance := [64]float32{
		16, 11, 10, 16, 24, 40, 51, 61,
		12, 12, 14, 19, 26, 58, 60, 55,
		14, 13, 16, 24, 40, 57, 69, 56,
		14, 17, 22, 29, 51, 87, 80, 62,
		18, 22, 37, 56, 68, 109, 103, 77,
		24, 35, 55, 64, 81, 104, 113, 92,
		49, 64, 78, 87, 103, 121, 120, 101,
		72, 92, 95, 98, 112, 100, 103, 99,
	}
	annexKChrominance := [64]float32{
		17, 18, 24, 47, 99, 99, 99, 99,
		18, 21, 26, 66, 99, 99, 99, 99,
		24, 26, 56, 99, 99, 99, 99, 99,
		47, 66, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
	}

	for i := range annexKLuminance {
		if dwaJpegQuantY[i] != annexKLuminance[i] {
			t.Errorf("dwaJpegQuantY[%d] (row %d, column %d) = %v, Table K.1 says %v",
				i, i/8, i%8, dwaJpegQuantY[i], annexKLuminance[i])
		}
		if dwaJpegQuantCbCr[i] != annexKChrominance[i] {
			t.Errorf("dwaJpegQuantCbCr[%d] (row %d, column %d) = %v, Table K.2 says %v",
				i, i/8, i%8, dwaJpegQuantCbCr[i], annexKChrominance[i])
		}
	}

	// The normalisers are the tables' own minima, which is what makes the
	// tolerance for the DC term equal to the base error.
	minY, minC := annexKLuminance[0], annexKChrominance[0]
	for i := range annexKLuminance {
		if annexKLuminance[i] < minY {
			minY = annexKLuminance[i]
		}
		if annexKChrominance[i] < minC {
			minC = annexKChrominance[i]
		}
	}
	if dwaJpegQuantYMin != minY {
		t.Errorf("dwaJpegQuantYMin = %v, but the smallest entry of Table K.1 is %v", dwaJpegQuantYMin, minY)
	}
	if dwaJpegQuantCbCrMin != minC {
		t.Errorf("dwaJpegQuantCbCrMin = %v, but the smallest entry of Table K.2 is %v", dwaJpegQuantCbCrMin, minC)
	}
}
