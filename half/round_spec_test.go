package half

import "testing"

// Mutation testing (scripts/mutation/run.py, ids half-tie-normal and
// half-tie-subnormal) replaced this package's tie-breaking rule with
// round-half-up, in the normalised path and in the subnormal path. Nothing in
// the package failed — not TestRoundToNearestEven, which contained only
// exactly-representable values, and not TestFromFloat32_RoundTrip, which
// converts back and compares with a tolerance.
//
// The rule is not a detail. IEEE 754-2019 clause 4.3.1 makes roundTiesToEven
// the default rounding attribute, OpenEXR's half uses it, and it is what makes
// float32 -> half conversion reproducible between implementations: every EXR
// HALF sample this library writes goes through it.
//
// The expectations below are stated from the standard. Each input is exactly
// halfway between two adjacent half values, so it is the tie rule alone that
// decides the answer, and the answer is the neighbour with the even
// significand.

// TestTiesRoundToEvenIEEE754 covers ties in the normalised range.
func TestTiesRoundToEvenIEEE754(t *testing.T) {
	tests := []struct {
		name  string
		input float32
		want  uint16
	}{
		// Around 1.0 the half spacing is 2^-10, so the ties sit at 2^-11.
		// 0x3C00 has significand 0 (even), 0x3C01 has 1 (odd).
		{"1 + 2^-11 -> 0x3C00", 1 + 1.0/2048, 0x3C00},
		{"1 + 3*2^-11 -> 0x3C02", 1 + 3.0/2048, 0x3C02},
		{"1 + 5*2^-11 -> 0x3C02", 1 + 5.0/2048, 0x3C02},
		{"1 + 7*2^-11 -> 0x3C04", 1 + 7.0/2048, 0x3C04},

		// Just below the halfway point the tie rule is not involved: these
		// pin the "nearest" half of round-to-nearest-even.
		{"1 + 2^-11 - 2^-20 -> 0x3C00", 1 + 1.0/2048 - 1.0/1048576, 0x3C00},
		{"1 + 2^-11 + 2^-20 -> 0x3C01", 1 + 1.0/2048 + 1.0/1048576, 0x3C01},

		// Around 2048 the spacing is 2, so the ties are the odd integers.
		// 2048 = 0x6800 (significand 0, even), 2050 = 0x6801, 2052 = 0x6802.
		{"2049 -> 2048 (0x6800)", 2049, 0x6800},
		{"2051 -> 2052 (0x6802)", 2051, 0x6802},
		{"-2049 -> -2048 (0xE800)", -2049, 0xE800},
		{"-2051 -> -2052 (0xE802)", -2051, 0xE802},

		// The overflow tie: 65520 is halfway between the largest finite half,
		// 65504 = 0x7BFF (significand 0x3FF, odd), and 65536, which is not
		// representable. IEEE 754 clause 7.4 makes the even choice infinity.
		{"65520 -> +Inf", 65520, 0x7C00},
		{"-65520 -> -Inf", -65520, 0xFC00},
		// One ulp of float32 below the tie is still finite.
		{"65519 -> 65504 (0x7BFF)", 65519, 0x7BFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromFloat32(tt.input).Bits(); got != tt.want {
				t.Errorf("FromFloat32(%v).Bits() = 0x%04X, IEEE 754 roundTiesToEven gives 0x%04X",
					tt.input, got, tt.want)
			}
		})
	}
}

// TestSubnormalTiesRoundToEven covers ties in the subnormal range, where the
// spacing is a fixed 2^-24 and the ties sit at odd multiples of 2^-25.
func TestSubnormalTiesRoundToEven(t *testing.T) {
	const ulp = 1.0 / 16777216 // 2^-24, the subnormal spacing
	const halfUlp = ulp / 2    // 2^-25

	tests := []struct {
		name  string
		input float32
		want  uint16
	}{
		// Halfway between +0 (significand 0, even) and 2^-24 (0x0001, odd).
		{"2^-25 -> +0", halfUlp, 0x0000},
		{"-2^-25 -> -0", -halfUlp, 0x8000},
		// Halfway between 0x0001 (odd) and 0x0002 (even).
		{"3*2^-25 -> 0x0002", 3 * halfUlp, 0x0002},
		// Halfway between 0x0002 (even) and 0x0003 (odd).
		{"5*2^-25 -> 0x0002", 5 * halfUlp, 0x0002},
		// Halfway between 0x0003 (odd) and 0x0004 (even).
		{"7*2^-25 -> 0x0004", 7 * halfUlp, 0x0004},

		// Not ties: the nearest value wins outright.
		{"2^-24 -> 0x0001", ulp, 0x0001},
		{"2^-25 + 2^-30 -> 0x0001", halfUlp + 1.0/1073741824, 0x0001},
		{"2^-25 - 2^-30 -> +0", halfUlp - 1.0/1073741824, 0x0000},

		// The largest subnormal and the smallest normal, for the boundary.
		{"1023*2^-24 -> 0x03FF", 1023 * ulp, 0x03FF},
		{"1024*2^-24 -> 0x0400", 1024 * ulp, 0x0400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := FromFloat32(tt.input).Bits(); got != tt.want {
				t.Errorf("FromFloat32(%v).Bits() = 0x%04X, IEEE 754 roundTiesToEven gives 0x%04X",
					tt.input, got, tt.want)
			}
		})
	}
}
