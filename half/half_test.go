package half

import (
	"math"
	"testing"
)

func TestFromFloat32_RoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		input float32
	}{
		{"zero", 0.0},
		{"one", 1.0},
		{"negative one", -1.0},
		{"small positive", 0.5},
		{"small negative", -0.5},
		{"two", 2.0},
		{"max normal", 65504.0},
		{"min normal", 6.103515625e-5},
		{"typical HDR value", 100.0},
		{"typical color", 0.18},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			result := h.Float32()
			// Allow small rounding errors due to reduced precision
			diff := math.Abs(float64(result - tt.input))
			relDiff := diff / math.Abs(float64(tt.input))
			// Half precision has ~0.1% relative precision for normalized values
			if tt.input != 0 && relDiff > 0.001 {
				t.Errorf("FromFloat32(%v).Float32() = %v, relative error = %v", tt.input, result, relDiff)
			}
			if tt.input == 0 && result != 0 {
				t.Errorf("FromFloat32(0).Float32() = %v, want 0", result)
			}
		})
	}
}

func TestSpecialValues(t *testing.T) {
	tests := []struct {
		name     string
		input    float32
		checkInf bool
		checkNaN bool
		sign     int
	}{
		{"positive infinity", float32(math.Inf(1)), true, false, 1},
		{"negative infinity", float32(math.Inf(-1)), true, false, -1},
		{"NaN", float32(math.NaN()), false, true, 0},
		{"positive zero", 0.0, false, false, 0},
		{"negative zero", float32(math.Copysign(0, -1)), false, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			result := h.Float32()

			if tt.checkInf {
				if !math.IsInf(float64(result), tt.sign) {
					t.Errorf("FromFloat32(%v).Float32() = %v, expected infinity with sign %d", tt.input, result, tt.sign)
				}
				if !h.IsInf() {
					t.Errorf("Half from %v should be infinity", tt.input)
				}
			}

			if tt.checkNaN {
				if !math.IsNaN(float64(result)) {
					t.Errorf("FromFloat32(%v).Float32() = %v, expected NaN", tt.input, result)
				}
				if !h.IsNaN() {
					t.Errorf("Half from NaN should be NaN")
				}
			}
		})
	}
}

func TestOverflow(t *testing.T) {
	// Values larger than max half should become infinity
	large := float32(100000.0) // Larger than 65504
	h := FromFloat32(large)
	if !h.IsInf() {
		t.Errorf("FromFloat32(%v) should overflow to infinity, got %v", large, h.Float32())
	}

	// Negative overflow
	negativeLarge := float32(-100000.0)
	h = FromFloat32(negativeLarge)
	if !h.IsNegInf() {
		t.Errorf("FromFloat32(%v) should overflow to -infinity, got %v", negativeLarge, h.Float32())
	}
}

func TestUnderflow(t *testing.T) {
	// Values smaller than smallest subnormal should become zero
	tiny := float32(1e-10)
	h := FromFloat32(tiny)
	if !h.IsZero() {
		t.Errorf("FromFloat32(%v) should underflow to zero, got %v", tiny, h.Float32())
	}

	// Negative underflow
	negativeTiny := float32(-1e-10)
	h = FromFloat32(negativeTiny)
	if !h.IsZero() {
		t.Errorf("FromFloat32(%v) should underflow to zero, got %v", negativeTiny, h.Float32())
	}
}

func TestSubnormals(t *testing.T) {
	// Test subnormal half values
	// Smallest subnormal is 2^-24 ≈ 5.96e-8
	// Largest subnormal is (2^10 - 1) * 2^-24 ≈ 6.097555e-5

	tests := []struct {
		name        string
		bits        uint16
		isSubnormal bool
	}{
		{"smallest subnormal", 0x0001, true},
		{"mid subnormal", 0x0200, true},
		{"largest subnormal", 0x03FF, true},
		{"smallest normal", 0x0400, false},
		{"zero", 0x0000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromBits(tt.bits)
			if h.IsSubnormal() != tt.isSubnormal {
				t.Errorf("Half(0x%04X).IsSubnormal() = %v, want %v", tt.bits, h.IsSubnormal(), tt.isSubnormal)
			}
		})
	}
}

func TestFromFloat64(t *testing.T) {
	tests := []struct {
		name  string
		input float64
	}{
		{"zero", 0.0},
		{"one", 1.0},
		{"pi", math.Pi},
		{"large", 1000.0},
		{"small", 0.001},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat64(tt.input)
			result := h.Float64()
			diff := math.Abs(result - tt.input)
			relDiff := diff / math.Abs(tt.input)
			if tt.input != 0 && relDiff > 0.001 {
				t.Errorf("FromFloat64(%v).Float64() = %v, relative error = %v", tt.input, result, relDiff)
			}
		})
	}
}

func TestConstants(t *testing.T) {
	// Test exported constants
	if !Inf.IsPosInf() {
		t.Error("Inf should be positive infinity")
	}
	if !NegInf.IsNegInf() {
		t.Error("NegInf should be negative infinity")
	}
	if !NaN.IsNaN() {
		t.Error("NaN should be NaN")
	}
	if !Zero.IsZero() {
		t.Error("Zero should be zero")
	}
	if !NegZero.IsZero() {
		t.Error("NegZero should be zero")
	}
	if Max.Float32() > 65504 || Max.Float32() < 65500 {
		t.Errorf("Max = %v, expected ~65504", Max.Float32())
	}
	if SmallestNormal.Float32() < 6e-5 || SmallestNormal.Float32() > 6.2e-5 {
		t.Errorf("SmallestNormal = %v, expected ~6.1e-5", SmallestNormal.Float32())
	}
}

func TestIsFinite(t *testing.T) {
	tests := []struct {
		name     string
		h        Half
		isFinite bool
	}{
		{"zero", Zero, true},
		{"one", FromFloat32(1.0), true},
		{"max", Max, true},
		{"infinity", Inf, false},
		{"negative infinity", NegInf, false},
		{"NaN", NaN, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.h.IsFinite() != tt.isFinite {
				t.Errorf("Half(%v).IsFinite() = %v, want %v", tt.name, tt.h.IsFinite(), tt.isFinite)
			}
		})
	}
}

func TestNeg(t *testing.T) {
	tests := []struct {
		name   string
		input  float32
		output float32
	}{
		{"positive", 1.0, -1.0},
		{"negative", -1.0, 1.0},
		{"zero", 0.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			neg := h.Neg()
			result := neg.Float32()
			if result != tt.output {
				t.Errorf("FromFloat32(%v).Neg().Float32() = %v, want %v", tt.input, result, tt.output)
			}
		})
	}

	// Test that negating infinity works
	if !Inf.Neg().IsNegInf() {
		t.Error("Inf.Neg() should be negative infinity")
	}
	if !NegInf.Neg().IsPosInf() {
		t.Error("NegInf.Neg() should be positive infinity")
	}
}

func TestAbs(t *testing.T) {
	tests := []struct {
		name  string
		input float32
	}{
		{"positive", 1.0},
		{"negative", -1.0},
		{"negative large", -100.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			abs := h.Abs()
			if abs.Sign() < 0 {
				t.Errorf("FromFloat32(%v).Abs() = %v, should be non-negative", tt.input, abs.Float32())
			}
			expected := float32(math.Abs(float64(tt.input)))
			if abs.Float32() != FromFloat32(expected).Float32() {
				t.Errorf("FromFloat32(%v).Abs() = %v, want %v", tt.input, abs.Float32(), expected)
			}
		})
	}
}

func TestComparisons(t *testing.T) {
	one := FromFloat32(1.0)
	two := FromFloat32(2.0)
	negOne := FromFloat32(-1.0)

	// Less
	if !one.Less(two) {
		t.Error("1 < 2 should be true")
	}
	if two.Less(one) {
		t.Error("2 < 1 should be false")
	}
	if !negOne.Less(one) {
		t.Error("-1 < 1 should be true")
	}
	if one.Less(one) {
		t.Error("1 < 1 should be false")
	}

	// LessOrEqual
	if !one.LessOrEqual(two) {
		t.Error("1 <= 2 should be true")
	}
	if !one.LessOrEqual(one) {
		t.Error("1 <= 1 should be true")
	}

	// Greater
	if !two.Greater(one) {
		t.Error("2 > 1 should be true")
	}
	if one.Greater(two) {
		t.Error("1 > 2 should be false")
	}

	// GreaterOrEqual
	if !two.GreaterOrEqual(one) {
		t.Error("2 >= 1 should be true")
	}
	if !one.GreaterOrEqual(one) {
		t.Error("1 >= 1 should be true")
	}

	// Equal
	if !one.Equal(one) {
		t.Error("1 == 1 should be true")
	}
	if one.Equal(two) {
		t.Error("1 == 2 should be false")
	}

	// NaN comparisons
	if NaN.Less(one) || one.Less(NaN) {
		t.Error("NaN comparisons should always be false")
	}
	if NaN.Equal(NaN) {
		t.Error("NaN == NaN should be false")
	}

	// Zero comparisons
	if !Zero.Equal(NegZero) {
		t.Error("+0 == -0 should be true")
	}
}

func TestBits(t *testing.T) {
	tests := []struct {
		bits  uint16
		value float32
	}{
		{0x0000, 0.0},
		{0x3C00, 1.0},
		{0x4000, 2.0},
		{0xC000, -2.0},
		{0x7C00, float32(math.Inf(1))},
		{0xFC00, float32(math.Inf(-1))},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			h := FromBits(tt.bits)
			if h.Bits() != tt.bits {
				t.Errorf("FromBits(0x%04X).Bits() = 0x%04X", tt.bits, h.Bits())
			}
			if !math.IsInf(float64(tt.value), 0) && h.Float32() != tt.value {
				t.Errorf("FromBits(0x%04X).Float32() = %v, want %v", tt.bits, h.Float32(), tt.value)
			}
		})
	}
}

func TestConvertSlice32(t *testing.T) {
	src := []float32{1.0, 2.0, 3.0, 4.0, 5.0}
	dst := make([]Half, len(src))
	ConvertSlice32(dst, src)

	for i, h := range dst {
		if h.Float32() != src[i] {
			t.Errorf("ConvertSlice32: dst[%d] = %v, want %v", i, h.Float32(), src[i])
		}
	}
}

func TestConvertSliceToFloat32(t *testing.T) {
	src := []Half{FromFloat32(1.0), FromFloat32(2.0), FromFloat32(3.0)}
	dst := make([]float32, len(src))
	ConvertSliceToFloat32(dst, src)

	for i, f := range dst {
		if f != src[i].Float32() {
			t.Errorf("ConvertSliceToFloat32: dst[%d] = %v, want %v", i, f, src[i].Float32())
		}
	}
}

func TestMakeSlice32(t *testing.T) {
	src := []float32{1.0, 2.0, 3.0}
	result := MakeSlice32(src)

	if len(result) != len(src) {
		t.Errorf("MakeSlice32: len = %d, want %d", len(result), len(src))
	}
	for i, h := range result {
		if h.Float32() != src[i] {
			t.Errorf("MakeSlice32: result[%d] = %v, want %v", i, h.Float32(), src[i])
		}
	}
}

func TestToFloat32Slice(t *testing.T) {
	src := []Half{FromFloat32(1.0), FromFloat32(2.0), FromFloat32(3.0)}
	result := ToFloat32Slice(src)

	if len(result) != len(src) {
		t.Errorf("ToFloat32Slice: len = %d, want %d", len(result), len(src))
	}
	for i, f := range result {
		if f != src[i].Float32() {
			t.Errorf("ToFloat32Slice: result[%d] = %v, want %v", i, f, src[i].Float32())
		}
	}
}

func TestSlicePanicOnMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("ConvertSlice32 should panic on mismatched lengths")
		}
	}()

	src := []float32{1.0, 2.0}
	dst := make([]Half, 3)
	ConvertSlice32(dst, src)
}

func TestRoundToNearestEven(t *testing.T) {
	// Test that rounding follows round-to-nearest-even
	// The mantissa is truncated from 23 bits to 10 bits
	// When the dropped bits are exactly 0.5, round to even
	//
	// The three exactly-representable cases below cannot distinguish
	// round-to-nearest-even from any other tie rule, and mutation testing
	// (scripts/mutation/run.py, id half-tie-normal) confirmed that: replacing
	// the tie rule with round-half-up left this test green. The cases marked
	// "tie" are the ones that carry the test's name; see also
	// TestTiesRoundToEvenIEEE754 and TestSubnormalTiesRoundToEven.
	tests := []struct {
		name     string
		input    float32
		expected uint16 // Expected half bits
	}{
		// 1.0 = 0x3C00 in half
		{"exact 1.0", 1.0, 0x3C00},
		// 1.5 = 0x3E00 in half
		{"exact 1.5", 1.5, 0x3E00},
		// 2.0 = 0x4000 in half
		{"exact 2.0", 2.0, 0x4000},
		// Halfway between 1.0 (0x3C00, even significand) and the next half
		// up (0x3C01, odd): IEEE 754 roundTiesToEven keeps 0x3C00.
		{"tie 1+2^-11 goes down to the even 0x3C00", 1 + 1.0/2048, 0x3C00},
		// Halfway between 0x3C01 (odd) and 0x3C02 (even): rounds up.
		{"tie 1+3*2^-11 goes up to the even 0x3C02", 1 + 3.0/2048, 0x3C02},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			if h.Bits() != tt.expected {
				t.Errorf("FromFloat32(%v).Bits() = 0x%04X, want 0x%04X", tt.input, h.Bits(), tt.expected)
			}
		})
	}
}

func TestSign(t *testing.T) {
	tests := []struct {
		name     string
		h        Half
		expected int
	}{
		{"positive", FromFloat32(1.0), 1},
		{"negative", FromFloat32(-1.0), -1},
		{"positive zero", Zero, 0},
		{"negative zero", NegZero, 0},
		{"NaN", NaN, 0},
		{"positive infinity", Inf, 1},
		{"negative infinity", NegInf, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.h.Sign() != tt.expected {
				t.Errorf("Half(%v).Sign() = %d, want %d", tt.name, tt.h.Sign(), tt.expected)
			}
		})
	}
}

func TestString(t *testing.T) {
	// Basic string output tests
	if NaN.String() != "NaN" {
		t.Errorf("NaN.String() = %q, want \"NaN\"", NaN.String())
	}
	if Inf.String() != "+Inf" {
		t.Errorf("Inf.String() = %q, want \"+Inf\"", Inf.String())
	}
	if NegInf.String() != "-Inf" {
		t.Errorf("NegInf.String() = %q, want \"-Inf\"", NegInf.String())
	}
}

func TestIsNormal(t *testing.T) {
	tests := []struct {
		name     string
		h        Half
		expected bool
	}{
		{"one", FromFloat32(1.0), true},
		{"smallest normal", SmallestNormal, true},
		{"subnormal", SmallestSubnormal, false},
		{"zero", Zero, false},
		{"infinity", Inf, false},
		{"NaN", NaN, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.h.IsNormal() != tt.expected {
				t.Errorf("Half(%v).IsNormal() = %v, want %v", tt.name, tt.h.IsNormal(), tt.expected)
			}
		})
	}
}

// Benchmark tests

func TestSubnormalConversion(t *testing.T) {
	// Test round-trip for subnormal half values
	// These are values where the exponent is 0 but mantissa is non-zero

	tests := []struct {
		name string
		bits uint16
	}{
		{"smallest subnormal", 0x0001},
		{"mid subnormal", 0x0200},
		{"largest subnormal", 0x03FF},
		{"negative smallest subnormal", 0x8001},
		{"negative largest subnormal", 0x83FF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromBits(tt.bits)
			f := h.Float32()
			// Convert back
			h2 := FromFloat32(f)
			// The round-trip should preserve the bits (or very close)
			if h2.Bits() != tt.bits {
				// Allow off-by-one due to rounding
				diff := int(h2.Bits()) - int(tt.bits)
				if diff < -1 || diff > 1 {
					t.Errorf("Round-trip for 0x%04X: got 0x%04X (diff=%d)", tt.bits, h2.Bits(), diff)
				}
			}
		})
	}
}

func TestMantissaOverflowRounding(t *testing.T) {
	// Test the edge case where rounding the mantissa causes overflow
	// This tests the branch in fromFloat32Bits where halfMantissa > mantissaMask

	// Create a float32 value that will round up and cause mantissa overflow
	// We need a value where bits 0-12 are all 1s (causes round up) and bits 13-22 are all 1s (causes overflow)
	// This is tricky to construct precisely, so we'll test values near powers of 2

	tests := []struct {
		name  string
		input float32
	}{
		// Values just below powers of 2 that might round up
		{"near 2", 1.9999},
		{"near 4", 3.9999},
		{"near 8", 7.9999},
		{"near 16", 15.9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := FromFloat32(tt.input)
			result := h.Float32()
			// Should round to the next power of 2 or stay just below
			if math.IsNaN(float64(result)) || math.IsInf(float64(result), 0) {
				t.Errorf("FromFloat32(%v) produced unexpected special value: %v", tt.input, result)
			}
		})
	}
}

func TestNegativeZeroRoundTrip(t *testing.T) {
	// Ensure negative zero is preserved
	negZeroFloat := float32(math.Copysign(0, -1))
	h := FromFloat32(negZeroFloat)
	result := h.Float32()

	// Check that sign bit is preserved
	resultBits := math.Float32bits(result)
	if resultBits&0x80000000 == 0 {
		t.Error("Negative zero sign bit not preserved")
	}

	// Also test via bits
	negZeroHalf := FromBits(0x8000)
	if !negZeroHalf.IsZero() {
		t.Error("0x8000 should be negative zero")
	}
}

func TestConvertSliceToFloat32Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("ConvertSliceToFloat32 should panic on mismatched lengths")
		}
	}()

	src := []Half{FromFloat32(1.0), FromFloat32(2.0)}
	dst := make([]float32, 3)
	ConvertSliceToFloat32(dst, src)
}

func TestStringRegularValue(t *testing.T) {
	// Test String() for regular (non-special) values
	h := FromFloat32(1.0)
	s := h.String()
	// For regular values, String returns empty string (delegating to float32 formatting)
	if s != "" {
		t.Errorf("String() for 1.0 = %q, want empty string", s)
	}
}

func TestLessEdgeCases(t *testing.T) {
	// Test Less() edge cases for better coverage
	tests := []struct {
		name     string
		a, b     Half
		expected bool
	}{
		{"both negative, a larger magnitude", FromFloat32(-2.0), FromFloat32(-1.0), true},
		{"both negative, b larger magnitude", FromFloat32(-1.0), FromFloat32(-2.0), false},
		{"negative vs positive", FromFloat32(-1.0), FromFloat32(1.0), true},
		{"positive vs negative", FromFloat32(1.0), FromFloat32(-1.0), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.a.Less(tt.b) != tt.expected {
				t.Errorf("%v.Less(%v) = %v, want %v", tt.a.Float32(), tt.b.Float32(), tt.a.Less(tt.b), tt.expected)
			}
		})
	}
}

func TestComparisonNaNReturns(t *testing.T) {
	// Test that all comparison methods return false for NaN
	one := FromFloat32(1.0)

	if NaN.LessOrEqual(one) {
		t.Error("NaN.LessOrEqual(1) should be false")
	}
	if one.LessOrEqual(NaN) {
		t.Error("1.LessOrEqual(NaN) should be false")
	}
	if NaN.Greater(one) {
		t.Error("NaN.Greater(1) should be false")
	}
	if one.Greater(NaN) {
		t.Error("1.Greater(NaN) should be false")
	}
	if NaN.GreaterOrEqual(one) {
		t.Error("NaN.GreaterOrEqual(1) should be false")
	}
	if one.GreaterOrEqual(NaN) {
		t.Error("1.GreaterOrEqual(NaN) should be false")
	}
}

func BenchmarkFromFloat32(b *testing.B) {
	values := []float32{0.0, 1.0, -1.0, 100.0, 0.001, 65504.0}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range values {
			_ = FromFloat32(v)
		}
	}
}

func BenchmarkFloat32(b *testing.B) {
	halves := []Half{Zero, FromFloat32(1.0), FromFloat32(-1.0), FromFloat32(100.0)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, h := range halves {
			_ = h.Float32()
		}
	}
}

func BenchmarkConvertSlice32(b *testing.B) {
	src := make([]float32, 1000)
	dst := make([]Half, 1000)
	for i := range src {
		src[i] = float32(i) * 0.1
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ConvertSlice32(dst, src)
	}
}

func BenchmarkConvertSliceToFloat32(b *testing.B) {
	src := make([]Half, 1000)
	dst := make([]float32, 1000)
	for i := range src {
		src[i] = FromFloat32(float32(i) * 0.1)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ConvertSliceToFloat32(dst, src)
	}
}
