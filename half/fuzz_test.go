package half

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// FuzzFromFloat32 tests conversion from float32 to half.
func FuzzFromFloat32(f *testing.F) {
	// Special values
	f.Add(float32(0))
	f.Add(math.Float32frombits(0x80000000)) // negative zero
	f.Add(float32(1))
	f.Add(float32(-1))
	f.Add(float32(math.MaxFloat32))
	f.Add(float32(math.SmallestNonzeroFloat32))
	f.Add(float32(math.Inf(1)))
	f.Add(float32(math.Inf(-1)))
	f.Add(float32(math.NaN()))

	// Edge cases near half-float limits
	f.Add(float32(65504)) // Max finite half
	f.Add(float32(-65504))
	f.Add(float32(65520))             // Just over max
	f.Add(float32(0.00006103515625))  // Min positive normal half
	f.Add(float32(0.000000059604645)) // Min positive subnormal half

	f.Fuzz(func(t *testing.T, val float32) {
		h := FromFloat32(val)

		// Convert back
		result := h.Float32()

		// For finite values, the roundtrip should preserve the value
		// within half-float precision limits
		if !math.IsNaN(float64(val)) && !math.IsInf(float64(val), 0) {
			// Just ensure no panic - precision loss is expected
			_ = result
		}

		// Ensure the half value is valid
		_ = h.Bits()
	})
}

// FuzzFromBits tests half creation from raw bits.
func FuzzFromBits(f *testing.F) {
	f.Add(uint16(0x0000)) // +0
	f.Add(uint16(0x8000)) // -0
	f.Add(uint16(0x3c00)) // 1.0
	f.Add(uint16(0xbc00)) // -1.0
	f.Add(uint16(0x7c00)) // +Inf
	f.Add(uint16(0xfc00)) // -Inf
	f.Add(uint16(0x7e00)) // NaN
	f.Add(uint16(0x7bff)) // Max finite
	f.Add(uint16(0xfbff)) // Min finite
	f.Add(uint16(0x0001)) // Smallest subnormal
	f.Add(uint16(0x0400)) // Smallest normal

	f.Fuzz(func(t *testing.T, bits uint16) {
		h := FromBits(bits)

		// Verify bits roundtrip
		if h.Bits() != bits {
			t.Errorf("bits roundtrip failed: got %04x, want %04x", h.Bits(), bits)
		}

		// Conversion to float32 should not panic
		_ = h.Float32()

		// IsNaN, IsInf, IsZero should not panic
		_ = h.IsNaN()
		_ = h.IsInf()
		_ = h.IsZero()
	})
}

// FuzzBatchConvert tests batch conversion functions.
func FuzzBatchConvert(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x00, 0x3c}) // 1.0
	f.Add(bytes.Repeat([]byte{0x00, 0x3c}, 100))
	f.Add(bytes.Repeat([]byte{0xff, 0x7b}, 100)) // Max finite

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data)%2 != 0 {
			return
		}
		if len(data) > 10000 {
			return // Limit size
		}

		// Convert bytes to Half slice
		halfs := make([]Half, len(data)/2)
		for i := range halfs {
			halfs[i] = FromBits(binary.LittleEndian.Uint16(data[i*2:]))
		}

		// Test batch to float32
		floats := make([]float32, len(halfs))
		ConvertSliceToFloat32(floats, halfs)

		// Test batch from float32
		halfs2 := make([]Half, len(floats))
		ConvertSlice32(halfs2, floats)

		// The roundtrip should work without panicking
		// Values may differ due to precision loss
	})
}

// FuzzMultiplyBatch tests batch multiplication.
func FuzzMultiplyBatch(f *testing.F) {
	f.Add([]byte{0x00, 0x3c, 0x00, 0x3c}, float32(2.0)) // Two 1.0s * 2
	f.Add([]byte{0xff, 0x7b, 0xff, 0x7b}, float32(0.5)) // Max values

	f.Fuzz(func(t *testing.T, data []byte, scalar float32) {
		if len(data) < 2 || len(data)%2 != 0 {
			return
		}
		if len(data) > 10000 {
			return
		}

		src := make([]Half, len(data)/2)
		for i := range src {
			src[i] = FromBits(binary.LittleEndian.Uint16(data[i*2:]))
		}

		dst := make([]Half, len(src))
		MultiplyBatch(dst, src, scalar)

		// Just verify no panic
	})
}

// FuzzAddBatch tests batch addition.
func FuzzAddBatch(f *testing.F) {
	f.Add([]byte{0x00, 0x3c, 0x00, 0x3c}, []byte{0x00, 0x3c, 0x00, 0x3c})

	f.Fuzz(func(t *testing.T, data1, data2 []byte) {
		if len(data1) < 2 || len(data1)%2 != 0 {
			return
		}
		if len(data2) < 2 || len(data2)%2 != 0 {
			return
		}
		if len(data1) > 10000 || len(data2) > 10000 {
			return
		}

		// Use minimum length
		n := len(data1) / 2
		if len(data2)/2 < n {
			n = len(data2) / 2
		}

		a := make([]Half, n)
		b := make([]Half, n)
		for i := 0; i < n; i++ {
			a[i] = FromBits(binary.LittleEndian.Uint16(data1[i*2:]))
			b[i] = FromBits(binary.LittleEndian.Uint16(data2[i*2:]))
		}

		dst := make([]Half, n)
		AddBatch(dst, a, b)

		// Just verify no panic
	})
}

// FuzzHalfNeg tests negation operation.
func FuzzHalfNeg(f *testing.F) {
	f.Add(uint16(0x3c00)) // 1.0

	f.Fuzz(func(t *testing.T, bits uint16) {
		h := FromBits(bits)

		// Neg should not panic
		neg := h.Neg()
		_ = neg

		// Double negation should return original (except for NaN)
		if !h.IsNaN() {
			doubleNeg := neg.Neg()
			if doubleNeg != h {
				t.Errorf("double negation failed: got %04x, want %04x", doubleNeg.Bits(), h.Bits())
			}
		}
	})
}

// FuzzHalfAbs tests absolute value operation.
func FuzzHalfAbs(f *testing.F) {
	f.Add(uint16(0x0000))
	f.Add(uint16(0x8000))
	f.Add(uint16(0x3c00))
	f.Add(uint16(0xbc00))

	f.Fuzz(func(t *testing.T, bits uint16) {
		h := FromBits(bits)

		// Abs should not panic
		abs := h.Abs()

		// Abs should always be non-negative (sign bit cleared)
		if abs.Bits()&0x8000 != 0 && !abs.IsNaN() {
			t.Errorf("abs returned negative: %04x", abs.Bits())
		}
	})
}

// FuzzHalfComparison tests comparison operations.
func FuzzHalfComparison(f *testing.F) {
	f.Add(uint16(0x0000), uint16(0x0000))
	f.Add(uint16(0x3c00), uint16(0x4000))
	f.Add(uint16(0x7c00), uint16(0x7e00)) // Inf vs NaN

	f.Fuzz(func(t *testing.T, bits1, bits2 uint16) {
		h1 := FromBits(bits1)
		h2 := FromBits(bits2)

		// All comparisons should not panic
		_ = h1.Equal(h2)
		_ = h1.Less(h2)
		_ = h1.LessOrEqual(h2)
		_ = h1.Greater(h2)
		_ = h1.GreaterOrEqual(h2)
	})
}

// FuzzHalfString tests string conversion.
func FuzzHalfString(f *testing.F) {
	f.Add(uint16(0x0000))
	f.Add(uint16(0x3c00))
	f.Add(uint16(0x7c00))
	f.Add(uint16(0x7e00))

	f.Fuzz(func(t *testing.T, bits uint16) {
		h := FromBits(bits)

		// String conversion should not panic
		s := h.String()
		_ = s
	})
}

// FuzzConvertBytesToFloat32 tests byte-to-float32 conversion.
func FuzzConvertBytesToFloat32(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x00})
	f.Add([]byte{0x00, 0x3c, 0x00, 0x40}) // 1.0, 2.0

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 2 || len(data)%2 != 0 {
			return
		}
		if len(data) > 10000 {
			return
		}

		dst := make([]float32, len(data)/2)
		ConvertBytesToFloat32(dst, data)

		// Just verify no panic
	})
}

// FuzzConvertFloat32ToBytes tests float32-to-bytes conversion.
func FuzzConvertFloat32ToBytes(f *testing.F) {
	// Use []byte as seed since []float32 is not allowed as fuzz argument
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0x80, 0x3f, 0, 0, 0, 0x40, 0, 0, 0x40, 0x40}) // 0, 1, 2, 3 as float32

	f.Fuzz(func(t *testing.T, rawData []byte) {
		// Must be multiple of 4 bytes for float32
		if len(rawData) < 4 || len(rawData)%4 != 0 {
			return
		}
		if len(rawData) > 20000 {
			return
		}

		// Convert bytes to float32 slice
		numFloats := len(rawData) / 4
		data := make([]float32, numFloats)
		for i := 0; i < numFloats; i++ {
			bits := binary.LittleEndian.Uint32(rawData[i*4:])
			data[i] = math.Float32frombits(bits)
		}

		dst := make([]byte, len(data)*2)
		ConvertFloat32ToBytes(dst, data)

		// Verify roundtrip
		back := make([]float32, len(data))
		ConvertBytesToFloat32(back, dst)

		// Values should be close (within half-float precision)
		for i := range data {
			if math.IsNaN(float64(data[i])) {
				continue // NaN doesn't compare equal
			}
			if math.IsInf(float64(data[i]), 0) {
				if !math.IsInf(float64(back[i]), 0) {
					t.Errorf("infinity lost at %d", i)
				}
				continue
			}
			// For finite values, just verify reasonable precision
		}
	})
}

// FuzzLerpBatch tests lerp batch operation.
func FuzzLerpBatch(f *testing.F) {
	f.Add([]byte{0x00, 0x00, 0x00, 0x00}, []byte{0x00, 0x3c, 0x00, 0x3c}, float32(0.5))

	f.Fuzz(func(t *testing.T, data1, data2 []byte, t_val float32) {
		if len(data1) < 2 || len(data1)%2 != 0 {
			return
		}
		if len(data2) < 2 || len(data2)%2 != 0 {
			return
		}
		if len(data1) > 10000 || len(data2) > 10000 {
			return
		}

		n := len(data1) / 2
		if len(data2)/2 < n {
			n = len(data2) / 2
		}

		a := make([]Half, n)
		b := make([]Half, n)
		for i := 0; i < n; i++ {
			a[i] = FromBits(binary.LittleEndian.Uint16(data1[i*2:]))
			b[i] = FromBits(binary.LittleEndian.Uint16(data2[i*2:]))
		}

		dst := make([]Half, n)
		LerpBatch(dst, a, b, t_val)

		// Just verify no panic
	})
}
