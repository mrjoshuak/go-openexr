package compression

import (
	"math"
	"testing"
)

// TestRLERunLengthLimitsMatchTheFormat pins the two constants that decide what
// RLE emits, against the reference implementation's own values.
//
// A round trip cannot see either of them: an encoder that starts runs at four
// identical bytes instead of three, or caps a run at 126 instead of 127,
// produces a stream its own decoder reads back perfectly and a shorter or
// longer one than every other implementation writes. Only the bytes say which.
//
// ImfRle.cpp: MIN_RUN_LENGTH 3, MAX_RUN_LENGTH 127.
func TestRLERunLengthLimitsMatchTheFormat(t *testing.T) {
	if rleMinRunLength != 3 {
		t.Errorf("rleMinRunLength = %d, the reference uses 3", rleMinRunLength)
	}
	if rleMaxRunLength != 127 {
		t.Errorf("rleMaxRunLength = %d, the reference uses 127", rleMaxRunLength)
	}

	// Three identical bytes are a run; two are literals. That boundary is
	// exactly what MIN_RUN_LENGTH decides, and it is visible in the output.
	three := RLECompress([]byte{9, 9, 9})
	if len(three) != 2 || three[0] != 2 || three[1] != 9 {
		t.Errorf("three identical bytes encoded as % x, want a run: count 2 then the byte", three)
	}
	two := RLECompress([]byte{9, 9})
	if len(two) == 0 || int8(two[0]) >= 0 {
		t.Errorf("two identical bytes encoded as % x, want a literal run (negative count)", two)
	}

	// A run longer than the maximum has to split, and the first control byte
	// is the maximum rather than the whole length.
	long := make([]byte, 200)
	for i := range long {
		long[i] = 7
	}
	enc := RLECompress(long)
	if len(enc) < 2 || enc[0] != byte(rleMaxRunLength) {
		t.Errorf("a 200-byte run starts with control %d, want %d", enc[0], rleMaxRunLength)
	}

	// And all of it must survive, whatever the split.
	back, err := RLEDecompress(enc, len(long))
	if err != nil {
		t.Fatalf("RLEDecompress: %v", err)
	}
	if len(back) != len(long) {
		t.Fatalf("decompressed %d bytes, compressed %d", len(back), len(long))
	}
	for i := range long {
		if back[i] != long[i] {
			t.Fatalf("byte %d differs after a split run", i)
		}
	}
}

// TestPXR24KeepsTwentyFourBits pins what PXR24 is: a float with 24 of its 32
// bits kept, the low 8 of the mantissa discarded.
//
// The name is the specification. An implementation that shifted by 7 or 9
// would round-trip through itself — the error is deterministic and the decoder
// undoes exactly what the encoder did — and would disagree with every other
// implementation about the third significant digit.
func TestPXR24KeepsTwentyFourBits(t *testing.T) {
	for _, f := range []float32{
		0, 1, -1, 0.5, 3.14159265, 1e-8, 1e8, -2.7182818,
	} {
		got := floatToFloat24(f)
		if got > 0xFFFFFF {
			t.Errorf("floatToFloat24(%v) = %#x, which does not fit in 24 bits", f, got)
		}
		// The value is the top 24 bits of the float, so shifting it back up by
		// eight and reading it as a float must land within one unit of the
		// discarded low byte.
		back := math.Float32frombits(got << 8)
		if f == 0 {
			if back != 0 {
				t.Errorf("zero became %v", back)
			}
			continue
		}
		rel := math.Abs(float64(back-f) / float64(f))
		// Dropping 8 of 24 mantissa bits bounds the relative error at 2^-16.
		if rel > 1.0/65536 {
			t.Errorf("floatToFloat24(%v) reconstructs as %v, relative error %g exceeds 2^-16; "+
				"that is not 24 bits of the original", f, back, rel)
		}
	}
}
