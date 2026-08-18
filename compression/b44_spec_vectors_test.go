package compression

import (
	"math/rand"
	"testing"
)

// The B44 tests in this package were proven unable to fail. Mutation testing
// (scripts/mutation/run.py, id b44-bias) moved the 0x20 difference bias in
// packB44 and unpack14 together — the value that every packed byte of a B44
// block is expressed relative to — and TestPackUnpack14, TestB44RoundtripSimple,
// TestB44RoundtripVaried, TestB44HalfOnlyPreservesValues and
// TestB44CompressionDeterminism all stayed green, because each of them either
// decodes what it just encoded or compares this library against itself. The
// same run showed TestPackUnpack14 also survives a broken unpack14 chain
// (s[8] reconstructed from s[0] instead of s[4]): its 5% tolerance is wide
// enough to swallow it.
//
// This file pins the packed bytes themselves: once against a block whose
// 14 bytes are derived by hand below, and once against a transcription of
// ImfB44Compressor.cpp that shares no code with the implementation.

// refShiftAndRound is ImfB44Compressor.cpp's shiftAndRound, transcribed:
//
//	x <<= 1; a = (1 << shift) - 1; shift += 1; b = (x >> shift) & 1;
//	return (x + a + b) >> shift;
func refShiftAndRound(x int, shift uint) int {
	x <<= 1
	a := (1 << shift) - 1
	shift++
	b := (x >> shift) & 1
	return (x + a + b) >> shift
}

// refPackB44 is a transcription of ImfB44Compressor.cpp pack() with
// optFlatFields false. It deliberately reimplements the sign-magnitude to
// ordered conversion, the maximum search and the shift search rather than
// calling this package's SIMD helpers, so a defect shared by the encoder and
// the decoder cannot hide in it.
func refPackB44(s [16]uint16) [14]byte {
	// t[i]: infinities and NaNs collapse to 0x8000; negatives are inverted;
	// positives get their high bit set. The result orders as the half values do.
	var t [16]uint16
	for i := 0; i < 16; i++ {
		switch {
		case s[i]&0x7c00 == 0x7c00:
			t[i] = 0x8000
		case s[i]&0x8000 != 0:
			t[i] = ^s[i]
		default:
			t[i] = s[i] | 0x8000
		}
	}

	tMax := uint16(0)
	for i := 0; i < 16; i++ {
		if tMax < t[i] {
			tMax = t[i]
		}
	}

	const bias = 0x20 // ImfB44Compressor.cpp pack(): const int bias = 0x20

	var d [16]int
	var r [15]int
	var rMin, rMax int
	shift := -1
	for {
		shift++
		for i := 0; i < 16; i++ {
			d[i] = refShiftAndRound(int(tMax)-int(t[i]), uint(shift))
		}

		// The running differences: down column 0, then across each row.
		r[0] = d[0] - d[4] + bias
		r[1] = d[4] - d[8] + bias
		r[2] = d[8] - d[12] + bias
		r[3] = d[0] - d[1] + bias
		r[4] = d[4] - d[5] + bias
		r[5] = d[8] - d[9] + bias
		r[6] = d[12] - d[13] + bias
		r[7] = d[1] - d[2] + bias
		r[8] = d[5] - d[6] + bias
		r[9] = d[9] - d[10] + bias
		r[10] = d[13] - d[14] + bias
		r[11] = d[2] - d[3] + bias
		r[12] = d[6] - d[7] + bias
		r[13] = d[10] - d[11] + bias
		r[14] = d[14] - d[15] + bias

		rMin, rMax = r[0], r[0]
		for i := 1; i < 15; i++ {
			if r[i] < rMin {
				rMin = r[i]
			}
			if r[i] > rMax {
				rMax = r[i]
			}
		}

		if rMin >= 0 && rMax <= 0x3f {
			break
		}
	}

	// exactMax: the block's largest value is written back exactly.
	t0 := tMax - uint16(uint(d[0])<<uint(shift))

	// 6-bit fields, most significant bit first, after a 16-bit base value and
	// a 6-bit shift.
	var b [14]byte
	b[0] = byte(t0 >> 8)
	b[1] = byte(t0)
	b[2] = byte((shift << 2) | (r[0] >> 4))
	b[3] = byte((r[0] << 4) | (r[1] >> 2))
	b[4] = byte((r[1] << 6) | r[2])
	b[5] = byte((r[3] << 2) | (r[4] >> 4))
	b[6] = byte((r[4] << 4) | (r[5] >> 2))
	b[7] = byte((r[5] << 6) | r[6])
	b[8] = byte((r[7] << 2) | (r[8] >> 4))
	b[9] = byte((r[8] << 4) | (r[9] >> 2))
	b[10] = byte((r[9] << 6) | r[10])
	b[11] = byte((r[11] << 2) | (r[12] >> 4))
	b[12] = byte((r[12] << 4) | (r[13] >> 2))
	b[13] = byte((r[13] << 6) | r[14])
	return b
}

// refUnpack14 is a transcription of ImfB44Compressor.cpp unpack14().
func refUnpack14(b []byte) [16]uint16 {
	var s [16]uint16
	s[0] = uint16(b[0])<<8 | uint16(b[1])

	shift := uint16(b[2] >> 2)
	bias := uint16(0x20) << shift

	field := func(hi, lo byte, hiShift, loShift uint) uint16 {
		return ((uint16(hi) << hiShift) | (uint16(lo) >> loShift)) & 0x3f
	}
	add := func(prev, code uint16) uint16 { return prev + (code << shift) - bias }

	s[4] = add(s[0], field(b[2], b[3], 4, 4))
	s[8] = add(s[4], field(b[3], b[4], 2, 6))
	s[12] = add(s[8], uint16(b[4])&0x3f)

	s[1] = add(s[0], uint16(b[5])>>2)
	s[5] = add(s[4], field(b[5], b[6], 4, 4))
	s[9] = add(s[8], field(b[6], b[7], 2, 6))
	s[13] = add(s[12], uint16(b[7])&0x3f)

	s[2] = add(s[1], uint16(b[8])>>2)
	s[6] = add(s[5], field(b[8], b[9], 4, 4))
	s[10] = add(s[9], field(b[9], b[10], 2, 6))
	s[14] = add(s[13], uint16(b[10])&0x3f)

	s[3] = add(s[2], uint16(b[11])>>2)
	s[7] = add(s[6], field(b[11], b[12], 4, 4))
	s[11] = add(s[10], field(b[12], b[13], 2, 6))
	s[15] = add(s[14], uint16(b[13])&0x3f)

	for i := 0; i < 16; i++ {
		if s[i]&0x8000 != 0 {
			s[i] &= 0x7fff
		} else {
			s[i] = ^s[i]
		}
	}
	return s
}

// TestPackB44MatchesSpecVector pins one block's 14 packed bytes to a value
// derived by hand.
//
// Every sample is 1.0 = 0x3c00. All sixteen are positive, so the ordered form
// is t[i] = 0x3c00 | 0x8000 = 0xbc00 and tMax = 0xbc00. With shift 0 every
// d[i] = shiftAndRound(0, 0) = 0, so all fifteen running differences are the
// bias 0x20 = 32, which is inside [0, 0x3f] at the first attempt. exactMax
// writes t[0] = tMax - (d[0] << 0) = 0xbc00. Packing the fields
// shift = 0 and r[0..14] = 32:
//
//	b[0..1] = 0xbc 0x00                        the base value
//	b[2]    = (0 << 2) | (32 >> 4)      = 0x02  shift and the top 2 bits of r0
//	b[3]    = (32 << 4) | (32 >> 2)     = 0x08
//	b[4]    = (32 << 6) | 32            = 0x20
//	b[5]    = (32 << 2) | (32 >> 4)     = 0x82
//	b[6..13] repeat 0x08 0x20 0x82 ...
func TestPackB44MatchesSpecVector(t *testing.T) {
	var block [16]uint16
	for i := range block {
		block[i] = 0x3c00 // 1.0
	}
	want := []byte{0xbc, 0x00, 0x02, 0x08, 0x20, 0x82, 0x08, 0x20, 0x82, 0x08, 0x20, 0x82, 0x08, 0x20}

	var got [14]byte
	n := packB44(block, got[:], false, true)
	if n != 14 {
		t.Fatalf("packB44 wrote %d bytes, want 14", n)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("packB44 = % x, want % x", got, want)
		}
	}

	// And the read direction, from the same hand-derived bytes.
	var s [16]uint16
	unpack14(want, &s)
	for i := range s {
		if s[i] != 0x3c00 {
			t.Fatalf("unpack14(% x)[%d] = 0x%04x, want 0x3c00", want, i, s[i])
		}
	}
}

// TestPackB44MatchesReferenceTranscription checks packB44 against a
// transcription of the reference packer over blocks it did not produce.
func TestPackB44MatchesReferenceTranscription(t *testing.T) {
	r := rand.New(rand.NewSource(20260818))

	blocks := make([][16]uint16, 0, 64)
	// Constant, ramp, and two-binade blocks, then random halves.
	var b [16]uint16
	for i := range b {
		b[i] = 0x3555
	}
	blocks = append(blocks, b)
	for i := range b {
		b[i] = uint16(0x3c00 + i*7)
	}
	blocks = append(blocks, b)
	for i := range b {
		if i%2 == 0 {
			b[i] = uint16(0x2400 + i)
		} else {
			b[i] = uint16(0xb000 + i) // negative halves
		}
	}
	blocks = append(blocks, b)
	for n := 0; n < 60; n++ {
		for i := range b {
			b[i] = uint16(r.Intn(0x7c00)) | uint16(r.Intn(2))<<15
		}
		blocks = append(blocks, b)
	}

	for bi, block := range blocks {
		want := refPackB44(block)
		var got [14]byte
		if n := packB44(block, got[:], false, true); n != 14 {
			t.Fatalf("block %d: packB44 wrote %d bytes, want 14", bi, n)
		}
		if got != want {
			t.Fatalf("block %d: packB44 = % x, reference packer gives % x", bi, got, want)
		}
	}
}

// TestUnpack14MatchesReferenceTranscription checks unpack14 against a
// transcription of the reference unpacker over byte blocks the encoder never
// produced, which is what a round-trip test cannot do.
func TestUnpack14MatchesReferenceTranscription(t *testing.T) {
	r := rand.New(rand.NewSource(818))

	for n := 0; n < 200; n++ {
		var b [14]byte
		for i := range b {
			b[i] = byte(r.Intn(256))
		}
		if n < 8 {
			b[2] = byte(n << 2) // sweep the shift field through 0..7
		}

		want := refUnpack14(b[:])
		var got [16]uint16
		unpack14(b[:], &got)
		if got != want {
			t.Fatalf("unpack14(% x):\n got %v\nwant %v", b, got, want)
		}
	}
}
