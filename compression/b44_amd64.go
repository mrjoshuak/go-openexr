//go:build amd64

package compression

// toOrderedSIMD converts 16 half-float values from sign-magnitude to ordered representation.
// Uses SSE2 SIMD instructions to process 8 values at a time.
// The ordered representation makes comparison operations work correctly:
// - NaN/Inf (exponent all 1s) -> 0x8000
// - Negative values -> bitwise NOT of original
// - Positive values -> original with high bit set
//
//go:noescape
func toOrderedSIMD(dst, src *[16]uint16)

// findMaxSIMD finds the maximum value among 16 uint16 values.
// Uses SSE2 PMAXUW for horizontal reduction.
//
//go:noescape
func findMaxSIMD(src *[16]uint16) uint16

// fromOrderedSIMD converts 16 values from ordered back to sign-magnitude representation.
// This is the inverse of toOrderedSIMD.
//
//go:noescape
func fromOrderedSIMD(dst, src *[16]uint16)

// shiftRoundSIMD computes d[i] = shiftAndRound(tMax - t[i], shift) for 16 values.
//
// This is scalar Go, not the SSE2 routine that used to live in
// b44_pack_amd64.s. That routine computed the whole expression in 16-bit lanes
// (PSUBW, PADDW, PSRLW), so `(tMax - t[i]) << 1` wrapped at 65536 whenever the
// difference exceeded 32767 — which happens for any block spanning a wide
// enough range once the ordered transform has flipped the negatives. The
// reference packer in ImfB44Compressor.cpp does this arithmetic in int, so the
// vectorised path silently produced non-conforming B44 for those blocks, on
// amd64 only. Nothing caught it because the conformance gate ran on arm64,
// where this has always been scalar.
//
// Restoring the vector form means widening to 32-bit lanes and verifying it on
// amd64 hardware; see ROADMAP.md. Correct and slower beats fast and wrong.
//
//go:nosplit
func shiftRoundSIMD(d *[16]uint16, t *[16]uint16, tMax uint16, shift uint) {
	a := (1 << shift) - 1
	shiftP1 := shift + 1
	tMaxInt := int(tMax)

	// Unrolled loop for better inlining
	var x int
	x = (tMaxInt - int(t[0])) << 1
	d[0] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[1])) << 1
	d[1] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[2])) << 1
	d[2] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[3])) << 1
	d[3] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[4])) << 1
	d[4] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[5])) << 1
	d[5] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[6])) << 1
	d[6] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[7])) << 1
	d[7] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[8])) << 1
	d[8] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[9])) << 1
	d[9] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[10])) << 1
	d[10] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[11])) << 1
	d[11] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[12])) << 1
	d[12] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[13])) << 1
	d[13] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[14])) << 1
	d[14] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
	x = (tMaxInt - int(t[15])) << 1
	d[15] = uint16((x + a + ((x >> shiftP1) & 1)) >> shiftP1)
}
