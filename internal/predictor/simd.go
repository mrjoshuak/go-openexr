package predictor

import (
	"unsafe"
)

// DecodeSIMD performs predictor decode using SIMD assembly when available.
// On amd64 and arm64, this uses SSE2/NEON instructions for parallel prefix sum.
// On other platforms, it falls back to loop-unrolled pure Go.
//
// The assembly kernels compute an unbiased prefix sum, so the OpenEXR -128
// predictor bias is folded in afterwards by applyDecodeBias. See that function
// for why this is exact.
func DecodeSIMD(data []byte) {
	if len(data) < 2 {
		return
	}
	// Use assembly implementation (falls back to pure Go on unsupported platforms)
	decodeASM(data)
	applyDecodeBias(data)
}

// applyDecodeBias converts an unbiased prefix sum into OpenEXR's biased
// predictor decode, in place.
//
// The reference decoder is d[i] = d[i-1] + e[i] - 128. Expanding the
// recurrence gives
//
//	d[i] = d[0] + sum(e[1..i]) - 128*i = prefixSum[i] - 128*i
//
// so the only difference from a plain prefix sum is a per-index offset of
// -128*i. Modulo 256 that offset is 0 for even i and 128 for odd i, and
// adding or subtracting 128 modulo 256 is a flip of the high bit. Correcting
// a plain prefix sum therefore reduces to XOR-ing 0x80 into every
// odd-indexed byte, which is exact for all inputs and lets the hand-written
// SSE2/NEON prefix-sum kernels stay untouched.
func applyDecodeBias(data []byte) {
	for i := 1; i < len(data); i += 2 {
		data[i] ^= bias
	}
}

// DecodeSIMDWithOffset performs predictor decode starting from an offset value.
// Useful when processing chunks where the first byte should be adjusted.
func DecodeSIMDWithOffset(data []byte, offset byte) {
	if len(data) == 0 {
		return
	}
	data[0] = data[0] + offset
	if len(data) == 1 {
		return
	}
	DecodeSIMD(data)
}

// ReconstructBytes combines predictor decode and deinterleave for ZIP
// decompression. It is the exact inverse of DeconstructBytes.
//
// The OpenEXR pipeline (ImfZipCompressor::uncompress) is: inflate, then undo
// the predictor, then undo the byte reordering. The order matters — the
// predictor runs over the *reordered* stream, so undoing the reordering first
// would apply the differencing across the wrong byte pairs. Doing both steps
// in the wrong order is self-consistent with a matching compressor and so
// still round-trips, but produces and expects a byte stream no conforming
// OpenEXR implementation agrees with.
//
// source contains the inflated data as [even bytes | odd bytes] and IS
// MODIFIED IN PLACE by the predictor pass. out receives the reconstructed
// bytes [e0, o0, e1, o1, ...].
func ReconstructBytes(out, source []byte) {
	n := len(source)
	if n == 0 {
		return
	}
	if len(out) < n {
		panic("output buffer too small")
	}

	// Step 1: Undo the predictor, in place, on the still-reordered stream.
	DecodeSIMD(source)

	// Step 2: Undo the byte reordering from source into output
	// Source format: [even bytes | odd bytes] -> Output format: [e0, o0, e1, o1, ...]
	half := (n + 1) / 2

	// Process 8 bytes at a time for deinterleave
	i := 0
	for ; i+8 <= half && (i+8)*2 <= n; i += 8 {
		// Load 8 bytes from first half (even bytes)
		v1 := *(*uint64)(unsafe.Pointer(&source[i]))
		// Load 8 bytes from second half (odd bytes)
		v2 := *(*uint64)(unsafe.Pointer(&source[half+i]))

		// Interleave: produce 16 bytes of output
		// out[0,2,4,6,8,10,12,14] = v1[0..7]
		// out[1,3,5,7,9,11,13,15] = v2[0..7]
		lo, hi := interleaveBytes(v1, v2)

		*(*uint64)(unsafe.Pointer(&out[i*2])) = lo
		*(*uint64)(unsafe.Pointer(&out[i*2+8])) = hi
	}

	// Handle remaining bytes
	for ; i < half; i++ {
		out[i*2] = source[i]
		if half+i < n {
			out[i*2+1] = source[half+i]
		}
	}
}

// interleaveBytes takes two uint64 values and interleaves their bytes.
// Input:  a = [a0,a1,a2,a3,a4,a5,a6,a7], b = [b0,b1,b2,b3,b4,b5,b6,b7]
// Output: lo = [a0,b0,a1,b1,a2,b2,a3,b3], hi = [a4,b4,a5,b5,a6,b6,a7,b7]
func interleaveBytes(a, b uint64) (lo, hi uint64) {
	// Extract and interleave lower 4 bytes
	a0, a1, a2, a3 := byte(a), byte(a>>8), byte(a>>16), byte(a>>24)
	b0, b1, b2, b3 := byte(b), byte(b>>8), byte(b>>16), byte(b>>24)
	lo = uint64(a0) | uint64(b0)<<8 | uint64(a1)<<16 | uint64(b1)<<24 |
		uint64(a2)<<32 | uint64(b2)<<40 | uint64(a3)<<48 | uint64(b3)<<56

	// Extract and interleave upper 4 bytes
	a4, a5, a6, a7 := byte(a>>32), byte(a>>40), byte(a>>48), byte(a>>56)
	b4, b5, b6, b7 := byte(b>>32), byte(b>>40), byte(b>>48), byte(b>>56)
	hi = uint64(a4) | uint64(b4)<<8 | uint64(a5)<<16 | uint64(b5)<<24 |
		uint64(a6)<<32 | uint64(b6)<<40 | uint64(a7)<<48 | uint64(b7)<<56

	return
}

// DeconstructBytes performs the inverse of ReconstructBytes.
// It reorders the source bytes into [even | odd] halves and then applies the
// predictor to the reordered stream, matching ImfZipCompressor::compress and
// the C++ internal_zip_deconstruct_bytes function.
//
// The reorder must happen before the predictor; see ReconstructBytes.
func DeconstructBytes(scratch, source []byte) {
	n := len(source)
	if n == 0 {
		return
	}
	if len(scratch) < n {
		panic("scratch buffer too small")
	}

	half := (n + 1) / 2

	// Step 1: Deinterleave (split even/odd bytes)
	// Process 16 bytes at a time -> 8 to each half
	i := 0
	for ; i+16 <= n; i += 16 {
		lo := *(*uint64)(unsafe.Pointer(&source[i]))
		hi := *(*uint64)(unsafe.Pointer(&source[i+8]))

		even, odd := deinterleaveBytes(lo, hi)

		*(*uint64)(unsafe.Pointer(&scratch[i/2])) = even
		*(*uint64)(unsafe.Pointer(&scratch[half+i/2])) = odd
	}

	// Handle remaining bytes
	for ; i < n; i++ {
		if i%2 == 0 {
			scratch[i/2] = source[i]
		} else {
			scratch[half+i/2] = source[i]
		}
	}

	// Step 2: Predictor encode
	EncodeSIMD(scratch[:n])
}

// deinterleaveBytes takes 16 bytes (as two uint64) and separates even/odd.
// Input: lo = [e0,o0,e1,o1,e2,o2,e3,o3], hi = [e4,o4,e5,o5,e6,o6,e7,o7]
// Output: even = [e0,e1,e2,e3,e4,e5,e6,e7], odd = [o0,o1,o2,o3,o4,o5,o6,o7]
func deinterleaveBytes(lo, hi uint64) (even, odd uint64) {
	// Extract even bytes (indices 0, 2, 4, 6)
	e0 := lo & 0xFF
	e1 := (lo >> 16) & 0xFF
	e2 := (lo >> 32) & 0xFF
	e3 := (lo >> 48) & 0xFF
	e4 := hi & 0xFF
	e5 := (hi >> 16) & 0xFF
	e6 := (hi >> 32) & 0xFF
	e7 := (hi >> 48) & 0xFF

	// Extract odd bytes (indices 1, 3, 5, 7)
	o0 := (lo >> 8) & 0xFF
	o1 := (lo >> 24) & 0xFF
	o2 := (lo >> 40) & 0xFF
	o3 := (lo >> 56) & 0xFF
	o4 := (hi >> 8) & 0xFF
	o5 := (hi >> 24) & 0xFF
	o6 := (hi >> 40) & 0xFF
	o7 := (hi >> 56) & 0xFF

	even = e0 | (e1 << 8) | (e2 << 16) | (e3 << 24) | (e4 << 32) | (e5 << 40) | (e6 << 48) | (e7 << 56)
	odd = o0 | (o1 << 8) | (o2 << 16) | (o3 << 24) | (o4 << 32) | (o5 << 40) | (o6 << 48) | (o7 << 56)
	return
}

// EncodeSIMD performs predictor encode.
// Note: Encoding is inherently sequential (each diff depends on previous value),
// so SIMD optimization is limited. We use loop unrolling instead.
func EncodeSIMD(data []byte) {
	n := len(data)
	if n < 2 {
		return
	}

	// Encode works backwards to avoid overwriting values we need
	i := n - 1
	for ; i >= 8; i -= 8 {
		data[i] = data[i] - data[i-1] + bias
		data[i-1] = data[i-1] - data[i-2] + bias
		data[i-2] = data[i-2] - data[i-3] + bias
		data[i-3] = data[i-3] - data[i-4] + bias
		data[i-4] = data[i-4] - data[i-5] + bias
		data[i-5] = data[i-5] - data[i-6] + bias
		data[i-6] = data[i-6] - data[i-7] + bias
		data[i-7] = data[i-7] - data[i-8] + bias
	}

	for ; i >= 1; i-- {
		data[i] = data[i] - data[i-1] + bias
	}
}
