// Package predictor implements the horizontal differencing predictor
// used by OpenEXR compression algorithms.
//
// The predictor converts absolute pixel values to differences from
// the previous value, which tends to produce more compressible data
// for images with local coherence.
//
// # Specification
//
// The encoding matches OpenEXR's ImfZipCompressor.cpp exactly, including its
// +128 bias. The reference encoder is:
//
//	int d = int(t[0]) - p + (128 + 256);   // p is the *original* previous byte
//	p = t[0];
//	t[0] = d;                              // truncated to 8 bits
//
// and the reference decoder is:
//
//	int d = int(t[-1]) + int(t[0]) - 128;
//	t[0] = d;                              // truncated to 8 bits
//
// The bias is not optional. Omitting it on both sides still round-trips
// perfectly, but produces a byte stream that no conforming OpenEXR reader can
// decode, and misdecodes every conforming file. Because +384 and -128 are
// congruent to +128 and -128 modulo 256, the arithmetic below adds or
// subtracts 128 directly.
package predictor

// biasEncode applies the reference +128 bias to a delta byte.
//
// Adding 128 modulo 256 is exactly a flip of the high bit, so this is written
// as an XOR: for any byte b, (b+128) mod 256 == b^0x80. The XOR form is used
// because it lets the SIMD decode path correct a plain prefix sum without
// changing the assembly (see DecodeSIMD).
const bias = 0x80

// Encode applies horizontal differencing to the data in place.
// The first byte remains unchanged, subsequent bytes become the biased
// difference from their predecessor, matching OpenEXR's ImfZipCompressor.
//
// This is used before compression to improve compression ratios.
func Encode(data []byte) {
	n := len(data)
	if n < 2 {
		return
	}

	// Work backwards so each step still reads the original predecessor.
	// Process in chunks of 8 for better pipelining.
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

	// Handle remaining bytes
	for ; i >= 1; i-- {
		data[i] = data[i] - data[i-1] + bias
	}
}

// Decode reverses horizontal differencing in place, undoing the +128 bias
// applied by Encode.
//
// This is used after decompression to restore the original values.
func Decode(data []byte) {
	n := len(data)
	if n < 2 {
		return
	}

	// Process in chunks of 8 for better pipelining
	i := 1
	for ; i+7 < n; i += 8 {
		data[i] = data[i] + data[i-1] - bias
		data[i+1] = data[i+1] + data[i] - bias
		data[i+2] = data[i+2] + data[i+1] - bias
		data[i+3] = data[i+3] + data[i+2] - bias
		data[i+4] = data[i+4] + data[i+3] - bias
		data[i+5] = data[i+5] + data[i+4] - bias
		data[i+6] = data[i+6] + data[i+5] - bias
		data[i+7] = data[i+7] + data[i+6] - bias
	}

	// Handle remaining bytes
	for ; i < n; i++ {
		data[i] = data[i] + data[i-1] - bias
	}
}

// EncodeRow applies horizontal differencing to a single scanline,
// treating it as interleaved channel data.
//
// For OpenEXR, the predictor operates on individual bytes within
// each channel's data, not across channels.
func EncodeRow(data []byte, width, numChannels, bytesPerPixel int) {
	if width == 0 || numChannels == 0 || bytesPerPixel == 0 {
		return
	}

	// OpenEXR applies predictor to the interleaved byte stream
	// Each byte is predicted from the previous byte
	Encode(data)
}

// DecodeRow reverses horizontal differencing for a scanline.
func DecodeRow(data []byte, width, numChannels, bytesPerPixel int) {
	if width == 0 || numChannels == 0 || bytesPerPixel == 0 {
		return
	}

	Decode(data)
}
