// Package compression provides batch-optimized transform operations.
//
// This file contains loop-unrolled implementations of wavelet and DCT
// transforms for improved performance on large data sets.
package compression

import "math"

// batchSize is the number of elements processed per unrolled iteration.
const batchSize = 8

// WaveletEncodeRow applies forward wavelet transform to a single row with loop unrolling.
// This is optimized for large data arrays.
func WaveletEncodeRow(data, temp []uint16, n int) {
	if n < 2 {
		return
	}

	p := n
	pEnd := 1
	for p > pEnd {
		p2 := p >> 1

		// Process pairs with loop unrolling for better performance
		a := 0
		c := 0

		// Unrolled loop - process 4 pairs at a time (8 values)
		for c+4 <= p2 {
			// Pair 0
			l0, h0 := wenc14(data[a], data[a+1])
			temp[c] = l0
			temp[c+p2] = h0
			a += 2
			c++

			// Pair 1
			l1, h1 := wenc14(data[a], data[a+1])
			temp[c] = l1
			temp[c+p2] = h1
			a += 2
			c++

			// Pair 2
			l2, h2 := wenc14(data[a], data[a+1])
			temp[c] = l2
			temp[c+p2] = h2
			a += 2
			c++

			// Pair 3
			l3, h3 := wenc14(data[a], data[a+1])
			temp[c] = l3
			temp[c+p2] = h3
			a += 2
			c++
		}

		// Handle remaining pairs
		for c < p2 {
			l, h := wenc14(data[a], data[a+1])
			temp[c] = l
			temp[c+p2] = h
			a += 2
			c++
		}

		// Handle odd length - last element just passes through
		if p&1 != 0 {
			temp[p2+p2] = data[a]
		}

		// Copy back with loop unrolling
		copyBatch16(data[:p], temp[:p])

		p = p2
	}
}

// WaveletDecodeRow applies inverse wavelet transform to a single row with loop unrolling.
func WaveletDecodeRow(data, temp []uint16, n int) {
	if n < 2 {
		return
	}

	// Find the sequence of p values (from smallest to largest)
	var pStack []int
	p := n
	for p > 1 {
		pStack = append(pStack, p)
		p = p >> 1
	}

	// Process in reverse order (smallest to largest)
	for i := len(pStack) - 1; i >= 0; i-- {
		p := pStack[i]
		p2 := p >> 1

		// Decode pairs with loop unrolling
		a := 0
		c := 0

		// Unrolled loop - process 4 pairs at a time
		for c+4 <= p2 {
			// Pair 0
			la0, lb0 := wdec14(data[c], data[c+p2])
			temp[a] = la0
			temp[a+1] = lb0
			a += 2
			c++

			// Pair 1
			la1, lb1 := wdec14(data[c], data[c+p2])
			temp[a] = la1
			temp[a+1] = lb1
			a += 2
			c++

			// Pair 2
			la2, lb2 := wdec14(data[c], data[c+p2])
			temp[a] = la2
			temp[a+1] = lb2
			a += 2
			c++

			// Pair 3
			la3, lb3 := wdec14(data[c], data[c+p2])
			temp[a] = la3
			temp[a+1] = lb3
			a += 2
			c++
		}

		// Handle remaining pairs
		for c < p2 {
			la, lb := wdec14(data[c], data[c+p2])
			temp[a] = la
			temp[a+1] = lb
			a += 2
			c++
		}

		// Handle odd length - last element just passes through
		if p&1 != 0 {
			temp[a] = data[p2+p2]
		}

		// Copy back with loop unrolling
		copyBatch16(data[:p], temp[:p])
	}
}

// WaveletEncode2D applies forward 2D wavelet transform with optimized column handling.
// Uses batch processing for better cache utilization.
func WaveletEncode2D(data []uint16, width, height int) {
	if len(data) == 0 || width == 0 || height == 0 {
		return
	}

	temp := make([]uint16, max(width, height))

	// Transform rows (cache-friendly access pattern)
	for y := 0; y < height; y++ {
		row := data[y*width : (y+1)*width]
		WaveletEncodeRow(row, temp, width)
	}

	// Transform columns (using batched column extraction)
	colBuf := make([]uint16, height)
	for x := 0; x < width; x++ {
		// Extract column
		extractColumn(data, colBuf, x, width, height)
		// Transform
		WaveletEncodeRow(colBuf, temp, height)
		// Store back
		storeColumn(data, colBuf, x, width, height)
	}
}

// WaveletDecode2D applies inverse 2D wavelet transform with optimized column handling.
func WaveletDecode2D(data []uint16, width, height int) {
	if len(data) == 0 || width == 0 || height == 0 {
		return
	}

	temp := make([]uint16, max(width, height))

	// Inverse transform columns first (reverse of encode)
	colBuf := make([]uint16, height)
	for x := 0; x < width; x++ {
		extractColumn(data, colBuf, x, width, height)
		WaveletDecodeRow(colBuf, temp, height)
		storeColumn(data, colBuf, x, width, height)
	}

	// Inverse transform rows
	for y := 0; y < height; y++ {
		row := data[y*width : (y+1)*width]
		WaveletDecodeRow(row, temp, width)
	}
}

// extractColumn extracts a column from a 2D array with loop unrolling.
func extractColumn(data, col []uint16, x, width, height int) {
	y := 0
	// Unrolled loop - process 8 elements at a time
	for ; y+batchSize <= height; y += batchSize {
		col[y] = data[y*width+x]
		col[y+1] = data[(y+1)*width+x]
		col[y+2] = data[(y+2)*width+x]
		col[y+3] = data[(y+3)*width+x]
		col[y+4] = data[(y+4)*width+x]
		col[y+5] = data[(y+5)*width+x]
		col[y+6] = data[(y+6)*width+x]
		col[y+7] = data[(y+7)*width+x]
	}
	// Handle remainder
	for ; y < height; y++ {
		col[y] = data[y*width+x]
	}
}

// storeColumn stores a column back into a 2D array with loop unrolling.
func storeColumn(data, col []uint16, x, width, height int) {
	y := 0
	// Unrolled loop - process 8 elements at a time
	for ; y+batchSize <= height; y += batchSize {
		data[y*width+x] = col[y]
		data[(y+1)*width+x] = col[y+1]
		data[(y+2)*width+x] = col[y+2]
		data[(y+3)*width+x] = col[y+3]
		data[(y+4)*width+x] = col[y+4]
		data[(y+5)*width+x] = col[y+5]
		data[(y+6)*width+x] = col[y+6]
		data[(y+7)*width+x] = col[y+7]
	}
	// Handle remainder
	for ; y < height; y++ {
		data[y*width+x] = col[y]
	}
}

// copyBatch16 copies uint16 slices with loop unrolling.
func copyBatch16(dst, src []uint16) {
	n := len(src)
	if len(dst) < n {
		n = len(dst)
	}

	i := 0
	// Unrolled loop - copy 8 elements at a time
	for ; i+batchSize <= n; i += batchSize {
		dst[i] = src[i]
		dst[i+1] = src[i+1]
		dst[i+2] = src[i+2]
		dst[i+3] = src[i+3]
		dst[i+4] = src[i+4]
		dst[i+5] = src[i+5]
		dst[i+6] = src[i+6]
		dst[i+7] = src[i+7]
	}
	// Handle remainder
	for ; i < n; i++ {
		dst[i] = src[i]
	}
}

// dctCoeff is the orthonormal DCT-II basis:
//
//	C[k][n] = alpha(k) * cos((2n+1) * k * pi / 16),  alpha(0) = 1/sqrt(8),
//	                                                 alpha(k) = sqrt(2/8)
//
// It backs DCT8x8Forward and DCT8x8Inverse below, which are a general-purpose
// pair with exact round-trip properties. No codec in this package uses them:
// DWA's transform is a different one, built from the factored butterfly and
// truncated-pi constants the OpenEXR reference uses, and lives in dwa_dct.go.
// Do not substitute one for the other.
var dctCoeff [8][8]float32

func init() {
	sqrt8 := float32(math.Sqrt(8))
	sqrt2_8 := float32(math.Sqrt(2.0 / 8.0))
	for k := 0; k < 8; k++ {
		for n := 0; n < 8; n++ {
			c := float32(math.Cos(float64(2*n+1) * float64(k) * math.Pi / 16.0))
			if k == 0 {
				dctCoeff[k][n] = c / sqrt8
			} else {
				dctCoeff[k][n] = c * sqrt2_8
			}
		}
	}
}

// DCT8x8Forward performs a forward 8x8 DCT with loop-unrolled matrix operations.
// Uses separable 1D transforms for better cache performance.
func DCT8x8Forward(data *[64]float32) {
	var workspace [64]float32

	// First pass: transform rows (8 rows of 8 values each)
	for row := 0; row < 8; row++ {
		offset := row * 8
		dct1d8Forward(data[offset:offset+8], workspace[offset:offset+8])
	}

	// Transpose workspace to data for column transform
	transpose8x8(&workspace, data)

	// Second pass: transform columns (now in row order after transpose)
	for row := 0; row < 8; row++ {
		offset := row * 8
		dct1d8Forward(data[offset:offset+8], workspace[offset:offset+8])
	}

	// Transpose back
	transpose8x8(&workspace, data)
}

// DCT8x8Inverse performs an inverse 8x8 DCT with loop-unrolled matrix operations.
func DCT8x8Inverse(data *[64]float32) {
	var workspace [64]float32

	// First pass: inverse transform columns
	transpose8x8(data, &workspace)
	for row := 0; row < 8; row++ {
		offset := row * 8
		dct1d8Inverse(workspace[offset:offset+8], data[offset:offset+8])
	}

	// Second pass: inverse transform rows
	transpose8x8(data, &workspace)
	for row := 0; row < 8; row++ {
		offset := row * 8
		dct1d8Inverse(workspace[offset:offset+8], data[offset:offset+8])
	}
}

// dct1d8Forward performs a 1D DCT on 8 elements using direct matrix multiplication.
func dct1d8Forward(input, output []float32) {
	// Using precomputed DCT coefficients
	for k := 0; k < 8; k++ {
		var sum float32
		for n := 0; n < 8; n++ {
			sum += input[n] * dctCoeff[k][n]
		}
		output[k] = sum
	}
}

// dct1d8Inverse performs a 1D inverse DCT on 8 elements.
func dct1d8Inverse(input, output []float32) {
	// IDCT uses transposed coefficients
	for n := 0; n < 8; n++ {
		var sum float32
		for k := 0; k < 8; k++ {
			sum += input[k] * dctCoeff[k][n]
		}
		output[n] = sum
	}
}

// transpose8x8 transposes an 8x8 matrix with loop unrolling.
func transpose8x8(src, dst *[64]float32) {
	// Unrolled transpose for 8x8 matrix
	dst[0] = src[0]
	dst[1] = src[8]
	dst[2] = src[16]
	dst[3] = src[24]
	dst[4] = src[32]
	dst[5] = src[40]
	dst[6] = src[48]
	dst[7] = src[56]

	dst[8] = src[1]
	dst[9] = src[9]
	dst[10] = src[17]
	dst[11] = src[25]
	dst[12] = src[33]
	dst[13] = src[41]
	dst[14] = src[49]
	dst[15] = src[57]

	dst[16] = src[2]
	dst[17] = src[10]
	dst[18] = src[18]
	dst[19] = src[26]
	dst[20] = src[34]
	dst[21] = src[42]
	dst[22] = src[50]
	dst[23] = src[58]

	dst[24] = src[3]
	dst[25] = src[11]
	dst[26] = src[19]
	dst[27] = src[27]
	dst[28] = src[35]
	dst[29] = src[43]
	dst[30] = src[51]
	dst[31] = src[59]

	dst[32] = src[4]
	dst[33] = src[12]
	dst[34] = src[20]
	dst[35] = src[28]
	dst[36] = src[36]
	dst[37] = src[44]
	dst[38] = src[52]
	dst[39] = src[60]

	dst[40] = src[5]
	dst[41] = src[13]
	dst[42] = src[21]
	dst[43] = src[29]
	dst[44] = src[37]
	dst[45] = src[45]
	dst[46] = src[53]
	dst[47] = src[61]

	dst[48] = src[6]
	dst[49] = src[14]
	dst[50] = src[22]
	dst[51] = src[30]
	dst[52] = src[38]
	dst[53] = src[46]
	dst[54] = src[54]
	dst[55] = src[62]

	dst[56] = src[7]
	dst[57] = src[15]
	dst[58] = src[23]
	dst[59] = src[31]
	dst[60] = src[39]
	dst[61] = src[47]
	dst[62] = src[55]
	dst[63] = src[63]
}

// CSC709ForwardBatch performs RGB to YCbCr conversion on multiple 8x8 blocks.
func CSC709ForwardBatch(r, g, b []float32) {
	n := len(r)
	if len(g) < n || len(b) < n {
		return
	}

	i := 0
	// Process 8 pixels at a time
	for ; i+batchSize <= n; i += batchSize {
		for j := 0; j < batchSize; j++ {
			srcR := r[i+j]
			srcG := g[i+j]
			srcB := b[i+j]

			// Y'  = 0.2126 R' + 0.7152 G' + 0.0722 B'
			// Cb  = -0.1146 R' - 0.3854 G' + 0.5000 B'
			// Cr  = 0.5000 R' - 0.4542 G' - 0.0458 B'
			r[i+j] = 0.2126*srcR + 0.7152*srcG + 0.0722*srcB
			g[i+j] = -0.1146*srcR - 0.3854*srcG + 0.5000*srcB
			b[i+j] = 0.5000*srcR - 0.4542*srcG - 0.0458*srcB
		}
	}

	// Handle remainder
	for ; i < n; i++ {
		srcR := r[i]
		srcG := g[i]
		srcB := b[i]
		r[i] = 0.2126*srcR + 0.7152*srcG + 0.0722*srcB
		g[i] = -0.1146*srcR - 0.3854*srcG + 0.5000*srcB
		b[i] = 0.5000*srcR - 0.4542*srcG - 0.0458*srcB
	}
}

// CSC709InverseBatch performs YCbCr to RGB conversion on multiple pixels.
func CSC709InverseBatch(y, cb, cr []float32) {
	n := len(y)
	if len(cb) < n || len(cr) < n {
		return
	}

	i := 0
	// Process 8 pixels at a time
	for ; i+batchSize <= n; i += batchSize {
		for j := 0; j < batchSize; j++ {
			srcY := y[i+j]
			srcCb := cb[i+j]
			srcCr := cr[i+j]

			// R' = Y' + 1.5747 Cr
			// G' = Y' - 0.1873 Cb - 0.4682 Cr
			// B' = Y' + 1.8556 Cb
			y[i+j] = srcY + 1.5747*srcCr
			cb[i+j] = srcY - 0.1873*srcCb - 0.4682*srcCr
			cr[i+j] = srcY + 1.8556*srcCb
		}
	}

	// Handle remainder
	for ; i < n; i++ {
		srcY := y[i]
		srcCb := cb[i]
		srcCr := cr[i]
		y[i] = srcY + 1.5747*srcCr
		cb[i] = srcY - 0.1873*srcCb - 0.4682*srcCr
		cr[i] = srcY + 1.8556*srcCb
	}
}

// InterleaveChannelsBatch interleaves multiple channel buffers with loop unrolling.
// Output format: channel0[0], channel1[0], ..., channel0[1], channel1[1], ...
func InterleaveChannelsBatch(dst []byte, channels [][]byte, bytesPerSample int) {
	if len(channels) == 0 {
		return
	}

	numPixels := len(channels[0]) / bytesPerSample
	numChannels := len(channels)

	dstOffset := 0
	for pixel := 0; pixel < numPixels; pixel++ {
		srcOffset := pixel * bytesPerSample
		for ch := 0; ch < numChannels; ch++ {
			for b := 0; b < bytesPerSample; b++ {
				dst[dstOffset] = channels[ch][srcOffset+b]
				dstOffset++
			}
		}
	}
}

// DeinterleaveChannelsBatch deinterleaves pixel data into separate channel buffers.
func DeinterleaveChannelsBatch(src []byte, channels [][]byte, bytesPerSample int) {
	if len(channels) == 0 {
		return
	}

	numChannels := len(channels)
	numPixels := len(src) / (numChannels * bytesPerSample)

	srcOffset := 0
	for pixel := 0; pixel < numPixels; pixel++ {
		dstOffset := pixel * bytesPerSample
		for ch := 0; ch < numChannels; ch++ {
			for b := 0; b < bytesPerSample; b++ {
				channels[ch][dstOffset+b] = src[srcOffset]
				srcOffset++
			}
		}
	}
}

// ZigzagReorderBatch reorders DCT coefficients in zigzag order with loop unrolling.
func ZigzagReorderBatch(dst, src *[64]float32) {
	// Fully unrolled zigzag reorder for 8x8 DCT coefficients
	dst[0] = src[0]  // 0
	dst[1] = src[1]  // 1
	dst[2] = src[8]  // 2
	dst[3] = src[16] // 3
	dst[4] = src[9]  // 4
	dst[5] = src[2]  // 5
	dst[6] = src[3]  // 6
	dst[7] = src[10] // 7

	dst[8] = src[17]  // 8
	dst[9] = src[24]  // 9
	dst[10] = src[32] // 10
	dst[11] = src[25] // 11
	dst[12] = src[18] // 12
	dst[13] = src[11] // 13
	dst[14] = src[4]  // 14
	dst[15] = src[5]  // 15

	dst[16] = src[12] // 16
	dst[17] = src[19] // 17
	dst[18] = src[26] // 18
	dst[19] = src[33] // 19
	dst[20] = src[40] // 20
	dst[21] = src[48] // 21
	dst[22] = src[41] // 22
	dst[23] = src[34] // 23

	dst[24] = src[27] // 24
	dst[25] = src[20] // 25
	dst[26] = src[13] // 26
	dst[27] = src[6]  // 27
	dst[28] = src[7]  // 28
	dst[29] = src[14] // 29
	dst[30] = src[21] // 30
	dst[31] = src[28] // 31

	dst[32] = src[35] // 32
	dst[33] = src[42] // 33
	dst[34] = src[49] // 34
	dst[35] = src[56] // 35
	dst[36] = src[57] // 36
	dst[37] = src[50] // 37
	dst[38] = src[43] // 38
	dst[39] = src[36] // 39

	dst[40] = src[29] // 40
	dst[41] = src[22] // 41
	dst[42] = src[15] // 42
	dst[43] = src[23] // 43
	dst[44] = src[30] // 44
	dst[45] = src[37] // 45
	dst[46] = src[44] // 46
	dst[47] = src[51] // 47

	dst[48] = src[58] // 48
	dst[49] = src[59] // 49
	dst[50] = src[52] // 50
	dst[51] = src[45] // 51
	dst[52] = src[38] // 52
	dst[53] = src[31] // 53
	dst[54] = src[39] // 54
	dst[55] = src[46] // 55

	dst[56] = src[53] // 56
	dst[57] = src[60] // 57
	dst[58] = src[61] // 58
	dst[59] = src[54] // 59
	dst[60] = src[47] // 60
	dst[61] = src[55] // 61
	dst[62] = src[62] // 62
	dst[63] = src[63] // 63
}

// ZigzagUnreorderBatch reverses zigzag ordering.
func ZigzagUnreorderBatch(dst, src *[64]float32) {
	// Inverse of zigzag ordering
	dst[0] = src[0]
	dst[1] = src[1]
	dst[8] = src[2]
	dst[16] = src[3]
	dst[9] = src[4]
	dst[2] = src[5]
	dst[3] = src[6]
	dst[10] = src[7]

	dst[17] = src[8]
	dst[24] = src[9]
	dst[32] = src[10]
	dst[25] = src[11]
	dst[18] = src[12]
	dst[11] = src[13]
	dst[4] = src[14]
	dst[5] = src[15]

	dst[12] = src[16]
	dst[19] = src[17]
	dst[26] = src[18]
	dst[33] = src[19]
	dst[40] = src[20]
	dst[48] = src[21]
	dst[41] = src[22]
	dst[34] = src[23]

	dst[27] = src[24]
	dst[20] = src[25]
	dst[13] = src[26]
	dst[6] = src[27]
	dst[7] = src[28]
	dst[14] = src[29]
	dst[21] = src[30]
	dst[28] = src[31]

	dst[35] = src[32]
	dst[42] = src[33]
	dst[49] = src[34]
	dst[56] = src[35]
	dst[57] = src[36]
	dst[50] = src[37]
	dst[43] = src[38]
	dst[36] = src[39]

	dst[29] = src[40]
	dst[22] = src[41]
	dst[15] = src[42]
	dst[23] = src[43]
	dst[30] = src[44]
	dst[37] = src[45]
	dst[44] = src[46]
	dst[51] = src[47]

	dst[58] = src[48]
	dst[59] = src[49]
	dst[52] = src[50]
	dst[45] = src[51]
	dst[38] = src[52]
	dst[31] = src[53]
	dst[39] = src[54]
	dst[46] = src[55]

	dst[53] = src[56]
	dst[60] = src[57]
	dst[61] = src[58]
	dst[54] = src[59]
	dst[47] = src[60]
	dst[55] = src[61]
	dst[62] = src[62]
	dst[63] = src[63]
}
