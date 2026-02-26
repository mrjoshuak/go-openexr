// Package compression provides compression algorithms for OpenEXR files.
package compression

// Haar wavelet transform for PIZ compression.
// Based on OpenEXR's ImfWav.cpp implementation.
//
// The transform operates on 16-bit unsigned integers and produces
// wavelet coefficients that are also 16-bit unsigned.
//
// There are two encoding modes:
// - wenc14/wdec14: For 14-bit data (max < 16384), uses simple signed arithmetic
// - wenc16/wdec16: For full 16-bit data, uses modulo arithmetic with offsets

// Constants for 16-bit wavelet encoding with modulo arithmetic
const (
	wavNBits    = 16
	wavAOffset  = 1 << (wavNBits - 1) // 32768
	wavMOffset  = 1 << (wavNBits - 1) // 32768
	wavModMask  = (1 << wavNBits) - 1 // 0xFFFF
	wavMaxFor14 = 1 << 14             // 16384 - threshold for choosing wenc14 vs wenc16
)

// wenc14 encodes a pair of values into average and difference.
// Uses 14-bit signed arithmetic for the difference to match OpenEXR.
// Only valid for data where all values are less than 16384.
func wenc14(a, b uint16) (l, h uint16) {
	// Compute average and difference
	as := int(int16(a))
	bs := int(int16(b))

	ms := (as + bs) >> 1
	ds := as - bs

	l = uint16(int16(ms))
	h = uint16(int16(ds))
	return
}

// wdec14 decodes average and difference back to original values.
func wdec14(l, h uint16) (a, b uint16) {
	ms := int(int16(l))
	ds := int(int16(h))

	as := ms + ((ds + 1) >> 1)
	bs := ms - (ds >> 1)

	a = uint16(int16(as))
	b = uint16(int16(bs))
	return
}

// wenc16 encodes a pair of values into average and difference using modulo arithmetic.
// Works with full 16-bit data values. Uses offset arithmetic to keep small
// differences as small unsigned values.
func wenc16(a, b uint16) (l, h uint16) {
	ao := (int(a) + wavAOffset) & wavModMask
	m := (ao + int(b)) >> 1
	d := ao - int(b)

	if d < 0 {
		m = (m + wavMOffset) & wavModMask
	}
	d &= wavModMask

	l = uint16(m)
	h = uint16(d)
	return
}

// wdec16 decodes average and difference back to original values using modulo arithmetic.
func wdec16(l, h uint16) (a, b uint16) {
	m := int(l)
	d := int(h)
	bb := (m - (d >> 1)) & wavModMask
	aa := (d + bb - wavAOffset) & wavModMask
	b = uint16(bb)
	a = uint16(aa)
	return
}

// WaveletEncode applies forward Haar wavelet transform in place.
// The data is organized as a 2D array of width x height 16-bit values.
func WaveletEncode(data []uint16, width, height int) {
	if len(data) == 0 || width == 0 || height == 0 {
		return
	}

	temp := make([]uint16, max(width, height))

	// Transform rows
	for y := 0; y < height; y++ {
		row := data[y*width : (y+1)*width]
		wav16Encode(row, temp, width)
	}

	// Transform columns
	col := make([]uint16, height)
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			col[y] = data[y*width+x]
		}
		wav16Encode(col, temp, height)
		for y := 0; y < height; y++ {
			data[y*width+x] = col[y]
		}
	}
}

// WaveletDecode applies inverse Haar wavelet transform in place.
func WaveletDecode(data []uint16, width, height int) {
	if len(data) == 0 || width == 0 || height == 0 {
		return
	}

	temp := make([]uint16, max(width, height))

	// Inverse transform columns first (reverse of encode)
	col := make([]uint16, height)
	for x := 0; x < width; x++ {
		for y := 0; y < height; y++ {
			col[y] = data[y*width+x]
		}
		wav16Decode(col, temp, height)
		for y := 0; y < height; y++ {
			data[y*width+x] = col[y]
		}
	}

	// Inverse transform rows
	for y := 0; y < height; y++ {
		row := data[y*width : (y+1)*width]
		wav16Decode(row, temp, width)
	}
}

// wav16Encode applies forward wavelet transform to a 1D array
func wav16Encode(data, temp []uint16, n int) {
	if n < 2 {
		return
	}

	p := n
	pEnd := 1
	for p > pEnd {
		p2 := p >> 1

		// Process pairs
		a := 0
		c := 0
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

		// Copy back
		copy(data[:p], temp[:p])

		p = p2
	}
}

// wav16Decode applies inverse wavelet transform to a 1D array
func wav16Decode(data, temp []uint16, n int) {
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

		// Decode pairs
		a := 0
		c := 0
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

		// Copy back
		copy(data[:p], temp[:p])
	}
}

// wdec14_4 decodes a 2x2 block of wavelet coefficients in one operation.
// This is equivalent to calling wdec14 four times but with explicit data reuse.
// Arguments are pointers to the 4 positions: px (top-left), p01 (top-right),
// p10 (bottom-left), p11 (bottom-right).
func wdec14_4(data []uint16, px, p01, p10, p11 int) {
	// Load values as signed 16-bit
	a := int(int16(data[px]))
	b := int(int16(data[p10]))
	c := int(int16(data[p01]))
	d := int(int16(data[p11]))

	// First level inverse transform
	i00 := a + (b & 1) + (b >> 1)
	i10 := i00 - b
	i01 := c + (d & 1) + (d >> 1)
	i11 := i01 - d

	// Second level inverse transform
	a = i00 + (i01 & 1) + (i01 >> 1)
	b = a - i01
	c = i10 + (i11 & 1) + (i11 >> 1)
	d = c - i11

	// Store results
	data[px] = uint16(int16(a))
	data[p01] = uint16(int16(b))
	data[p10] = uint16(int16(c))
	data[p11] = uint16(int16(d))
}

// wdec16_4 decodes a 2x2 block of wavelet coefficients using modulo arithmetic.
// This is equivalent to calling wdec16 four times.
func wdec16_4(data []uint16, px, p01, p10, p11 int) {
	// Load values
	l0 := int(data[px])
	h0 := int(data[p10])
	l1 := int(data[p01])
	h1 := int(data[p11])

	// First level inverse transform (rows)
	bb0 := (l0 - (h0 >> 1)) & wavModMask
	aa0 := (h0 + bb0 - wavAOffset) & wavModMask
	bb1 := (l1 - (h1 >> 1)) & wavModMask
	aa1 := (h1 + bb1 - wavAOffset) & wavModMask

	// Second level inverse transform (columns)
	bb := (aa0 - (aa1 >> 1)) & wavModMask
	aa := (aa1 + bb - wavAOffset) & wavModMask
	dd := (bb0 - (bb1 >> 1)) & wavModMask
	cc := (bb1 + dd - wavAOffset) & wavModMask

	// Store results
	data[px] = uint16(aa)
	data[p01] = uint16(bb)
	data[p10] = uint16(cc)
	data[p11] = uint16(dd)
}

// Wav2DEncode applies forward 2D Haar wavelet transform in place.
// This is an optimized version that processes 2x2 blocks directly.
// maxValue is the maximum value in the data (after LUT remapping).
// If maxValue < 16384, uses wenc14 (signed arithmetic).
// Otherwise, uses wenc16 (modulo arithmetic).
func Wav2DEncode(data []uint16, nx, ny int, maxValue uint16) {
	Wav2DEncodeStrided(data, nx, 1, ny, nx, maxValue)
}

// Wav2DEncodeStrided applies forward 2D Haar wavelet transform in place with
// arbitrary strides. ox is the stride between adjacent X samples, oy is the
// stride between adjacent Y rows. This matches the C++ OpenEXR wav2Encode
// signature and is needed for PIZ compression of float/uint channels where
// each channel's uint16 sub-planes are interleaved (stride=2).
func Wav2DEncodeStrided(data []uint16, nx, ox, ny, oy int, maxValue uint16) {
	if len(data) == 0 || nx == 0 || ny == 0 {
		return
	}

	w14 := maxValue < wavMaxFor14

	n := nx
	if ny < nx {
		n = ny
	}

	p := 1
	p2 := 2

	// Hierarchical loop on smaller dimension
	for p2 <= n {
		oy1 := oy * p
		oy2 := oy * p2
		ox1 := ox * p
		ox2 := ox * p2

		// Y loop
		for py := 0; py <= oy*(ny-p2); py += oy2 {
			// X loop - process 2x2 blocks
			for px := py; px <= py+ox*(nx-p2); px += ox2 {
				p01 := px + ox1
				p10 := px + oy1
				p11 := p10 + ox1

				// 2D wavelet encoding
				if w14 {
					i00l, i01h := wenc14(data[px], data[p01])
					i10l, i11h := wenc14(data[p10], data[p11])
					data[px], data[p10] = wenc14(i00l, i10l)
					data[p01], data[p11] = wenc14(i01h, i11h)
				} else {
					i00l, i01h := wenc16(data[px], data[p01])
					i10l, i11h := wenc16(data[p10], data[p11])
					data[px], data[p10] = wenc16(i00l, i10l)
					data[p01], data[p11] = wenc16(i01h, i11h)
				}
			}

			// Encode odd column (1D)
			if nx&p != 0 {
				px := py + ox*(nx-p)
				p10 := px + oy1
				if w14 {
					data[px], data[p10] = wenc14(data[px], data[p10])
				} else {
					data[px], data[p10] = wenc16(data[px], data[p10])
				}
			}
		}

		// Encode odd line (1D)
		if ny&p != 0 {
			py := oy * (ny - p)
			for px := py; px <= py+ox*(nx-p2); px += ox2 {
				p01 := px + ox1
				if w14 {
					data[px], data[p01] = wenc14(data[px], data[p01])
				} else {
					data[px], data[p01] = wenc16(data[px], data[p01])
				}
			}
		}

		p = p2
		p2 <<= 1
	}
}

// Wav2DDecode applies inverse 2D Haar wavelet transform in place.
// This is an optimized version that processes 2x2 blocks using wdec14_4 or wdec16_4.
// maxValue is the maximum value (after LUT remapping).
// If maxValue < 16384, uses wdec14 (signed arithmetic).
// Otherwise, uses wdec16 (modulo arithmetic).
func Wav2DDecode(data []uint16, nx, ny int, maxValue uint16) {
	Wav2DDecodeStrided(data, nx, 1, ny, nx, maxValue)
}

// Wav2DDecodeStrided applies inverse 2D Haar wavelet transform in place with
// arbitrary strides. ox is the stride between adjacent X samples, oy is the
// stride between adjacent Y rows. This matches the C++ OpenEXR wav2Decode
// signature and is needed for PIZ decompression of float/uint channels where
// each channel's uint16 sub-planes are interleaved (stride=2).
func Wav2DDecodeStrided(data []uint16, nx, ox, ny, oy int, maxValue uint16) {
	if len(data) == 0 || nx == 0 || ny == 0 {
		return
	}

	w14 := maxValue < wavMaxFor14

	n := nx
	if ny < nx {
		n = ny
	}

	// Find max level
	p := 1
	for p <= n {
		p <<= 1
	}
	p >>= 1
	p2 := p
	p >>= 1

	// Hierarchical loop from coarsest to finest
	for p >= 1 {
		oy1 := oy * p
		oy2 := oy * p2
		ox1 := ox * p
		ox2 := ox * p2

		// Y loop
		for py := 0; py <= oy*(ny-p2); py += oy2 {
			// X loop - process 2x2 blocks with optimized decoder
			for px := py; px <= py+ox*(nx-p2); px += ox2 {
				p01 := px + ox1
				p10 := px + oy1
				p11 := p10 + ox1

				if w14 {
					wdec14_4(data, px, p01, p10, p11)
				} else {
					wdec16_4(data, px, p01, p10, p11)
				}
			}

			// Decode odd column (1D)
			if nx&p != 0 {
				px := py + ox*(nx-p)
				p10 := px + oy1
				var a, b uint16
				if w14 {
					a, b = wdec14(data[px], data[p10])
				} else {
					a, b = wdec16(data[px], data[p10])
				}
				data[px] = a
				data[p10] = b
			}
		}

		// Decode odd line (1D)
		if ny&p != 0 {
			py := oy * (ny - p)
			for px := py; px <= py+ox*(nx-p2); px += ox2 {
				p01 := px + ox1
				var a, b uint16
				if w14 {
					a, b = wdec14(data[px], data[p01])
				} else {
					a, b = wdec16(data[px], data[p01])
				}
				data[px] = a
				data[p01] = b
			}
		}

		p2 = p
		p >>= 1
	}
}
