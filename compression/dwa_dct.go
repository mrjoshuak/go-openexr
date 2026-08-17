package compression

import "math"

// The transforms in this file are transcriptions of the scalar paths in
// OpenEXR's src/lib/OpenEXRCore/internal_dwa_simd.h (v3.4.1): dctForward8x8,
// dctInverse8x8_scalar, dctInverse8x8DcOnly, csc709Forward, csc709Inverse and
// fromHalfZigZag_scalar.
//
// They are deliberately not "the textbook DCT". DWA's iDCT is built from
// constants of the form 0.5*cos(k*3.14159/16) -- the reference uses a
// truncated literal for pi, not M_PI -- and its forward DCT is a factored
// butterfly with its own rounding. Substituting a mathematically nicer
// transform would decode reference files to slightly different pixels than the
// reference does, which is the whole failure mode this package is trying to
// avoid, so the constants are reproduced as written.

// dwaPi is the value OpenEXR's DCT constants are derived from. It is a
// truncated decimal literal rounded to float32, not pi; see the note above.
var dwaPi = float32(3.14159)

// dwaCosf evaluates cosine the way cosf does: on a float32 argument, to a
// float32 result. The constants below are one or two ulps away from the
// double-precision values, and DWA files decoded with the double-precision
// ones come out a half-ulp different from the reference on a few pixels in
// every hundred thousand.
func dwaCosf(x float32) float32 { return float32(math.Cos(float64(x))) }

// Inverse DCT constants: a..g = .5f * cosf(k * 3.14159f / 16.f) for
// k = 4, 1, 2, 3, 5, 6, 7, in the order and grouping dctInverse8x8_scalar
// writes them.
var (
	dwaIDctA = 0.5 * dwaCosf(dwaPi/4.0)
	dwaIDctB = 0.5 * dwaCosf(dwaPi/16.0)
	dwaIDctC = 0.5 * dwaCosf(dwaPi/8.0)
	dwaIDctD = 0.5 * dwaCosf(3.0*dwaPi/16.0)
	dwaIDctE = 0.5 * dwaCosf(5.0*dwaPi/16.0)
	dwaIDctF = 0.5 * dwaCosf(3.0*dwaPi/8.0)
	dwaIDctG = 0.5 * dwaCosf(7.0*dwaPi/16.0)
)

// Forward DCT constants: cosf(3.14159f * k / 16.f).
var (
	dwaFDctC1 = dwaCosf(dwaPi * 1.0 / 16.0)
	dwaFDctC2 = dwaCosf(dwaPi * 2.0 / 16.0)
	dwaFDctC3 = dwaCosf(dwaPi * 3.0 / 16.0)
	dwaFDctC4 = dwaCosf(dwaPi * 4.0 / 16.0)
	dwaFDctC5 = dwaCosf(dwaPi * 5.0 / 16.0)
	dwaFDctC6 = dwaCosf(dwaPi * 6.0 / 16.0)
	dwaFDctC7 = dwaCosf(dwaPi * 7.0 / 16.0)
)

// dwaInvZigZag maps a natural 8x8 position to its index in zig-zag order, so
// dst[i] = src[dwaInvZigZag[i]] un-zig-zags a block. It is the inv_remap table
// from quantizeCoeffAndZigXDR, and the destination layout drawn in the comment
// above fromHalfZigZag_scalar.
var dwaInvZigZag = [64]int{
	0, 1, 5, 6, 14, 15, 27, 28,
	2, 4, 7, 13, 16, 26, 29, 42,
	3, 8, 12, 17, 25, 30, 41, 43,
	9, 11, 18, 24, 31, 40, 44, 53,
	10, 19, 23, 32, 39, 45, 52, 54,
	20, 22, 33, 38, 46, 51, 55, 60,
	21, 34, 37, 47, 50, 56, 59, 61,
	35, 36, 48, 49, 57, 58, 62, 63,
}

// dwaDctInverse8x8 is the inverse 8x8 DCT, operating in place.
//
// zeroedRows says how many rows at the bottom of the block are known to be
// entirely zero; the row pass skips them. It is an optimisation only, and
// passing 0 is always correct.
func dwaDctInverse8x8(data *[64]float32, zeroedRows int) {
	a, b, c := dwaIDctA, dwaIDctB, dwaIDctC
	d, e, f, g := dwaIDctD, dwaIDctE, dwaIDctF, dwaIDctG

	var alpha, beta, theta, gamma [4]float32

	// First pass: rows.
	for row := 0; row < 8-zeroedRows; row++ {
		p := data[row*8 : row*8+8 : row*8+8]

		alpha[0] = c * p[2]
		alpha[1] = f * p[2]
		alpha[2] = c * p[6]
		alpha[3] = f * p[6]

		beta[0] = float32(b*p[1]) + float32(d*p[3]) + float32(e*p[5]) + float32(g*p[7])
		beta[1] = float32(d*p[1]) - float32(g*p[3]) - float32(b*p[5]) - float32(e*p[7])
		beta[2] = float32(e*p[1]) - float32(b*p[3]) + float32(g*p[5]) + float32(d*p[7])
		beta[3] = float32(g*p[1]) - float32(e*p[3]) + float32(d*p[5]) - float32(b*p[7])

		theta[0] = a * (p[0] + p[4])
		theta[3] = a * (p[0] - p[4])
		theta[1] = alpha[0] + alpha[3]
		theta[2] = alpha[1] - alpha[2]

		gamma[0] = theta[0] + theta[1]
		gamma[1] = theta[3] + theta[2]
		gamma[2] = theta[3] - theta[2]
		gamma[3] = theta[0] - theta[1]

		p[0] = gamma[0] + beta[0]
		p[1] = gamma[1] + beta[1]
		p[2] = gamma[2] + beta[2]
		p[3] = gamma[3] + beta[3]
		p[4] = gamma[3] - beta[3]
		p[5] = gamma[2] - beta[2]
		p[6] = gamma[1] - beta[1]
		p[7] = gamma[0] - beta[0]
	}

	// Second pass: columns.
	for col := 0; col < 8; col++ {
		alpha[0] = c * data[16+col]
		alpha[1] = f * data[16+col]
		alpha[2] = c * data[48+col]
		alpha[3] = f * data[48+col]

		beta[0] = float32(b*data[8+col]) + float32(d*data[24+col]) + float32(e*data[40+col]) + float32(g*data[56+col])
		beta[1] = float32(d*data[8+col]) - float32(g*data[24+col]) - float32(b*data[40+col]) - float32(e*data[56+col])
		beta[2] = float32(e*data[8+col]) - float32(b*data[24+col]) + float32(g*data[40+col]) + float32(d*data[56+col])
		beta[3] = float32(g*data[8+col]) - float32(e*data[24+col]) + float32(d*data[40+col]) - float32(b*data[56+col])

		theta[0] = a * (data[col] + data[32+col])
		theta[3] = a * (data[col] - data[32+col])
		theta[1] = alpha[0] + alpha[3]
		theta[2] = alpha[1] - alpha[2]

		gamma[0] = theta[0] + theta[1]
		gamma[1] = theta[3] + theta[2]
		gamma[2] = theta[3] - theta[2]
		gamma[3] = theta[0] - theta[1]

		data[col] = gamma[0] + beta[0]
		data[8+col] = gamma[1] + beta[1]
		data[16+col] = gamma[2] + beta[2]
		data[24+col] = gamma[3] + beta[3]
		data[32+col] = gamma[3] - beta[3]
		data[40+col] = gamma[2] - beta[2]
		data[48+col] = gamma[1] - beta[1]
		data[56+col] = gamma[0] - beta[0]
	}
}

// dwaDctInverse8x8DcOnly inverts a block whose AC coefficients are all zero.
// The result is constant, so only the DC term is scaled and broadcast. The
// scale factor 3.535536e-01 squared is the literal the reference uses.
func dwaDctInverse8x8DcOnly(data *[64]float32) {
	val := data[0] * 3.535536e-01 * 3.535536e-01
	for i := range data {
		data[i] = val
	}
}

// dwaDctForward8x8 is the forward 8x8 DCT, operating in place.
func dwaDctForward8x8(data *[64]float32) {
	c1, c2, c3 := dwaFDctC1, dwaFDctC2, dwaFDctC3
	c4, c5, c6, c7 := dwaFDctC4, dwaFDctC5, dwaFDctC6, dwaFDctC7

	c1Half := 0.5 * c1
	c2Half := 0.5 * c2
	c3Half := 0.5 * c3
	c5Half := 0.5 * c5
	c6Half := 0.5 * c6
	c7Half := 0.5 * c7

	var a0, a1, a2, a3, a4, a5, a6, a7 float32
	var k0, k1, rotX, rotY float32

	// First pass: rows.
	for row := 0; row < 8; row++ {
		p := data[row*8 : row*8+8 : row*8+8]

		a0 = p[0] + p[7]
		a1 = p[1] + p[2]
		a2 = p[1] - p[2]
		a3 = p[3] + p[4]
		a4 = p[3] - p[4]
		a5 = p[5] + p[6]
		a6 = p[5] - p[6]
		a7 = p[0] - p[7]

		k0 = c4 * (a0 + a3)
		k1 = c4 * (a1 + a5)

		p[0] = 0.5 * (k0 + k1)
		p[4] = 0.5 * (k0 - k1)

		rotX = a2 - a6
		rotY = a0 - a3
		p[2] = float32(c6Half*rotX) + float32(c2Half*rotY)
		p[6] = float32(c6Half*rotY) - float32(c2Half*rotX)

		k0 = c4 * (a1 - a5)
		k1 = -1 * c4 * (a2 + a6)

		rotX = a7 - k0
		rotY = a4 + k1
		p[3] = float32(c3Half*rotX) - float32(c5Half*rotY)
		p[5] = float32(c5Half*rotX) + float32(c3Half*rotY)

		rotX = a7 + k0
		rotY = k1 - a4
		p[1] = float32(c1Half*rotX) - float32(c7Half*rotY)
		p[7] = float32(c7Half*rotX) + float32(c1Half*rotY)
	}

	// Second pass: columns.
	for col := 0; col < 8; col++ {
		a0 = data[col] + data[56+col]
		a7 = data[col] - data[56+col]
		a1 = data[8+col] + data[16+col]
		a2 = data[8+col] - data[16+col]
		a3 = data[24+col] + data[32+col]
		a4 = data[24+col] - data[32+col]
		a5 = data[40+col] + data[48+col]
		a6 = data[40+col] - data[48+col]

		k0 = c4 * (a0 + a3)
		k1 = c4 * (a1 + a5)

		data[col] = 0.5 * (k0 + k1)
		data[32+col] = 0.5 * (k0 - k1)

		rotX = a2 - a6
		rotY = a0 - a3
		data[16+col] = 0.5 * (float32(c6*rotX) + float32(c2*rotY))
		data[48+col] = 0.5 * (float32(c6*rotY) - float32(c2*rotX))

		k0 = c4 * (a1 - a5)
		k1 = -1 * c4 * (a2 + a6)

		rotX = a7 - k0
		rotY = a4 + k1
		data[24+col] = 0.5 * (float32(c3*rotX) - float32(c5*rotY))
		data[40+col] = 0.5 * (float32(c5*rotX) + float32(c3*rotY))

		rotX = a7 + k0
		rotY = k1 - a4
		data[8+col] = 0.5 * (float32(c1*rotX) - float32(c7*rotY))
		data[56+col] = 0.5 * (float32(c7*rotX) + float32(c1*rotY))
	}
}

// dwaCsc709Inverse converts one Y'CbCr sample triple back to R'G'B'.
func dwaCsc709Inverse(comp0, comp1, comp2 *float32) {
	y, cb, cr := *comp0, *comp1, *comp2
	*comp0 = y + float32(1.5747*cr)
	*comp1 = y - float32(0.1873*cb) - float32(0.4682*cr)
	*comp2 = y + float32(1.8556*cb)
}

// dwaCsc709Inverse64 converts a whole 8x8 block from Y'CbCr to R'G'B'.
func dwaCsc709Inverse64(comp0, comp1, comp2 *[64]float32) {
	for i := 0; i < 64; i++ {
		dwaCsc709Inverse(&comp0[i], &comp1[i], &comp2[i])
	}
}

// dwaCsc709Forward64 converts a whole 8x8 block from R'G'B' to Y'CbCr.
func dwaCsc709Forward64(comp0, comp1, comp2 *[64]float32) {
	for i := 0; i < 64; i++ {
		r, g, b := comp0[i], comp1[i], comp2[i]
		comp0[i] = float32(0.2126*r) + float32(0.7152*g) + float32(0.0722*b)
		comp1[i] = float32(-0.1146*r) - float32(0.3854*g) + float32(0.5000*b)
		comp2[i] = float32(0.5000*r) - float32(0.4542*g) - float32(0.0458*b)
	}
}
