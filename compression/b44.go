package compression

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

// B44 errors
var (
	ErrB44ImageTooLarge = errors.New("compression: B44 image too large")

	// ErrB44BadGeometry reports a channel description this codec cannot make
	// sense of, such as a negative width or height.
	ErrB44BadGeometry = errors.New("compression: B44 invalid channel geometry")

	// ErrB44DataSize reports that the pixel buffer handed to the codec does not
	// match the size the channel descriptions imply. Guessing at the intended
	// layout would silently misplace whole channels, so this is an error.
	ErrB44DataSize = errors.New("compression: B44 pixel buffer size does not match the channel list")

	// ErrB44Truncated reports that the compressed stream ended before every
	// channel had been decoded. The remaining pixels are unknown, and returning
	// the zero-filled remainder would be indistinguishable from a correct read.
	ErrB44Truncated = errors.New("compression: B44 compressed data ended prematurely")
)

// b44BufferPool provides reusable scratch buffers for B44 compression/decompression
var b44BufferPool = sync.Pool{
	New: func() any {
		// Allocate buffer for typical 4K image channel (4096*4096 uint16 = 32MB)
		// Will grow as needed for larger images
		buf := make([]uint16, 4096*4096)
		return &buf
	},
}

// B44 compression for OpenEXR.
// B44 is a lossy compression for HALF (16-bit float) data.
// It divides the image into 4x4 blocks and compresses each block
// to either 14 bytes (normal) or 3 bytes (flat field).
// Non-HALF data is stored uncompressed.

// B44PixelType constants
const (
	b44PixelTypeUint  = 0
	b44PixelTypeHalf  = 1
	b44PixelTypeFloat = 2
)

// B44ChannelInfo describes a channel for B44 compression
type B44ChannelInfo struct {
	Type      int // b44PixelTypeUint, b44PixelTypeHalf, b44PixelTypeFloat
	Width     int
	Height    int
	IsLinear  bool // true for luminance/chroma channels
	XSampling int
	YSampling int
}

// packB44 packs 16 half-float values into 14 or 3 bytes.
// Returns number of bytes written (3 for flat field, 14 otherwise).
func packB44(s [16]uint16, b []byte, flatfields bool, exactmax bool) int {
	// Convert from sign-magnitude to ordered representation using SIMD
	// This is the main hot spot: 16 values with NaN/Inf check and sign handling
	var t [16]uint16
	toOrderedSIMD(&t, &s)

	// Find max using SIMD horizontal reduction
	tMax := findMaxSIMD(&t)

	// Extract t0 for flat field encoding and output
	t0 := t[0]

	// Find shift and compute running differences
	const bias = 0x20
	var d [16]uint16
	var r0, r1, r2, r3, r4, r5, r6, r7 int
	var r8, r9, r10, r11, r12, r13, r14 int
	var rMin, rMax int
	shift := uint(0)

	for {
		// Compute absolute differences from tMax, then shift and round
		// Uses SIMD on AMD64 with SSE2 PSRLW for runtime variable shifts
		shiftRoundSIMD(&d, &t, tMax, shift)

		// Convert to running differences (ints for signed arithmetic)
		r0 = int(d[0]) - int(d[4]) + bias
		r1 = int(d[4]) - int(d[8]) + bias
		r2 = int(d[8]) - int(d[12]) + bias
		r3 = int(d[0]) - int(d[1]) + bias
		r4 = int(d[4]) - int(d[5]) + bias
		r5 = int(d[8]) - int(d[9]) + bias
		r6 = int(d[12]) - int(d[13]) + bias
		r7 = int(d[1]) - int(d[2]) + bias
		r8 = int(d[5]) - int(d[6]) + bias
		r9 = int(d[9]) - int(d[10]) + bias
		r10 = int(d[13]) - int(d[14]) + bias
		r11 = int(d[2]) - int(d[3]) + bias
		r12 = int(d[6]) - int(d[7]) + bias
		r13 = int(d[10]) - int(d[11]) + bias
		r14 = int(d[14]) - int(d[15]) + bias

		// Find min and max in one pass
		rMin, rMax = r0, r0
		if r1 < rMin {
			rMin = r1
		} else if r1 > rMax {
			rMax = r1
		}
		if r2 < rMin {
			rMin = r2
		} else if r2 > rMax {
			rMax = r2
		}
		if r3 < rMin {
			rMin = r3
		} else if r3 > rMax {
			rMax = r3
		}
		if r4 < rMin {
			rMin = r4
		} else if r4 > rMax {
			rMax = r4
		}
		if r5 < rMin {
			rMin = r5
		} else if r5 > rMax {
			rMax = r5
		}
		if r6 < rMin {
			rMin = r6
		} else if r6 > rMax {
			rMax = r6
		}
		if r7 < rMin {
			rMin = r7
		} else if r7 > rMax {
			rMax = r7
		}
		if r8 < rMin {
			rMin = r8
		} else if r8 > rMax {
			rMax = r8
		}
		if r9 < rMin {
			rMin = r9
		} else if r9 > rMax {
			rMax = r9
		}
		if r10 < rMin {
			rMin = r10
		} else if r10 > rMax {
			rMax = r10
		}
		if r11 < rMin {
			rMin = r11
		} else if r11 > rMax {
			rMax = r11
		}
		if r12 < rMin {
			rMin = r12
		} else if r12 > rMax {
			rMax = r12
		}
		if r13 < rMin {
			rMin = r13
		} else if r13 > rMax {
			rMax = r13
		}
		if r14 < rMin {
			rMin = r14
		} else if r14 > rMax {
			rMax = r14
		}

		if rMin >= 0 && rMax <= 0x3f {
			break
		}
		shift++
	}

	// Check for flat field (all same value)
	if rMin == bias && rMax == bias && flatfields {
		b[0] = byte(t0 >> 8)
		b[1] = byte(t0)
		b[2] = 0xfc
		return 3
	}

	t0Out := t0
	if exactmax {
		t0Out = tMax - uint16(uint(d[0])<<shift)
	}

	// Pack into 14 bytes
	b[0] = byte(t0Out >> 8)
	b[1] = byte(t0Out)
	b[2] = byte((int(shift) << 2) | (r0 >> 4))
	b[3] = byte((r0 << 4) | (r1 >> 2))
	b[4] = byte((r1 << 6) | r2)
	b[5] = byte((r3 << 2) | (r4 >> 4))
	b[6] = byte((r4 << 4) | (r5 >> 2))
	b[7] = byte((r5 << 6) | r6)
	b[8] = byte((r7 << 2) | (r8 >> 4))
	b[9] = byte((r8 << 4) | (r9 >> 2))
	b[10] = byte((r9 << 6) | r10)
	b[11] = byte((r11 << 2) | (r12 >> 4))
	b[12] = byte((r12 << 4) | (r13 >> 2))
	b[13] = byte((r13 << 6) | r14)

	return 14
}

// signMagConvert converts from ordered to sign-magnitude representation.
// If high bit set, clear it; otherwise invert.
//
//go:inline
func signMagConvert(v uint16) uint16 {
	if (v & 0x8000) != 0 {
		return v & 0x7fff
	}
	return ^v
}

// unpack14 unpacks 14 bytes into 16 half-float values.
func unpack14(b []byte, s *[16]uint16) {
	s[0] = (uint16(b[0]) << 8) | uint16(b[1])

	shift := uint16(b[2] >> 2)
	bias := uint16(0x20) << shift

	// Unpack column 0: s[0], s[4], s[8], s[12]
	s[4] = s[0] + uint16((((uint32(b[2])<<4)|(uint32(b[3])>>4))&0x3f)<<shift) - bias
	s[8] = s[4] + uint16((((uint32(b[3])<<2)|(uint32(b[4])>>6))&0x3f)<<shift) - bias
	s[12] = s[8] + uint16((uint32(b[4])&0x3f)<<shift) - bias

	// Unpack column 1: s[1], s[5], s[9], s[13]
	s[1] = s[0] + uint16((uint32(b[5])>>2)<<shift) - bias
	s[5] = s[4] + uint16((((uint32(b[5])<<4)|(uint32(b[6])>>4))&0x3f)<<shift) - bias
	s[9] = s[8] + uint16((((uint32(b[6])<<2)|(uint32(b[7])>>6))&0x3f)<<shift) - bias
	s[13] = s[12] + uint16((uint32(b[7])&0x3f)<<shift) - bias

	// Unpack column 2: s[2], s[6], s[10], s[14]
	s[2] = s[1] + uint16((uint32(b[8])>>2)<<shift) - bias
	s[6] = s[5] + uint16((((uint32(b[8])<<4)|(uint32(b[9])>>4))&0x3f)<<shift) - bias
	s[10] = s[9] + uint16((((uint32(b[9])<<2)|(uint32(b[10])>>6))&0x3f)<<shift) - bias
	s[14] = s[13] + uint16((uint32(b[10])&0x3f)<<shift) - bias

	// Unpack column 3: s[3], s[7], s[11], s[15]
	s[3] = s[2] + uint16((uint32(b[11])>>2)<<shift) - bias
	s[7] = s[6] + uint16((((uint32(b[11])<<4)|(uint32(b[12])>>4))&0x3f)<<shift) - bias
	s[11] = s[10] + uint16((((uint32(b[12])<<2)|(uint32(b[13])>>6))&0x3f)<<shift) - bias
	s[15] = s[14] + uint16((uint32(b[13])&0x3f)<<shift) - bias

	// Convert back from ordered to sign-magnitude representation (unrolled)
	// If high bit set, clear it; otherwise invert
	s[0] = signMagConvert(s[0])
	s[1] = signMagConvert(s[1])
	s[2] = signMagConvert(s[2])
	s[3] = signMagConvert(s[3])
	s[4] = signMagConvert(s[4])
	s[5] = signMagConvert(s[5])
	s[6] = signMagConvert(s[6])
	s[7] = signMagConvert(s[7])
	s[8] = signMagConvert(s[8])
	s[9] = signMagConvert(s[9])
	s[10] = signMagConvert(s[10])
	s[11] = signMagConvert(s[11])
	s[12] = signMagConvert(s[12])
	s[13] = signMagConvert(s[13])
	s[14] = signMagConvert(s[14])
	s[15] = signMagConvert(s[15])
}

// unpack3 unpacks 3 bytes (flat field) into 16 identical half-float values.
func unpack3(b []byte, s *[16]uint16) {
	v := (uint16(b[0]) << 8) | uint16(b[1])

	// Convert back from ordered to sign-magnitude representation
	if (v & 0x8000) != 0 {
		v &= 0x7fff
	} else {
		v = ^v
	}

	for i := 0; i < 16; i++ {
		s[i] = v
	}
}

// B44 lookup tables for linear conversion (lazy-initialized)
var (
	b44ExpTable   [65536]uint16
	b44LogTable   [65536]uint16
	b44TablesOnce sync.Once
)

// initB44Tables initializes the exp/log conversion tables (thread-safe).
func initB44Tables() {
	b44TablesOnce.Do(func() {
		for i := 0; i < 65536; i++ {
			b44ExpTable[i] = convertFromLinear(uint16(i))
			b44LogTable[i] = convertToLinear(uint16(i))
		}
	})
}

// halfToFloat32 converts a half-float to float32.
func halfToFloat32(h uint16) float32 {
	sign := uint32(h&0x8000) << 16
	exp := int32((h >> 10) & 0x1f)
	mant := uint32(h & 0x3ff)

	if exp == 0 {
		if mant == 0 {
			return math.Float32frombits(sign)
		}
		// Denormalized
		for (mant & 0x400) == 0 {
			mant <<= 1
			exp--
		}
		exp++
		mant &= 0x3ff
	} else if exp == 31 {
		// Inf/NaN
		return math.Float32frombits(sign | 0x7f800000 | (mant << 13))
	}

	exp = exp + (127 - 15)
	return math.Float32frombits(sign | uint32(exp)<<23 | (mant << 13))
}

// float32ToHalf converts a float32 to half-float.
func float32ToHalf(f float32) uint16 {
	bits := math.Float32bits(f)
	sign := uint16((bits >> 16) & 0x8000)
	exp := int32((bits >> 23) & 0xff)
	mant := bits & 0x7fffff

	if exp == 0xff {
		// Inf/NaN
		if mant != 0 {
			return sign | 0x7c00 | uint16(mant>>13)
		}
		return sign | 0x7c00
	}

	exp = exp - 127 + 15

	if exp <= 0 {
		if exp < -10 {
			return sign
		}
		mant = (mant | 0x800000) >> uint32(1-exp)
		return sign | uint16(mant>>13)
	}

	if exp >= 31 {
		return sign | 0x7c00
	}

	return sign | uint16(exp)<<10 | uint16(mant>>13)
}

// convertFromLinear converts from linear space using exp(x/8).
func convertFromLinear(x uint16) uint16 {
	if (x & 0x7c00) == 0x7c00 {
		return 0
	}
	if x >= 0x558c && x < 0x8000 {
		return 0x7bff
	}

	f := halfToFloat32(x)
	f = float32(math.Exp(float64(f) / 8))
	return float32ToHalf(f)
}

// convertToLinear converts to linear space using 8*log(x).
func convertToLinear(x uint16) uint16 {
	if (x & 0x7c00) == 0x7c00 {
		return 0
	}
	if x > 0x8000 {
		return 0
	}

	f := halfToFloat32(x)
	if f <= 0 {
		return 0
	}
	f = 8 * float32(math.Log(float64(f)))
	return float32ToHalf(f)
}

// b44Channel is the codec's view of one channel inside a chunk.
//
// OpenEXR's B44 compressor treats a chunk as a set of channel planes: HALF
// channels are compressed in 4x4 blocks, and UINT and FLOAT channels are copied
// through uncompressed. Both kinds appear in the compressed stream as one
// contiguous run per channel, in channel order; only the *uncompressed* pixel
// buffer is interleaved scanline by scanline. See ImfB44Compressor.cpp.
type b44Channel struct {
	nx     int  // samples per row
	ny     int  // rows of this channel present in the chunk
	isHalf bool // HALF channels are block-compressed, the rest pass through

	// rowBytes is what one row of this channel contributes to the interleaved
	// pixel buffer: 2 bytes per sample for HALF, 4 for UINT and FLOAT.
	rowBytes int

	// off is the channel's start in the scratch plane: an index into the HALF
	// sample scratch for HALF channels, a byte offset into the passthrough
	// scratch for the others.
	off int
}

// b44Plan is the geometry of a whole chunk, derived from the channel list.
type b44Plan struct {
	channels []b44Channel
	// halfSamples is the total number of HALF samples across all channels.
	halfSamples int
	// rawBytes is the total number of UINT/FLOAT bytes across all channels,
	// which B44 stores uncompressed.
	rawBytes int
	// interleaved is the size of the uncompressed, scanline-interleaved pixel
	// buffer that the codec consumes and produces.
	interleaved int
}

// b44MaxScratchBytes bounds the scratch buffers a single chunk may allocate, so
// a hostile header cannot turn a small file into a huge allocation.
const b44MaxScratchBytes = 256 * 1024 * 1024

// makeB44Plan validates a channel list and computes the scratch layout for it.
func makeB44Plan(channels []B44ChannelInfo) (*b44Plan, error) {
	p := &b44Plan{channels: make([]b44Channel, len(channels))}
	var halfSamples, rawBytes, interleaved int64

	for i := range channels {
		info := &channels[i]
		if info.Width < 0 || info.Height < 0 {
			return nil, fmt.Errorf("%w: channel %d is %dx%d", ErrB44BadGeometry, i, info.Width, info.Height)
		}
		c := b44Channel{
			nx:     info.Width,
			ny:     info.Height,
			isHalf: info.Type == b44PixelTypeHalf,
		}
		bytesPerSample := int64(4)
		if c.isHalf {
			bytesPerSample = 2
		}
		rowBytes := int64(c.nx) * bytesPerSample
		samples := int64(c.nx) * int64(c.ny)
		// Bound the row before it is narrowed to int, so a hostile width cannot
		// wrap on a 32-bit build.
		if rowBytes > b44MaxScratchBytes || samples*bytesPerSample > b44MaxScratchBytes {
			return nil, ErrB44ImageTooLarge
		}
		c.rowBytes = int(rowBytes)

		if c.isHalf {
			c.off = int(halfSamples)
			halfSamples += samples
		} else {
			c.off = int(rawBytes)
			rawBytes += samples * 4
		}
		interleaved += samples * bytesPerSample

		if halfSamples*2 > b44MaxScratchBytes || rawBytes > b44MaxScratchBytes || interleaved > b44MaxScratchBytes {
			return nil, ErrB44ImageTooLarge
		}
		p.channels[i] = c
	}

	p.halfSamples = int(halfSamples)
	p.rawBytes = int(rawBytes)
	p.interleaved = int(interleaved)
	return p, nil
}

// B44Compress compresses pixel data using B44 compression.
//
// data is one chunk of pixel data in OpenEXR's uncompressed layout: scanline by
// scanline, and within each scanline one row per channel in channel order.
// channels describes the geometry and pixel type of each channel, in that same
// order. flatfields enables the 3-byte flat field encoding (B44A mode).
//
// HALF channels are compressed in 4x4 blocks. UINT and FLOAT channels are, as
// in the reference implementation, copied through uncompressed; each channel
// occupies one contiguous run of the output, in channel order.
func B44Compress(data []byte, channels []B44ChannelInfo, width, height int, flatfields bool) ([]byte, error) {
	initB44Tables()

	plan, err := makeB44Plan(channels)
	if err != nil {
		return nil, err
	}
	if len(data) != plan.interleaved {
		return nil, fmt.Errorf("%w: got %d bytes, channel list describes %d",
			ErrB44DataSize, len(data), plan.interleaved)
	}

	// Scratch planes, one per channel kind. The reference implementation
	// de-interleaves into a single temporary buffer the same way; the copy is
	// what lets the second pass emit each channel contiguously.
	halfBufPtr := b44BufferPool.Get().(*[]uint16)
	halfBuf := *halfBufPtr
	if len(halfBuf) < plan.halfSamples {
		halfBuf = make([]uint16, plan.halfSamples)
	}
	defer func() {
		*halfBufPtr = halfBuf
		b44BufferPool.Put(halfBufPtr)
	}()
	rawBuf := make([]byte, plan.rawBytes)

	// First pass: de-interleave the scanlines into per-channel planes.
	offset := 0
	for y := 0; y < height; y++ {
		for i := range plan.channels {
			c := &plan.channels[i]
			if y >= c.ny {
				continue
			}
			if !c.isHalf {
				// UINT and FLOAT pass through byte for byte, little-endian as
				// they already are in the file.
				copy(rawBuf[c.off+y*c.rowBytes:], data[offset:offset+c.rowBytes])
				offset += c.rowBytes
				continue
			}
			dst := halfBuf[c.off+y*c.nx:]
			for x := 0; x < c.nx; x++ {
				dst[x] = binary.LittleEndian.Uint16(data[offset:])
				offset += 2
			}
		}
	}

	// Second pass: emit each channel, compressed or not.
	result := make([]byte, 0, len(data))

	for i := range plan.channels {
		c := &plan.channels[i]
		if c.ny == 0 {
			continue
		}
		if !c.isHalf {
			result = append(result, rawBuf[c.off:c.off+c.ny*c.rowBytes]...)
			continue
		}

		// HALF data: compress 4x4 blocks
		cd := halfBuf[c.off : c.off+c.nx*c.ny]
		isLinear := channels[i].IsLinear
		nx := c.nx
		ny := c.ny
		var block [14]byte

		for y := 0; y < ny; y += 4 {
			for x := 0; x < nx; x += 4 {
				var s [16]uint16

				// Fast path: interior blocks with no edge padding needed
				if x+3 < nx && y+3 < ny {
					// Direct copy of 4 rows of 4 pixels each
					row0 := y*nx + x
					copy(s[0:4], cd[row0:row0+4])
					copy(s[4:8], cd[row0+nx:row0+nx+4])
					copy(s[8:12], cd[row0+2*nx:row0+2*nx+4])
					copy(s[12:16], cd[row0+3*nx:row0+3*nx+4])
				} else {
					// Edge case: pad with boundary values
					for by := 0; by < 4; by++ {
						srcY := y + by
						if srcY >= ny {
							srcY = ny - 1
						}
						for bx := 0; bx < 4; bx++ {
							srcX := x + bx
							if srcX >= nx {
								srcX = nx - 1
							}
							s[by*4+bx] = cd[srcY*nx+srcX]
						}
					}
				}

				// Apply linear conversion if needed
				if isLinear {
					// Bounds check hint for compiler optimization
					_ = b44ExpTable[65535]
					_ = s[15]
					for j := 0; j < 16; j++ {
						s[j] = b44ExpTable[s[j]]
					}
				}

				// Pack the block
				n := packB44(s, block[:], flatfields, !isLinear)
				result = append(result, block[:n]...)
			}
		}
	}

	// If compressed is larger than original, return uncompressed
	if len(result) >= len(data) {
		return data, nil
	}

	return result, nil
}

// B44Decompress decompresses B44-compressed data.
//
// channels describes the geometry and pixel type of each channel, in file
// order; expectedSize is the size of the uncompressed, scanline-interleaved
// chunk the channel list implies. UINT and FLOAT channels are read back from
// the uncompressed run B44Compress wrote for them, bit for bit.
func B44Decompress(data []byte, channels []B44ChannelInfo, width, height, expectedSize int) ([]byte, error) {
	initB44Tables()

	plan, err := makeB44Plan(channels)
	if err != nil {
		return nil, err
	}
	if expectedSize != plan.interleaved {
		return nil, fmt.Errorf("%w: caller expects %d bytes, channel list describes %d",
			ErrB44DataSize, expectedSize, plan.interleaved)
	}

	halfBuf := make([]uint16, plan.halfSamples)
	rawBuf := make([]byte, plan.rawBytes)

	// Decompress each channel. Channels appear in the stream one after another,
	// each occupying either its block-compressed run (HALF) or its plain
	// uncompressed run (UINT, FLOAT).
	inOffset := 0
	for i := range plan.channels {
		c := &plan.channels[i]
		if c.ny == 0 {
			continue
		}
		nx := c.nx
		ny := c.ny

		if !c.isHalf {
			nBytes := ny * c.rowBytes
			if inOffset+nBytes > len(data) {
				return nil, fmt.Errorf("%w: channel %d needs %d uncompressed bytes at offset %d, only %d available",
					ErrB44Truncated, i, nBytes, inOffset, len(data)-inOffset)
			}
			copy(rawBuf[c.off:c.off+nBytes], data[inOffset:inOffset+nBytes])
			inOffset += nBytes
			continue
		}

		// HALF: decompress 4x4 blocks
		cd := halfBuf[c.off : c.off+nx*ny]
		isLinear := channels[i].IsLinear
		var s [16]uint16

		for y := 0; y < ny; y += 4 {
			for x := 0; x < nx; x += 4 {
				if inOffset+3 > len(data) {
					return nil, fmt.Errorf("%w: channel %d block at (%d,%d) starts past the end of %d bytes",
						ErrB44Truncated, i, x, y, len(data))
				}

				// Check for flat field (3-byte encoding)
				if data[inOffset+2] >= (13 << 2) {
					unpack3(data[inOffset:], &s)
					inOffset += 3
				} else {
					if inOffset+14 > len(data) {
						return nil, fmt.Errorf("%w: channel %d block at (%d,%d) is cut short by the end of %d bytes",
							ErrB44Truncated, i, x, y, len(data))
					}
					unpack14(data[inOffset:], &s)
					inOffset += 14
				}

				// Apply linear conversion if needed
				if isLinear {
					for j := 0; j < 16; j++ {
						s[j] = b44LogTable[s[j]]
					}
				}

				// Copy to output buffer, handling edge cases
				for by := 0; by < 4 && y+by < ny; by++ {
					for bx := 0; bx < 4 && x+bx < nx; bx++ {
						cd[(y+by)*nx+(x+bx)] = s[by*4+bx]
					}
				}
			}
		}
	}

	// Re-interleave the channel planes into the uncompressed chunk layout.
	result := make([]byte, plan.interleaved)
	outOffset := 0

	for y := 0; y < height; y++ {
		for i := range plan.channels {
			c := &plan.channels[i]
			if y >= c.ny {
				continue
			}
			if !c.isHalf {
				copy(result[outOffset:outOffset+c.rowBytes], rawBuf[c.off+y*c.rowBytes:])
				outOffset += c.rowBytes
				continue
			}
			src := halfBuf[c.off+y*c.nx:]
			for x := 0; x < c.nx; x++ {
				binary.LittleEndian.PutUint16(result[outOffset:], src[x])
				outOffset += 2
			}
		}
	}

	return result, nil
}
