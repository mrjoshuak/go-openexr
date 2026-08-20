package compression

import (
	"errors"
	"math"
)

// PXR24 errors
var (
	ErrPXR24ImageTooLarge = errors.New("compression: PXR24 image too large")
)

// PXR24 compression for OpenEXR.
// Converts 32-bit floats to 24-bit representation (lossy), then uses
// differencing and zlib compression.

// PixelType constants matching exr package
const (
	pxr24PixelTypeUint  = 0
	pxr24PixelTypeHalf  = 1
	pxr24PixelTypeFloat = 2
)

// ChannelInfo describes a channel for PXR24 compression
type ChannelInfo struct {
	Type   int // pxr24PixelTypeUint, pxr24PixelTypeHalf, pxr24PixelTypeFloat
	Width  int
	Height int

	// YSampling and FirstY say which of the chunk's rows this channel actually
	// stores. A channel with YSampling above one holds a sample only where the
	// image row is a multiple of it, so a chunk carries fewer of its rows than
	// of a full-rate channel's, and the rows are not the same rows.
	//
	// Zero means every row, which is what an unsubsampled channel wants and
	// what every caller before this passed. Without it the row loop consumed a
	// row per channel per scanline and ran off the end of a chunk holding
	// fewer: a vertically subsampled PXR24 chunk panicked with "index out of
	// range [768] with length 768".
	YSampling int
	FirstY    int
}

// storesRow reports whether this channel holds a sample on the given row of the
// chunk, counting from the chunk's first row.
func (c ChannelInfo) storesRow(row int) bool {
	if c.YSampling <= 1 {
		return true
	}
	return (c.FirstY+row)%c.YSampling == 0
}

// floatToFloat24 converts a 32-bit float to a 24-bit representation.
// The 24-bit format keeps sign, exponent, and top 15 bits of mantissa.
func floatToFloat24(f float32) uint32 {
	bits := math.Float32bits(f)

	// Disassemble into sign, exponent, and mantissa
	s := bits & 0x80000000
	e := bits & 0x7f800000
	m := bits & 0x007fffff

	if e == 0x7f800000 {
		// Infinity or NaN
		if m != 0 {
			// NaN: preserve sign and top 15 bits of mantissa
			m >>= 8
			i := (e >> 8) | m
			if m == 0 {
				// Would turn into infinity, set at least one bit
				i |= 1
			}
			return (s >> 8) | i
		}
		// Infinity
		return (s >> 8) | (e >> 8)
	}

	// Finite: round the mantissa to 15 bits
	i := ((e | m) + (m & 0x00000080)) >> 8

	if i >= 0x7f8000 {
		// Overflow from rounding, truncate instead
		i = (e | m) >> 8
	}

	return (s >> 8) | i
}

// float24ToFloat32 converts a 24-bit representation back to 32-bit float.
// The low 8 bits of the mantissa are set to zero.
func float24ToFloat32(f24 uint32) float32 {
	// Expand 24-bit to 32-bit by shifting left 8 bits
	bits := f24 << 8
	return math.Float32frombits(bits)
}

// PXR24Compress compresses pixel data using PXR24 compression.
// data is the raw pixel data (channels are sorted by name, then by scanline).
// channels describes the pixel type for each channel.
// Returns the compressed data.
func PXR24Compress(data []byte, channels []ChannelInfo, width, height int) ([]byte, error) {
	// Calculate output size using int64 to prevent overflow on 32-bit systems
	var outSize64 int64
	for _, ch := range channels {
		chSize := int64(ch.Width) * int64(ch.Height)
		switch ch.Type {
		case pxr24PixelTypeUint:
			outSize64 += chSize * 4
		case pxr24PixelTypeHalf:
			outSize64 += chSize * 2
		case pxr24PixelTypeFloat:
			outSize64 += chSize * 3 // 24-bit = 3 bytes
		}
	}
	// Check for overflow (max 1GB allocation)
	const maxPXR24Size = 1024 * 1024 * 1024
	if outSize64 > maxPXR24Size || outSize64 < 0 {
		return nil, ErrPXR24ImageTooLarge
	}
	outSize := int(outSize64)

	scratch := make([]byte, outSize)
	out := 0
	in := 0

	// Process each scanline
	for y := 0; y < height; y++ {
		for _, ch := range channels {
			if ch.Height == 0 || !ch.storesRow(y) {
				continue
			}
			w := ch.Width

			switch ch.Type {
			case pxr24PixelTypeUint:
				// 4 planes with differencing
				ptr0 := out
				ptr1 := out + w
				ptr2 := out + w*2
				ptr3 := out + w*3
				out += w * 4

				var prevPixel uint32
				for x := 0; x < w; x++ {
					pixel := uint32(data[in]) |
						uint32(data[in+1])<<8 |
						uint32(data[in+2])<<16 |
						uint32(data[in+3])<<24
					in += 4

					diff := pixel - prevPixel
					prevPixel = pixel

					scratch[ptr0] = byte(diff >> 24)
					scratch[ptr1] = byte(diff >> 16)
					scratch[ptr2] = byte(diff >> 8)
					scratch[ptr3] = byte(diff)
					ptr0++
					ptr1++
					ptr2++
					ptr3++
				}

			case pxr24PixelTypeHalf:
				// 2 planes with differencing
				ptr0 := out
				ptr1 := out + w
				out += w * 2

				var prevPixel uint32
				for x := 0; x < w; x++ {
					pixel := uint32(data[in]) | uint32(data[in+1])<<8
					in += 2

					diff := pixel - prevPixel
					prevPixel = pixel

					scratch[ptr0] = byte(diff >> 8)
					scratch[ptr1] = byte(diff)
					ptr0++
					ptr1++
				}

			case pxr24PixelTypeFloat:
				// Convert to 24-bit, then 3 planes with differencing
				ptr0 := out
				ptr1 := out + w
				ptr2 := out + w*2
				out += w * 3

				var prevPixel uint32
				for x := 0; x < w; x++ {
					bits := uint32(data[in]) |
						uint32(data[in+1])<<8 |
						uint32(data[in+2])<<16 |
						uint32(data[in+3])<<24
					in += 4

					pixel24 := floatToFloat24(math.Float32frombits(bits))

					diff := pixel24 - prevPixel
					prevPixel = pixel24

					scratch[ptr0] = byte(diff >> 16)
					scratch[ptr1] = byte(diff >> 8)
					scratch[ptr2] = byte(diff)
					ptr0++
					ptr1++
					ptr2++
				}
			}
		}
	}

	// Zlib compress
	compressed, err := ZIPCompress(scratch[:out])
	if err != nil {
		return nil, err
	}

	// If compressed is larger than original, return uncompressed
	if len(compressed) >= len(data) {
		result := make([]byte, len(data))
		copy(result, data)
		return result, nil
	}

	return compressed, nil
}

// PXR24Decompress decompresses PXR24-compressed data.
// channels describes the pixel type for each channel.
// Returns the decompressed pixel data.
func PXR24Decompress(data []byte, channels []ChannelInfo, width, height int, expectedSize int) ([]byte, error) {
	// First try to zlib decompress
	// Calculate size of intermediate format (after zlib, before expanding)
	scratchSize := 0
	for _, ch := range channels {
		switch ch.Type {
		case pxr24PixelTypeUint:
			scratchSize += ch.Width * ch.Height * 4
		case pxr24PixelTypeHalf:
			scratchSize += ch.Width * ch.Height * 2
		case pxr24PixelTypeFloat:
			scratchSize += ch.Width * ch.Height * 3
		}
	}

	scratch, err := ZIPDecompress(data, scratchSize)
	if err != nil {
		// If decompression fails, data might be uncompressed
		if len(data) == expectedSize {
			result := make([]byte, len(data))
			copy(result, data)
			return result, nil
		}
		return nil, err
	}

	// Reconstruct pixel data from planes
	result := make([]byte, expectedSize)
	in := 0
	out := 0

	for y := 0; y < height; y++ {
		for _, ch := range channels {
			if ch.Height == 0 || !ch.storesRow(y) {
				continue
			}
			w := ch.Width

			switch ch.Type {
			case pxr24PixelTypeUint:
				// 4 planes with differencing - use index-based access for better optimization
				base0 := in
				base1 := in + w
				base2 := in + w*2
				base3 := in + w*3
				in += w * 4

				var pixel uint32
				// Unroll 4 pixels per iteration
				x := 0
				for ; x+4 <= w; x += 4 {
					diff := uint32(scratch[base0+x])<<24 | uint32(scratch[base1+x])<<16 |
						uint32(scratch[base2+x])<<8 | uint32(scratch[base3+x])
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)
					result[out+2] = byte(pixel >> 16)
					result[out+3] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+1])<<24 | uint32(scratch[base1+x+1])<<16 |
						uint32(scratch[base2+x+1])<<8 | uint32(scratch[base3+x+1])
					pixel += diff
					result[out+4] = byte(pixel)
					result[out+5] = byte(pixel >> 8)
					result[out+6] = byte(pixel >> 16)
					result[out+7] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+2])<<24 | uint32(scratch[base1+x+2])<<16 |
						uint32(scratch[base2+x+2])<<8 | uint32(scratch[base3+x+2])
					pixel += diff
					result[out+8] = byte(pixel)
					result[out+9] = byte(pixel >> 8)
					result[out+10] = byte(pixel >> 16)
					result[out+11] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+3])<<24 | uint32(scratch[base1+x+3])<<16 |
						uint32(scratch[base2+x+3])<<8 | uint32(scratch[base3+x+3])
					pixel += diff
					result[out+12] = byte(pixel)
					result[out+13] = byte(pixel >> 8)
					result[out+14] = byte(pixel >> 16)
					result[out+15] = byte(pixel >> 24)

					out += 16
				}
				for ; x < w; x++ {
					diff := uint32(scratch[base0+x])<<24 | uint32(scratch[base1+x])<<16 |
						uint32(scratch[base2+x])<<8 | uint32(scratch[base3+x])
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)
					result[out+2] = byte(pixel >> 16)
					result[out+3] = byte(pixel >> 24)
					out += 4
				}

			case pxr24PixelTypeHalf:
				// 2 planes with differencing - use index-based access
				base0 := in
				base1 := in + w
				in += w * 2

				var pixel uint32
				x := 0
				for ; x+4 <= w; x += 4 {
					diff := uint32(scratch[base0+x])<<8 | uint32(scratch[base1+x])
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)

					diff = uint32(scratch[base0+x+1])<<8 | uint32(scratch[base1+x+1])
					pixel += diff
					result[out+2] = byte(pixel)
					result[out+3] = byte(pixel >> 8)

					diff = uint32(scratch[base0+x+2])<<8 | uint32(scratch[base1+x+2])
					pixel += diff
					result[out+4] = byte(pixel)
					result[out+5] = byte(pixel >> 8)

					diff = uint32(scratch[base0+x+3])<<8 | uint32(scratch[base1+x+3])
					pixel += diff
					result[out+6] = byte(pixel)
					result[out+7] = byte(pixel >> 8)

					out += 8
				}
				for ; x < w; x++ {
					diff := uint32(scratch[base0+x])<<8 | uint32(scratch[base1+x])
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)
					out += 2
				}

			case pxr24PixelTypeFloat:
				// 3 planes with differencing -> expand to 32-bit float
				base0 := in
				base1 := in + w
				base2 := in + w*2
				in += w * 3

				var pixel uint32
				x := 0
				for ; x+4 <= w; x += 4 {
					diff := uint32(scratch[base0+x])<<24 | uint32(scratch[base1+x])<<16 |
						uint32(scratch[base2+x])<<8
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)
					result[out+2] = byte(pixel >> 16)
					result[out+3] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+1])<<24 | uint32(scratch[base1+x+1])<<16 |
						uint32(scratch[base2+x+1])<<8
					pixel += diff
					result[out+4] = byte(pixel)
					result[out+5] = byte(pixel >> 8)
					result[out+6] = byte(pixel >> 16)
					result[out+7] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+2])<<24 | uint32(scratch[base1+x+2])<<16 |
						uint32(scratch[base2+x+2])<<8
					pixel += diff
					result[out+8] = byte(pixel)
					result[out+9] = byte(pixel >> 8)
					result[out+10] = byte(pixel >> 16)
					result[out+11] = byte(pixel >> 24)

					diff = uint32(scratch[base0+x+3])<<24 | uint32(scratch[base1+x+3])<<16 |
						uint32(scratch[base2+x+3])<<8
					pixel += diff
					result[out+12] = byte(pixel)
					result[out+13] = byte(pixel >> 8)
					result[out+14] = byte(pixel >> 16)
					result[out+15] = byte(pixel >> 24)

					out += 16
				}
				for ; x < w; x++ {
					diff := uint32(scratch[base0+x])<<24 | uint32(scratch[base1+x])<<16 |
						uint32(scratch[base2+x])<<8
					pixel += diff
					result[out] = byte(pixel)
					result[out+1] = byte(pixel >> 8)
					result[out+2] = byte(pixel >> 16)
					result[out+3] = byte(pixel >> 24)
					out += 4
				}
			}
		}
	}

	return result, nil
}
