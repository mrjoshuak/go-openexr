// Package compression provides compression algorithms for OpenEXR files.
package compression

import (
	"errors"
)

// RLE compression errors
var (
	ErrRLECorrupted = errors.New("compression: corrupted RLE data")
	ErrRLEOverflow  = errors.New("compression: RLE decompressed size overflow")
)

// RLE constants
const (
	// MinRunLength is the minimum run length that triggers encoding
	rleMinRunLength = 3
	// MaxRunLength is the maximum run length that can be encoded
	rleMaxRunLength = 127
)

// RLECompress compresses data using OpenEXR's RLE encoding.
//
// The control byte is a *signed* count, and its sign convention is the
// opposite of what is intuitive:
//   - Non-negative count n: the following single byte is repeated (n+1) times
//   - Negative count -n:    the following n bytes are copied literally
//
// For example:
//
//	[A, A, A, A, B, C, D] -> [3, A, -3, B, C, D]
//	(4 copies of A, then 3 literal bytes B, C, D)
//
// This mirrors rleCompress in OpenEXR's ImfRle.cpp. Inverting the two cases
// still round-trips against a matching decompressor, but produces a stream
// that no conforming OpenEXR reader can decode.
func RLECompress(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}

	// Worst case: each literal byte costs itself plus a control byte.
	dst := make([]byte, 0, len(src)+len(src)/2+1)

	runStart, runEnd, inEnd := 0, 1, len(src)
	for runStart < inEnd {
		for runEnd < inEnd && src[runStart] == src[runEnd] && runEnd-runStart-1 < rleMaxRunLength {
			runEnd++
		}

		if runEnd-runStart >= rleMinRunLength {
			// Compressible run: non-negative count, then the repeated byte.
			dst = append(dst, byte(runEnd-runStart-1), src[runStart])
			runStart = runEnd
		} else {
			// Incompressible run: extend until a run of 3 begins, then emit
			// the negated literal length followed by the literal bytes.
			for runEnd < inEnd &&
				((runEnd+1 >= inEnd || src[runEnd] != src[runEnd+1]) ||
					(runEnd+2 >= inEnd || src[runEnd+1] != src[runEnd+2])) &&
				runEnd-runStart < rleMaxRunLength {
				runEnd++
			}

			dst = append(dst, byte(runStart-runEnd))
			dst = append(dst, src[runStart:runEnd]...)
			runStart = runEnd
		}

		runEnd++
	}

	return dst
}

// RLEDecompressTo decompresses RLE-encoded data into a pre-allocated buffer.
// This avoids allocation when called repeatedly.
func RLEDecompressTo(src []byte, dst []byte) error {
	if len(src) == 0 {
		return nil
	}

	dstPos := 0
	expectedSize := len(dst)

	i := 0
	for i < len(src) {
		count := int(int8(src[i]))
		i++

		if count < 0 {
			// Literal: copy the next (-count) bytes verbatim.
			literalLength := -count
			if i+literalLength > len(src) {
				return ErrRLECorrupted
			}
			if dstPos+literalLength > expectedSize {
				return ErrRLEOverflow
			}
			copy(dst[dstPos:], src[i:i+literalLength])
			dstPos += literalLength
			i += literalLength
		} else {
			// Run: repeat the next byte (count + 1) times.
			runLength := count + 1
			if i >= len(src) {
				return ErrRLECorrupted
			}
			if dstPos+runLength > expectedSize {
				return ErrRLEOverflow
			}
			val := src[i]
			i++
			for end := dstPos + runLength; dstPos < end; dstPos++ {
				dst[dstPos] = val
			}
		}
	}

	if dstPos != expectedSize {
		return ErrRLECorrupted
	}

	return nil
}

// RLEDecompress decompresses RLE-encoded data.
// The expectedSize parameter is the expected decompressed size,
// which is used to preallocate the output buffer and validate the result.
func RLEDecompress(src []byte, expectedSize int) ([]byte, error) {
	if len(src) == 0 {
		if expectedSize != 0 {
			return nil, ErrRLECorrupted
		}
		return nil, nil
	}

	// Pre-allocate exact size and decode in place. This delegates to
	// RLEDecompressTo rather than repeating the loop: the two copies of this
	// decoder previously drifted apart, which is how an inverted control-byte
	// convention survived in one of them.
	dst := make([]byte, expectedSize)
	if err := RLEDecompressTo(src, dst); err != nil {
		return nil, err
	}

	return dst, nil
}
