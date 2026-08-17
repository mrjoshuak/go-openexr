// Package compression provides compression algorithms for OpenEXR files.
package compression

import (
	"bytes"
	"errors"
	"io"
	"sync"

	"github.com/klauspost/compress/zlib"
)

// ZIP compression errors
var (
	ErrZIPCorrupted = errors.New("compression: corrupted ZIP data")
	ErrZIPOverflow  = errors.New("compression: ZIP decompressed size overflow")
)

// CompressionLevel represents a zlib compression level.
// Valid values are -2 to 9, where:
//   - -2: Huffman-only compression (klauspost extension)
//   - -1: Default compression (level 6)
//   - 0: No compression (store)
//   - 1: Best speed
//   - 9: Best compression
type CompressionLevel int

// Standard compression levels
const (
	CompressionLevelHuffmanOnly CompressionLevel = -2 // Huffman-only (fastest, klauspost)
	CompressionLevelDefault     CompressionLevel = -1 // Default (level 6)
	CompressionLevelNone        CompressionLevel = 0  // No compression
	CompressionLevelBestSpeed   CompressionLevel = 1  // Best speed
	CompressionLevelBestSize    CompressionLevel = 9  // Best compression
)

// FLevel represents the compression level category from zlib header.
// This is a 2-bit field in the zlib header indicating the general
// compression level category, not the exact level.
type FLevel int

const (
	FLevelFastest FLevel = 0 // Fastest algorithm (levels -2, 0, 1)
	FLevelFast    FLevel = 1 // Fast algorithm (levels 2, 3, 4, 5)
	FLevelDefault FLevel = 2 // Default algorithm (levels 6, -1)
	FLevelBest    FLevel = 3 // Maximum compression (levels 7, 8, 9)
)

// FLevelToLevel returns a representative compression level for an FLevel.
// Since FLEVEL only encodes 4 categories, we return a typical level for each:
//   - FLevelFastest -> 1 (best speed)
//   - FLevelFast -> 4 (middle of fast range)
//   - FLevelDefault -> 6 (default)
//   - FLevelBest -> 9 (best compression)
func FLevelToLevel(fl FLevel) CompressionLevel {
	switch fl {
	case FLevelFastest:
		return 1
	case FLevelFast:
		return 4
	case FLevelDefault:
		return CompressionLevelDefault
	case FLevelBest:
		return 9
	default:
		return CompressionLevelDefault
	}
}

// DetectZlibFLevel extracts the FLEVEL from zlib compressed data.
// Returns the FLevel and true if successful, or 0 and false if the
// data is too short or has an invalid header.
func DetectZlibFLevel(data []byte) (FLevel, bool) {
	if len(data) < 2 {
		return 0, false
	}

	// Validate zlib header
	cmf := data[0]
	flg := data[1]

	// Check compression method (must be 8 = deflate)
	if cmf&0x0f != 8 {
		return 0, false
	}

	// Check header checksum
	h := uint16(cmf)<<8 | uint16(flg)
	if h%31 != 0 {
		return 0, false
	}

	// Extract FLEVEL from bits 6-7 of FLG byte
	flevel := FLevel((flg >> 6) & 0x03)
	return flevel, true
}

// Pool for zlib writers to reduce allocations.
// Each pooled item contains both the writer and its destination buffer.
type zlibWriterPoolItem struct {
	writer *zlib.Writer
	buf    *bytes.Buffer
}

var zlibWriterPool = sync.Pool{
	New: func() any {
		buf := new(bytes.Buffer)
		w, _ := zlib.NewWriterLevel(buf, zlib.DefaultCompression)
		return &zlibWriterPoolItem{writer: w, buf: buf}
	},
}

// ZIPCompress compresses data using OpenEXR's ZIP encoding at default level.
//
// The ZIP format in OpenEXR is:
//  1. Reorder bytes into even and odd halves
//  2. Apply the horizontal differencing predictor to the reordered stream
//  3. Compress with zlib
//
// The order matters: the predictor runs over the reordered bytes, not the
// original ones. Doing it the other way round is self-inverse, so it round-trips
// against a matching decompressor while producing a stream no conforming
// OpenEXR reader can decode.
//
// Note: the reorder and predictor steps are applied by the caller (see
// predictor.DeconstructBytes). This function only performs the zlib step.
func ZIPCompress(src []byte) ([]byte, error) {
	return ZIPCompressLevel(src, CompressionLevelDefault)
}

// ZIPCompressLevel compresses data using the specified compression level.
// Level should be -2 to 9:
//   - -2: Huffman-only (fastest, klauspost extension)
//   - -1: Default compression (level 6)
//   - 0: No compression
//   - 1-9: Increasing compression (1=fastest, 9=best)
//
// For deterministic round-trip, use the same level that was detected
// from the original compressed data via DetectZlibFLevel.
func ZIPCompressLevel(src []byte, level CompressionLevel) ([]byte, error) {
	if len(src) == 0 {
		return nil, nil
	}

	// Use pool for default level (most common case)
	if level == CompressionLevelDefault {
		item := zlibWriterPool.Get().(*zlibWriterPoolItem)
		item.buf.Reset()
		item.writer.Reset(item.buf)

		if _, err := item.writer.Write(src); err != nil {
			item.writer.Close()
			zlibWriterPool.Put(item)
			return nil, err
		}

		if err := item.writer.Close(); err != nil {
			zlibWriterPool.Put(item)
			return nil, err
		}

		result := make([]byte, item.buf.Len())
		copy(result, item.buf.Bytes())
		zlibWriterPool.Put(item)

		return result, nil
	}

	// Non-default level: create temporary writer
	buf := new(bytes.Buffer)
	w, err := zlib.NewWriterLevel(buf, int(level))
	if err != nil {
		return nil, err
	}

	if _, err := w.Write(src); err != nil {
		w.Close()
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// zlibReaderPoolItem wraps a zlib reader for pooling
type zlibReaderPoolItem struct {
	reader io.ReadCloser
	srcBuf *bytes.Reader
}

var zlibReaderPool = sync.Pool{
	New: func() any {
		return &zlibReaderPoolItem{
			srcBuf: bytes.NewReader(nil),
		}
	},
}

// ZIPDecompress decompresses ZIP-encoded data.
// The expectedSize parameter is the expected decompressed size.
func ZIPDecompress(src []byte, expectedSize int) ([]byte, error) {
	dst, _, err := ZIPDecompressWithLevel(src, expectedSize)
	return dst, err
}

// ZIPDecompressWithLevel decompresses ZIP-encoded data and returns the
// detected FLevel from the zlib header. This allows callers to preserve
// the compression level for deterministic round-trip.
func ZIPDecompressWithLevel(src []byte, expectedSize int) ([]byte, FLevel, error) {
	if len(src) == 0 {
		if expectedSize != 0 {
			return nil, FLevelDefault, ErrZIPCorrupted
		}
		return nil, FLevelDefault, nil
	}

	// Detect FLevel before decompression
	flevel, ok := DetectZlibFLevel(src)
	if !ok {
		return nil, FLevelDefault, ErrZIPCorrupted
	}

	dst := make([]byte, expectedSize)
	if err := ZIPDecompressTo(dst, src); err != nil {
		return nil, flevel, err
	}
	return dst, flevel, nil
}

// ZIPDecompressTo decompresses ZIP-encoded data into the provided buffer.
// The dst buffer must be exactly the right size for the decompressed data.
func ZIPDecompressTo(dst, src []byte) error {
	if len(src) == 0 {
		if len(dst) != 0 {
			return ErrZIPCorrupted
		}
		return nil
	}

	// Get pooled reader
	item := zlibReaderPool.Get().(*zlibReaderPoolItem)
	item.srcBuf.Reset(src)

	var err error
	if item.reader == nil {
		item.reader, err = zlib.NewReader(item.srcBuf)
		if err != nil {
			zlibReaderPool.Put(item)
			return ErrZIPCorrupted
		}
	} else {
		// Reset existing reader - zlib.Resetter interface
		if resetter, ok := item.reader.(zlib.Resetter); ok {
			err = resetter.Reset(item.srcBuf, nil)
			if err != nil {
				// If reset fails, create new reader
				item.reader.Close()
				item.reader, err = zlib.NewReader(item.srcBuf)
				if err != nil {
					zlibReaderPool.Put(item)
					return ErrZIPCorrupted
				}
			}
		} else {
			// Fallback: close and create new
			item.reader.Close()
			item.reader, err = zlib.NewReader(item.srcBuf)
			if err != nil {
				zlibReaderPool.Put(item)
				return ErrZIPCorrupted
			}
		}
	}

	n, err := io.ReadFull(item.reader, dst)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		zlibReaderPool.Put(item)
		return ErrZIPCorrupted
	}

	zlibReaderPool.Put(item)

	if n != len(dst) {
		return ErrZIPCorrupted
	}

	return nil
}
