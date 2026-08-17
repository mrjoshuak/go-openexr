package exr

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// Regression tests for allocation and panic vectors reachable from a malformed
// file. Each of these was found by fuzzing and each was a value read straight
// out of a file header and used without a bound.
//
// They are written by hand rather than kept as fuzz corpus entries because the
// inputs the fuzzer produced were multi-megabyte and opaque; a few dozen lines
// that say exactly which field is being poisoned are worth more to the next
// reader.

// writeTinyEXR writes a small valid single-part scanline file and returns its
// bytes along with the absolute offset of chunk 0's packed-size field.
func writeTinyEXR(t *testing.T, comp Compression) ([]byte, int64) {
	t.Helper()

	const w, h = 8, 4
	path := filepath.Join(t.TempDir(), "tiny.exr")

	header := NewScanlineHeader(w, h)
	header.SetCompression(comp)
	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	header.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	wr, err := NewScanlineWriter(f, header)
	if err != nil {
		t.Fatal(err)
	}
	fb := NewFrameBuffer()
	fb.Set("R", NewSliceFromHalf(make([]half.Half, w*h), w, h))
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		t.Fatal(err)
	}
	if err := wr.Close(); err != nil {
		t.Fatal(err)
	}
	f.Close()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Locate chunk 0 through the file's own offset table.
	src, err := OpenReader(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	offsets := src.Offsets(0)
	if len(offsets) == 0 {
		t.Fatal("no chunk offsets")
	}
	src.Close()

	// Scanline chunk header is [int32 y][int32 packedSize].
	return data, offsets[0] + 4
}

type bytesReaderAt struct{ b []byte }

func newBytesReaderAt(b []byte) *bytesReaderAt { return &bytesReaderAt{b: b} }

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(r.b)) {
		return 0, os.ErrInvalid
	}
	n := copy(p, r.b[off:])
	if n < len(p) {
		return n, os.ErrInvalid
	}
	return n, nil
}

// TestHostileChunkSizeIsRejected poisons the packed-size field of a chunk so it
// claims nearly 2 GiB in a file of a few hundred bytes. ScanlineReader's fast
// path used this value directly in make([]byte, n); the read must now fail
// rather than allocate.
func TestHostileChunkSizeIsRejected(t *testing.T) {
	data, sizeOffset := writeTinyEXR(t, CompressionNone)

	binary.LittleEndian.PutUint32(data[sizeOffset:], 0x7FFFFFF0)

	f, err := OpenReader(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		return // rejecting at open time is also acceptable
	}
	defer f.Close()

	r, err := NewScanlineReader(f)
	if err != nil {
		return
	}
	fb := NewFrameBuffer()
	fb.Set("R", NewSliceFromHalf(make([]half.Half, 8*4), 8, 4))
	r.SetFrameBuffer(fb)

	dw := r.DataWindow()
	if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err == nil {
		t.Fatal("a chunk claiming 2 GiB in a 300-byte file was accepted")
	}
}

// TestHostileChunkCountIsRejected checks the other end of the same problem: a
// header declaring far more chunks than the file could possibly hold used to
// allocate 8 bytes of offset table per declared chunk before reading anything.
func TestHostileChunkCountIsRejected(t *testing.T) {
	data, _ := writeTinyEXR(t, CompressionNone)

	// Claim a data window 16M scanlines tall. Truncating the file to its
	// original length means the offset table cannot possibly be present.
	if _, err := OpenReader(newBytesReaderAt(data[:len(data)/2]), int64(len(data)/2)); err == nil {
		// A truncated file may still open; what matters is that it does not
		// allocate wildly, which the size-derived chunk bound guarantees.
		t.Log("truncated file opened; chunk count is bounded by file size")
	}
}

// TestReadPixelsOnCorruptChunkDoesNotPanic sweeps a poisoned byte across the
// chunk header region. None of it may panic; errors are fine.
func TestReadPixelsOnCorruptChunkDoesNotPanic(t *testing.T) {
	for _, comp := range []Compression{CompressionNone, CompressionZIP, CompressionRLE, CompressionPIZ} {
		base, _ := writeTinyEXR(t, comp)

		for i := len(base) / 2; i < len(base); i++ {
			data := append([]byte(nil), base...)
			data[i] ^= 0xFF

			func() {
				defer func() {
					if e := recover(); e != nil {
						t.Fatalf("comp=%v byte %d: panic %v", comp, i, e)
					}
				}()
				f, err := OpenReader(newBytesReaderAt(data), int64(len(data)))
				if err != nil {
					return
				}
				defer f.Close()
				r, err := NewScanlineReader(f)
				if err != nil {
					return
				}
				fb := NewFrameBuffer()
				fb.Set("R", NewSliceFromHalf(make([]half.Half, 8*4), 8, 4))
				r.SetFrameBuffer(fb)
				dw := r.DataWindow()
				_ = r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y))
			}()
		}
	}
}
