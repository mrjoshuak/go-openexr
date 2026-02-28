// Package xdr provides little-endian binary encoding and decoding utilities
// for reading and writing OpenEXR file data.
//
// OpenEXR uses little-endian byte order for all multi-byte values throughout
// the file format. This package provides efficient, bounds-checked readers
// and writers for the primitive types used in OpenEXR files.
package xdr

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

var (
	// ErrShortBuffer is returned when a read or write operation cannot complete
	// because there isn't enough space in the buffer.
	ErrShortBuffer = errors.New("xdr: buffer too short")

	// ErrNegativeSize is returned when a size parameter is negative.
	ErrNegativeSize = errors.New("xdr: negative size")
)

// ByteOrder is the byte order used by OpenEXR files.
var ByteOrder = binary.LittleEndian

// Reader provides efficient little-endian binary reading from a byte slice.
// It maintains a read position and provides bounds checking on all operations.
type Reader struct {
	data []byte
	pos  int
}

// NewReader creates a Reader from a byte slice.
func NewReader(data []byte) *Reader {
	return &Reader{data: data, pos: 0}
}

// Len returns the number of unread bytes.
func (r *Reader) Len() int {
	if r.pos >= len(r.data) {
		return 0
	}
	return len(r.data) - r.pos
}

// Pos returns the current read position.
func (r *Reader) Pos() int {
	return r.pos
}

// Reset resets the reader to the beginning of the data.
func (r *Reader) Reset() {
	r.pos = 0
}

// SetPos sets the read position. Returns an error if the position is out of bounds.
func (r *Reader) SetPos(pos int) error {
	if pos < 0 || pos > len(r.data) {
		return ErrShortBuffer
	}
	r.pos = pos
	return nil
}

// Skip advances the read position by n bytes.
func (r *Reader) Skip(n int) error {
	if n < 0 {
		return ErrNegativeSize
	}
	if r.pos+n > len(r.data) {
		return ErrShortBuffer
	}
	r.pos += n
	return nil
}

// ReadByte reads a single byte.
func (r *Reader) ReadByte() (byte, error) {
	if r.pos >= len(r.data) {
		return 0, ErrShortBuffer
	}
	b := r.data[r.pos]
	r.pos++
	return b, nil
}

// ReadBytes reads n bytes into a new slice.
func (r *Reader) ReadBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrNegativeSize
	}
	if r.pos+n > len(r.data) {
		return nil, ErrShortBuffer
	}
	result := make([]byte, n)
	copy(result, r.data[r.pos:r.pos+n])
	r.pos += n
	return result, nil
}

// ReadBytesInto reads n bytes into the provided slice.
func (r *Reader) ReadBytesInto(dst []byte) error {
	n := len(dst)
	if r.pos+n > len(r.data) {
		return ErrShortBuffer
	}
	copy(dst, r.data[r.pos:r.pos+n])
	r.pos += n
	return nil
}

// ReadUint8 reads an unsigned 8-bit integer.
func (r *Reader) ReadUint8() (uint8, error) {
	return r.ReadByte()
}

// ReadInt8 reads a signed 8-bit integer.
func (r *Reader) ReadInt8() (int8, error) {
	b, err := r.ReadByte()
	return int8(b), err
}

// ReadUint16 reads an unsigned 16-bit integer in little-endian order.
func (r *Reader) ReadUint16() (uint16, error) {
	if r.pos+2 > len(r.data) {
		return 0, ErrShortBuffer
	}
	v := ByteOrder.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

// ReadInt16 reads a signed 16-bit integer in little-endian order.
func (r *Reader) ReadInt16() (int16, error) {
	v, err := r.ReadUint16()
	return int16(v), err
}

// ReadUint32 reads an unsigned 32-bit integer in little-endian order.
func (r *Reader) ReadUint32() (uint32, error) {
	if r.pos+4 > len(r.data) {
		return 0, ErrShortBuffer
	}
	v := ByteOrder.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

// ReadInt32 reads a signed 32-bit integer in little-endian order.
func (r *Reader) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	return int32(v), err
}

// ReadUint64 reads an unsigned 64-bit integer in little-endian order.
func (r *Reader) ReadUint64() (uint64, error) {
	if r.pos+8 > len(r.data) {
		return 0, ErrShortBuffer
	}
	v := ByteOrder.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

// ReadInt64 reads a signed 64-bit integer in little-endian order.
func (r *Reader) ReadInt64() (int64, error) {
	v, err := r.ReadUint64()
	return int64(v), err
}

// ReadFloat32 reads a 32-bit IEEE 754 floating-point number.
func (r *Reader) ReadFloat32() (float32, error) {
	v, err := r.ReadUint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

// ReadFloat64 reads a 64-bit IEEE 754 floating-point number.
func (r *Reader) ReadFloat64() (float64, error) {
	v, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// ReadString reads a null-terminated string.
// The null terminator is consumed but not included in the result.
func (r *Reader) ReadString() (string, error) {
	start := r.pos
	for r.pos < len(r.data) {
		if r.data[r.pos] == 0 {
			s := string(r.data[start:r.pos])
			r.pos++ // Skip the null terminator
			return s, nil
		}
		r.pos++
	}
	// Reset position on failure
	r.pos = start
	return "", ErrShortBuffer
}

// ReadStringN reads a string of at most n bytes, stopping at the first null byte.
// The null terminator (if present) is consumed but not included in the result.
// If no null byte is found within n bytes, all n bytes are returned as the string.
func (r *Reader) ReadStringN(n int) (string, error) {
	if n < 0 {
		return "", ErrNegativeSize
	}
	if r.pos+n > len(r.data) {
		return "", ErrShortBuffer
	}

	// Find null terminator within n bytes
	end := r.pos + n
	for i := r.pos; i < end; i++ {
		if r.data[i] == 0 {
			s := string(r.data[r.pos:i])
			r.pos = end // Always consume n bytes
			return s, nil
		}
	}

	// No null found, return all n bytes
	s := string(r.data[r.pos:end])
	r.pos = end
	return s, nil
}

// Writer provides efficient little-endian binary writing to a byte slice.
// It maintains a write position and provides bounds checking on all operations.
type Writer struct {
	data []byte
	pos  int
}

// NewWriter creates a Writer from a byte slice.
func NewWriter(data []byte) *Writer {
	return &Writer{data: data, pos: 0}
}

// Len returns the number of bytes that can still be written.
func (w *Writer) Len() int {
	if w.pos >= len(w.data) {
		return 0
	}
	return len(w.data) - w.pos
}

// Pos returns the current write position.
func (w *Writer) Pos() int {
	return w.pos
}

// Reset resets the writer to the beginning of the buffer.
func (w *Writer) Reset() {
	w.pos = 0
}

// SetPos sets the write position. Returns an error if the position is out of bounds.
func (w *Writer) SetPos(pos int) error {
	if pos < 0 || pos > len(w.data) {
		return ErrShortBuffer
	}
	w.pos = pos
	return nil
}

// Skip advances the write position by n bytes (leaving them unchanged).
func (w *Writer) Skip(n int) error {
	if n < 0 {
		return ErrNegativeSize
	}
	if w.pos+n > len(w.data) {
		return ErrShortBuffer
	}
	w.pos += n
	return nil
}

// WriteByte writes a single byte.
func (w *Writer) WriteByte(b byte) error {
	if w.pos >= len(w.data) {
		return ErrShortBuffer
	}
	w.data[w.pos] = b
	w.pos++
	return nil
}

// WriteBytes writes a byte slice.
func (w *Writer) WriteBytes(b []byte) error {
	if w.pos+len(b) > len(w.data) {
		return ErrShortBuffer
	}
	copy(w.data[w.pos:], b)
	w.pos += len(b)
	return nil
}

// WriteUint8 writes an unsigned 8-bit integer.
func (w *Writer) WriteUint8(v uint8) error {
	return w.WriteByte(v)
}

// WriteInt8 writes a signed 8-bit integer.
func (w *Writer) WriteInt8(v int8) error {
	return w.WriteByte(byte(v))
}

// WriteUint16 writes an unsigned 16-bit integer in little-endian order.
func (w *Writer) WriteUint16(v uint16) error {
	if w.pos+2 > len(w.data) {
		return ErrShortBuffer
	}
	ByteOrder.PutUint16(w.data[w.pos:], v)
	w.pos += 2
	return nil
}

// WriteInt16 writes a signed 16-bit integer in little-endian order.
func (w *Writer) WriteInt16(v int16) error {
	return w.WriteUint16(uint16(v))
}

// WriteUint32 writes an unsigned 32-bit integer in little-endian order.
func (w *Writer) WriteUint32(v uint32) error {
	if w.pos+4 > len(w.data) {
		return ErrShortBuffer
	}
	ByteOrder.PutUint32(w.data[w.pos:], v)
	w.pos += 4
	return nil
}

// WriteInt32 writes a signed 32-bit integer in little-endian order.
func (w *Writer) WriteInt32(v int32) error {
	return w.WriteUint32(uint32(v))
}

// WriteUint64 writes an unsigned 64-bit integer in little-endian order.
func (w *Writer) WriteUint64(v uint64) error {
	if w.pos+8 > len(w.data) {
		return ErrShortBuffer
	}
	ByteOrder.PutUint64(w.data[w.pos:], v)
	w.pos += 8
	return nil
}

// WriteInt64 writes a signed 64-bit integer in little-endian order.
func (w *Writer) WriteInt64(v int64) error {
	return w.WriteUint64(uint64(v))
}

// WriteFloat32 writes a 32-bit IEEE 754 floating-point number.
func (w *Writer) WriteFloat32(v float32) error {
	return w.WriteUint32(math.Float32bits(v))
}

// WriteFloat64 writes a 64-bit IEEE 754 floating-point number.
func (w *Writer) WriteFloat64(v float64) error {
	return w.WriteUint64(math.Float64bits(v))
}

// WriteString writes a null-terminated string.
func (w *Writer) WriteString(s string) error {
	// Need space for string + null terminator
	if w.pos+len(s)+1 > len(w.data) {
		return ErrShortBuffer
	}
	copy(w.data[w.pos:], s)
	w.pos += len(s)
	w.data[w.pos] = 0
	w.pos++
	return nil
}

// WriteStringN writes a string padded or truncated to exactly n bytes.
// If the string is shorter than n bytes, it is null-padded.
// If the string is longer than n bytes, it is truncated (no null terminator guaranteed).
func (w *Writer) WriteStringN(s string, n int) error {
	if n < 0 {
		return ErrNegativeSize
	}
	if w.pos+n > len(w.data) {
		return ErrShortBuffer
	}

	// Clear the destination
	for i := w.pos; i < w.pos+n; i++ {
		w.data[i] = 0
	}

	// Copy string (truncated if necessary)
	copyLen := len(s)
	if copyLen > n {
		copyLen = n
	}
	copy(w.data[w.pos:], s[:copyLen])
	w.pos += n
	return nil
}

// BufferWriter provides a growing buffer for writing binary data.
// Unlike Writer, it automatically expands to accommodate writes.
type BufferWriter struct {
	buf []byte
}

// NewBufferWriter creates a BufferWriter with an initial capacity.
func NewBufferWriter(capacity int) *BufferWriter {
	return &BufferWriter{buf: make([]byte, 0, capacity)}
}

// Len returns the number of bytes written.
func (w *BufferWriter) Len() int {
	return len(w.buf)
}

// Bytes returns the written data as a byte slice.
// The returned slice is valid until the next write operation.
func (w *BufferWriter) Bytes() []byte {
	return w.buf
}

// Reset clears the buffer.
func (w *BufferWriter) Reset() {
	w.buf = w.buf[:0]
}

// WriteByte writes a single byte.
func (w *BufferWriter) WriteByte(b byte) error {
	w.buf = append(w.buf, b)
	return nil
}

// WriteBytes writes a byte slice.
func (w *BufferWriter) WriteBytes(b []byte) {
	w.buf = append(w.buf, b...)
}

// WriteUint8 writes an unsigned 8-bit integer.
func (w *BufferWriter) WriteUint8(v uint8) {
	w.buf = append(w.buf, v)
}

// WriteInt8 writes a signed 8-bit integer.
func (w *BufferWriter) WriteInt8(v int8) {
	w.buf = append(w.buf, byte(v))
}

// WriteUint16 writes an unsigned 16-bit integer in little-endian order.
func (w *BufferWriter) WriteUint16(v uint16) {
	w.buf = append(w.buf, byte(v), byte(v>>8))
}

// WriteInt16 writes a signed 16-bit integer in little-endian order.
func (w *BufferWriter) WriteInt16(v int16) {
	w.WriteUint16(uint16(v))
}

// WriteUint32 writes an unsigned 32-bit integer in little-endian order.
func (w *BufferWriter) WriteUint32(v uint32) {
	w.buf = append(w.buf, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
}

// WriteInt32 writes a signed 32-bit integer in little-endian order.
func (w *BufferWriter) WriteInt32(v int32) {
	w.WriteUint32(uint32(v))
}

// WriteUint64 writes an unsigned 64-bit integer in little-endian order.
func (w *BufferWriter) WriteUint64(v uint64) {
	w.buf = append(w.buf,
		byte(v), byte(v>>8), byte(v>>16), byte(v>>24),
		byte(v>>32), byte(v>>40), byte(v>>48), byte(v>>56))
}

// WriteInt64 writes a signed 64-bit integer in little-endian order.
func (w *BufferWriter) WriteInt64(v int64) {
	w.WriteUint64(uint64(v))
}

// WriteFloat32 writes a 32-bit IEEE 754 floating-point number.
func (w *BufferWriter) WriteFloat32(v float32) {
	w.WriteUint32(math.Float32bits(v))
}

// WriteFloat64 writes a 64-bit IEEE 754 floating-point number.
func (w *BufferWriter) WriteFloat64(v float64) {
	w.WriteUint64(math.Float64bits(v))
}

// WriteString writes a null-terminated string.
func (w *BufferWriter) WriteString(s string) {
	w.buf = append(w.buf, s...)
	w.buf = append(w.buf, 0)
}

// WriteStringN writes a string padded or truncated to exactly n bytes.
func (w *BufferWriter) WriteStringN(s string, n int) {
	start := len(w.buf)
	w.buf = append(w.buf, make([]byte, n)...)
	copyLen := len(s)
	if copyLen > n {
		copyLen = n
	}
	copy(w.buf[start:], s[:copyLen])
}

// StreamReader wraps an io.Reader for little-endian binary reading.
type StreamReader struct {
	r   io.Reader
	buf [8]byte
}

// NewStreamReader creates a StreamReader from an io.Reader.
func NewStreamReader(r io.Reader) *StreamReader {
	return &StreamReader{r: r}
}

// ReadByte reads a single byte.
func (r *StreamReader) ReadByte() (byte, error) {
	_, err := io.ReadFull(r.r, r.buf[:1])
	return r.buf[0], err
}

// ReadBytes reads n bytes into a new slice.
func (r *StreamReader) ReadBytes(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrNegativeSize
	}
	result := make([]byte, n)
	_, err := io.ReadFull(r.r, result)
	return result, err
}

// ReadBytesInto reads bytes into the provided slice.
func (r *StreamReader) ReadBytesInto(dst []byte) error {
	_, err := io.ReadFull(r.r, dst)
	return err
}

// ReadUint8 reads an unsigned 8-bit integer.
func (r *StreamReader) ReadUint8() (uint8, error) {
	return r.ReadByte()
}

// ReadInt8 reads a signed 8-bit integer.
func (r *StreamReader) ReadInt8() (int8, error) {
	b, err := r.ReadByte()
	return int8(b), err
}

// ReadUint16 reads an unsigned 16-bit integer in little-endian order.
func (r *StreamReader) ReadUint16() (uint16, error) {
	_, err := io.ReadFull(r.r, r.buf[:2])
	if err != nil {
		return 0, err
	}
	return ByteOrder.Uint16(r.buf[:2]), nil
}

// ReadInt16 reads a signed 16-bit integer in little-endian order.
func (r *StreamReader) ReadInt16() (int16, error) {
	v, err := r.ReadUint16()
	return int16(v), err
}

// ReadUint32 reads an unsigned 32-bit integer in little-endian order.
func (r *StreamReader) ReadUint32() (uint32, error) {
	_, err := io.ReadFull(r.r, r.buf[:4])
	if err != nil {
		return 0, err
	}
	return ByteOrder.Uint32(r.buf[:4]), nil
}

// ReadInt32 reads a signed 32-bit integer in little-endian order.
func (r *StreamReader) ReadInt32() (int32, error) {
	v, err := r.ReadUint32()
	return int32(v), err
}

// ReadUint64 reads an unsigned 64-bit integer in little-endian order.
func (r *StreamReader) ReadUint64() (uint64, error) {
	_, err := io.ReadFull(r.r, r.buf[:8])
	if err != nil {
		return 0, err
	}
	return ByteOrder.Uint64(r.buf[:8]), nil
}

// ReadInt64 reads a signed 64-bit integer in little-endian order.
func (r *StreamReader) ReadInt64() (int64, error) {
	v, err := r.ReadUint64()
	return int64(v), err
}

// ReadFloat32 reads a 32-bit IEEE 754 floating-point number.
func (r *StreamReader) ReadFloat32() (float32, error) {
	v, err := r.ReadUint32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

// ReadFloat64 reads a 64-bit IEEE 754 floating-point number.
func (r *StreamReader) ReadFloat64() (float64, error) {
	v, err := r.ReadUint64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

// StreamWriter wraps an io.Writer for little-endian binary writing.
type StreamWriter struct {
	w   io.Writer
	buf [8]byte
}

// NewStreamWriter creates a StreamWriter from an io.Writer.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return &StreamWriter{w: w}
}

// WriteByte writes a single byte.
func (w *StreamWriter) WriteByte(b byte) error {
	w.buf[0] = b
	_, err := w.w.Write(w.buf[:1])
	return err
}

// WriteBytes writes a byte slice.
func (w *StreamWriter) WriteBytes(b []byte) error {
	_, err := w.w.Write(b)
	return err
}

// WriteUint8 writes an unsigned 8-bit integer.
func (w *StreamWriter) WriteUint8(v uint8) error {
	return w.WriteByte(v)
}

// WriteInt8 writes a signed 8-bit integer.
func (w *StreamWriter) WriteInt8(v int8) error {
	return w.WriteByte(byte(v))
}

// WriteUint16 writes an unsigned 16-bit integer in little-endian order.
func (w *StreamWriter) WriteUint16(v uint16) error {
	ByteOrder.PutUint16(w.buf[:2], v)
	_, err := w.w.Write(w.buf[:2])
	return err
}

// WriteInt16 writes a signed 16-bit integer in little-endian order.
func (w *StreamWriter) WriteInt16(v int16) error {
	return w.WriteUint16(uint16(v))
}

// WriteUint32 writes an unsigned 32-bit integer in little-endian order.
func (w *StreamWriter) WriteUint32(v uint32) error {
	ByteOrder.PutUint32(w.buf[:4], v)
	_, err := w.w.Write(w.buf[:4])
	return err
}

// WriteInt32 writes a signed 32-bit integer in little-endian order.
func (w *StreamWriter) WriteInt32(v int32) error {
	return w.WriteUint32(uint32(v))
}

// WriteUint64 writes an unsigned 64-bit integer in little-endian order.
func (w *StreamWriter) WriteUint64(v uint64) error {
	ByteOrder.PutUint64(w.buf[:8], v)
	_, err := w.w.Write(w.buf[:8])
	return err
}

// WriteInt64 writes a signed 64-bit integer in little-endian order.
func (w *StreamWriter) WriteInt64(v int64) error {
	return w.WriteUint64(uint64(v))
}

// WriteFloat32 writes a 32-bit IEEE 754 floating-point number.
func (w *StreamWriter) WriteFloat32(v float32) error {
	return w.WriteUint32(math.Float32bits(v))
}

// WriteFloat64 writes a 64-bit IEEE 754 floating-point number.
func (w *StreamWriter) WriteFloat64(v float64) error {
	return w.WriteUint64(math.Float64bits(v))
}

// WriteString writes a null-terminated string.
func (w *StreamWriter) WriteString(s string) error {
	if _, err := w.w.Write([]byte(s)); err != nil {
		return err
	}
	return w.WriteByte(0)
}
