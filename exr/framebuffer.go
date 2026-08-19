package exr

import (
	"errors"
	"unsafe"

	"github.com/mrjoshuak/go-openexr/half"
)

// FrameBuffer errors
var (
	ErrInvalidSlice    = errors.New("exr: invalid slice configuration")
	ErrChannelNotFound = errors.New("exr: channel not found")
	ErrTypeMismatch    = errors.New("exr: pixel type mismatch")
	ErrBoundsCheck     = errors.New("exr: pixel coordinates out of bounds")
)

// Slice describes a region of memory that holds pixel data for one channel.
// It provides the memory layout information needed to read/write pixel values.
type Slice struct {
	// Type is the pixel data type stored in this slice.
	Type PixelType

	// Base points at the first byte of the slice's own allocation: the pixel
	// at (OriginX, OriginY).
	//
	// It always points *into* the allocation. The C++ convention biases this
	// pointer so the window's minimum lands at buffer[0], which is correct in
	// C++ and not expressible in Go — an unsafe.Pointer must address a live
	// object, and the collector throws "found bad pointer in Go heap" when it
	// scans one that does not.
	Base unsafe.Pointer

	// OriginX and OriginY are the window's minimum corner: the coordinates of
	// the pixel Base addresses.
	//
	// Carrying the origin as data rather than folding it into Base is what
	// keeps coordinates window-absolute — GetFloat32(x, y) takes the same
	// (x, y) the data window is expressed in — while Base stays valid.
	OriginX int
	OriginY int

	// XStride is the number of bytes between adjacent pixels in the same row.
	XStride int

	// YStride is the number of bytes between adjacent pixels in the same column.
	YStride int

	// XSampling is the horizontal subsampling factor (1 = full resolution).
	XSampling int

	// YSampling is the vertical subsampling factor (1 = full resolution).
	YSampling int

	// FillValue is used for missing data (default 0).
	FillValue float64

	// XTileCoords and YTileCoords enable tile coordinate mode.
	// When true, pixel coordinates are relative to tile origin.
	XTileCoords bool
	YTileCoords bool
}

// NewSlice creates a Slice for a flat memory buffer.
// The buffer must be large enough to hold width * height pixels of the given type.
func NewSlice(pixelType PixelType, data []byte, width, height int) Slice {
	pixelSize := pixelType.Size()
	return Slice{
		Type:      pixelType,
		Base:      unsafe.Pointer(&data[0]),
		XStride:   pixelSize,
		YStride:   width * pixelSize,
		XSampling: 1,
		YSampling: 1,
	}
}

// NewSliceFromFloat32 creates a Slice backed by a []float32.
func NewSliceFromFloat32(data []float32, width, height int) Slice {
	return Slice{
		Type:      PixelTypeFloat,
		Base:      unsafe.Pointer(&data[0]),
		XStride:   4,
		YStride:   width * 4,
		XSampling: 1,
		YSampling: 1,
	}
}

// WithOrigin returns a copy of the slice whose coordinates are measured from
// (x, y) rather than from zero.
//
// A slice built over a bare buffer starts at the origin, which is right for a
// data window that also starts there and wrong for one that does not: the
// library addresses a frame buffer in the data window's own coordinates, so a
// slice over a window at (11, 5) must say so or its first pixel is read as if
// it were at (0, 0).
//
// AllocateChannels sets this from the window it is given, so callers that use
// it need not.
func (s Slice) WithOrigin(x, y int) Slice {
	s.OriginX, s.OriginY = x, y
	return s
}

// NewSliceFromHalf creates a Slice backed by a []half.Half.
func NewSliceFromHalf(data []half.Half, width, height int) Slice {
	return Slice{
		Type:      PixelTypeHalf,
		Base:      unsafe.Pointer(&data[0]),
		XStride:   2,
		YStride:   width * 2,
		XSampling: 1,
		YSampling: 1,
	}
}

// NewSliceFromUint32 creates a Slice backed by a []uint32.
func NewSliceFromUint32(data []uint32, width, height int) Slice {
	return Slice{
		Type:      PixelTypeUint,
		Base:      unsafe.Pointer(&data[0]),
		XStride:   4,
		YStride:   width * 4,
		XSampling: 1,
		YSampling: 1,
	}
}

// PixelAddr returns the address of the pixel at (x, y).
//
// The coordinates are window-absolute: the same system the data window is
// expressed in, so the pixel at the window's minimum corner is
// (OriginX, OriginY). A Slice with the zero origin behaves exactly as one
// addressed from zero, which is every slice built by hand over a bare buffer.
//
// No bounds check is performed and the result is written through directly, so
// a coordinate outside the slice's window corrupts memory rather than
// producing an error. A Slice built by hand must have a Base, strides and
// extent that agree.
//
//go:nocheckptr
func (s *Slice) PixelAddr(x, y int) unsafe.Pointer {
	// Subsampling first, then the window origin. Subtracting the origin here
	// rather than pre-biasing Base is what keeps Base inside its allocation.
	sx := (x - s.OriginX) / s.XSampling
	sy := (y - s.OriginY) / s.YSampling
	offset := sy*s.YStride + sx*s.XStride
	return unsafe.Pointer(uintptr(s.Base) + uintptr(offset))
}

// GetFloat32 reads a pixel as float32, converting from the slice's type.
//
//go:nocheckptr
func (s *Slice) GetFloat32(x, y int) float32 {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeFloat:
		return *(*float32)(addr)
	case PixelTypeHalf:
		h := *(*half.Half)(addr)
		return h.Float32()
	case PixelTypeUint:
		u := *(*uint32)(addr)
		return float32(u)
	default:
		return 0
	}
}

// SetFloat32 writes a pixel as float32, converting to the slice's type.
//
//go:nocheckptr
func (s *Slice) SetFloat32(x, y int, val float32) {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeFloat:
		*(*float32)(addr) = val
	case PixelTypeHalf:
		*(*half.Half)(addr) = half.FromFloat32(val)
	case PixelTypeUint:
		*(*uint32)(addr) = uint32(val)
	}
}

// GetHalf reads a pixel as half.Half.
//
//go:nocheckptr
func (s *Slice) GetHalf(x, y int) half.Half {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeHalf:
		return *(*half.Half)(addr)
	case PixelTypeFloat:
		return half.FromFloat32(*(*float32)(addr))
	case PixelTypeUint:
		return half.FromFloat32(float32(*(*uint32)(addr)))
	default:
		return half.Zero
	}
}

// SetHalf writes a pixel as half.Half.
//
//go:nocheckptr
func (s *Slice) SetHalf(x, y int, val half.Half) {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeHalf:
		*(*half.Half)(addr) = val
	case PixelTypeFloat:
		*(*float32)(addr) = val.Float32()
	case PixelTypeUint:
		*(*uint32)(addr) = uint32(val.Float32())
	}
}

// GetUint32 reads a pixel as uint32.
//
//go:nocheckptr
func (s *Slice) GetUint32(x, y int) uint32 {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeUint:
		return *(*uint32)(addr)
	case PixelTypeFloat:
		return uint32(*(*float32)(addr))
	case PixelTypeHalf:
		h := *(*half.Half)(addr)
		return uint32(h.Float32())
	default:
		return 0
	}
}

// SetUint32 writes a pixel as uint32.
//
//go:nocheckptr
func (s *Slice) SetUint32(x, y int, val uint32) {
	addr := s.PixelAddr(x, y)
	switch s.Type {
	case PixelTypeUint:
		*(*uint32)(addr) = val
	case PixelTypeFloat:
		*(*float32)(addr) = float32(val)
	case PixelTypeHalf:
		*(*half.Half)(addr) = half.FromFloat32(float32(val))
	}
}

// The row functions below take xStart relative to the window's left edge,
// which is where RowAddr points: a caller asking for the whole row passes 0
// whatever the window's origin is. Their per-pixel fallbacks go through the
// absolute accessors and so add OriginX — the two paths inside one function
// disagreeing about that is a defect that only shows on a non-zero origin,
// where the fast path is taken for one pixel type and the fallback for another.
//
// RowAddr returns the starting address of row y and the stride between pixels.
// This allows bulk operations without per-pixel method call overhead.
func (s *Slice) RowAddr(y int) (base unsafe.Pointer, stride int) {
	// y is window-absolute, as it is for PixelAddr. The returned address is
	// the row's first stored pixel — the window's left edge — so a caller
	// asking for the whole row adds nothing.
	//
	// Subtracting OriginX here as well would name absolute column zero, which
	// for a window starting at x=7 is seven pixels before the row's own data.
	sy := (y - s.OriginY) / s.YSampling
	return unsafe.Pointer(uintptr(s.Base) + uintptr(sy*s.YStride)), s.XStride
}

// IsContiguous returns true if the slice has unit stride (no gaps between pixels in a row).
func (s *Slice) IsContiguous() bool {
	return s.XStride == s.Type.Size() && s.XSampling == 1
}

// WriteRowHalfBytes writes a row of half values directly from raw bytes (little-endian).
// This is the fastest path for uncompressed data on little-endian systems.
func (s *Slice) WriteRowHalfBytes(y int, data []byte, xStart, width int) {
	if s.Type == PixelTypeHalf && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 2 {
			// Contiguous: direct memcpy - no conversion needed on little-endian
			dst := unsafe.Add(base, xStart*2)
			copy(unsafe.Slice((*byte)(dst), width*2), data[:width*2])
			return
		}
		// Non-contiguous: strided write - copy 2 bytes at a time
		for i := 0; i < width; i++ {
			ptr := unsafe.Add(base, (xStart+i)*stride)
			*(*[2]byte)(ptr) = *(*[2]byte)(unsafe.Pointer(&data[i*2]))
		}
		return
	}
	// Fallback to per-pixel conversion
	for i := 0; i < width; i++ {
		val := uint16(data[i*2]) | uint16(data[i*2+1])<<8
		s.SetHalf(s.OriginX+xStart+i, y, half.FromBits(val))
	}
}

// WriteRowHalf writes a row of half values from raw uint16 data.
// This is an optimized path for the common case of half-precision data.
func (s *Slice) WriteRowHalf(y int, data []uint16, xStart, width int) {
	if s.Type == PixelTypeHalf && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 2 {
			// Contiguous: direct copy
			dst := unsafe.Slice((*uint16)(unsafe.Add(base, xStart*2)), width)
			copy(dst, data[:width])
		} else {
			// Non-contiguous: strided write
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				*(*uint16)(ptr) = data[i]
			}
		}
	} else {
		// Fallback to per-pixel
		for i := 0; i < width; i++ {
			s.SetHalf(s.OriginX+xStart+i, y, half.FromBits(data[i]))
		}
	}
}

// WriteRowFloat writes a row of float32 values from raw bytes (little-endian).
func (s *Slice) WriteRowFloat(y int, data []byte, xStart, width int) {
	if s.Type == PixelTypeFloat && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 4 {
			// Contiguous: direct copy
			dst := unsafe.Add(base, xStart*4)
			copy(unsafe.Slice((*byte)(dst), width*4), data[:width*4])
		} else {
			// Non-contiguous: strided write
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				*(*[4]byte)(ptr) = *(*[4]byte)(unsafe.Pointer(&data[i*4]))
			}
		}
	} else {
		// Fallback to per-pixel (with conversion)
		for i := 0; i < width; i++ {
			val := *(*float32)(unsafe.Pointer(&data[i*4]))
			s.SetFloat32(s.OriginX+xStart+i, y, val)
		}
	}
}

// WriteRowUint writes a row of uint32 values from raw bytes (little-endian).
func (s *Slice) WriteRowUint(y int, data []byte, xStart, width int) {
	if s.Type == PixelTypeUint && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 4 {
			// Contiguous: direct copy
			dst := unsafe.Add(base, xStart*4)
			copy(unsafe.Slice((*byte)(dst), width*4), data[:width*4])
		} else {
			// Non-contiguous: strided write
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				*(*[4]byte)(ptr) = *(*[4]byte)(unsafe.Pointer(&data[i*4]))
			}
		}
	} else {
		// Fallback to per-pixel (with conversion)
		for i := 0; i < width; i++ {
			val := *(*uint32)(unsafe.Pointer(&data[i*4]))
			s.SetUint32(s.OriginX+xStart+i, y, val)
		}
	}
}

// ReadRowHalf reads a row of half values into raw uint16 data.
func (s *Slice) ReadRowHalf(y int, data []uint16, xStart, width int) {
	if s.Type == PixelTypeHalf && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 2 {
			// Contiguous: direct copy
			src := unsafe.Slice((*uint16)(unsafe.Add(base, xStart*2)), width)
			copy(data[:width], src)
		} else {
			// Non-contiguous: strided read
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				data[i] = *(*uint16)(ptr)
			}
		}
	} else {
		// Fallback to per-pixel
		for i := 0; i < width; i++ {
			data[i] = s.GetHalf(s.OriginX+xStart+i, y).Bits()
		}
	}
}

// ReadRowFloat reads a row of float32 values into raw bytes (little-endian).
func (s *Slice) ReadRowFloat(y int, data []byte, xStart, width int) {
	if s.Type == PixelTypeFloat && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 4 {
			// Contiguous: direct copy
			src := unsafe.Add(base, xStart*4)
			copy(data[:width*4], unsafe.Slice((*byte)(src), width*4))
		} else {
			// Non-contiguous: strided read
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				*(*[4]byte)(unsafe.Pointer(&data[i*4])) = *(*[4]byte)(ptr)
			}
		}
	} else {
		// Fallback to per-pixel (with conversion)
		for i := 0; i < width; i++ {
			val := s.GetFloat32(s.OriginX+xStart+i, y)
			*(*float32)(unsafe.Pointer(&data[i*4])) = val
		}
	}
}

// ReadRowUint reads a row of uint32 values into raw bytes (little-endian).
func (s *Slice) ReadRowUint(y int, data []byte, xStart, width int) {
	if s.Type == PixelTypeUint && s.XSampling == 1 {
		base, stride := s.RowAddr(y)
		if stride == 4 {
			// Contiguous: direct copy
			src := unsafe.Add(base, xStart*4)
			copy(data[:width*4], unsafe.Slice((*byte)(src), width*4))
		} else {
			// Non-contiguous: strided read
			for i := 0; i < width; i++ {
				ptr := unsafe.Add(base, (xStart+i)*stride)
				*(*[4]byte)(unsafe.Pointer(&data[i*4])) = *(*[4]byte)(ptr)
			}
		}
	} else {
		// Fallback to per-pixel (with conversion)
		for i := 0; i < width; i++ {
			val := s.GetUint32(s.OriginX+xStart+i, y)
			*(*uint32)(unsafe.Pointer(&data[i*4])) = val
		}
	}
}

// FrameBuffer holds a collection of channel slices for reading/writing pixels.
type FrameBuffer struct {
	slices map[string]Slice
}

// NewFrameBuffer creates an empty frame buffer.
func NewFrameBuffer() *FrameBuffer {
	return &FrameBuffer{
		slices: make(map[string]Slice),
	}
}

// Insert adds a slice for a channel. Returns an error if the channel already exists.
func (fb *FrameBuffer) Insert(name string, slice Slice) error {
	if _, exists := fb.slices[name]; exists {
		return errors.New("exr: channel already exists: " + name)
	}
	fb.slices[name] = slice
	return nil
}

// Set adds or replaces a slice for a channel.
func (fb *FrameBuffer) Set(name string, slice Slice) {
	fb.slices[name] = slice
}

// Get returns the slice for a channel, or nil if not found.
func (fb *FrameBuffer) Get(name string) *Slice {
	slice, exists := fb.slices[name]
	if !exists {
		return nil
	}
	return &slice
}

// Has returns true if a slice for the channel exists.
func (fb *FrameBuffer) Has(name string) bool {
	_, exists := fb.slices[name]
	return exists
}

// Remove removes a slice for a channel.
func (fb *FrameBuffer) Remove(name string) {
	delete(fb.slices, name)
}

// Names returns all channel names in the frame buffer.
func (fb *FrameBuffer) Names() []string {
	names := make([]string, 0, len(fb.slices))
	for name := range fb.slices {
		names = append(names, name)
	}
	return names
}

// Len returns the number of channels in the frame buffer.
func (fb *FrameBuffer) Len() int {
	return len(fb.slices)
}

// DefaultAllocationLimit is the total number of bytes AllocateChannels will
// allocate across all channels before refusing. A channel list and data window
// both come straight from a file header, so their product is attacker-
// controlled; 4 GiB is far beyond any legitimate single image this API is used
// for while still bounding a hostile one.
const DefaultAllocationLimit = 4 << 30

// ErrAllocationTooLarge is returned when a channel list and data window would
// require more memory than the caller permitted.
var ErrAllocationTooLarge = errors.New("exr: channel allocation exceeds limit")

// AllocateChannels creates slices for all channels in a channel list.
// Returns a map of channel name to backing buffer.
//
// The geometry comes from a file header, so it is validated before anything is
// allocated: see AllocateChannelsLimit. If it cannot be honoured this returns
// an empty (non-nil) frame buffer rather than allocating wildly or panicking.
// Callers that need to know why should use AllocateChannelsLimit directly.
func AllocateChannels(cl *ChannelList, dataWindow Box2i) (*FrameBuffer, map[string][]byte) {
	fb, buffers, err := AllocateChannelsLimit(cl, dataWindow, DefaultAllocationLimit)
	if err != nil {
		return NewFrameBuffer(), map[string][]byte{}
	}
	return fb, buffers
}

// AllocateChannelsLimit is AllocateChannels with an explicit ceiling on the
// total bytes allocated across all channels, and an error explaining any
// refusal.
//
// It rejects, rather than panics on, the things a malformed or hostile header
// can produce: zero sampling factors (a division by zero), a degenerate data
// window, a per-channel size that overflows int, and a total that exceeds
// maxBytes. Pass 0 for maxBytes to use DefaultAllocationLimit.
//
//go:nocheckptr
func AllocateChannelsLimit(cl *ChannelList, dataWindow Box2i, maxBytes int64) (*FrameBuffer, map[string][]byte, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultAllocationLimit
	}
	if cl == nil {
		return NewFrameBuffer(), map[string][]byte{}, nil
	}

	width := int(dataWindow.Width())
	height := int(dataWindow.Height())
	if width <= 0 || height <= 0 {
		return nil, nil, errors.New("exr: invalid data window dimensions")
	}

	// Size everything in int64 first: nothing is allocated until the whole
	// request is known to fit.
	sizes := make([]int64, cl.Len())
	var total int64
	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)
		if ch.XSampling <= 0 || ch.YSampling <= 0 {
			return nil, nil, errors.New("exr: channel has invalid sampling")
		}
		pixelSize := int64(ch.Type.Size())
		if pixelSize <= 0 {
			return nil, nil, errors.New("exr: channel has unknown pixel type")
		}
		sampledWidth := (int64(width) + int64(ch.XSampling) - 1) / int64(ch.XSampling)
		sampledHeight := (int64(height) + int64(ch.YSampling) - 1) / int64(ch.YSampling)
		size := sampledWidth * sampledHeight * pixelSize
		if size <= 0 || size > maxBytes {
			return nil, nil, ErrAllocationTooLarge
		}
		total += size
		if total > maxBytes {
			return nil, nil, ErrAllocationTooLarge
		}
		sizes[i] = size
	}

	fb := NewFrameBuffer()
	buffers := make(map[string][]byte)

	for i := 0; i < cl.Len(); i++ {
		ch := cl.At(i)

		// Account for subsampling
		sampledWidth := (width + int(ch.XSampling) - 1) / int(ch.XSampling)

		// Allocate buffer
		pixelSize := ch.Type.Size()
		buf := make([]byte, sizes[i])
		buffers[ch.Name] = buf

		// Create slice
		slice := Slice{
			Type:      ch.Type,
			Base:      unsafe.Pointer(&buf[0]),
			XStride:   pixelSize,
			YStride:   sampledWidth * pixelSize,
			XSampling: int(ch.XSampling),
			YSampling: int(ch.YSampling),
		}

		// The origin is recorded, not folded into Base.
		//
		// Biasing Base so the window's minimum lands at buffer[0] is the C++
		// convention and is not expressible in Go: the resulting pointer is
		// outside its allocation, and the collector rejects it with "found bad
		// pointer in Go heap". Carrying the origin as data keeps Base inside
		// the allocation and leaves coordinates window-absolute, so a caller
		// addresses pixels in the coordinates the data window is expressed in.
		// See TestAllocateChannelsAddressesStayInBounds.
		slice.OriginX = int(dataWindow.Min.X)
		slice.OriginY = int(dataWindow.Min.Y)

		fb.Set(ch.Name, slice)
	}

	return fb, buffers, nil
}

// RGBAFrameBuffer is a convenience wrapper for RGBA images.
type RGBAFrameBuffer struct {
	R, G, B, A []float32
	Width      int
	Height     int
	HasAlpha   bool
}

// NewRGBAFrameBuffer creates an RGBA frame buffer of the given dimensions.
func NewRGBAFrameBuffer(width, height int, hasAlpha bool) *RGBAFrameBuffer {
	fb := &RGBAFrameBuffer{
		R:        make([]float32, width*height),
		G:        make([]float32, width*height),
		B:        make([]float32, width*height),
		Width:    width,
		Height:   height,
		HasAlpha: hasAlpha,
	}
	if hasAlpha {
		fb.A = make([]float32, width*height)
	}
	return fb
}

// ToFrameBuffer converts to a generic FrameBuffer.
func (rgba *RGBAFrameBuffer) ToFrameBuffer() *FrameBuffer {
	fb := NewFrameBuffer()
	fb.Set("R", NewSliceFromFloat32(rgba.R, rgba.Width, rgba.Height))
	fb.Set("G", NewSliceFromFloat32(rgba.G, rgba.Width, rgba.Height))
	fb.Set("B", NewSliceFromFloat32(rgba.B, rgba.Width, rgba.Height))
	if rgba.HasAlpha {
		fb.Set("A", NewSliceFromFloat32(rgba.A, rgba.Width, rgba.Height))
	}
	return fb
}

// GetPixel returns the RGBA value at (x, y).
func (rgba *RGBAFrameBuffer) GetPixel(x, y int) (r, g, b, a float32) {
	idx := y*rgba.Width + x
	r = rgba.R[idx]
	g = rgba.G[idx]
	b = rgba.B[idx]
	if rgba.HasAlpha {
		a = rgba.A[idx]
	} else {
		a = 1.0
	}
	return
}

// SetPixel sets the RGBA value at (x, y).
func (rgba *RGBAFrameBuffer) SetPixel(x, y int, r, g, b, a float32) {
	idx := y*rgba.Width + x
	rgba.R[idx] = r
	rgba.G[idx] = g
	rgba.B[idx] = b
	if rgba.HasAlpha {
		rgba.A[idx] = a
	}
}
