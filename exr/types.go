// Package exr provides reading and writing of OpenEXR image files.
//
// OpenEXR is a high dynamic range (HDR) image file format developed by
// Industrial Light & Magic for use in computer imaging applications.
// It supports multiple compression algorithms, multiple image channels,
// and high precision pixel formats.
package exr

import (
	"errors"
	"fmt"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// V2i represents a 2D integer vector.
type V2i struct {
	X, Y int32
}

// V2f represents a 2D float vector.
type V2f struct {
	X, Y float32
}

// V3i represents a 3D integer vector.
type V3i struct {
	X, Y, Z int32
}

// V3f represents a 3D float vector.
type V3f struct {
	X, Y, Z float32
}

// V2d represents a 2D double-precision vector.
type V2d struct {
	X, Y float64
}

// V3d represents a 3D double-precision vector.
type V3d struct {
	X, Y, Z float64
}

// Box2i represents an axis-aligned 2D integer bounding box.
// The box is defined by its minimum and maximum corners.
// Both corners are inclusive.
type Box2i struct {
	Min, Max V2i
}

// Box2f represents an axis-aligned 2D float bounding box.
// The box is defined by its minimum and maximum corners.
type Box2f struct {
	Min, Max V2f
}

// Width returns the width of the box.
func (b Box2i) Width() int32 {
	return b.Max.X - b.Min.X + 1
}

// Height returns the height of the box.
func (b Box2i) Height() int32 {
	return b.Max.Y - b.Min.Y + 1
}

// IsEmpty returns true if the box has no area.
func (b Box2i) IsEmpty() bool {
	return b.Max.X < b.Min.X || b.Max.Y < b.Min.Y
}

// Contains returns true if the point (x, y) is inside the box.
func (b Box2i) Contains(x, y int32) bool {
	return x >= b.Min.X && x <= b.Max.X && y >= b.Min.Y && y <= b.Max.Y
}

// Area returns the area of the box.
func (b Box2i) Area() int64 {
	if b.IsEmpty() {
		return 0
	}
	return int64(b.Width()) * int64(b.Height())
}

// Width returns the width of the box.
func (b Box2f) Width() float32 {
	return b.Max.X - b.Min.X
}

// Height returns the height of the box.
func (b Box2f) Height() float32 {
	return b.Max.Y - b.Min.Y
}

// IsEmpty returns true if the box has no area.
func (b Box2f) IsEmpty() bool {
	return b.Max.X < b.Min.X || b.Max.Y < b.Min.Y
}

// Contains returns true if the point (x, y) is inside the box.
func (b Box2f) Contains(x, y float32) bool {
	return x >= b.Min.X && x <= b.Max.X && y >= b.Min.Y && y <= b.Max.Y
}

// M33f represents a 3x3 float matrix stored in row-major order.
type M33f [9]float32

// M44f represents a 4x4 float matrix stored in row-major order.
type M44f [16]float32

// M33d represents a 3x3 double-precision matrix stored in row-major order.
type M33d [9]float64

// M44d represents a 4x4 double-precision matrix stored in row-major order.
type M44d [16]float64

// FloatVector represents a variable-length array of floats.
type FloatVector []float32

// Identity33 returns the 3x3 identity matrix.
func Identity33() M33f {
	return M33f{
		1, 0, 0,
		0, 1, 0,
		0, 0, 1,
	}
}

// Identity44 returns the 4x4 identity matrix.
func Identity44() M44f {
	return M44f{
		1, 0, 0, 0,
		0, 1, 0, 0,
		0, 0, 1, 0,
		0, 0, 0, 1,
	}
}

// Rational represents a rational number as numerator/denominator.
type Rational struct {
	Num   int32
	Denom uint32
}

// Float64 returns the rational as a float64.
func (r Rational) Float64() float64 {
	if r.Denom == 0 {
		return 0
	}
	return float64(r.Num) / float64(r.Denom)
}

// Chromaticities defines color primaries and white point
// using CIE xy chromaticity coordinates.
type Chromaticities struct {
	RedX, RedY     float32 // Red primary
	GreenX, GreenY float32 // Green primary
	BlueX, BlueY   float32 // Blue primary
	WhiteX, WhiteY float32 // White point
}

// DefaultChromaticities returns the default Rec. 709 chromaticities
// used when none are specified.
func DefaultChromaticities() Chromaticities {
	return Chromaticities{
		RedX: 0.6400, RedY: 0.3300,
		GreenX: 0.3000, GreenY: 0.6000,
		BlueX: 0.1500, BlueY: 0.0600,
		WhiteX: 0.3127, WhiteY: 0.3290,
	}
}

// TimeCodePacking specifies how time code bits are packed.
// Different broadcast standards use different bit layouts.
type TimeCodePacking int

const (
	// TV60Packing is the packing for 60-field television (NTSC).
	TV60Packing TimeCodePacking = iota
	// TV50Packing is the packing for 50-field television (PAL).
	TV50Packing
	// Film24Packing is the packing for 24-frame film.
	Film24Packing
)

// TimeCode represents an SMPTE 12M-1999 time code.
//
// Time values are stored in BCD (Binary Coded Decimal) format
// as required by the SMPTE standard. The packed representation
// varies based on the packing mode (TV60, TV50, or Film24).
//
// Bit layout for TV60 packing (default):
//
//	bits 0-3:   frame units
//	bits 4-5:   frame tens
//	bit 6:      drop frame flag
//	bit 7:      color frame flag
//	bits 8-11:  seconds units
//	bits 12-14: seconds tens
//	bit 15:     field/phase flag
//	bits 16-19: minutes units
//	bits 20-22: minutes tens
//	bit 23:     bgf0
//	bits 24-27: hours units
//	bits 28-29: hours tens
//	bit 30:     bgf1
//	bit 31:     bgf2
type TimeCode struct {
	time uint32 // Internal storage (TV60 format)
	user uint32 // Binary user groups
}

// TimeCode validation errors
var (
	ErrTimeCodeHoursOutOfRange   = errors.New("timecode: hours out of range (0-23)")
	ErrTimeCodeMinutesOutOfRange = errors.New("timecode: minutes out of range (0-59)")
	ErrTimeCodeSecondsOutOfRange = errors.New("timecode: seconds out of range (0-59)")
	ErrTimeCodeFramesOutOfRange  = errors.New("timecode: frames out of range (0-29)")
	ErrTimeCodeBinaryGroup       = errors.New("timecode: binary group number out of range (1-8)")
)

// bcdToBinary converts a BCD value to binary.
func bcdToBinary(bcd uint32) int {
	return int((bcd & 0x0f) + 10*((bcd>>4)&0x0f))
}

// binaryToBcd converts a binary value to BCD.
func binaryToBcd(binary int) uint32 {
	units := binary % 10
	tens := (binary / 10) % 10
	return uint32(units | (tens << 4))
}

// bitField extracts a bit field from a value.
func bitField(value uint32, minBit, maxBit int) uint32 {
	shift := minBit
	mask := (^(^uint32(0) << (maxBit - minBit + 1))) << minBit
	return (value & mask) >> shift
}

// setBitField sets a bit field in a value.
func setBitField(value *uint32, minBit, maxBit int, field uint32) {
	shift := minBit
	mask := (^(^uint32(0) << (maxBit - minBit + 1))) << minBit
	*value = (*value & ^mask) | ((field << shift) & mask)
}

// NewTimeCode creates a TimeCode from hours, minutes, seconds, frames.
// Returns an error if any value is out of range.
func NewTimeCode(hours, minutes, seconds, frames int, dropFrame bool) (TimeCode, error) {
	var tc TimeCode
	if err := tc.SetHours(hours); err != nil {
		return TimeCode{}, err
	}
	if err := tc.SetMinutes(minutes); err != nil {
		return TimeCode{}, err
	}
	if err := tc.SetSeconds(seconds); err != nil {
		return TimeCode{}, err
	}
	if err := tc.SetFrame(frames); err != nil {
		return TimeCode{}, err
	}
	tc.SetDropFrame(dropFrame)
	return tc, nil
}

// MustNewTimeCode creates a TimeCode, panicking if values are out of range.
func MustNewTimeCode(hours, minutes, seconds, frames int, dropFrame bool) TimeCode {
	tc, err := NewTimeCode(hours, minutes, seconds, frames, dropFrame)
	if err != nil {
		panic(err)
	}
	return tc
}

// NewTimeCodeFromPacked creates a TimeCode from packed representation.
func NewTimeCodeFromPacked(timeAndFlags, userData uint32, packing TimeCodePacking) TimeCode {
	var tc TimeCode
	tc.SetTimeAndFlags(timeAndFlags, packing)
	tc.SetUserData(userData)
	return tc
}

// Hours returns the hours component (0-23).
func (tc TimeCode) Hours() int {
	return bcdToBinary(bitField(tc.time, 24, 29))
}

// SetHours sets the hours component.
func (tc *TimeCode) SetHours(value int) error {
	if value < 0 || value > 23 {
		return ErrTimeCodeHoursOutOfRange
	}
	setBitField(&tc.time, 24, 29, binaryToBcd(value))
	return nil
}

// Minutes returns the minutes component (0-59).
func (tc TimeCode) Minutes() int {
	return bcdToBinary(bitField(tc.time, 16, 22))
}

// SetMinutes sets the minutes component.
func (tc *TimeCode) SetMinutes(value int) error {
	if value < 0 || value > 59 {
		return ErrTimeCodeMinutesOutOfRange
	}
	setBitField(&tc.time, 16, 22, binaryToBcd(value))
	return nil
}

// Seconds returns the seconds component (0-59).
func (tc TimeCode) Seconds() int {
	return bcdToBinary(bitField(tc.time, 8, 14))
}

// SetSeconds sets the seconds component.
func (tc *TimeCode) SetSeconds(value int) error {
	if value < 0 || value > 59 {
		return ErrTimeCodeSecondsOutOfRange
	}
	setBitField(&tc.time, 8, 14, binaryToBcd(value))
	return nil
}

// Frame returns the frames component (0-29).
func (tc TimeCode) Frame() int {
	return bcdToBinary(bitField(tc.time, 0, 5))
}

// Frames is an alias for Frame.
//
// Deprecated: Use Frame instead. This alias will be removed in a future version.
func (tc TimeCode) Frames() int {
	return tc.Frame()
}

// SetFrame sets the frames component.
func (tc *TimeCode) SetFrame(value int) error {
	if value < 0 || value > 29 {
		return ErrTimeCodeFramesOutOfRange
	}
	setBitField(&tc.time, 0, 5, binaryToBcd(value))
	return nil
}

// DropFrame returns true if this is a drop-frame time code.
func (tc TimeCode) DropFrame() bool {
	return bitField(tc.time, 6, 6) != 0
}

// SetDropFrame sets the drop frame flag.
func (tc *TimeCode) SetDropFrame(value bool) {
	if value {
		setBitField(&tc.time, 6, 6, 1)
	} else {
		setBitField(&tc.time, 6, 6, 0)
	}
}

// ColorFrame returns the color frame flag.
func (tc TimeCode) ColorFrame() bool {
	return bitField(tc.time, 7, 7) != 0
}

// SetColorFrame sets the color frame flag.
func (tc *TimeCode) SetColorFrame(value bool) {
	if value {
		setBitField(&tc.time, 7, 7, 1)
	} else {
		setBitField(&tc.time, 7, 7, 0)
	}
}

// FieldPhase returns the field/phase flag.
func (tc TimeCode) FieldPhase() bool {
	return bitField(tc.time, 15, 15) != 0
}

// SetFieldPhase sets the field/phase flag.
func (tc *TimeCode) SetFieldPhase(value bool) {
	if value {
		setBitField(&tc.time, 15, 15, 1)
	} else {
		setBitField(&tc.time, 15, 15, 0)
	}
}

// Bgf0 returns the bgf0 flag.
func (tc TimeCode) Bgf0() bool {
	return bitField(tc.time, 23, 23) != 0
}

// SetBgf0 sets the bgf0 flag.
func (tc *TimeCode) SetBgf0(value bool) {
	if value {
		setBitField(&tc.time, 23, 23, 1)
	} else {
		setBitField(&tc.time, 23, 23, 0)
	}
}

// Bgf1 returns the bgf1 flag.
func (tc TimeCode) Bgf1() bool {
	return bitField(tc.time, 30, 30) != 0
}

// SetBgf1 sets the bgf1 flag.
func (tc *TimeCode) SetBgf1(value bool) {
	if value {
		setBitField(&tc.time, 30, 30, 1)
	} else {
		setBitField(&tc.time, 30, 30, 0)
	}
}

// Bgf2 returns the bgf2 flag.
func (tc TimeCode) Bgf2() bool {
	return bitField(tc.time, 31, 31) != 0
}

// SetBgf2 sets the bgf2 flag.
func (tc *TimeCode) SetBgf2(value bool) {
	if value {
		setBitField(&tc.time, 31, 31, 1)
	} else {
		setBitField(&tc.time, 31, 31, 0)
	}
}

// BinaryGroup returns a binary group value (group 1-8).
func (tc TimeCode) BinaryGroup(group int) (int, error) {
	if group < 1 || group > 8 {
		return 0, ErrTimeCodeBinaryGroup
	}
	minBit := 4 * (group - 1)
	maxBit := minBit + 3
	return int(bitField(tc.user, minBit, maxBit)), nil
}

// SetBinaryGroup sets a binary group value (group 1-8, value 0-15).
func (tc *TimeCode) SetBinaryGroup(group, value int) error {
	if group < 1 || group > 8 {
		return ErrTimeCodeBinaryGroup
	}
	minBit := 4 * (group - 1)
	maxBit := minBit + 3
	setBitField(&tc.user, minBit, maxBit, uint32(value&0x0F))
	return nil
}

// TimeAndFlags returns the packed time and flags value.
func (tc TimeCode) TimeAndFlags(packing TimeCodePacking) uint32 {
	switch packing {
	case TV50Packing:
		// TV50 has different flag positions
		t := tc.time
		t &= ^(uint32(1<<6) | uint32(1<<15) | uint32(1<<23) | uint32(1<<30) | uint32(1<<31))
		if tc.Bgf0() {
			t |= 1 << 15
		}
		if tc.Bgf2() {
			t |= 1 << 23
		}
		if tc.Bgf1() {
			t |= 1 << 30
		}
		if tc.FieldPhase() {
			t |= 1 << 31
		}
		return t
	case Film24Packing:
		// Film24 doesn't use drop frame or color frame
		return tc.time & ^(uint32(1<<6) | uint32(1<<7))
	default: // TV60Packing
		return tc.time
	}
}

// SetTimeAndFlags sets from a packed time and flags value.
func (tc *TimeCode) SetTimeAndFlags(value uint32, packing TimeCodePacking) {
	switch packing {
	case TV50Packing:
		tc.time = value & ^(uint32(1<<6) | uint32(1<<15) | uint32(1<<23) | uint32(1<<30) | uint32(1<<31))
		if value&(1<<15) != 0 {
			tc.SetBgf0(true)
		}
		if value&(1<<23) != 0 {
			tc.SetBgf2(true)
		}
		if value&(1<<30) != 0 {
			tc.SetBgf1(true)
		}
		if value&(1<<31) != 0 {
			tc.SetFieldPhase(true)
		}
	case Film24Packing:
		tc.time = value & ^(uint32(1<<6) | uint32(1<<7))
	default: // TV60Packing
		tc.time = value
	}
}

// UserData returns the user data value.
func (tc TimeCode) UserData() uint32 {
	return tc.user
}

// SetUserData sets the user data value.
func (tc *TimeCode) SetUserData(value uint32) {
	tc.user = value
}

// KeyCode represents a film key code (edge code).
type KeyCode struct {
	FilmMfcCode   int32 // Film manufacturer code
	FilmType      int32 // Film type code
	Prefix        int32 // Prefix
	Count         int32 // Count
	PerfOffset    int32 // Perforation offset
	PerfsPerFrame int32 // Perforations per frame
	PerfsPerCount int32 // Perforations per count
}

// Preview represents a small preview image for the EXR file.
// Preview images are always stored as RGBA, 8 bits per channel.
type Preview struct {
	Width  uint32
	Height uint32
	Pixels []byte // RGBA, 8 bits per channel, length = Width * Height * 4
}

// Binary serialization methods

// ReadV2i reads a V2i from the reader.
func ReadV2i(r *xdr.Reader) (V2i, error) {
	var v V2i
	var err error
	v.X, err = r.ReadInt32()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadInt32()
	return v, err
}

// WriteV2i writes a V2i to the writer.
func WriteV2i(w *xdr.BufferWriter, v V2i) {
	w.WriteInt32(v.X)
	w.WriteInt32(v.Y)
}

// ReadV2f reads a V2f from the reader.
func ReadV2f(r *xdr.Reader) (V2f, error) {
	var v V2f
	var err error
	v.X, err = r.ReadFloat32()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadFloat32()
	return v, err
}

// WriteV2f writes a V2f to the writer.
func WriteV2f(w *xdr.BufferWriter, v V2f) {
	w.WriteFloat32(v.X)
	w.WriteFloat32(v.Y)
}

// ReadV3i reads a V3i from the reader.
func ReadV3i(r *xdr.Reader) (V3i, error) {
	var v V3i
	var err error
	v.X, err = r.ReadInt32()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadInt32()
	if err != nil {
		return v, err
	}
	v.Z, err = r.ReadInt32()
	return v, err
}

// WriteV3i writes a V3i to the writer.
func WriteV3i(w *xdr.BufferWriter, v V3i) {
	w.WriteInt32(v.X)
	w.WriteInt32(v.Y)
	w.WriteInt32(v.Z)
}

// ReadV3f reads a V3f from the reader.
func ReadV3f(r *xdr.Reader) (V3f, error) {
	var v V3f
	var err error
	v.X, err = r.ReadFloat32()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadFloat32()
	if err != nil {
		return v, err
	}
	v.Z, err = r.ReadFloat32()
	return v, err
}

// WriteV3f writes a V3f to the writer.
func WriteV3f(w *xdr.BufferWriter, v V3f) {
	w.WriteFloat32(v.X)
	w.WriteFloat32(v.Y)
	w.WriteFloat32(v.Z)
}

// ReadBox2i reads a Box2i from the reader.
func ReadBox2i(r *xdr.Reader) (Box2i, error) {
	var b Box2i
	var err error
	b.Min, err = ReadV2i(r)
	if err != nil {
		return b, err
	}
	b.Max, err = ReadV2i(r)
	return b, err
}

// WriteBox2i writes a Box2i to the writer.
func WriteBox2i(w *xdr.BufferWriter, b Box2i) {
	WriteV2i(w, b.Min)
	WriteV2i(w, b.Max)
}

// ReadBox2f reads a Box2f from the reader.
func ReadBox2f(r *xdr.Reader) (Box2f, error) {
	var b Box2f
	var err error
	b.Min, err = ReadV2f(r)
	if err != nil {
		return b, err
	}
	b.Max, err = ReadV2f(r)
	return b, err
}

// WriteBox2f writes a Box2f to the writer.
func WriteBox2f(w *xdr.BufferWriter, b Box2f) {
	WriteV2f(w, b.Min)
	WriteV2f(w, b.Max)
}

// ReadM33f reads a M33f from the reader.
func ReadM33f(r *xdr.Reader) (M33f, error) {
	var m M33f
	for i := 0; i < 9; i++ {
		var err error
		m[i], err = r.ReadFloat32()
		if err != nil {
			return m, err
		}
	}
	return m, nil
}

// WriteM33f writes a M33f to the writer.
func WriteM33f(w *xdr.BufferWriter, m M33f) {
	for i := 0; i < 9; i++ {
		w.WriteFloat32(m[i])
	}
}

// ReadM44f reads a M44f from the reader.
func ReadM44f(r *xdr.Reader) (M44f, error) {
	var m M44f
	for i := 0; i < 16; i++ {
		var err error
		m[i], err = r.ReadFloat32()
		if err != nil {
			return m, err
		}
	}
	return m, nil
}

// WriteM44f writes a M44f to the writer.
func WriteM44f(w *xdr.BufferWriter, m M44f) {
	for i := 0; i < 16; i++ {
		w.WriteFloat32(m[i])
	}
}

// ReadV2d reads a V2d from the reader.
func ReadV2d(r *xdr.Reader) (V2d, error) {
	var v V2d
	var err error
	v.X, err = r.ReadFloat64()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadFloat64()
	return v, err
}

// WriteV2d writes a V2d to the writer.
func WriteV2d(w *xdr.BufferWriter, v V2d) {
	w.WriteFloat64(v.X)
	w.WriteFloat64(v.Y)
}

// ReadV3d reads a V3d from the reader.
func ReadV3d(r *xdr.Reader) (V3d, error) {
	var v V3d
	var err error
	v.X, err = r.ReadFloat64()
	if err != nil {
		return v, err
	}
	v.Y, err = r.ReadFloat64()
	if err != nil {
		return v, err
	}
	v.Z, err = r.ReadFloat64()
	return v, err
}

// WriteV3d writes a V3d to the writer.
func WriteV3d(w *xdr.BufferWriter, v V3d) {
	w.WriteFloat64(v.X)
	w.WriteFloat64(v.Y)
	w.WriteFloat64(v.Z)
}

// ReadM33d reads a M33d from the reader.
func ReadM33d(r *xdr.Reader) (M33d, error) {
	var m M33d
	for i := 0; i < 9; i++ {
		var err error
		m[i], err = r.ReadFloat64()
		if err != nil {
			return m, err
		}
	}
	return m, nil
}

// WriteM33d writes a M33d to the writer.
func WriteM33d(w *xdr.BufferWriter, m M33d) {
	for i := 0; i < 9; i++ {
		w.WriteFloat64(m[i])
	}
}

// ReadM44d reads a M44d from the reader.
func ReadM44d(r *xdr.Reader) (M44d, error) {
	var m M44d
	for i := 0; i < 16; i++ {
		var err error
		m[i], err = r.ReadFloat64()
		if err != nil {
			return m, err
		}
	}
	return m, nil
}

// WriteM44d writes a M44d to the writer.
func WriteM44d(w *xdr.BufferWriter, m M44d) {
	for i := 0; i < 16; i++ {
		w.WriteFloat64(m[i])
	}
}

// ReadFloatVector reads a FloatVector from the reader.
//
// The wire format is the float values and nothing else: the count is the
// attribute's declared size divided by four, exactly as the reference derives
// it (ImfFloatVectorAttribute readValueFrom reads size/sizeof(float) values).
//
// This used to expect a leading int32 count, and wrote one too. Both halves
// were wrong and each was wrong in a different way. Reading a
// reference-written vector of three floats took the bits of the first as a
// count, failed the size check, and the error failed the whole open — one
// unrecognised attribute made the file unreadable. Writing produced a payload
// the reference happily read as one value too many, with the count leaking in
// as a denormal: measured with oiiotool on a vector of (1.5, 2.5, 3.5), the
// reference reported "4.2039e-45, 1.5, 2.5, 3.5".
func ReadFloatVector(r *xdr.Reader, size int) (FloatVector, error) {
	if size < 0 || size%4 != 0 {
		return nil, fmt.Errorf("floatvector: attribute size %d is not a whole number of floats", size)
	}
	data, err := r.ReadBytes(size)
	if err != nil {
		return nil, err
	}
	sub := xdr.NewReader(data)
	fv := make(FloatVector, size/4)
	for i := range fv {
		fv[i], err = sub.ReadFloat32()
		if err != nil {
			return nil, err
		}
	}
	return fv, nil
}

// WriteFloatVector writes a FloatVector to the writer: the values alone, with
// the count carried by the attribute's size field.
func WriteFloatVector(w *xdr.BufferWriter, fv FloatVector) {
	for _, f := range fv {
		w.WriteFloat32(f)
	}
}

// ReadRational reads a Rational from the reader.
func ReadRational(r *xdr.Reader) (Rational, error) {
	var rat Rational
	var err error
	rat.Num, err = r.ReadInt32()
	if err != nil {
		return rat, err
	}
	rat.Denom, err = r.ReadUint32()
	return rat, err
}

// WriteRational writes a Rational to the writer.
func WriteRational(w *xdr.BufferWriter, r Rational) {
	w.WriteInt32(r.Num)
	w.WriteUint32(r.Denom)
}

// ReadChromaticities reads Chromaticities from the reader.
func ReadChromaticities(r *xdr.Reader) (Chromaticities, error) {
	var c Chromaticities
	var err error
	c.RedX, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.RedY, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.GreenX, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.GreenY, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.BlueX, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.BlueY, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.WhiteX, err = r.ReadFloat32()
	if err != nil {
		return c, err
	}
	c.WhiteY, err = r.ReadFloat32()
	return c, err
}

// WriteChromaticities writes Chromaticities to the writer.
func WriteChromaticities(w *xdr.BufferWriter, c Chromaticities) {
	w.WriteFloat32(c.RedX)
	w.WriteFloat32(c.RedY)
	w.WriteFloat32(c.GreenX)
	w.WriteFloat32(c.GreenY)
	w.WriteFloat32(c.BlueX)
	w.WriteFloat32(c.BlueY)
	w.WriteFloat32(c.WhiteX)
	w.WriteFloat32(c.WhiteY)
}

// ReadTimeCode reads a TimeCode from the reader.
// The file format uses TV60 packing.
func ReadTimeCode(r *xdr.Reader) (TimeCode, error) {
	var tc TimeCode
	var err error
	tc.time, err = r.ReadUint32()
	if err != nil {
		return tc, err
	}
	tc.user, err = r.ReadUint32()
	return tc, err
}

// WriteTimeCode writes a TimeCode to the writer.
// The file format uses TV60 packing.
func WriteTimeCode(w *xdr.BufferWriter, tc TimeCode) {
	w.WriteUint32(tc.time)
	w.WriteUint32(tc.user)
}

// ReadKeyCode reads a KeyCode from the reader.
func ReadKeyCode(r *xdr.Reader) (KeyCode, error) {
	var kc KeyCode
	var err error
	kc.FilmMfcCode, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.FilmType, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.Prefix, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.Count, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.PerfOffset, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.PerfsPerFrame, err = r.ReadInt32()
	if err != nil {
		return kc, err
	}
	kc.PerfsPerCount, err = r.ReadInt32()
	return kc, err
}

// WriteKeyCode writes a KeyCode to the writer.
func WriteKeyCode(w *xdr.BufferWriter, kc KeyCode) {
	w.WriteInt32(kc.FilmMfcCode)
	w.WriteInt32(kc.FilmType)
	w.WriteInt32(kc.Prefix)
	w.WriteInt32(kc.Count)
	w.WriteInt32(kc.PerfOffset)
	w.WriteInt32(kc.PerfsPerFrame)
	w.WriteInt32(kc.PerfsPerCount)
}

// ReadPreview reads a Preview from the reader.
// Note: This reads the width, height, and pixel data.
func ReadPreview(r *xdr.Reader) (Preview, error) {
	var p Preview
	var err error
	p.Width, err = r.ReadUint32()
	if err != nil {
		return p, err
	}
	p.Height, err = r.ReadUint32()
	if err != nil {
		return p, err
	}
	pixelSize := int(p.Width) * int(p.Height) * 4
	p.Pixels, err = r.ReadBytes(pixelSize)
	return p, err
}

// WritePreview writes a Preview to the writer.
func WritePreview(w *xdr.BufferWriter, p Preview) {
	w.WriteUint32(p.Width)
	w.WriteUint32(p.Height)
	w.WriteBytes(p.Pixels)
}
