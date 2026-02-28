package exr

import (
	"testing"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

func TestBox2i(t *testing.T) {
	b := Box2i{
		Min: V2i{0, 0},
		Max: V2i{99, 49},
	}

	if w := b.Width(); w != 100 {
		t.Errorf("Width() = %d, want 100", w)
	}
	if h := b.Height(); h != 50 {
		t.Errorf("Height() = %d, want 50", h)
	}
	if a := b.Area(); a != 5000 {
		t.Errorf("Area() = %d, want 5000", a)
	}
	if b.IsEmpty() {
		t.Error("IsEmpty() should be false")
	}
	if !b.Contains(50, 25) {
		t.Error("Contains(50, 25) should be true")
	}
	if b.Contains(100, 50) {
		t.Error("Contains(100, 50) should be false")
	}
}

func TestBox2iEmpty(t *testing.T) {
	empty := Box2i{
		Min: V2i{10, 10},
		Max: V2i{5, 5},
	}

	if !empty.IsEmpty() {
		t.Error("IsEmpty() should be true for inverted box")
	}
	if a := empty.Area(); a != 0 {
		t.Errorf("Area() of empty box = %d, want 0", a)
	}
}

func TestBox2f(t *testing.T) {
	b := Box2f{
		Min: V2f{0.0, 0.0},
		Max: V2f{1.0, 2.0},
	}

	if w := b.Width(); w != 1.0 {
		t.Errorf("Width() = %f, want 1.0", w)
	}
	if h := b.Height(); h != 2.0 {
		t.Errorf("Height() = %f, want 2.0", h)
	}
	if b.IsEmpty() {
		t.Error("IsEmpty() should be false")
	}
	if !b.Contains(0.5, 1.0) {
		t.Error("Contains(0.5, 1.0) should be true")
	}
	if b.Contains(1.5, 1.0) {
		t.Error("Contains(1.5, 1.0) should be false")
	}
}

func TestBox2fEmpty(t *testing.T) {
	empty := Box2f{
		Min: V2f{1.0, 1.0},
		Max: V2f{0.0, 0.0},
	}
	if !empty.IsEmpty() {
		t.Error("IsEmpty() should be true for inverted box")
	}
}

func TestIdentityMatrices(t *testing.T) {
	m33 := Identity33()
	expected33 := M33f{1, 0, 0, 0, 1, 0, 0, 0, 1}
	if m33 != expected33 {
		t.Errorf("Identity33() = %v, want %v", m33, expected33)
	}

	m44 := Identity44()
	expected44 := M44f{1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1}
	if m44 != expected44 {
		t.Errorf("Identity44() = %v, want %v", m44, expected44)
	}
}

func TestRational(t *testing.T) {
	r := Rational{Num: 1, Denom: 2}
	if v := r.Float64(); v != 0.5 {
		t.Errorf("Float64() = %f, want 0.5", v)
	}

	// Test zero denominator
	zero := Rational{Num: 1, Denom: 0}
	if v := zero.Float64(); v != 0 {
		t.Errorf("Float64() with zero denom = %f, want 0", v)
	}
}

func TestDefaultChromaticities(t *testing.T) {
	c := DefaultChromaticities()

	// Rec. 709 values
	if c.RedX != 0.6400 {
		t.Errorf("RedX = %f, want 0.6400", c.RedX)
	}
	if c.WhiteY != 0.3290 {
		t.Errorf("WhiteY = %f, want 0.3290", c.WhiteY)
	}
}

func TestTimeCode(t *testing.T) {
	tc := MustNewTimeCode(1, 30, 45, 12, true)

	if h := tc.Hours(); h != 1 {
		t.Errorf("Hours() = %d, want 1", h)
	}
	if m := tc.Minutes(); m != 30 {
		t.Errorf("Minutes() = %d, want 30", m)
	}
	if s := tc.Seconds(); s != 45 {
		t.Errorf("Seconds() = %d, want 45", s)
	}
	if f := tc.Frames(); f != 12 {
		t.Errorf("Frames() = %d, want 12", f)
	}
	if !tc.DropFrame() {
		t.Error("DropFrame() should be true")
	}

	tc2 := MustNewTimeCode(10, 20, 30, 24, false)
	if tc2.DropFrame() {
		t.Error("DropFrame() should be false")
	}
}

// Test serialization round-trips

func TestV2iSerialization(t *testing.T) {
	original := V2i{X: 100, Y: -200}
	w := xdr.NewBufferWriter(16)
	WriteV2i(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV2i(r)
	if err != nil {
		t.Fatalf("ReadV2i() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV2i() = %v, want %v", result, original)
	}
}

func TestV2fSerialization(t *testing.T) {
	original := V2f{X: 1.5, Y: -2.5}
	w := xdr.NewBufferWriter(16)
	WriteV2f(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV2f(r)
	if err != nil {
		t.Fatalf("ReadV2f() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV2f() = %v, want %v", result, original)
	}
}

func TestV3iSerialization(t *testing.T) {
	original := V3i{X: 1, Y: 2, Z: 3}
	w := xdr.NewBufferWriter(16)
	WriteV3i(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV3i(r)
	if err != nil {
		t.Fatalf("ReadV3i() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV3i() = %v, want %v", result, original)
	}
}

func TestV3fSerialization(t *testing.T) {
	original := V3f{X: 1.0, Y: 2.0, Z: 3.0}
	w := xdr.NewBufferWriter(16)
	WriteV3f(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV3f(r)
	if err != nil {
		t.Fatalf("ReadV3f() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV3f() = %v, want %v", result, original)
	}
}

func TestBox2iSerialization(t *testing.T) {
	original := Box2i{
		Min: V2i{0, 0},
		Max: V2i{1919, 1079},
	}
	w := xdr.NewBufferWriter(32)
	WriteBox2i(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadBox2i(r)
	if err != nil {
		t.Fatalf("ReadBox2i() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadBox2i() = %v, want %v", result, original)
	}
}

func TestBox2fSerialization(t *testing.T) {
	original := Box2f{
		Min: V2f{-1.0, -1.0},
		Max: V2f{1.0, 1.0},
	}
	w := xdr.NewBufferWriter(32)
	WriteBox2f(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadBox2f(r)
	if err != nil {
		t.Fatalf("ReadBox2f() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadBox2f() = %v, want %v", result, original)
	}
}

func TestM33fSerialization(t *testing.T) {
	original := M33f{1, 2, 3, 4, 5, 6, 7, 8, 9}
	w := xdr.NewBufferWriter(64)
	WriteM33f(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadM33f(r)
	if err != nil {
		t.Fatalf("ReadM33f() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadM33f() = %v, want %v", result, original)
	}
}

func TestM44fSerialization(t *testing.T) {
	original := M44f{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	w := xdr.NewBufferWriter(128)
	WriteM44f(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadM44f(r)
	if err != nil {
		t.Fatalf("ReadM44f() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadM44f() = %v, want %v", result, original)
	}
}

func TestRationalSerialization(t *testing.T) {
	original := Rational{Num: 24000, Denom: 1001}
	w := xdr.NewBufferWriter(16)
	WriteRational(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadRational(r)
	if err != nil {
		t.Fatalf("ReadRational() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadRational() = %v, want %v", result, original)
	}
}

func TestChromaticitiesSerialization(t *testing.T) {
	original := DefaultChromaticities()
	w := xdr.NewBufferWriter(64)
	WriteChromaticities(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadChromaticities(r)
	if err != nil {
		t.Fatalf("ReadChromaticities() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadChromaticities() = %v, want %v", result, original)
	}
}

func TestTimeCodeSerialization(t *testing.T) {
	original := MustNewTimeCode(12, 34, 56, 29, true)
	w := xdr.NewBufferWriter(16)
	WriteTimeCode(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadTimeCode(r)
	if err != nil {
		t.Fatalf("ReadTimeCode() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadTimeCode() = %v, want %v", result, original)
	}
}

func TestKeyCodeSerialization(t *testing.T) {
	original := KeyCode{
		FilmMfcCode:   1,
		FilmType:      2,
		Prefix:        3,
		Count:         4,
		PerfOffset:    5,
		PerfsPerFrame: 4,
		PerfsPerCount: 64,
	}
	w := xdr.NewBufferWriter(64)
	WriteKeyCode(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadKeyCode(r)
	if err != nil {
		t.Fatalf("ReadKeyCode() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadKeyCode() = %v, want %v", result, original)
	}
}

func TestPreviewSerialization(t *testing.T) {
	original := Preview{
		Width:  2,
		Height: 2,
		Pixels: []byte{
			255, 0, 0, 255, // Red
			0, 255, 0, 255, // Green
			0, 0, 255, 255, // Blue
			255, 255, 0, 255, // Yellow
		},
	}
	w := xdr.NewBufferWriter(128)
	WritePreview(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadPreview(r)
	if err != nil {
		t.Fatalf("ReadPreview() error = %v", err)
	}
	if result.Width != original.Width || result.Height != original.Height {
		t.Errorf("Preview dimensions = %dx%d, want %dx%d", result.Width, result.Height, original.Width, original.Height)
	}
	if len(result.Pixels) != len(original.Pixels) {
		t.Errorf("Pixels length = %d, want %d", len(result.Pixels), len(original.Pixels))
	}
	for i := range original.Pixels {
		if result.Pixels[i] != original.Pixels[i] {
			t.Errorf("Pixel[%d] = %d, want %d", i, result.Pixels[i], original.Pixels[i])
		}
	}
}

// Test error handling on short buffers

func TestReadErrorsOnShortBuffer(t *testing.T) {
	empty := xdr.NewReader([]byte{})

	_, err := ReadV2i(empty)
	if err == nil {
		t.Error("ReadV2i on empty should error")
	}

	_, err = ReadV2f(empty)
	if err == nil {
		t.Error("ReadV2f on empty should error")
	}

	_, err = ReadV3i(empty)
	if err == nil {
		t.Error("ReadV3i on empty should error")
	}

	_, err = ReadV3f(empty)
	if err == nil {
		t.Error("ReadV3f on empty should error")
	}

	_, err = ReadBox2i(empty)
	if err == nil {
		t.Error("ReadBox2i on empty should error")
	}

	_, err = ReadBox2f(empty)
	if err == nil {
		t.Error("ReadBox2f on empty should error")
	}

	_, err = ReadM33f(empty)
	if err == nil {
		t.Error("ReadM33f on empty should error")
	}

	_, err = ReadM44f(empty)
	if err == nil {
		t.Error("ReadM44f on empty should error")
	}

	_, err = ReadRational(empty)
	if err == nil {
		t.Error("ReadRational on empty should error")
	}

	_, err = ReadChromaticities(empty)
	if err == nil {
		t.Error("ReadChromaticities on empty should error")
	}

	_, err = ReadTimeCode(empty)
	if err == nil {
		t.Error("ReadTimeCode on empty should error")
	}

	_, err = ReadKeyCode(empty)
	if err == nil {
		t.Error("ReadKeyCode on empty should error")
	}

	_, err = ReadPreview(empty)
	if err == nil {
		t.Error("ReadPreview on empty should error")
	}
}

func TestReadPartialData(t *testing.T) {
	// Test partial V2i (only X available)
	r := xdr.NewReader([]byte{1, 0, 0, 0}) // Only 4 bytes, need 8
	_, err := ReadV2i(r)
	if err == nil {
		t.Error("ReadV2i with partial data should error")
	}

	// Test partial V3i (X and Y available, Z missing)
	r = xdr.NewReader([]byte{1, 0, 0, 0, 2, 0, 0, 0}) // Only 8 bytes, need 12
	_, err = ReadV3i(r)
	if err == nil {
		t.Error("ReadV3i with partial data should error")
	}

	// Test partial Box2i (Min available, Max missing)
	r = xdr.NewReader([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // Only 8 bytes, need 16
	_, err = ReadBox2i(r)
	if err == nil {
		t.Error("ReadBox2i with partial data should error")
	}

	// Test partial M33f
	r = xdr.NewReader(make([]byte, 32)) // Only 32 bytes, need 36
	_, err = ReadM33f(r)
	if err == nil {
		t.Error("ReadM33f with partial data should error")
	}

	// Test partial M44f
	r = xdr.NewReader(make([]byte, 60)) // Only 60 bytes, need 64
	_, err = ReadM44f(r)
	if err == nil {
		t.Error("ReadM44f with partial data should error")
	}

	// Test partial Chromaticities
	r = xdr.NewReader(make([]byte, 28)) // Only 28 bytes, need 32
	_, err = ReadChromaticities(r)
	if err == nil {
		t.Error("ReadChromaticities with partial data should error")
	}

	// Test partial KeyCode
	r = xdr.NewReader(make([]byte, 24)) // Only 24 bytes, need 28
	_, err = ReadKeyCode(r)
	if err == nil {
		t.Error("ReadKeyCode with partial data should error")
	}

	// Test Preview with valid header but missing pixels
	r = xdr.NewReader([]byte{2, 0, 0, 0, 2, 0, 0, 0}) // 2x2 preview, but no pixel data
	_, err = ReadPreview(r)
	if err == nil {
		t.Error("ReadPreview with missing pixels should error")
	}
}

func TestReadV3iError(t *testing.T) {
	// Test with insufficient data
	r := xdr.NewReader([]byte{1, 0, 0, 0, 2, 0, 0, 0}) // Only 8 bytes, need 12
	_, err := ReadV3i(r)
	if err == nil {
		t.Error("ReadV3i with insufficient data should error")
	}
}

func TestReadV3fError(t *testing.T) {
	// Test with insufficient data
	r := xdr.NewReader([]byte{0, 0, 0x80, 0x3f, 0, 0, 0, 0x40}) // Only 8 bytes, need 12
	_, err := ReadV3f(r)
	if err == nil {
		t.Error("ReadV3f with insufficient data should error")
	}
}

func TestReadPreviewError(t *testing.T) {
	// Test with insufficient data for preview
	data := []byte{2, 0, 0, 0, 2, 0, 0, 0} // width=2, height=2, but no pixel data
	r := xdr.NewReader(data)
	_, err := ReadPreview(r)
	if err == nil {
		t.Error("ReadPreview with insufficient pixel data should error")
	}
}

func TestReadChromaticitiesErrors(t *testing.T) {
	// Test with truncated data at various points
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"4 bytes", make([]byte, 4)},   // only RedX
		{"8 bytes", make([]byte, 8)},   // RedX + RedY
		{"12 bytes", make([]byte, 12)}, // + GreenX
		{"16 bytes", make([]byte, 16)}, // + GreenY
		{"20 bytes", make([]byte, 20)}, // + BlueX
		{"24 bytes", make([]byte, 24)}, // + BlueY
		{"28 bytes", make([]byte, 28)}, // + WhiteX (missing WhiteY)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := xdr.NewReader(tt.data)
			_, err := ReadChromaticities(r)
			if err == nil {
				t.Errorf("ReadChromaticities with %s should error", tt.name)
			}
		})
	}
}

func TestReadKeyCodeErrors(t *testing.T) {
	// Test with insufficient data
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"4 bytes", make([]byte, 4)},
		{"8 bytes", make([]byte, 8)},
		{"12 bytes", make([]byte, 12)},
		{"16 bytes", make([]byte, 16)},
		{"20 bytes", make([]byte, 20)},
		{"24 bytes", make([]byte, 24)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := xdr.NewReader(tt.data)
			_, err := ReadKeyCode(r)
			if err == nil {
				t.Errorf("ReadKeyCode with %s should error", tt.name)
			}
		})
	}
}

func TestTimeCodeUserData(t *testing.T) {
	tc := MustNewTimeCode(1, 30, 45, 12, false)

	// Test default user data
	if tc.UserData() != 0 {
		t.Errorf("Default UserData = %d, want 0", tc.UserData())
	}

	// Test setting user data
	tc.SetUserData(0x12345678)
	if tc.UserData() != 0x12345678 {
		t.Errorf("UserData after Set = %d, want 0x12345678", tc.UserData())
	}
}

func TestTimeCodePackings(t *testing.T) {
	tests := []struct {
		name    string
		packing TimeCodePacking
	}{
		{"TV50Packing", TV50Packing},
		{"TV60Packing", TV60Packing},
		{"Film24Packing", Film24Packing},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tc := MustNewTimeCode(12, 30, 45, 20, false)

			// Set time and flags with packing
			tc.SetTimeAndFlags(0x12345678, tt.packing)

			// Get time and flags with packing — roundtrip must preserve value
			val := tc.TimeAndFlags(tt.packing)

			// Set again from the retrieved value and read back — must be stable
			tc2 := MustNewTimeCode(0, 0, 0, 0, false)
			tc2.SetTimeAndFlags(val, tt.packing)
			val2 := tc2.TimeAndFlags(tt.packing)

			if val != val2 {
				t.Errorf("SetTimeAndFlags/TimeAndFlags roundtrip not stable: first=0x%08X, second=0x%08X", val, val2)
			}
		})
	}
}

func TestV2dSerialization(t *testing.T) {
	original := V2d{X: 1.5, Y: -2.5}
	w := xdr.NewBufferWriter(32)
	WriteV2d(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV2d(r)
	if err != nil {
		t.Fatalf("ReadV2d() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV2d() = %v, want %v", result, original)
	}
}

func TestV3dSerialization(t *testing.T) {
	original := V3d{X: 1.0, Y: 2.0, Z: 3.0}
	w := xdr.NewBufferWriter(32)
	WriteV3d(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadV3d(r)
	if err != nil {
		t.Fatalf("ReadV3d() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadV3d() = %v, want %v", result, original)
	}
}

func TestM33dSerialization(t *testing.T) {
	original := M33d{1, 2, 3, 4, 5, 6, 7, 8, 9}
	w := xdr.NewBufferWriter(128)
	WriteM33d(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadM33d(r)
	if err != nil {
		t.Fatalf("ReadM33d() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadM33d() = %v, want %v", result, original)
	}
}

func TestM44dSerialization(t *testing.T) {
	original := M44d{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	w := xdr.NewBufferWriter(256)
	WriteM44d(w, original)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadM44d(r)
	if err != nil {
		t.Fatalf("ReadM44d() error = %v", err)
	}
	if result != original {
		t.Errorf("ReadM44d() = %v, want %v", result, original)
	}
}

func TestReadFloatVector(t *testing.T) {
	// Create valid float vector data: count=3, followed by 3 floats
	w := xdr.NewBufferWriter(32)
	w.WriteInt32(3) // count
	w.WriteFloat32(1.0)
	w.WriteFloat32(2.0)
	w.WriteFloat32(3.0)

	r := xdr.NewReader(w.Bytes())
	result, err := ReadFloatVector(r, 16) // 4 bytes count + 3*4 bytes floats
	if err != nil {
		t.Fatalf("ReadFloatVector() error = %v", err)
	}
	if len(result) != 3 {
		t.Errorf("ReadFloatVector() length = %d, want 3", len(result))
	}
	if result[0] != 1.0 || result[1] != 2.0 || result[2] != 3.0 {
		t.Errorf("ReadFloatVector() = %v, want [1.0, 2.0, 3.0]", result)
	}
}

func TestReadFloatVectorErrors(t *testing.T) {
	t.Run("TooSmallSize", func(t *testing.T) {
		r := xdr.NewReader([]byte{0, 0, 0, 0})
		_, err := ReadFloatVector(r, 3) // Size too small
		if err == nil {
			t.Error("ReadFloatVector with size < 4 should error")
		}
	})

	t.Run("NegativeCount", func(t *testing.T) {
		// Create data with negative count
		w := xdr.NewBufferWriter(8)
		w.WriteInt32(-1)
		r := xdr.NewReader(w.Bytes())
		_, err := ReadFloatVector(r, 4)
		if err == nil {
			t.Error("ReadFloatVector with negative count should error")
		}
	})

	t.Run("SizeMismatch", func(t *testing.T) {
		// Create data with count=2 but provide wrong size
		w := xdr.NewBufferWriter(32)
		w.WriteInt32(2)
		w.WriteFloat32(1.0)
		w.WriteFloat32(2.0)
		r := xdr.NewReader(w.Bytes())
		_, err := ReadFloatVector(r, 8) // Wrong size - should be 12
		if err == nil {
			t.Error("ReadFloatVector with size mismatch should error")
		}
	})
}

func TestReadFloatVectorEmpty(t *testing.T) {
	// Create valid float vector with count=0
	w := xdr.NewBufferWriter(8)
	w.WriteInt32(0) // count = 0

	r := xdr.NewReader(w.Bytes())
	result, err := ReadFloatVector(r, 4) // Just the count, no floats
	if err != nil {
		t.Fatalf("ReadFloatVector() error = %v", err)
	}
	if len(result) != 0 {
		t.Errorf("ReadFloatVector() length = %d, want 0", len(result))
	}
}

func TestWriteFloatVector(t *testing.T) {
	original := FloatVector{1.5, 2.5, 3.5}
	w := xdr.NewBufferWriter(32)
	WriteFloatVector(w, original)

	// Read back
	r := xdr.NewReader(w.Bytes())
	result, err := ReadFloatVector(r, 16) // 4 + 3*4 = 16
	if err != nil {
		t.Fatalf("ReadFloatVector() error = %v", err)
	}
	if len(result) != len(original) {
		t.Errorf("Length = %d, want %d", len(result), len(original))
	}
	for i := range original {
		if result[i] != original[i] {
			t.Errorf("FloatVector[%d] = %f, want %f", i, result[i], original[i])
		}
	}
}

func TestMustNewTimeCodePanic(t *testing.T) {
	// MustNewTimeCode should panic on invalid input
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewTimeCode with invalid frames should panic")
		}
	}()

	// 60 frames is invalid (max is typically 59)
	_ = MustNewTimeCode(0, 0, 0, 60, false)
}

func TestMustNewTimeCodeInvalidHours(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewTimeCode with invalid hours should panic")
		}
	}()

	_ = MustNewTimeCode(25, 0, 0, 0, false) // 25 hours is invalid
}

func TestMustNewTimeCodeInvalidMinutes(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewTimeCode with invalid minutes should panic")
		}
	}()

	_ = MustNewTimeCode(0, 60, 0, 0, false) // 60 minutes is invalid
}

func TestMustNewTimeCodeInvalidSeconds(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("MustNewTimeCode with invalid seconds should panic")
		}
	}()

	_ = MustNewTimeCode(0, 0, 60, 0, false) // 60 seconds is invalid
}
