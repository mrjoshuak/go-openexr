// Package exr provides compliance tests that verify behavior matches the
// C++ OpenEXR reference implementation from upstream/src/lib/OpenEXR/.
package exr

import (
	"strings"
	"testing"
)

// =============================================================================
// TimeCode Compliance Tests
// =============================================================================
//
// Reference: upstream/src/lib/OpenEXR/ImfTimeCode.cpp
// These tests verify SMPTE 12M-1999 compliance including BCD encoding.

// TestTimeCode_BCDEncoding verifies that time values are stored in BCD format.
func TestTimeCode_BCDEncoding(t *testing.T) {
	testCases := []struct {
		name             string
		hours            int
		minutes          int
		seconds          int
		frames           int
		wantTimeAndFlags uint32 // Expected TV60 packed value
	}{
		{
			name:  "midnight",
			hours: 0, minutes: 0, seconds: 0, frames: 0,
			wantTimeAndFlags: 0x00000000,
		},
		{
			name:  "simple incrementing",
			hours: 1, minutes: 2, seconds: 3, frames: 4,
			// BCD: hours=0x01, mins=0x02, secs=0x03, frames=0x04
			wantTimeAndFlags: 0x01020304,
		},
		{
			name:  "common timecode 12:34:56:29",
			hours: 12, minutes: 34, seconds: 56, frames: 29,
			// BCD encoding:
			// hours=12 → BCD 0x12, bits 24-29
			// minutes=34 → BCD 0x34, bits 16-22
			// seconds=56 → BCD 0x56, bits 8-14
			// frames=29 → BCD 0x29, bits 0-5
			wantTimeAndFlags: 0x12345629,
		},
		{
			name:  "max values 23:59:59:29",
			hours: 23, minutes: 59, seconds: 59, frames: 29,
			// BCD: 0x23, 0x59, 0x59, 0x29
			wantTimeAndFlags: 0x23595929,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			timecode, err := NewTimeCode(tc.hours, tc.minutes, tc.seconds, tc.frames, false)
			if err != nil {
				t.Fatalf("NewTimeCode() error = %v", err)
			}

			got := timecode.TimeAndFlags(TV60Packing)
			if got != tc.wantTimeAndFlags {
				t.Errorf("TimeAndFlags(TV60) = %#08x, want %#08x", got, tc.wantTimeAndFlags)
			}

			// Verify round-trip
			if timecode.Hours() != tc.hours {
				t.Errorf("Hours() = %d, want %d", timecode.Hours(), tc.hours)
			}
			if timecode.Minutes() != tc.minutes {
				t.Errorf("Minutes() = %d, want %d", timecode.Minutes(), tc.minutes)
			}
			if timecode.Seconds() != tc.seconds {
				t.Errorf("Seconds() = %d, want %d", timecode.Seconds(), tc.seconds)
			}
			if timecode.Frames() != tc.frames {
				t.Errorf("Frames() = %d, want %d", timecode.Frames(), tc.frames)
			}
		})
	}
}

// TestTimeCode_AllFlags verifies all SMPTE 12M-1999 flags.
func TestTimeCode_AllFlags(t *testing.T) {
	tc := MustNewTimeCode(0, 0, 0, 0, false)

	// Test each flag
	flags := []struct {
		name string
		set  func(*TimeCode, bool)
		get  func(TimeCode) bool
		bit  int // Bit position in TV60 packing
	}{
		{"DropFrame", (*TimeCode).SetDropFrame, TimeCode.DropFrame, 6},
		{"ColorFrame", (*TimeCode).SetColorFrame, TimeCode.ColorFrame, 7},
		{"FieldPhase", (*TimeCode).SetFieldPhase, TimeCode.FieldPhase, 15},
		{"Bgf0", (*TimeCode).SetBgf0, TimeCode.Bgf0, 23},
		{"Bgf1", (*TimeCode).SetBgf1, TimeCode.Bgf1, 30},
		{"Bgf2", (*TimeCode).SetBgf2, TimeCode.Bgf2, 31},
	}

	for _, f := range flags {
		t.Run(f.name, func(t *testing.T) {
			// Initially false
			if f.get(tc) {
				t.Errorf("%s should initially be false", f.name)
			}

			// Set to true
			f.set(&tc, true)
			if !f.get(tc) {
				t.Errorf("%s should be true after setting", f.name)
			}

			// Verify bit position in packed representation
			packed := tc.TimeAndFlags(TV60Packing)
			if packed&(1<<f.bit) == 0 {
				t.Errorf("%s: bit %d not set in packed value %#08x", f.name, f.bit, packed)
			}

			// Set back to false
			f.set(&tc, false)
			if f.get(tc) {
				t.Errorf("%s should be false after unsetting", f.name)
			}
		})
	}
}

// TestTimeCode_PackingVariants verifies TV60, TV50, and Film24 packing.
func TestTimeCode_PackingVariants(t *testing.T) {
	// Create a timecode with all flags set
	tc := MustNewTimeCode(12, 34, 56, 29, true)
	tc.SetColorFrame(true)
	tc.SetFieldPhase(true)
	tc.SetBgf0(true)
	tc.SetBgf1(true)
	tc.SetBgf2(true)

	t.Run("TV60Packing", func(t *testing.T) {
		packed := tc.TimeAndFlags(TV60Packing)
		// All flags should be present at their TV60 positions
		if packed&(1<<6) == 0 {
			t.Error("dropFrame bit should be set")
		}
		if packed&(1<<7) == 0 {
			t.Error("colorFrame bit should be set")
		}
		if packed&(1<<15) == 0 {
			t.Error("fieldPhase bit should be set")
		}
		if packed&(1<<23) == 0 {
			t.Error("bgf0 bit should be set")
		}
		if packed&(1<<30) == 0 {
			t.Error("bgf1 bit should be set")
		}
		if packed&(1<<31) == 0 {
			t.Error("bgf2 bit should be set")
		}
	})

	t.Run("TV50Packing", func(t *testing.T) {
		packed := tc.TimeAndFlags(TV50Packing)
		// TV50 has different flag positions:
		// - bit 6: unused (dropFrame cleared)
		// - bit 15: bgf0
		// - bit 23: bgf2
		// - bit 30: bgf1
		// - bit 31: fieldPhase
		if packed&(1<<6) != 0 {
			t.Error("bit 6 should be clear in TV50 packing")
		}
		if packed&(1<<15) == 0 {
			t.Error("bgf0 should be at bit 15")
		}
		if packed&(1<<23) == 0 {
			t.Error("bgf2 should be at bit 23")
		}
		if packed&(1<<30) == 0 {
			t.Error("bgf1 should be at bit 30")
		}
		if packed&(1<<31) == 0 {
			t.Error("fieldPhase should be at bit 31")
		}
	})

	t.Run("Film24Packing", func(t *testing.T) {
		packed := tc.TimeAndFlags(Film24Packing)
		// Film24 clears dropFrame and colorFrame
		if packed&(1<<6) != 0 {
			t.Error("dropFrame should be clear in Film24 packing")
		}
		if packed&(1<<7) != 0 {
			t.Error("colorFrame should be clear in Film24 packing")
		}
	})

	t.Run("PackingRoundTrip", func(t *testing.T) {
		// Verify round-trip through each packing
		for _, packing := range []TimeCodePacking{TV60Packing, TV50Packing, Film24Packing} {
			packed := tc.TimeAndFlags(packing)
			tc2 := NewTimeCodeFromPacked(packed, 0, packing)

			// Time values should always round-trip
			if tc2.Hours() != 12 {
				t.Errorf("Hours round-trip failed for packing %d", packing)
			}
			if tc2.Minutes() != 34 {
				t.Errorf("Minutes round-trip failed for packing %d", packing)
			}
			if tc2.Seconds() != 56 {
				t.Errorf("Seconds round-trip failed for packing %d", packing)
			}
			if tc2.Frames() != 29 {
				t.Errorf("Frames round-trip failed for packing %d", packing)
			}
		}
	})
}

// TestTimeCode_RangeValidation verifies range checking matches C++.
func TestTimeCode_RangeValidation(t *testing.T) {
	tests := []struct {
		name    string
		hours   int
		minutes int
		seconds int
		frames  int
		wantErr error
	}{
		{"valid", 12, 30, 45, 24, nil},
		{"hours too high", 24, 0, 0, 0, ErrTimeCodeHoursOutOfRange},
		{"hours negative", -1, 0, 0, 0, ErrTimeCodeHoursOutOfRange},
		{"minutes too high", 0, 60, 0, 0, ErrTimeCodeMinutesOutOfRange},
		{"seconds too high", 0, 0, 60, 0, ErrTimeCodeSecondsOutOfRange},
		{"frames too high", 0, 0, 0, 30, ErrTimeCodeFramesOutOfRange},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTimeCode(tt.hours, tt.minutes, tt.seconds, tt.frames, false)
			if err != tt.wantErr {
				t.Errorf("NewTimeCode() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

// TestTimeCode_BinaryGroups verifies binary group accessors.
func TestTimeCode_BinaryGroups(t *testing.T) {
	tc := MustNewTimeCode(0, 0, 0, 0, false)

	// Set each binary group
	for group := 1; group <= 8; group++ {
		value := group % 16 // 0-15
		if err := tc.SetBinaryGroup(group, value); err != nil {
			t.Fatalf("SetBinaryGroup(%d, %d) error = %v", group, value, err)
		}
	}

	// Verify each binary group
	for group := 1; group <= 8; group++ {
		expected := group % 16
		got, err := tc.BinaryGroup(group)
		if err != nil {
			t.Fatalf("BinaryGroup(%d) error = %v", group, err)
		}
		if got != expected {
			t.Errorf("BinaryGroup(%d) = %d, want %d", group, got, expected)
		}
	}

	// Verify error for invalid group numbers
	_, err := tc.BinaryGroup(0)
	if err != ErrTimeCodeBinaryGroup {
		t.Errorf("BinaryGroup(0) should return ErrTimeCodeBinaryGroup")
	}
	_, err = tc.BinaryGroup(9)
	if err != ErrTimeCodeBinaryGroup {
		t.Errorf("BinaryGroup(9) should return ErrTimeCodeBinaryGroup")
	}
}

// =============================================================================
// KeyCode Compliance Tests
// =============================================================================

// TestKeyCode_Validation verifies KeyCode field validation.
// Note: The C++ OpenEXR library does not validate KeyCode fields.
// These tests document the SMPTE 254M standard ranges.
func TestKeyCode_Validation(t *testing.T) {
	// KeyCode field ranges per SMPTE 254M:
	// - FilmMfcCode:   Manufacturer code (typically 1-99)
	// - FilmType:      Film type code (manufacturer-specific)
	// - Prefix:        Production prefix (0-999999)
	// - Count:         Frame count (0-999999)
	// - PerfOffset:    Perforation offset (typically 0-3)
	// - PerfsPerFrame: Perforations per frame (typically 3, 4, or 8)
	// - PerfsPerCount: Perforations per count (typically 64 for 35mm)

	// Document typical valid values
	kc := KeyCode{
		FilmMfcCode:   1,
		FilmType:      52,
		Prefix:        123456,
		Count:         1000,
		PerfOffset:    0,
		PerfsPerFrame: 4,
		PerfsPerCount: 64,
	}

	// These are just documentation tests - we don't enforce validation
	// to match C++ behavior
	if kc.FilmMfcCode != 1 {
		t.Error("FilmMfcCode should be preserved")
	}
	if kc.PerfsPerFrame != 4 {
		t.Error("PerfsPerFrame should be preserved")
	}
}

// =============================================================================
// HTJ2K Status
// =============================================================================

// TestHTJ2KIsSupported replaces a test that logged "HTJ2K: Not supported
// (intentional limitation)" and asserted nothing.
//
// Both HTJ2K compressions are implemented and verified bit-identical against
// the reference implementation for half, float and uint at both block sizes, so
// the old test was not merely vacuous — it stated the opposite of the truth,
// and a reader looking for whether this package could write HTJ2K would have
// been told no.
func TestHTJ2KIsSupported(t *testing.T) {
	for _, c := range []Compression{CompressionHTJ2K256, CompressionHTJ2K32} {
		if got := c.String(); got == "" || strings.Contains(strings.ToLower(got), "unknown") {
			t.Errorf("%d has no name, so it is not a compression this package knows", int(c))
		}
		if n := c.ScanlinesPerChunk(); n <= 0 {
			t.Errorf("%v claims %d scanlines per chunk", c, n)
		}
		if c.IsLossy() {
			t.Errorf("%v reports itself lossy; both HTJ2K compressions in the format are lossless", c)
		}
	}

	// The chunk size is the difference between the two, and it is what a caller
	// chooses between them for.
	if a, b := CompressionHTJ2K256.ScanlinesPerChunk(), CompressionHTJ2K32.ScanlinesPerChunk(); a <= b {
		t.Errorf("HTJ2K256 packs %d scanlines per chunk and HTJ2K32 packs %d; "+
			"the first must pack more, or the two names mean nothing", a, b)
	}
}
