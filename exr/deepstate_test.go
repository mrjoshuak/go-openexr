package exr

import (
	"errors"
	"testing"
)

// buildDeepState makes a 4x4 deep frame buffer with three samples per pixel,
// laid out according to the caller's choices.
func buildDeepState(t *testing.T, sorted, overlapping bool) *DeepFrameBuffer {
	t.Helper()
	const w, h = 4, 4
	fb := NewDeepFrameBuffer(w, h)
	fb.Insert("Z", PixelTypeFloat)
	fb.Insert("ZBack", PixelTypeFloat)
	fb.Insert("A", PixelTypeFloat)

	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			fb.SetSampleCount(x, y, 3)
			fb.AllocateSamples(x, y)
			// Fronts at 1, 2, 3 and a depth of 0.5 each: adjacent but not
			// overlapping. Widening them to 1.5 makes each sample run into
			// the next.
			depth := float32(0.5)
			if overlapping {
				depth = 1.5
			}
			order := []int{0, 1, 2}
			if !sorted {
				order = []int{2, 0, 1}
			}
			for s, which := range order {
				front := float32(which) + 1
				fb.Slices["Z"].SetSampleFloat32(x, y, s, front)
				fb.Slices["ZBack"].SetSampleFloat32(x, y, s, front+depth)
				fb.Slices["A"].SetSampleFloat32(x, y, s, 0.25)
			}
		}
	}
	return fb
}

// TestVerifyDeepImageStateCatchesFalseClaims is the point of the verifier.
//
// deepImageState is a claim about the samples that nothing in the format
// checks: no reader complains, and the consequence appears much later as a
// composite that is subtly wrong. A writer that declares "tidy" over unsorted
// or overlapping samples is lying in a way only a check like this can catch.
func TestVerifyDeepImageStateCatchesFalseClaims(t *testing.T) {
	cases := []struct {
		name             string
		sorted, overlap  bool
		state            DeepImageState
		wantViolation    bool
		violationBecause string
	}{
		{"tidy over tidy samples", true, false, DeepImageStateTidy, false, ""},
		{"sorted over sorted samples", true, true, DeepImageStateSorted, false, ""},
		{"messy promises nothing", false, true, DeepImageStateMessy, false, ""},
		{"non-overlapping over separated samples", true, false, DeepImageStateNonOverlapping, false, ""},

		{"tidy over unsorted samples", false, false, DeepImageStateTidy, true, "unsorted"},
		{"sorted over unsorted samples", false, false, DeepImageStateSorted, true, "unsorted"},
		{"tidy over overlapping samples", true, true, DeepImageStateTidy, true, "overlapping"},
		{"non-overlapping over overlapping samples", true, true, DeepImageStateNonOverlapping, true, "overlapping"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fb := buildDeepState(t, tc.sorted, tc.overlap)
			err := VerifyDeepImageState(fb, tc.state, "Z")
			if tc.wantViolation {
				if err == nil {
					t.Fatalf("declaring %v over %s samples was accepted; the claim is unfalsifiable if nothing checks it",
						tc.state, tc.violationBecause)
				}
				if !errors.Is(err, ErrDeepStateViolated) {
					t.Errorf("error does not wrap ErrDeepStateViolated: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("declaring %v over samples that satisfy it was refused: %v", tc.state, err)
			}
		})
	}
}

// TestVerifyDeepImageStateNeedsADepthChannel: a state that orders samples
// cannot be checked, or honestly claimed, without the channel it orders by.
func TestVerifyDeepImageStateNeedsADepthChannel(t *testing.T) {
	fb := NewDeepFrameBuffer(2, 2)
	fb.Insert("A", PixelTypeFloat)
	for y := 0; y < 2; y++ {
		for x := 0; x < 2; x++ {
			fb.SetSampleCount(x, y, 2)
			fb.AllocateSamples(x, y)
		}
	}
	if err := VerifyDeepImageState(fb, DeepImageStateTidy, "Z"); err == nil {
		t.Error("a tidy claim over an image with no Z channel was accepted")
	}
	// Messy promises nothing, so it needs nothing.
	if err := VerifyDeepImageState(fb, DeepImageStateMessy, "Z"); err != nil {
		t.Errorf("messy was refused: %v", err)
	}
}

// TestDeepImageStateRoundTripsThroughAHeader pins the attribute itself: an
// absent one means messy, and a written one comes back as written.
func TestDeepImageStateRoundTripsThroughAHeader(t *testing.T) {
	h := NewHeader()
	if s, present := h.DeepImageState(); present || s != DeepImageStateMessy {
		t.Errorf("a header with no attribute reports %v present=%v, want messy and absent", s, present)
	}
	for _, want := range []DeepImageState{
		DeepImageStateMessy, DeepImageStateSorted,
		DeepImageStateNonOverlapping, DeepImageStateTidy,
	} {
		h.SetDeepImageState(want)
		got, present := h.DeepImageState()
		if !present || got != want {
			t.Errorf("set %v, read back %v present=%v", want, got, present)
		}
		if got.String() == "" {
			t.Errorf("%v has no name", want)
		}
	}
}
