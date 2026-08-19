package exr

import "fmt"

// DeepImageState declares what a deep image's samples promise about themselves.
//
// It is a claim, not a transformation: nothing in the format checks it, and a
// file that says its samples are sorted and non-overlapping while they are
// neither is a file every consumer will mis-composite. The claim is what a
// compositor reads to decide whether it can merge samples without sorting them
// first, which is why writing one that is not true is worse than writing none.
//
// See ISO/IEC 22028-5 (OpenEXR) deepImageState.
type DeepImageState uint8

const (
	// DeepImageStateMessy makes no promise: samples may be in any order and
	// may overlap in Z. This is the safe default and what an image with no
	// deepImageState attribute means.
	DeepImageStateMessy DeepImageState = 0

	// DeepImageStateSorted promises every pixel's samples are in increasing
	// Z order. They may still overlap.
	DeepImageStateSorted DeepImageState = 1

	// DeepImageStateNonOverlapping promises no two samples of a pixel overlap
	// in Z. They may be in any order.
	DeepImageStateNonOverlapping DeepImageState = 2

	// DeepImageStateTidy promises both: sorted by Z and non-overlapping. This
	// is what a compositor wants, and the only state that lets it merge
	// samples without sorting them first.
	DeepImageStateTidy DeepImageState = 3
)

func (s DeepImageState) String() string {
	switch s {
	case DeepImageStateMessy:
		return "messy"
	case DeepImageStateSorted:
		return "sorted"
	case DeepImageStateNonOverlapping:
		return "non-overlapping"
	case DeepImageStateTidy:
		return "tidy"
	}
	return fmt.Sprintf("unknown(%d)", uint8(s))
}

// IsValid reports whether s is one of the four states the format defines.
func (s DeepImageState) IsValid() bool { return s <= DeepImageStateTidy }

// Sorted reports whether the state promises samples are in increasing Z order.
func (s DeepImageState) Sorted() bool {
	return s == DeepImageStateSorted || s == DeepImageStateTidy
}

// NonOverlapping reports whether the state promises no two samples of a pixel
// overlap in Z.
func (s DeepImageState) NonOverlapping() bool {
	return s == DeepImageStateNonOverlapping || s == DeepImageStateTidy
}

// DeepImageState returns the image's declared sample state, and whether the
// attribute was present. An absent attribute means messy: no promise.
func (h *Header) DeepImageState() (DeepImageState, bool) {
	attr := h.Get(AttrNameDeepImageState)
	if attr == nil {
		return DeepImageStateMessy, false
	}
	if v, ok := attr.Value.(DeepImageState); ok {
		return v, true
	}
	return DeepImageStateMessy, false
}

// SetDeepImageState declares what the samples promise.
//
// It does not sort or merge anything: use VerifyDeepImageState to check that
// the samples actually satisfy the claim before making it.
func (h *Header) SetDeepImageState(s DeepImageState) {
	h.Set(&Attribute{Name: AttrNameDeepImageState, Type: AttrTypeDeepImageState, Value: s})
}

// ErrDeepStateViolated reports samples that do not satisfy the state their
// header declares.
var ErrDeepStateViolated = fmt.Errorf("exr: deep samples do not satisfy the declared deepImageState")

// VerifyDeepImageState checks a deep frame buffer against a claim.
//
// zChannel names the depth channel to sort on — "Z" in every image that follows
// the convention. A sample's extent is [Z, ZBack] when a ZBack channel is
// present and the single point Z when it is not, which is what "overlapping"
// is measured against.
//
// This exists because the claim is otherwise unfalsifiable: nothing in the
// format checks it, no reader complains, and the consequence appears much later
// as a composite that is subtly wrong.
func VerifyDeepImageState(fb *DeepFrameBuffer, state DeepImageState, zChannel string) error {
	if fb == nil {
		return fmt.Errorf("exr: nil deep frame buffer")
	}
	if !state.IsValid() {
		return fmt.Errorf("exr: deepImageState %d is not one of the four the format defines", uint8(state))
	}
	if state == DeepImageStateMessy {
		return nil // promises nothing
	}
	z := fb.Slices[zChannel]
	if z == nil {
		return fmt.Errorf("exr: deepImageState %v claims an ordering but there is no %q channel to order by",
			state, zChannel)
	}
	back := fb.Slices["ZBack"]

	for y := 0; y < fb.Height; y++ {
		for x := 0; x < fb.Width; x++ {
			n := int(fb.GetSampleCount(x, y))
			if n < 2 {
				continue
			}
			prevFront, prevBack := float32(0), float32(0)
			for s := 0; s < n; s++ {
				front := z.GetSampleFloat32(x, y, s)
				rear := front
				if back != nil {
					rear = back.GetSampleFloat32(x, y, s)
				}
				if s > 0 {
					if state.Sorted() && front < prevFront {
						return fmt.Errorf("%w: pixel (%d,%d) sample %d has %s %g after %g",
							ErrDeepStateViolated, x, y, s, zChannel, front, prevFront)
					}
					// Overlap is only meaningful between samples that are
					// ordered; for an unsorted image every pair is compared.
					if state.NonOverlapping() && front < prevBack {
						return fmt.Errorf("%w: pixel (%d,%d) sample %d starts at %g, inside the previous sample ending at %g",
							ErrDeepStateViolated, x, y, s, front, prevBack)
					}
				}
				prevFront, prevBack = front, rear
			}
		}
	}
	return nil
}
