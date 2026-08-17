package exr

import (
	"errors"
	"testing"
)

// AllocateChannels takes its geometry straight from a file header, so a
// malformed or hostile file reaches it directly. These cases previously
// panicked or tried to allocate absurd amounts; none of them may do either.
func TestAllocateChannelsRejectsHostileGeometry(t *testing.T) {
	huge := Box2i{Min: V2i{0, 0}, Max: V2i{1 << 20, 1 << 20}}
	normal := Box2i{Min: V2i{0, 0}, Max: V2i{15, 15}}

	tests := []struct {
		name       string
		channels   []Channel
		dataWindow Box2i
		wantErr    bool
	}{
		{
			name:       "zero XSampling would divide by zero",
			channels:   []Channel{{Name: "R", Type: PixelTypeHalf, XSampling: 0, YSampling: 1}},
			dataWindow: normal,
			wantErr:    true,
		},
		{
			name:       "zero YSampling would divide by zero",
			channels:   []Channel{{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 0}},
			dataWindow: normal,
			wantErr:    true,
		},
		{
			name:       "negative sampling",
			channels:   []Channel{{Name: "R", Type: PixelTypeHalf, XSampling: -1, YSampling: 1}},
			dataWindow: normal,
			wantErr:    true,
		},
		{
			name:       "degenerate data window",
			channels:   []Channel{{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1}},
			dataWindow: Box2i{Min: V2i{10, 10}, Max: V2i{0, 0}},
			wantErr:    true,
		},
		{
			name:       "single channel over the limit",
			channels:   []Channel{{Name: "R", Type: PixelTypeFloat, XSampling: 1, YSampling: 1}},
			dataWindow: huge,
			wantErr:    true,
		},
		{
			name: "many channels that individually fit but together do not",
			channels: func() []Channel {
				out := make([]Channel, 0, 512)
				for i := 0; i < 512; i++ {
					out = append(out, Channel{
						Name: string(rune('a'+i%26)) + string(rune('a'+i/26)),
						Type: PixelTypeFloat, XSampling: 1, YSampling: 1,
					})
				}
				return out
			}(),
			dataWindow: Box2i{Min: V2i{0, 0}, Max: V2i{8191, 8191}},
			wantErr:    true,
		},
		{
			name:       "ordinary image is allocated",
			channels:   []Channel{{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1}},
			dataWindow: normal,
			wantErr:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cl := NewChannelList()
			for _, ch := range tc.channels {
				cl.Add(ch)
			}

			// Must not panic, whatever the input.
			fb, bufs, err := AllocateChannelsLimit(cl, tc.dataWindow, DefaultAllocationLimit)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got a frame buffer with %d channels", fb.Len())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if fb == nil || fb.Len() != len(tc.channels) {
				t.Fatalf("expected %d channels", len(tc.channels))
			}
			if len(bufs) != len(tc.channels) {
				t.Fatalf("expected %d buffers, got %d", len(tc.channels), len(bufs))
			}

			// The legacy signature must never panic either, and must degrade to
			// an empty frame buffer rather than a nil one.
			legacyFB, legacyBufs := AllocateChannels(cl, tc.dataWindow)
			if legacyFB == nil || legacyBufs == nil {
				t.Fatal("AllocateChannels returned nil; callers dereference these")
			}
		})
	}
}

// TestAllocateChannelsLegacyDegradesSafely pins the behaviour of the
// error-free signature on input it cannot honour: an empty but usable frame
// buffer, never a panic and never a nil that a caller would dereference.
func TestAllocateChannelsLegacyDegradesSafely(t *testing.T) {
	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeFloat, XSampling: 1, YSampling: 1})

	fb, bufs := AllocateChannels(cl, Box2i{Min: V2i{0, 0}, Max: V2i{1 << 20, 1 << 20}})
	if fb == nil {
		t.Fatal("frame buffer is nil")
	}
	if fb.Len() != 0 {
		t.Fatalf("expected an empty frame buffer, got %d channels", fb.Len())
	}
	if bufs == nil {
		t.Fatal("buffer map is nil")
	}

	// And it stays usable: setting it on a reader must not panic.
	fb.Set("R", Slice{})
}

func TestAllocateChannelsLimitHonoursCustomCeiling(t *testing.T) {
	cl := NewChannelList()
	cl.Add(Channel{Name: "R", Type: PixelTypeFloat, XSampling: 1, YSampling: 1})
	dw := Box2i{Min: V2i{0, 0}, Max: V2i{99, 99}} // 100x100x4 = 40000 bytes

	if _, _, err := AllocateChannelsLimit(cl, dw, 40000); err != nil {
		t.Fatalf("exactly-at-limit should be allowed: %v", err)
	}
	_, _, err := AllocateChannelsLimit(cl, dw, 39999)
	if !errors.Is(err, ErrAllocationTooLarge) {
		t.Fatalf("expected ErrAllocationTooLarge, got %v", err)
	}
}
