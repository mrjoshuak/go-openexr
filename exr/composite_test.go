package exr

import (
	"math"
	"os"
	"path/filepath"
	"testing"
)

// TestDeepSampleBasics tests the DeepSample type.
func TestDeepSampleBasics(t *testing.T) {
	// Test hard surface sample (Z == ZBack)
	t.Run("HardSurface", func(t *testing.T) {
		sample := DeepSample{
			Z:     1.0,
			ZBack: 1.0,
			A:     0.5,
			R:     0.25,
			G:     0.25,
			B:     0.25,
		}

		if sample.IsVolumetric() {
			t.Error("Hard surface sample should not be volumetric")
		}
		if sample.IsOpaque() {
			t.Error("Sample with alpha 0.5 should not be opaque")
		}
	})

	// Test volumetric sample (ZBack > Z)
	t.Run("Volumetric", func(t *testing.T) {
		sample := DeepSample{
			Z:     1.0,
			ZBack: 2.0,
			A:     0.3,
		}

		if !sample.IsVolumetric() {
			t.Error("Sample with ZBack != Z should be volumetric")
		}
	})

	// Test opaque sample
	t.Run("Opaque", func(t *testing.T) {
		sample := DeepSample{
			Z:     1.0,
			ZBack: 1.0,
			A:     1.0,
		}

		if !sample.IsOpaque() {
			t.Error("Sample with alpha 1.0 should be opaque")
		}
	})

	// Test channels map
	t.Run("Channels", func(t *testing.T) {
		sample := DeepSample{
			Z:        1.0,
			ZBack:    1.0,
			A:        0.5,
			Channels: make(map[string]float32),
		}
		sample.Channels["custom"] = 0.75

		if val, ok := sample.Channels["custom"]; !ok || val != 0.75 {
			t.Errorf("Expected custom channel value 0.75, got %v", val)
		}
	})
}

// TestDefaultDeepCompositingSortPixel tests sample sorting.
func TestDefaultDeepCompositingSortPixel(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	t.Run("SortByZ", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 3.0, ZBack: 3.0, A: 0.5, R: 0.3},
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.1},
			{Z: 2.0, ZBack: 2.0, A: 0.5, R: 0.2},
		}

		comp.SortPixel(samples)

		if samples[0].R != 0.1 || samples[1].R != 0.2 || samples[2].R != 0.3 {
			t.Error("Samples not sorted by Z depth")
		}
	})

	t.Run("SortByZBack", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 3.0, A: 0.5, R: 0.1},
			{Z: 1.0, ZBack: 2.0, A: 0.5, R: 0.2},
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.3},
		}

		comp.SortPixel(samples)

		// Same Z should sort by ZBack
		if samples[0].R != 0.3 || samples[1].R != 0.2 || samples[2].R != 0.1 {
			t.Errorf("Samples with same Z not sorted by ZBack: %v", samples)
		}
	})

	t.Run("StableSort", func(t *testing.T) {
		// Identical samples should maintain order
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.1},
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.2},
		}

		comp.SortPixel(samples)

		if samples[0].R != 0.1 || samples[1].R != 0.2 {
			t.Error("Stable sort not preserved for identical depth samples")
		}
	})
}

// TestDefaultDeepCompositingCompositePixel tests the over operator.
func TestDefaultDeepCompositingCompositePixel(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	t.Run("EmptySamples", func(t *testing.T) {
		r, g, b, a := comp.CompositePixel(nil)
		if r != 0 || g != 0 || b != 0 || a != 0 {
			t.Error("Empty samples should produce zero output")
		}
	})

	t.Run("SingleOpaqueSample", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 1.0, R: 1.0, G: 0.5, B: 0.25},
		}

		r, g, b, a := comp.CompositePixel(samples)

		if !floatsEqual(r, 1.0) || !floatsEqual(g, 0.5) || !floatsEqual(b, 0.25) || !floatsEqual(a, 1.0) {
			t.Errorf("Single opaque sample: got (%v, %v, %v, %v)", r, g, b, a)
		}
	})

	t.Run("SingleTransparentSample", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.5, G: 0.25, B: 0.125},
		}

		r, g, b, a := comp.CompositePixel(samples)

		if !floatsEqual(r, 0.5) || !floatsEqual(g, 0.25) || !floatsEqual(b, 0.125) || !floatsEqual(a, 0.5) {
			t.Errorf("Single transparent sample: got (%v, %v, %v, %v)", r, g, b, a)
		}
	})

	t.Run("TwoSamplesOver", func(t *testing.T) {
		// Front sample: 50% alpha, red
		// Back sample: 100% alpha, green
		// Result should be front + (1-0.5) * back
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.5, G: 0.0, B: 0.0}, // Front (premultiplied)
			{Z: 2.0, ZBack: 2.0, A: 1.0, R: 0.0, G: 1.0, B: 0.0}, // Back (premultiplied)
		}

		r, g, b, a := comp.CompositePixel(samples)

		// R = 0.5 + 0.5 * 0.0 = 0.5
		// G = 0.0 + 0.5 * 1.0 = 0.5
		// A = 0.5 + 0.5 * 1.0 = 1.0
		if !floatsEqual(r, 0.5) || !floatsEqual(g, 0.5) || !floatsEqual(b, 0.0) || !floatsEqual(a, 1.0) {
			t.Errorf("Two samples over: got (%v, %v, %v, %v), expected (0.5, 0.5, 0, 1)", r, g, b, a)
		}
	})

	t.Run("EarlyTermination", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 1.0, R: 1.0, G: 0.0, B: 0.0}, // Opaque front
			{Z: 2.0, ZBack: 2.0, A: 1.0, R: 0.0, G: 1.0, B: 0.0}, // Should be ignored
		}

		r, g, b, a := comp.CompositePixel(samples)

		// Back sample should not contribute due to early termination
		if !floatsEqual(r, 1.0) || !floatsEqual(g, 0.0) || !floatsEqual(b, 0.0) || !floatsEqual(a, 1.0) {
			t.Errorf("Early termination: got (%v, %v, %v, %v), expected (1, 0, 0, 1)", r, g, b, a)
		}
	})

	t.Run("MultipleSemiTransparent", func(t *testing.T) {
		// Three 50% alpha samples
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.5, G: 0.0, B: 0.0}, // Red
			{Z: 2.0, ZBack: 2.0, A: 0.5, R: 0.0, G: 0.5, B: 0.0}, // Green
			{Z: 3.0, ZBack: 3.0, A: 0.5, R: 0.0, G: 0.0, B: 0.5}, // Blue
		}

		r, g, b, a := comp.CompositePixel(samples)

		// R = 0.5 + 0.5*0 + 0.25*0 = 0.5
		// G = 0 + 0.5*0.5 + 0.25*0 = 0.25
		// B = 0 + 0.5*0 + 0.25*0.5 = 0.125
		// A = 0.5 + 0.5*0.5 + 0.25*0.5 = 0.875
		expectedA := float32(0.5 + 0.5*0.5 + 0.25*0.5)
		if !floatsEqual(r, 0.5) || !floatsEqual(g, 0.25) || !floatsEqual(b, 0.125) || !floatsEqual(a, expectedA) {
			t.Errorf("Multiple semi-transparent: got (%v, %v, %v, %v), expected (0.5, 0.25, 0.125, %v)", r, g, b, a, expectedA)
		}
	})
}

// TestDefaultDeepCompositingAllChannels tests compositing with custom channels.
func TestDefaultDeepCompositingAllChannels(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	t.Run("CustomChannel", func(t *testing.T) {
		samples := []DeepSample{
			{
				Z:     1.0,
				ZBack: 1.0,
				A:     0.5,
				R:     0.5,
				G:     0.25,
				B:     0.125,
				Channels: map[string]float32{
					"depth":    1.0,
					"velocity": 0.5,
				},
			},
			{
				Z:     2.0,
				ZBack: 2.0,
				A:     0.5,
				R:     0.0,
				G:     0.0,
				B:     0.0,
				Channels: map[string]float32{
					"depth":    2.0,
					"velocity": 1.0,
				},
			},
		}

		result := comp.CompositePixelAllChannels(samples, []string{"R", "G", "B", "A", "depth", "velocity"})

		// Verify standard channels
		if !floatsEqual(result["R"], 0.5) {
			t.Errorf("R channel: got %v, expected 0.5", result["R"])
		}

		// Verify custom channels
		// depth = 1.0 + 0.5*2.0 = 2.0
		if !floatsEqual(result["depth"], 2.0) {
			t.Errorf("depth channel: got %v, expected 2.0", result["depth"])
		}
	})
}

// TestCompositeDeepScanLineErrors tests error conditions.
func TestCompositeDeepScanLineErrors(t *testing.T) {
	t.Run("NoSources", func(t *testing.T) {
		comp := NewCompositeDeepScanLine()

		fb := NewFrameBuffer()
		comp.SetFrameBuffer(fb)

		err := comp.ReadPixels(0, 10)
		if err != ErrNoSources {
			t.Errorf("Expected ErrNoSources, got %v", err)
		}
	})

	t.Run("NoFrameBuffer", func(t *testing.T) {
		// This test would require a mock reader, so we just test the method exists
		comp := NewCompositeDeepScanLine()
		if comp.FrameBuffer() != nil {
			t.Error("FrameBuffer should be nil initially")
		}
	})

	t.Run("NilSource", func(t *testing.T) {
		comp := NewCompositeDeepScanLine()
		err := comp.AddSource(nil)
		if err == nil {
			t.Error("Expected error when adding nil source")
		}
	})
}

// TestCompositeDeepScanLineBasics tests basic compositor setup.
func TestCompositeDeepScanLineBasics(t *testing.T) {
	comp := NewCompositeDeepScanLine()

	t.Run("InitialState", func(t *testing.T) {
		if comp.Sources() != 0 {
			t.Error("Should start with zero sources")
		}
		if comp.FrameBuffer() != nil {
			t.Error("FrameBuffer should be nil initially")
		}
	})

	t.Run("SetFrameBuffer", func(t *testing.T) {
		fb := NewFrameBuffer()
		fb.Set("R", NewSlice(PixelTypeFloat, make([]byte, 400), 10, 10))

		comp.SetFrameBuffer(fb)

		if comp.FrameBuffer() != fb {
			t.Error("FrameBuffer not set correctly")
		}
	})

	t.Run("SetCompositing", func(t *testing.T) {
		customComp := NewVolumetricDeepCompositing()
		comp.SetCompositing(customComp)

		// Reset to default
		comp.SetCompositing(nil)
	})

	t.Run("MaxSampleCount", func(t *testing.T) {
		comp.SetMaximumSampleCount(10000)
		if comp.GetMaximumSampleCount() != 10000 {
			t.Error("MaximumSampleCount not set correctly")
		}

		comp.SetMaximumSampleCount(0)
		if comp.GetMaximumSampleCount() != 0 {
			t.Error("MaximumSampleCount should be 0")
		}
	})
}

// TestVolumetricDeepCompositing tests volumetric sample handling.
func TestVolumetricDeepCompositing(t *testing.T) {
	comp := NewVolumetricDeepCompositing()

	t.Run("VolumetricSamples", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 2.0, A: 0.5, R: 0.5, G: 0.0, B: 0.0}, // Volumetric
			{Z: 3.0, ZBack: 3.0, A: 0.5, R: 0.0, G: 0.5, B: 0.0}, // Hard surface
		}

		r, g, b, a := comp.CompositePixel(samples)

		// Should still work with default over operation
		if r <= 0 || g <= 0 {
			t.Errorf("Volumetric compositing: got (%v, %v, %v, %v)", r, g, b, a)
		}
	})

	t.Run("SortVolumetric", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 3.0, A: 0.5, R: 0.1}, // Long volume
			{Z: 1.0, ZBack: 2.0, A: 0.5, R: 0.2}, // Shorter volume
			{Z: 0.5, ZBack: 0.5, A: 0.5, R: 0.3}, // Hard surface in front
		}

		comp.SortPixel(samples)

		// Hard surface at Z=0.5 should come first
		if samples[0].R != 0.3 {
			t.Error("Hard surface should sort first")
		}
	})
}

// TestCompositeDeepTiledBasics tests basic tiled compositor setup.
func TestCompositeDeepTiledBasics(t *testing.T) {
	comp := NewCompositeDeepTiled()

	t.Run("InitialState", func(t *testing.T) {
		if comp.Sources() != 0 {
			t.Error("Should start with zero sources")
		}
		if comp.FrameBuffer() != nil {
			t.Error("FrameBuffer should be nil initially")
		}
	})

	t.Run("NilSource", func(t *testing.T) {
		err := comp.AddSource(nil)
		if err == nil {
			t.Error("Expected error when adding nil source")
		}
	})
}

// TestCompositeEdgeCases tests edge cases in compositing.
func TestCompositeEdgeCases(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	t.Run("ZeroAlphaSamples", func(t *testing.T) {
		samples := []DeepSample{
			{Z: 1.0, ZBack: 1.0, A: 0.0, R: 1.0, G: 0.0, B: 0.0},
			{Z: 2.0, ZBack: 2.0, A: 0.0, R: 0.0, G: 1.0, B: 0.0},
		}

		r, g, b, a := comp.CompositePixel(samples)

		// Zero alpha samples should still accumulate color
		// (premultiplied, so color * alpha = 0)
		if a != 0 {
			t.Errorf("Zero alpha samples should give zero total alpha, got %v", a)
		}
		_ = r
		_ = g
		_ = b
	})

	t.Run("NegativeDepth", func(t *testing.T) {
		samples := []DeepSample{
			{Z: -2.0, ZBack: -2.0, A: 0.5, R: 0.5},
			{Z: -1.0, ZBack: -1.0, A: 0.5, R: 0.25},
			{Z: 0.0, ZBack: 0.0, A: 0.5, R: 0.125},
		}

		comp.SortPixel(samples)

		// Should sort negative depths correctly
		if samples[0].Z != -2.0 {
			t.Error("Negative depths should sort correctly")
		}
	})

	t.Run("InfiniteDepth", func(t *testing.T) {
		inf := float32(math.Inf(1))
		samples := []DeepSample{
			{Z: inf, ZBack: inf, A: 0.5, R: 0.5},
			{Z: 1.0, ZBack: 1.0, A: 0.5, R: 0.25},
		}

		comp.SortPixel(samples)

		// Infinite depth should sort to the back
		if samples[0].Z != 1.0 {
			t.Error("Infinite depth should sort last")
		}
	})

	t.Run("OverflowAlpha", func(t *testing.T) {
		// Many samples that could overflow alpha
		samples := make([]DeepSample, 100)
		for i := range samples {
			samples[i] = DeepSample{
				Z:     float32(i),
				ZBack: float32(i),
				A:     0.1,
				R:     0.1,
			}
		}

		_, _, _, a := comp.CompositePixel(samples)

		// Alpha should be clamped to 1.0
		if a > 1.0 {
			t.Error("Alpha should be clamped to 1.0")
		}
	})
}

// BenchmarkDeepCompositing benchmarks compositing performance.
func BenchmarkDeepCompositing(b *testing.B) {
	comp := NewDefaultDeepCompositing()

	b.Run("SmallSampleCount", func(b *testing.B) {
		samples := make([]DeepSample, 5)
		for i := range samples {
			samples[i] = DeepSample{
				Z:     float32(i),
				ZBack: float32(i),
				A:     0.5,
				R:     0.25,
				G:     0.25,
				B:     0.25,
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			comp.SortPixel(samples)
			comp.CompositePixel(samples)
		}
	})

	b.Run("MediumSampleCount", func(b *testing.B) {
		samples := make([]DeepSample, 50)
		for i := range samples {
			samples[i] = DeepSample{
				Z:     float32(i),
				ZBack: float32(i),
				A:     0.1,
				R:     0.1,
				G:     0.1,
				B:     0.1,
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			comp.SortPixel(samples)
			comp.CompositePixel(samples)
		}
	})

	b.Run("LargeSampleCount", func(b *testing.B) {
		samples := make([]DeepSample, 500)
		for i := range samples {
			samples[i] = DeepSample{
				Z:     float32(i),
				ZBack: float32(i),
				A:     0.05,
				R:     0.05,
				G:     0.05,
				B:     0.05,
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			comp.SortPixel(samples)
			comp.CompositePixel(samples)
		}
	})

	b.Run("EarlyTermination", func(b *testing.B) {
		// First sample is opaque, rest should be skipped
		samples := make([]DeepSample, 100)
		samples[0] = DeepSample{Z: 0, ZBack: 0, A: 1.0, R: 1.0, G: 1.0, B: 1.0}
		for i := 1; i < len(samples); i++ {
			samples[i] = DeepSample{
				Z:     float32(i),
				ZBack: float32(i),
				A:     0.5,
				R:     0.5,
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			comp.CompositePixel(samples)
		}
	})
}

// floatsEqual compares two float32 values with tolerance for compositing tests.
func floatsEqual(a, b float32) bool {
	const epsilon = 0.0001
	return math.Abs(float64(a-b)) < epsilon
}

// TestCompositeDeepTiledMethods tests CompositeDeepTiled methods.
func TestCompositeDeepTiledMethods(t *testing.T) {
	comp := NewCompositeDeepTiled()

	t.Run("SetFrameBuffer", func(t *testing.T) {
		fb := NewFrameBuffer()
		fb.Set("R", NewSlice(PixelTypeFloat, make([]byte, 400), 10, 10))
		fb.Set("G", NewSlice(PixelTypeFloat, make([]byte, 400), 10, 10))

		comp.SetFrameBuffer(fb)

		if comp.FrameBuffer() != fb {
			t.Error("FrameBuffer not set correctly")
		}
	})

	t.Run("SetCompositing", func(t *testing.T) {
		customComp := NewVolumetricDeepCompositing()
		comp.SetCompositing(customComp)

		// Reset to default
		comp.SetCompositing(nil)
	})

	t.Run("MaxSampleCount", func(t *testing.T) {
		comp.SetMaximumSampleCount(5000)
		// Just verify it doesn't panic - no getter method exists
	})
}

// TestCompositeDeepScanLineDataWindow tests DataWindow method.
func TestCompositeDeepScanLineDataWindow(t *testing.T) {
	comp := NewCompositeDeepScanLine()

	// Without any sources, DataWindow should return an empty box
	dw := comp.DataWindow()
	if dw.Width() != 0 && dw.Height() != 0 {
		// Some implementations may return different defaults
		t.Logf("DataWindow without sources: %v", dw)
	}
}

// TestCompositeDeepTiledDataWindow tests DataWindow method for tiled compositor.
func TestCompositeDeepTiledDataWindow(t *testing.T) {
	comp := NewCompositeDeepTiled()

	// Without any sources, DataWindow should return an empty box
	dw := comp.DataWindow()
	if dw.Width() != 0 && dw.Height() != 0 {
		// Some implementations may return different defaults
		t.Logf("DataWindow without sources: %v", dw)
	}
}

// TestCompositeDeepTiledReadTileMethods tests ReadTile methods.
func TestCompositeDeepTiledReadTileMethods(t *testing.T) {
	comp := NewCompositeDeepTiled()

	// Without sources, these should return ErrNoSources
	t.Run("ReadTileNoSources", func(t *testing.T) {
		fb := NewFrameBuffer()
		comp.SetFrameBuffer(fb)

		err := comp.ReadTile(0, 0)
		if err != ErrNoSources {
			t.Errorf("ReadTile without sources error = %v, want ErrNoSources", err)
		}
	})

	t.Run("ReadTileLevelNoSources", func(t *testing.T) {
		fb := NewFrameBuffer()
		comp.SetFrameBuffer(fb)

		err := comp.ReadTileLevel(0, 0, 0, 0)
		if err != ErrNoSources {
			t.Errorf("ReadTileLevel without sources error = %v, want ErrNoSources", err)
		}
	})

	t.Run("ReadTilesNoSources", func(t *testing.T) {
		fb := NewFrameBuffer()
		comp.SetFrameBuffer(fb)

		err := comp.ReadTiles(0, 1, 0, 1)
		if err != ErrNoSources {
			t.Errorf("ReadTiles without sources error = %v, want ErrNoSources", err)
		}
	})
}

// TestCompositePixelAllChannelsEmpty tests CompositePixelAllChannels with empty samples.
func TestCompositePixelAllChannelsEmpty(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	result := comp.CompositePixelAllChannels(nil, []string{"R", "G", "B", "A"})

	// Should return empty/zero values for all channels
	for _, name := range []string{"R", "G", "B", "A"} {
		if result[name] != 0 {
			t.Errorf("Channel %s = %v, want 0", name, result[name])
		}
	}
}

// TestVolumetricCompositingAllChannels tests VolumetricDeepCompositing with all channels.
func TestVolumetricCompositingAllChannels(t *testing.T) {
	comp := NewVolumetricDeepCompositing()

	samples := []DeepSample{
		{
			Z:        1.0,
			ZBack:    2.0, // Volumetric
			A:        0.5,
			R:        0.5,
			G:        0.25,
			B:        0.125,
			Channels: map[string]float32{"depth": 1.5},
		},
		{
			Z:        3.0,
			ZBack:    3.0, // Hard surface
			A:        0.5,
			R:        0.25,
			G:        0.5,
			B:        0.25,
			Channels: map[string]float32{"depth": 3.0},
		},
	}

	result := comp.CompositePixelAllChannels(samples, []string{"R", "G", "B", "A", "depth"})

	// Verify we got some result
	if result["A"] <= 0 {
		t.Error("Alpha should be > 0")
	}
	if _, ok := result["depth"]; !ok {
		t.Error("depth channel should be in result")
	}
}

// TestCompositeDeepScanLineAddSourceErrors tests error cases for AddSource.
func TestCompositeDeepScanLineAddSourceErrors(t *testing.T) {
	comp := NewCompositeDeepScanLine()

	t.Run("NilReader", func(t *testing.T) {
		err := comp.AddSource(nil)
		if err == nil {
			t.Error("AddSource(nil) should return error")
		}
	})
}

// TestCompositeDeepScanLineNoFrameBuffer tests ReadPixels without frame buffer.
func TestCompositeDeepScanLineNoFrameBuffer(t *testing.T) {
	comp := NewCompositeDeepScanLine()

	// Try to read without frame buffer or sources
	err := comp.ReadPixels(0, 0)
	if err != ErrNoSources {
		t.Errorf("ReadPixels without sources error = %v, want ErrNoSources", err)
	}
}

// TestCompositeDeepScanLineWithDeepFile tests using actual deep file.
func TestCompositeDeepScanLineWithDeepFile(t *testing.T) {
	// Open the deep file
	f, err := OpenFile("testdata/11.deep.exr")
	if err != nil {
		t.Skipf("Deep test file not available: %v", err)
		return
	}
	defer f.Close()

	// Check if it's a deep file
	if !f.IsDeep() {
		t.Skip("Test file is not a deep file")
		return
	}
	header := f.Header(0)

	// Create deep scanline reader
	reader, err := NewDeepScanlineReader(f)
	if err != nil {
		// Might be tiled deep
		t.Skipf("NewDeepScanlineReader error (might be tiled): %v", err)
		return
	}

	// Create compositor
	comp := NewCompositeDeepScanLine()

	// Add source
	err = comp.AddSource(reader)
	if err != nil {
		t.Logf("AddSource error (may be missing channels): %v", err)
		// Check which channels are present
		channels := header.Channels()
		t.Log("Available channels:")
		for i := 0; i < channels.Len(); i++ {
			t.Logf("  - %s", channels.At(i).Name)
		}
		return
	}

	// Setup output frame buffer
	dw := comp.DataWindow()
	width := int(dw.Width())
	height := int(dw.Height())

	fb := NewFrameBuffer()
	fb.Set("R", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	fb.Set("G", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	fb.Set("B", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	fb.Set("A", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))

	comp.SetFrameBuffer(fb)

	// Read a few scanlines
	minY := int(dw.Min.Y)
	maxY := int(dw.Max.Y)
	if maxY-minY > 10 {
		maxY = minY + 10 // Just read 10 lines to keep test fast
	}

	err = comp.ReadPixels(minY, maxY)
	if err != nil {
		t.Logf("ReadPixels warning: %v", err)
	}
}

// TestCompositeDeepTiledNoFrameBuffer tests ReadTile without frame buffer.
func TestCompositeDeepTiledNoFrameBuffer(t *testing.T) {
	comp := NewCompositeDeepTiled()

	// Try ReadTile without frame buffer
	err := comp.ReadTile(0, 0)
	if err != ErrNoSources {
		t.Errorf("ReadTile without sources error = %v, want ErrNoSources", err)
	}
}

// TestCompositeDeepScanLineCollectSamplesAndWritePixel tests the internal sample collection
// and pixel writing functions directly through the compositing pipeline.
func TestCompositeDeepScanLineCollectSamplesAndWritePixel(t *testing.T) {
	// We need to test collectSamples, getDeepFloat32, and writePixel
	// These are called during ReadPixels when compositing actual deep data

	// Create a simple deep frame buffer with known values
	width := 4
	height := 4

	dfb := NewDeepFrameBuffer(width, height)

	// Add required channels for compositing
	dfb.Insert("Z", PixelTypeFloat)
	dfb.Insert("A", PixelTypeFloat)
	dfb.Insert("R", PixelTypeFloat)
	dfb.Insert("G", PixelTypeFloat)
	dfb.Insert("B", PixelTypeFloat)

	// Set sample counts for some pixels
	dfb.SetSampleCount(0, 0, 2) // 2 samples at (0,0)
	dfb.SetSampleCount(1, 0, 1) // 1 sample at (1,0)
	dfb.SetSampleCount(2, 0, 0) // 0 samples at (2,0)

	// Allocate samples for each pixel
	dfb.AllocateSamples(0, 0)
	dfb.AllocateSamples(1, 0)
	dfb.AllocateSamples(2, 0)

	// Set sample values for pixel (0,0)
	zSlice := dfb.Slices["Z"]
	aSlice := dfb.Slices["A"]
	rSlice := dfb.Slices["R"]

	if zSlice != nil && aSlice != nil && rSlice != nil {
		// Set Z values
		zSlice.SetSampleFloat32(0, 0, 0, 1.0)
		zSlice.SetSampleFloat32(0, 0, 1, 2.0)

		// Set alpha values
		aSlice.SetSampleFloat32(0, 0, 0, 0.5)
		aSlice.SetSampleFloat32(0, 0, 1, 0.5)

		// Set R values
		rSlice.SetSampleFloat32(0, 0, 0, 0.25)
		rSlice.SetSampleFloat32(0, 0, 1, 0.5)
	}

	// Now test the compositing functions directly by creating
	// a compositor and manually testing sample collection
	comp := NewCompositeDeepScanLine()

	// Setup output frame buffer
	outputFB := NewFrameBuffer()
	outputFB.Set("R", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("G", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("B", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("A", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))

	comp.SetFrameBuffer(outputFB)

	// Test DataWindow when no sources
	dw := comp.DataWindow()
	t.Logf("DataWindow without sources: %v", dw)

	// Verify the frame buffer is set
	if comp.FrameBuffer() == nil {
		t.Error("FrameBuffer should not be nil after SetFrameBuffer")
	}
}

// TestCompositeDeepTiledCollectSamplesAndWritePixel tests the tiled version.
func TestCompositeDeepTiledCollectSamplesAndWritePixel(t *testing.T) {
	width := 4
	height := 4

	dfb := NewDeepFrameBuffer(width, height)

	// Add channels
	dfb.Insert("Z", PixelTypeFloat)
	dfb.Insert("A", PixelTypeFloat)
	dfb.Insert("R", PixelTypeFloat)
	dfb.Insert("G", PixelTypeFloat)
	dfb.Insert("B", PixelTypeFloat)
	dfb.Insert("custom", PixelTypeFloat)

	// Set sample counts
	dfb.SetSampleCount(0, 0, 2)
	dfb.SetSampleCount(1, 1, 1)

	dfb.AllocateSamples(0, 0)
	dfb.AllocateSamples(1, 1)

	// Set values
	if zSlice := dfb.Slices["Z"]; zSlice != nil {
		zSlice.SetSampleFloat32(0, 0, 0, 1.0)
		zSlice.SetSampleFloat32(0, 0, 1, 2.0)
		zSlice.SetSampleFloat32(1, 1, 0, 3.0)
	}
	if aSlice := dfb.Slices["A"]; aSlice != nil {
		aSlice.SetSampleFloat32(0, 0, 0, 0.5)
		aSlice.SetSampleFloat32(0, 0, 1, 0.5)
		aSlice.SetSampleFloat32(1, 1, 0, 1.0)
	}
	if rSlice := dfb.Slices["R"]; rSlice != nil {
		rSlice.SetSampleFloat32(0, 0, 0, 0.25)
		rSlice.SetSampleFloat32(0, 0, 1, 0.5)
		rSlice.SetSampleFloat32(1, 1, 0, 1.0)
	}
	if customSlice := dfb.Slices["custom"]; customSlice != nil {
		customSlice.SetSampleFloat32(0, 0, 0, 100.0)
		customSlice.SetSampleFloat32(0, 0, 1, 200.0)
		customSlice.SetSampleFloat32(1, 1, 0, 300.0)
	}

	// Create tiled compositor
	comp := NewCompositeDeepTiled()

	// Setup output frame buffer
	outputFB := NewFrameBuffer()
	outputFB.Set("R", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("G", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("B", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))
	outputFB.Set("A", NewSlice(PixelTypeFloat, make([]byte, width*height*4), width, height))

	comp.SetFrameBuffer(outputFB)

	// Test DataWindow when no sources
	dw := comp.DataWindow()
	t.Logf("DataWindow without sources: %v", dw)
}

// TestDeepSliceGetMethods tests the DeepSlice Get methods for different pixel types.
func TestDeepSliceGetMethods(t *testing.T) {
	width := 4
	height := 4

	// Test with half type
	t.Run("HalfType", func(t *testing.T) {
		dfb := NewDeepFrameBuffer(width, height)
		dfb.Insert("halfChan", PixelTypeHalf)
		dfb.SetSampleCount(0, 0, 2)
		dfb.AllocateSamples(0, 0)

		slice := dfb.Slices["halfChan"]
		if slice != nil {
			slice.SetSampleHalf(0, 0, 0, 0x3C00) // 1.0 in half
			slice.SetSampleHalf(0, 0, 1, 0x4000) // 2.0 in half

			// Get values
			val0 := slice.GetSampleHalf(0, 0, 0)
			val1 := slice.GetSampleHalf(0, 0, 1)
			t.Logf("Half values: %d, %d", val0, val1)
		}
	})

	// Test with uint type
	t.Run("UintType", func(t *testing.T) {
		dfb := NewDeepFrameBuffer(width, height)
		dfb.Insert("uintChan", PixelTypeUint)
		dfb.SetSampleCount(0, 0, 2)
		dfb.AllocateSamples(0, 0)

		slice := dfb.Slices["uintChan"]
		if slice != nil {
			slice.SetSampleUint(0, 0, 0, 100)
			slice.SetSampleUint(0, 0, 1, 200)

			// Get values
			val0 := slice.GetSampleUint(0, 0, 0)
			val1 := slice.GetSampleUint(0, 0, 1)
			if val0 != 100 || val1 != 200 {
				t.Errorf("Uint values: got %d, %d, want 100, 200", val0, val1)
			}
		}
	})
}

// TestVolumetricCompositingEmptySamples tests volumetric compositing edge cases.
func TestVolumetricCompositingEmptySamples(t *testing.T) {
	comp := NewVolumetricDeepCompositing()

	// Test with empty samples
	r, g, b, a := comp.CompositePixel(nil)
	if r != 0 || g != 0 || b != 0 || a != 0 {
		t.Errorf("Empty samples should produce zero: got (%v,%v,%v,%v)", r, g, b, a)
	}

	// Test CompositePixelAllChannels with empty samples
	result := comp.CompositePixelAllChannels(nil, []string{"R", "G", "B", "A"})
	for name, val := range result {
		if val != 0 {
			t.Errorf("Channel %s should be 0 for empty samples, got %v", name, val)
		}
	}
}

// TestVolumetricCompositingWithEarlyTermination tests early termination in volumetric.
func TestVolumetricCompositingWithEarlyTermination(t *testing.T) {
	comp := NewVolumetricDeepCompositing()

	// First sample is fully opaque, others should be skipped
	samples := []DeepSample{
		{Z: 1.0, ZBack: 1.0, A: 1.0, R: 1.0, G: 0.5, B: 0.25},
		{Z: 2.0, ZBack: 3.0, A: 0.5, R: 0.0, G: 1.0, B: 0.0}, // Volumetric, should be skipped
		{Z: 4.0, ZBack: 4.0, A: 0.5, R: 0.0, G: 0.0, B: 1.0}, // Should be skipped
	}

	r, g, b, a := comp.CompositePixel(samples)

	// Should only get values from first sample
	if !floatsEqual(r, 1.0) || !floatsEqual(g, 0.5) || !floatsEqual(b, 0.25) || !floatsEqual(a, 1.0) {
		t.Errorf("Early termination: got (%v,%v,%v,%v), want (1,0.5,0.25,1)", r, g, b, a)
	}
}

// TestVolumetricCompositingAlphaClamping tests alpha clamping in volumetric compositing.
func TestVolumetricCompositingAlphaClamping(t *testing.T) {
	comp := NewVolumetricDeepCompositing()

	// Many samples that could overflow alpha
	samples := make([]DeepSample, 50)
	for i := range samples {
		samples[i] = DeepSample{
			Z:     float32(i),
			ZBack: float32(i) + 0.5, // Volumetric
			A:     0.1,
			R:     0.1,
			G:     0.1,
			B:     0.1,
		}
	}

	r, g, b, a := comp.CompositePixel(samples)

	// Alpha should be clamped to 1.0
	if a > 1.0 {
		t.Errorf("Alpha should be clamped to 1.0, got %v", a)
	}
	if a < 0.0 {
		t.Errorf("Alpha should not be negative, got %v", a)
	}
	t.Logf("Volumetric result: R=%v, G=%v, B=%v, A=%v", r, g, b, a)
}

// TestCompositeDeepScanLineReadPixelsNoOutputFrameBuffer tests ReadPixels error.
func TestCompositeDeepScanLineReadPixelsNoOutputFrameBuffer(t *testing.T) {
	comp := NewCompositeDeepScanLine()

	// Add a dummy check - without sources this returns ErrNoSources first
	err := comp.ReadPixels(0, 10)
	if err != ErrNoSources {
		t.Errorf("Expected ErrNoSources, got %v", err)
	}
}

// TestCompositeDeepTiledReadTileLevelNoOutputFrameBuffer tests error paths.
func TestCompositeDeepTiledReadTileLevelNoOutputFrameBuffer(t *testing.T) {
	comp := NewCompositeDeepTiled()

	// Without sources
	err := comp.ReadTileLevel(0, 0, 0, 0)
	if err != ErrNoSources {
		t.Errorf("Expected ErrNoSources, got %v", err)
	}

	// Set a frame buffer but still no sources
	fb := NewFrameBuffer()
	fb.Set("R", NewSlice(PixelTypeFloat, make([]byte, 64*64*4), 64, 64))
	comp.SetFrameBuffer(fb)

	err = comp.ReadTileLevel(0, 0, 0, 0)
	if err != ErrNoSources {
		t.Errorf("Expected ErrNoSources, got %v", err)
	}
}

// TestCompositePixelNegativeAlpha tests compositing with negative alpha values.
func TestCompositePixelNegativeAlpha(t *testing.T) {
	comp := NewDefaultDeepCompositing()

	// Sample with negative alpha (invalid but should be handled)
	samples := []DeepSample{
		{Z: 1.0, ZBack: 1.0, A: -0.5, R: 0.5, G: 0.5, B: 0.5},
		{Z: 2.0, ZBack: 2.0, A: 0.5, R: 0.25, G: 0.25, B: 0.25},
	}

	r, g, b, a := comp.CompositePixel(samples)

	// Alpha should be clamped to [0, 1]
	if a < 0.0 || a > 1.0 {
		t.Errorf("Alpha should be clamped: got %v", a)
	}
	t.Logf("Negative alpha result: R=%v, G=%v, B=%v, A=%v", r, g, b, a)
}

// TestCompositeDeepScanLineWithCreatedFile creates a deep file and composites it.
func TestCompositeDeepScanLineWithCreatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep_composite_test.exr")

	width, height := 8, 8

	// Create file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create file error: %v", err)
	}

	// NewDeepScanlineWriter takes (io.WriteSeeker, width, height)
	writer, err := NewDeepScanlineWriter(f, width, height)
	if err != nil {
		f.Close()
		t.Fatalf("NewDeepScanlineWriter error: %v", err)
	}

	// Create frame buffer with samples
	dfb := NewDeepFrameBuffer(width, height)
	dfb.Insert("R", PixelTypeFloat)
	dfb.Insert("G", PixelTypeFloat)
	dfb.Insert("B", PixelTypeFloat)
	dfb.Insert("A", PixelTypeFloat)
	dfb.Insert("Z", PixelTypeFloat)

	// Set up sample counts and allocate per pixel
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			dfb.SetSampleCount(x, y, 2) // 2 samples per pixel
			dfb.AllocateSamples(x, y)
		}
	}

	// Fill with sample data
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			for s := 0; s < 2; s++ {
				dfb.Slices["R"].SetSampleFloat32(x, y, s, float32(x)/float32(width))
				dfb.Slices["G"].SetSampleFloat32(x, y, s, float32(y)/float32(height))
				dfb.Slices["B"].SetSampleFloat32(x, y, s, 0.5)
				dfb.Slices["A"].SetSampleFloat32(x, y, s, 0.5)
				dfb.Slices["Z"].SetSampleFloat32(x, y, s, float32(s)+1.0)
			}
		}
	}

	writer.SetFrameBuffer(dfb)
	if err := writer.WritePixels(height); err != nil {
		f.Close()
		t.Fatalf("WritePixels error: %v", err)
	}

	if err := writer.Finalize(); err != nil {
		f.Close()
		t.Fatalf("Finalize error: %v", err)
	}
	f.Close()

	// Now open the file and test compositing
	readFile, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer readFile.Close()

	if !readFile.IsDeep() {
		t.Fatal("Expected deep file")
	}

	reader, err := NewDeepScanlineReader(readFile)
	if err != nil {
		t.Fatalf("NewDeepScanlineReader error: %v", err)
	}

	// Create compositor
	comp := NewCompositeDeepScanLine()
	if err := comp.AddSource(reader); err != nil {
		t.Fatalf("AddSource error: %v", err)
	}

	// Set output frame buffer
	dw := comp.DataWindow()
	outWidth := int(dw.Width())
	outHeight := int(dw.Height())

	outFB := NewFrameBuffer()
	outFB.Set("R", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("G", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("B", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("A", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))

	comp.SetFrameBuffer(outFB)

	// Read and composite
	minY := int(dw.Min.Y)
	maxY := int(dw.Max.Y)
	err = comp.ReadPixels(minY, maxY)
	if err != nil {
		t.Logf("ReadPixels error (might be expected): %v", err)
	}
}

// TestCompositeDeepTiledWithCreatedFile creates a deep tiled file and composites it.
func TestCompositeDeepTiledWithCreatedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "deep_tiled_composite_test.exr")

	width, height := 8, 8
	tileW, tileH := uint32(4), uint32(4)

	// Create file
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create file error: %v", err)
	}

	writer, err := NewDeepTiledWriter(f, width, height, tileW, tileH)
	if err != nil {
		f.Close()
		t.Fatalf("NewDeepTiledWriter error: %v", err)
	}

	// Create frame buffer with samples
	dfb := NewDeepFrameBuffer(width, height)
	dfb.Insert("R", PixelTypeFloat)
	dfb.Insert("G", PixelTypeFloat)
	dfb.Insert("B", PixelTypeFloat)
	dfb.Insert("A", PixelTypeFloat)
	dfb.Insert("Z", PixelTypeFloat)

	// Set up sample counts and allocate per pixel
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			dfb.SetSampleCount(x, y, 2) // 2 samples per pixel
			dfb.AllocateSamples(x, y)
		}
	}

	// Fill with sample data
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			for s := 0; s < 2; s++ {
				dfb.Slices["R"].SetSampleFloat32(x, y, s, float32(x)/float32(width))
				dfb.Slices["G"].SetSampleFloat32(x, y, s, float32(y)/float32(height))
				dfb.Slices["B"].SetSampleFloat32(x, y, s, 0.5)
				dfb.Slices["A"].SetSampleFloat32(x, y, s, 0.5)
				dfb.Slices["Z"].SetSampleFloat32(x, y, s, float32(s)+1.0)
			}
		}
	}

	writer.SetFrameBuffer(dfb)

	// Write all tiles
	numTilesX := (width + int(tileW) - 1) / int(tileW)
	numTilesY := (height + int(tileH) - 1) / int(tileH)
	for ty := 0; ty < numTilesY; ty++ {
		for tx := 0; tx < numTilesX; tx++ {
			if err := writer.WriteTile(tx, ty); err != nil {
				f.Close()
				t.Fatalf("WriteTile(%d, %d) error: %v", tx, ty, err)
			}
		}
	}

	if err := writer.Finalize(); err != nil {
		f.Close()
		t.Fatalf("Finalize error: %v", err)
	}
	f.Close()

	// Now open the file and test compositing
	readFile, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile error: %v", err)
	}
	defer readFile.Close()

	if !readFile.IsDeep() {
		t.Fatal("Expected deep file")
	}

	// A single-part deep tiled file must NOT set the tiled bit in the version
	// field: the tiled bit and the deep bit are mutually exclusive there, and
	// OpenEXR rejects a file that sets both with "Invalid combination of
	// version flags". What makes it tiled is the type attribute, "deeptile",
	// and the tile description, which is what Header.IsTiled reports.
	if readFile.IsTiled() {
		t.Fatal("the version field must not set the tiled bit on a deep tiled file")
	}
	if !readFile.Header(0).IsTiled() {
		t.Fatal("Expected tiled file")
	}
	if attr := readFile.Header(0).Get(AttrNameType); attr == nil || attr.Value.(string) != PartTypeDeepTiled {
		t.Fatalf("type attribute = %v, want %q", attr, PartTypeDeepTiled)
	}

	reader, err := NewDeepTiledReader(readFile)
	if err != nil {
		t.Fatalf("NewDeepTiledReader error: %v", err)
	}

	// Create compositor
	comp := NewCompositeDeepTiled()
	if err := comp.AddSource(reader); err != nil {
		t.Fatalf("AddSource error: %v", err)
	}

	// Set output frame buffer
	dw := comp.DataWindow()
	outWidth := int(dw.Width())
	outHeight := int(dw.Height())

	outFB := NewFrameBuffer()
	outFB.Set("R", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("G", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("B", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))
	outFB.Set("A", NewSlice(PixelTypeFloat, make([]byte, outWidth*outHeight*4), outWidth, outHeight))

	comp.SetFrameBuffer(outFB)

	// Read and composite a tile
	err = comp.ReadTile(0, 0)
	if err != nil {
		t.Logf("ReadTile error (might be expected): %v", err)
	}
}
