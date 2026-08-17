package exr

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// writeUnclosed writes a file and deliberately never calls Close on the
// writer, so the chunk offset table is left as written-but-unpatched zeroes
// while the chunk data itself is complete. This is what an interrupted render
// or a caller who forgets Close produces, and it is the situation issue #4 was
// actually reporting.
func writeUnclosed(t *testing.T, path string, comp Compression, pt PixelType, w, h int) []float32 {
	t.Helper()

	want := make([]float32, w*h)
	for i := range want {
		want[i] = float32(i) * 0.25
	}

	header := NewScanlineHeader(w, h)
	header.SetCompression(comp)
	cl := NewChannelList()
	for _, n := range []string{"A", "B", "G", "R"} {
		cl.Add(Channel{Name: n, Type: pt, XSampling: 1, YSampling: 1})
	}
	header.SetChannels(cl)

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	wr, err := NewScanlineWriter(f, header)
	if err != nil {
		t.Fatalf("NewScanlineWriter: %v", err)
	}
	fb := NewFrameBuffer()
	for _, n := range []string{"R", "G", "B", "A"} {
		switch pt {
		case PixelTypeFloat:
			fb.Set(n, NewSliceFromFloat32(want, w, h))
		case PixelTypeHalf:
			hv := make([]half.Half, len(want))
			for i, v := range want {
				hv[i] = half.FromFloat32(v)
			}
			fb.Set(n, NewSliceFromHalf(hv, w, h))
		}
	}
	wr.SetFrameBuffer(fb)
	if err := wr.WritePixels(0, h-1); err != nil {
		t.Fatalf("WritePixels: %v", err)
	}
	// Deliberately no wr.Close().
	f.Close()

	return want
}

// TestReadsFileWithUnwrittenOffsetTable is the regression test for issue #4 as
// it was actually experienced: a float32 scanline file read back as all zeroes,
// silently. The offset table was never patched because Close was never called.
// The OpenEXR reference implementation reads such a file by rebuilding the
// table, so we must too — returning zeroes with no error is the worst possible
// outcome, because a round-trip check against a zero-initialised buffer passes.
func TestReadsFileWithUnwrittenOffsetTable(t *testing.T) {
	const w, h = 8, 5

	for _, tc := range []struct {
		name string
		comp Compression
		pt   PixelType
	}{
		{"float_none", CompressionNone, PixelTypeFloat},
		{"float_zip", CompressionZIP, PixelTypeFloat},
		{"float_zips", CompressionZIPS, PixelTypeFloat},
		{"float_rle", CompressionRLE, PixelTypeFloat},
		{"half_zip", CompressionZIP, PixelTypeHalf},
		{"half_piz", CompressionPIZ, PixelTypeHalf},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "unclosed.exr")
			want := writeUnclosed(t, path, tc.comp, tc.pt, w, h)

			f, err := OpenFile(path)
			if err != nil {
				t.Fatalf("OpenFile: %v", err)
			}
			defer f.Close()

			r, err := NewScanlineReader(f)
			if err != nil {
				t.Fatalf("NewScanlineReader: %v", err)
			}

			got := make([]float32, w*h)
			fb := NewFrameBuffer()
			var read func() []float32
			switch tc.pt {
			case PixelTypeFloat:
				fb.Set("R", NewSliceFromFloat32(got, w, h))
				read = func() []float32 { return got }
			case PixelTypeHalf:
				hv := make([]half.Half, w*h)
				fb.Set("R", NewSliceFromHalf(hv, w, h))
				read = func() []float32 {
					out := make([]float32, len(hv))
					for i, v := range hv {
						out[i] = v.Float32()
					}
					return out
				}
			}
			r.SetFrameBuffer(fb)
			dw := r.DataWindow()
			if err := r.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
				t.Fatalf("ReadPixels: %v", err)
			}

			out := read()
			allZero := true
			for _, v := range out {
				if v != 0 {
					allZero = false
					break
				}
			}
			if allZero {
				t.Fatal("decoded to all zeroes: the offset table was not reconstructed")
			}

			for i := range want {
				w32 := want[i]
				if tc.pt == PixelTypeHalf {
					w32 = half.FromFloat32(w32).Float32()
				}
				if out[i] != w32 {
					t.Fatalf("sample %d = %v, want %v", i, out[i], w32)
				}
			}
		})
	}
}

// TestWellFormedFileOffsetsUntouched guards against the reconstruction pass
// disturbing a file whose table is already valid.
func TestWellFormedFileOffsetsUntouched(t *testing.T) {
	path := filepath.Join(conformanceDir, "grad_half_zip.exr")
	if _, err := os.Stat(path); err != nil {
		t.Skip("conformance corpus not generated")
	}

	f, err := OpenFile(path)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()

	offsets := f.Offsets(0)
	if len(offsets) == 0 {
		t.Fatal("no chunk offsets")
	}
	for i, off := range offsets {
		if off <= 0 {
			t.Errorf("chunk %d has offset %d in a well-formed file", i, off)
		}
	}
}
