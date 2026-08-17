package predictor

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestDecodeSIMD(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{
			name:  "small",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:  "16 bytes",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},
		{
			name:  "17 bytes",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17},
		},
		{
			name:  "zeros",
			input: make([]byte, 32),
		},
		{
			name:  "all same",
			input: bytes.Repeat([]byte{42}, 64),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Make copies for both methods
			input1 := make([]byte, len(tc.input))
			input2 := make([]byte, len(tc.input))
			copy(input1, tc.input)
			copy(input2, tc.input)

			// Apply both decode methods
			Decode(input1)
			DecodeSIMD(input2)

			// Compare results
			if !bytes.Equal(input1, input2) {
				t.Errorf("DecodeSIMD mismatch:\nwant: %v\ngot:  %v", input1, input2)
			}
		})
	}
}

func TestDecodeSIMDRandom(t *testing.T) {
	r := rand.New(rand.NewSource(42))

	sizes := []int{7, 8, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 256, 1000}
	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			input := make([]byte, size)
			r.Read(input)

			input1 := make([]byte, len(input))
			input2 := make([]byte, len(input))
			copy(input1, input)
			copy(input2, input)

			Decode(input1)
			DecodeSIMD(input2)

			if !bytes.Equal(input1, input2) {
				t.Errorf("DecodeSIMD mismatch for size %d:\nfirst 32 bytes want: %v\nfirst 32 bytes got:  %v",
					size, input1[:min(32, len(input1))], input2[:min(32, len(input2))])
			}
		})
	}
}

func TestEncodeSIMD(t *testing.T) {
	testCases := []struct {
		name  string
		input []byte
	}{
		{
			name:  "small",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			name:  "16 bytes",
			input: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		},
		{
			name:  "prefix sum",
			input: []byte{1, 3, 6, 10, 15, 21, 28, 36, 45, 55},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			input1 := make([]byte, len(tc.input))
			input2 := make([]byte, len(tc.input))
			copy(input1, tc.input)
			copy(input2, tc.input)

			Encode(input1)
			EncodeSIMD(input2)

			if !bytes.Equal(input1, input2) {
				t.Errorf("EncodeSIMD mismatch:\nwant: %v\ngot:  %v", input1, input2)
			}
		})
	}
}

// refReconstruct is an independent transcription of
// ImfZipCompressor::uncompress: undo the predictor over the still-reordered
// stream, then undo the reordering. Asserting against this rather than against
// a copy of ReconstructBytes's own steps is deliberate — the previous version
// of this test reimplemented the wrong order and so agreed with the bug.
func refReconstruct(source []byte) []byte {
	n := len(source)
	tmp := append([]byte(nil), source...)
	for i := 1; i < n; i++ {
		tmp[i] = byte(int(tmp[i-1]) + int(tmp[i]) - 128)
	}

	out := make([]byte, n)
	half := (n + 1) / 2
	t1, t2, s := 0, half, 0
	for s < n {
		out[s] = tmp[t1]
		t1++
		s++
		if s >= n {
			break
		}
		out[s] = tmp[t2]
		t2++
		s++
	}
	return out
}

func TestReconstructBytes(t *testing.T) {
	sizes := []int{0, 1, 2, 3, 7, 8, 15, 16, 17, 32, 64, 100, 255, 256}

	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(size)))
			original := make([]byte, size)
			r.Read(original)

			want := refReconstruct(original)

			source := append([]byte(nil), original...)
			got := make([]byte, size)
			ReconstructBytes(got, source)

			if !bytes.Equal(got, want) {
				t.Errorf("ReconstructBytes mismatch for size %d:\nwant: %v\ngot:  %v",
					size, want[:min(32, len(want))], got[:min(32, len(got))])
			}
		})
	}
}

// TestReconstructInvertsDeconstruct pins the two combined helpers as exact
// inverses. Together with TestReconstructBytes (which anchors one of them to
// the specification) this makes both spec-correct, which neither check
// establishes on its own.
func TestReconstructInvertsDeconstruct(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 7, 8, 15, 16, 17, 32, 64, 100, 255, 256, 1000} {
		r := rand.New(rand.NewSource(int64(size) + 7))
		original := make([]byte, size)
		r.Read(original)

		scratch := make([]byte, size)
		DeconstructBytes(scratch, original)

		got := make([]byte, size)
		ReconstructBytes(got, scratch)

		if !bytes.Equal(got, original) {
			t.Errorf("size %d: Reconstruct(Deconstruct(x)) != x", size)
		}
	}
}

// refDeconstruct is an independent transcription of
// ImfZipCompressor::compress: split the stream into even and odd halves, then
// apply the biased predictor to the reordered result.
func refDeconstruct(source []byte) []byte {
	n := len(source)
	out := make([]byte, n)
	half := (n + 1) / 2
	t1, t2, s := 0, half, 0
	for s < n {
		out[t1] = source[s]
		t1++
		s++
		if s >= n {
			break
		}
		out[t2] = source[s]
		t2++
		s++
	}

	if n >= 2 {
		p := int(out[0])
		for i := 1; i < n; i++ {
			d := int(out[i]) - p + (128 + 256)
			p = int(out[i])
			out[i] = byte(d)
		}
	}
	return out
}

func TestDeconstructBytes(t *testing.T) {
	// Asserted against an independent transcription of the reference rather
	// than against a copy of DeconstructBytes's own steps: the previous version
	// reimplemented the same pipeline inline, so any misconception shared by
	// both passed.
	sizes := []int{0, 1, 2, 3, 7, 8, 15, 16, 17, 32, 64, 100, 255, 256}

	for _, size := range sizes {
		t.Run("", func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(size)))
			original := make([]byte, size)
			r.Read(original)

			want := refDeconstruct(original)

			got := make([]byte, size)
			DeconstructBytes(got, original)

			if !bytes.Equal(got, want) {
				t.Errorf("DeconstructBytes mismatch for size %d:\nwant: %v\ngot:  %v",
					size, want[:min(32, len(want))], got[:min(32, len(got))])
			}
		})
	}
}

func BenchmarkDecodeSIMD(b *testing.B) {
	r := rand.New(rand.NewSource(42))
	sizes := []int{1024, 4096, 16384, 65536}
	for _, size := range sizes {
		data := make([]byte, size)
		r.Read(data)

		b.Run("Decode", func(b *testing.B) {
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(buf, data)
				Decode(buf)
			}
		})

		b.Run("DecodeSIMD", func(b *testing.B) {
			buf := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(buf, data)
				DecodeSIMD(buf)
			}
		})
	}
}

func BenchmarkReconstructBytes(b *testing.B) {
	r := rand.New(rand.NewSource(42))
	sizes := []int{1024, 4096, 16384, 65536}
	for _, size := range sizes {
		data := make([]byte, size)
		r.Read(data)

		b.Run("Separate", func(b *testing.B) {
			source := make([]byte, size)
			out := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(source, data)
				// Simulate separate steps
				Decode(source)
				half := (size + 1) / 2
				for j := 0; j < half; j++ {
					out[j*2] = source[j]
					if half+j < size {
						out[j*2+1] = source[half+j]
					}
				}
			}
		})

		b.Run("Combined", func(b *testing.B) {
			source := make([]byte, size)
			out := make([]byte, size)
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				copy(source, data)
				ReconstructBytes(out, source)
			}
		})
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
