package compression

import (
	"math/rand"
	"testing"
)

// refWav2Encode is a deliberately literal transcription of wav2Encode from
// OpenEXR's ImfWav.cpp. It keeps the reference's pointer-walking structure —
// in particular, the left-over column and left-over row are handled at
// whatever position the preceding loop *stopped at*, which is not the same as
// recomputing an index from nx or ny.
//
// It exists so the optimized implementations are pinned to the specification
// rather than to each other. A wavelet that is merely self-inverse round-trips
// perfectly while disagreeing with every real OpenEXR file.
func refWav2Encode(data []uint16, nx, ox, ny, oy int, mx uint16) {
	w14 := mx < (1 << 14)
	n := nx
	if ny < nx {
		n = ny
	}

	p := 1
	p2 := 2

	for p2 <= n {
		ey := oy * (ny - p2)
		oy1 := oy * p
		oy2 := oy * p2
		ox1 := ox * p
		ox2 := ox * p2

		var i00, i01, i10, i11 uint16

		py := 0
		for ; py <= ey; py += oy2 {
			ex := py + ox*(nx-p2)
			px := py
			for ; px <= ex; px += ox2 {
				p01 := px + ox1
				p10 := px + oy1
				p11 := p10 + ox1

				if w14 {
					i00, i01 = wenc14(data[px], data[p01])
					i10, i11 = wenc14(data[p10], data[p11])
					data[px], data[p10] = wenc14(i00, i10)
					data[p01], data[p11] = wenc14(i01, i11)
				} else {
					i00, i01 = wenc16(data[px], data[p01])
					i10, i11 = wenc16(data[p10], data[p11])
					data[px], data[p10] = wenc16(i00, i10)
					data[p01], data[p11] = wenc16(i01, i11)
				}
			}

			// Left-over column, at the position the x loop stopped at.
			if nx&p != 0 {
				p10 := px + oy1
				if w14 {
					i00, data[p10] = wenc14(data[px], data[p10])
				} else {
					i00, data[p10] = wenc16(data[px], data[p10])
				}
				data[px] = i00
			}
		}

		// Left-over row, at the position the y loop stopped at.
		if ny&p != 0 {
			ex := py + ox*(nx-p2)
			for px := py; px <= ex; px += ox2 {
				p01 := px + ox1
				if w14 {
					i00, data[p01] = wenc14(data[px], data[p01])
				} else {
					i00, data[p01] = wenc16(data[px], data[p01])
				}
				data[px] = i00
			}
		}

		p = p2
		p2 <<= 1
	}
}

// refWav2Decode is the matching literal transcription of wav2Decode.
func refWav2Decode(data []uint16, nx, ox, ny, oy int, mx uint16) {
	w14 := mx < (1 << 14)
	n := nx
	if ny < nx {
		n = ny
	}

	// Find the largest p2 <= n, then walk back down.
	p := 1
	for p <= n {
		p <<= 1
	}
	p >>= 1
	p2 := p
	p >>= 1

	for p >= 1 {
		ey := oy * (ny - p2)
		oy1 := oy * p
		oy2 := oy * p2
		ox1 := ox * p
		ox2 := ox * p2

		var i00, i01, i10, i11 uint16

		py := 0
		for ; py <= ey; py += oy2 {
			ex := py + ox*(nx-p2)
			px := py
			for ; px <= ex; px += ox2 {
				p01 := px + ox1
				p10 := px + oy1
				p11 := p10 + ox1

				if w14 {
					i00, i10 = wdec14(data[px], data[p10])
					i01, i11 = wdec14(data[p01], data[p11])
					data[px], data[p01] = wdec14(i00, i01)
					data[p10], data[p11] = wdec14(i10, i11)
				} else {
					i00, i10 = wdec16(data[px], data[p10])
					i01, i11 = wdec16(data[p01], data[p11])
					data[px], data[p01] = wdec16(i00, i01)
					data[p10], data[p11] = wdec16(i10, i11)
				}
			}

			if nx&p != 0 {
				p10 := px + oy1
				if w14 {
					i00, data[p10] = wdec14(data[px], data[p10])
				} else {
					i00, data[p10] = wdec16(data[px], data[p10])
				}
				data[px] = i00
			}
		}

		if ny&p != 0 {
			ex := py + ox*(nx-p2)
			for px := py; px <= ex; px += ox2 {
				p01 := px + ox1
				if w14 {
					i00, data[p01] = wdec14(data[px], data[p01])
				} else {
					i00, data[p01] = wdec16(data[px], data[p01])
				}
				data[px] = i00
			}
		}

		p2 = p
		p >>= 1
	}
}

var wavShapes = []struct{ nx, ny int }{
	{1, 1}, {2, 1}, {1, 2}, {2, 2}, {3, 3}, {4, 4}, {5, 3}, {7, 7},
	{8, 8}, {9, 5}, {16, 16}, {17, 9}, {31, 15}, {32, 32}, {33, 17},
	{64, 32}, {71, 32}, {71, 8}, {100, 50}, {128, 64}, {256, 32},
}

func TestWav2EncodeMatchesReference(t *testing.T) {
	for _, shape := range wavShapes {
		for _, mx := range []uint16{63, 1929, 16383, 16384, 65535} {
			rng := rand.New(rand.NewSource(int64(shape.nx*1000 + shape.ny)))
			src := make([]uint16, shape.nx*shape.ny)
			for i := range src {
				src[i] = uint16(rng.Intn(int(mx) + 1))
			}

			want := append([]uint16(nil), src...)
			refWav2Encode(want, shape.nx, 1, shape.ny, shape.nx, mx)

			got := append([]uint16(nil), src...)
			Wav2DEncodeStrided(got, shape.nx, 1, shape.ny, shape.nx, mx)

			for i := range want {
				if want[i] != got[i] {
					t.Errorf("Wav2DEncodeStrided(nx=%d ny=%d mx=%d): first mismatch at %d: got %d, reference %d",
						shape.nx, shape.ny, mx, i, got[i], want[i])
					break
				}
			}
		}
	}
}

func TestWav2DecodeMatchesReference(t *testing.T) {
	for _, shape := range wavShapes {
		for _, mx := range []uint16{63, 1929, 16383, 16384, 65535} {
			rng := rand.New(rand.NewSource(int64(shape.nx*7919 + shape.ny)))
			src := make([]uint16, shape.nx*shape.ny)
			for i := range src {
				src[i] = uint16(rng.Intn(65536))
			}

			want := append([]uint16(nil), src...)
			refWav2Decode(want, shape.nx, 1, shape.ny, shape.nx, mx)

			got := append([]uint16(nil), src...)
			Wav2DDecodeStrided(got, shape.nx, 1, shape.ny, shape.nx, mx)

			for i := range want {
				if want[i] != got[i] {
					t.Errorf("Wav2DDecodeStrided(nx=%d ny=%d mx=%d): first mismatch at %d: got %d, reference %d",
						shape.nx, shape.ny, mx, i, got[i], want[i])
					break
				}
			}
		}
	}
}

// TestWav2ReferenceRoundTrips checks the transcription itself is self-consistent,
// so a failure above indicts the optimized code rather than the reference.
func TestWav2ReferenceRoundTrips(t *testing.T) {
	for _, shape := range wavShapes {
		for _, mx := range []uint16{63, 1929, 16383, 65535} {
			rng := rand.New(rand.NewSource(int64(shape.nx*31 + shape.ny)))
			src := make([]uint16, shape.nx*shape.ny)
			for i := range src {
				src[i] = uint16(rng.Intn(int(mx) + 1))
			}
			buf := append([]uint16(nil), src...)
			refWav2Encode(buf, shape.nx, 1, shape.ny, shape.nx, mx)
			refWav2Decode(buf, shape.nx, 1, shape.ny, shape.nx, mx)
			for i := range src {
				if src[i] != buf[i] {
					t.Fatalf("reference transcription failed to round-trip at nx=%d ny=%d mx=%d index %d",
						shape.nx, shape.ny, mx, i)
				}
			}
		}
	}
}
