package compression

import "testing"

// The tests in this file exist because mutation testing proved the PIZ tests
// could not fail. scripts/mutation/run.py halved the wavelet's A_OFFSET in
// wenc16 and wdec16 together, and moved the packed code-length table's short
// zero-run base code from 59 to 58 in the packer and the unpacker together;
// both defects change every byte this library writes, and every PIZ test in the
// package stayed green. The reason is structural: TestWenc16Wdec16RoundTrip
// checks wdec16(wenc16(x)) == x, the wavelet "spec" test's reference encoder
// calls the very wenc16 it is meant to be pinning, and the compress/decompress
// tests share both halves of the defect.
//
// The expectations below are therefore not produced by this library. Each is
// computed by hand from the arithmetic in OpenEXR's ImfWav.cpp and ImfHuf.cpp,
// with the derivation written out, so a value that changes has to be argued
// against the format rather than re-recorded.

// TestWenc16MatchesImfWavVectors pins wenc16/wdec16 to hand-computed pairs.
//
// ImfWav.cpp, with NBITS = 16, A_OFFSET = M_OFFSET = 1 << 15 = 32768 and
// MOD_MASK = 0xffff:
//
//	wenc16 (a, b, l, h):
//	    ao = (a + A_OFFSET) & MOD_MASK
//	    m  = (ao + b) >> 1
//	    d  = ao - b
//	    if (d < 0) m = (m + M_OFFSET) & MOD_MASK
//	    d &= MOD_MASK
//	    l = m; h = d
//
// The round trip is exact for *any* offset, so a round-trip assertion says
// nothing about which offset is on the wire. 32768 is the one the format uses.
func TestWenc16MatchesImfWavVectors(t *testing.T) {
	tests := []struct {
		a, b       uint16
		l, h       uint16
		derivation string
	}{
		// ao = 0+32768 = 32768; m = (32768+0)>>1 = 16384; d = 32768 >= 0.
		{0, 0, 16384, 32768, "ao=32768 m=16384 d=32768"},
		// ao = 32768; m = (32768+1)>>1 = 16384; d = 32767 >= 0.
		{0, 1, 16384, 32767, "ao=32768 m=16384 d=32767"},
		// ao = 32769; m = (32769+0)>>1 = 16384; d = 32769 >= 0.
		{1, 0, 16384, 32769, "ao=32769 m=16384 d=32769"},
		// ao = (65535+32768) & 0xffff = 32767; m = 16383; d = 32767 >= 0.
		{0xFFFF, 0, 16383, 32767, "ao=32767 m=16383 d=32767"},
		// ao = 32768; m = (32768+65535)>>1 = 49151; d = -32767 < 0, so
		// m = (49151+32768) & 0xffff = 16383 and d & 0xffff = 32769.
		{0, 0xFFFF, 16383, 32769, "ao=32768 m wraps to 16383 d=32769"},
		// ao = 32868; m = (32868+200)>>1 = 16534; d = 32668 >= 0.
		{100, 200, 16534, 32668, "ao=32868 m=16534 d=32668"},
		// ao = (32768+32768) & 0xffff = 0; m = (0+16384)>>1 = 8192;
		// d = -16384 < 0, so m = 8192+32768 = 40960 and d & 0xffff = 49152.
		{32768, 16384, 40960, 49152, "ao=0 m wraps to 40960 d=49152"},
		// ao = (43690+32768) & 0xffff = 10922; m = (10922+21845)>>1 = 16383;
		// d = -10923 < 0, so m = 16383+32768 = 49151 and d & 0xffff = 54613.
		{0xAAAA, 0x5555, 49151, 54613, "ao=10922 m wraps to 49151 d=54613"},
	}

	for _, tt := range tests {
		l, h := wenc16(tt.a, tt.b)
		if l != tt.l || h != tt.h {
			t.Errorf("wenc16(%d, %d) = (%d, %d), want (%d, %d)  [%s]",
				tt.a, tt.b, l, h, tt.l, tt.h, tt.derivation)
		}
		// The decoder is pinned to the same literals, not to the encoder's
		// output: it is handed the specified (l, h) and must produce (a, b).
		a, b := wdec16(tt.l, tt.h)
		if a != tt.a || b != tt.b {
			t.Errorf("wdec16(%d, %d) = (%d, %d), want (%d, %d)",
				tt.l, tt.h, a, b, tt.a, tt.b)
		}
	}
}

// TestWenc14MatchesImfWavVectors pins wenc14/wdec14 to hand-computed pairs.
//
// ImfWav.cpp:
//
//	wenc14 (a, b, l, h):  ms = (a + b) >> 1;  ds = a - b;  l = ms; h = ds
//	wdec14 (l, h, a, b):  as = l + ((h + 1) >> 1);  bs = l - (h >> 1)
//
// with 16-bit *signed* arithmetic and arithmetic (sign-propagating) shifts.
// The +1 in the decoder is what makes the reconstruction exact for odd
// differences; dropping it still round-trips for every even difference, which
// is why a round-trip test misses it.
func TestWenc14MatchesImfWavVectors(t *testing.T) {
	tests := []struct {
		a, b       uint16
		l, h       uint16
		derivation string
	}{
		{0, 0, 0, 0, "ms=0 ds=0"},
		// ms = (1+0)>>1 = 0; ds = 1.
		{1, 0, 0, 1, "ms=0 ds=1"},
		// ms = (0+1)>>1 = 0; ds = -1 -> 0xffff.
		{0, 1, 0, 0xFFFF, "ms=0 ds=-1"},
		// ms = (3+0)>>1 = 1; ds = 3.
		{3, 0, 1, 3, "ms=1 ds=3"},
		// ms = (0+3)>>1 = 1; ds = -3 -> 0xfffd.
		{0, 3, 1, 0xFFFD, "ms=1 ds=-3"},
		// ms = (4+10)>>1 = 7; ds = -6 -> 0xfffa.
		{4, 10, 7, 0xFFFA, "ms=7 ds=-6"},
		{10, 4, 7, 6, "ms=7 ds=6"},
		// a = -3, b = -4: ms = (-7)>>1 = -4 (arithmetic shift), ds = 1.
		{0xFFFD, 0xFFFC, 0xFFFC, 1, "ms=-4 ds=1"},
	}

	for _, tt := range tests {
		l, h := wenc14(tt.a, tt.b)
		if l != tt.l || h != tt.h {
			t.Errorf("wenc14(%d, %d) = (0x%04x, 0x%04x), want (0x%04x, 0x%04x)  [%s]",
				int16(tt.a), int16(tt.b), l, h, tt.l, tt.h, tt.derivation)
		}
		// Decoded from the specified pair, so the +1 rounding is pinned:
		// for (l=1, h=3) the specification gives a = 1 + ((3+1)>>1) = 3, and
		// dropping the +1 would give 2.
		a, b := wdec14(tt.l, tt.h)
		if a != tt.a || b != tt.b {
			t.Errorf("wdec14(0x%04x, 0x%04x) = (0x%04x, 0x%04x), want (0x%04x, 0x%04x)",
				tt.l, tt.h, a, b, tt.a, tt.b)
		}
	}
}

// TestHufTableRangeMatchesSpecVector pins the packed Huffman code-length table
// to a bit sequence written out by hand.
//
// ImfHuf.cpp packs one 6-bit field per symbol, most significant bit first,
// with SHORT_ZEROCODE_RUN = 59, LONG_ZEROCODE_RUN = 63 and
// SHORTEST_LONG_RUN = 2 + 63 - 59 = 6:
//
//	a run of 2..5 zeros  -> the single code 59 + (run - 2)
//	a run of 6..261      -> the code 63, then an 8-bit (run - 6)
//
// For the lengths 3, 0, 0, 0, 5, 0, 0, 2 that is the field sequence
//
//	3 (000011), 60 (111100, a run of three zeros), 5 (000101),
//	59 (111011, a run of two zeros), 2 (000010)
//
// = 30 bits, zero-padded to 32:
//
//	00001111 11000001 01111011 00001000  =  0x0f 0xc1 0x7b 0x08
//
// Both constants are wire-visible: the packed table is written into every PIZ
// chunk, and a decoder that disagrees about them reads a different code book.
func TestHufTableRangeMatchesSpecVector(t *testing.T) {
	lengths := []int{3, 0, 0, 0, 5, 0, 0, 2}
	want := []byte{0x0f, 0xc1, 0x7b, 0x08}

	got := packHufTableRange(nil, lengths, 0, len(lengths)-1)
	if len(got) != len(want) {
		t.Fatalf("packed table is %d bytes (% x), want %d bytes (% x)",
			len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("packed code-length table = % x, want % x", got, want)
		}
	}

	// The read direction is pinned to the same hand-derived bytes, so the two
	// directions cannot drift together.
	back, _, err := unpackHufTableRange(want, 0, len(lengths)-1)
	if err != nil {
		t.Fatalf("unpackHufTableRange(% x) failed: %v", want, err)
	}
	for i, l := range lengths {
		if i >= len(back) {
			t.Fatalf("unpacked only %d lengths, want %d", len(back), len(lengths))
		}
		if back[i] != l {
			t.Errorf("unpacked length[%d] = %d, want %d (from % x)", i, back[i], l, want)
		}
	}
}
