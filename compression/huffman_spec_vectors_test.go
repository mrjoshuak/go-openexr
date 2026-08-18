package compression

import "testing"

// TestCanonicalCodesMatchImfHuf pins the canonical code assignment to codes
// computed by hand.
//
// Mutation testing (scripts/mutation/run.py, id huffman-canonical-order)
// handed the codes out in descending symbol order in all three places this
// package builds them. That produces a different, still perfectly valid,
// prefix code: every round trip in the package stayed green, including
// TestPIZOldVsNewHuffman, which compares two of this library's own decoders.
// Files written that way are unreadable by any conforming OpenEXR reader.
//
// ImfHuf.cpp hufCanonicalCodeTable:
//
//	n[l] = number of symbols with code length l
//	c = 0
//	for (l = 58; l > 0; --l) { nc = (c + n[l]) >> 1; n[l] = c; c = nc; }
//	for (i = 0; i < HUF_ENCSIZE; ++i)
//	    if (hcode[i]) hcode[i] = length | (n[length]++ << 6);
//
// The second loop walks symbols in *ascending* index, so within one code
// length the lower symbol index gets the lower code.
func TestCanonicalCodesMatchImfHuf(t *testing.T) {
	tests := []struct {
		name       string
		lengths    []int
		wantCodes  []uint64
		derivation string
	}{
		{
			// n[1]=1, n[2]=1, n[3]=2.
			//   l=3: nc = (0+2)>>1 = 1, start[3] = 0, c = 1
			//   l=2: nc = (1+1)>>1 = 1, start[2] = 1, c = 1
			//   l=1: nc = (1+1)>>1 = 1, start[1] = 1, c = 1
			// Assigning in ascending symbol order gives 1, 01, 000, 001.
			name:       "1-2-3-3",
			lengths:    []int{1, 2, 3, 3},
			wantCodes:  []uint64{1, 1, 0, 1},
			derivation: "start[1]=1 start[2]=1 start[3]=0",
		},
		{
			// n[2]=4. l=2: nc = (0+4)>>1 = 2, start[2] = 0.
			// Four equal-length symbols get 00, 01, 10, 11 in symbol order.
			name:       "four equal lengths",
			lengths:    []int{2, 2, 2, 2},
			wantCodes:  []uint64{0, 1, 2, 3},
			derivation: "start[2]=0",
		},
		{
			// n[1]=1, n[2]=1, n[3]=1, n[4]=2.
			//   l=4: nc = (0+2)>>1 = 1, start[4] = 0, c = 1
			//   l=3: nc = (1+1)>>1 = 1, start[3] = 1, c = 1
			//   l=2: nc = (1+1)>>1 = 1, start[2] = 1, c = 1
			//   l=1: nc = (1+1)>>1 = 1, start[1] = 1, c = 1
			// -> 1, 01, 001, 0000, 0001.
			name:       "1-2-3-4-4",
			lengths:    []int{1, 2, 3, 4, 4},
			wantCodes:  []uint64{1, 1, 1, 0, 1},
			derivation: "start[1]=1 start[2]=1 start[3]=1 start[4]=0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			codes := make([]huffmanCode, len(tt.lengths))
			generateCanonicalCodes(codes, tt.lengths)
			for i := range tt.lengths {
				if codes[i].length != tt.lengths[i] || codes[i].code != tt.wantCodes[i] {
					t.Errorf("symbol %d: got code %d length %d, want code %d length %d  [%s]",
						i, codes[i].code, codes[i].length,
						tt.wantCodes[i], tt.lengths[i], tt.derivation)
				}
			}
		})
	}
}

// TestHuffmanDecoderReadsSpecBitstream decodes a bit sequence written out by
// hand from the canonical codes above, so the decoder is anchored to the
// format and not to this package's encoder.
//
// Code book for lengths {1, 2, 3, 3}: symbol 0 = "1", 1 = "01", 2 = "000",
// 3 = "001". The symbol sequence 0, 1, 2, 3 is therefore
//
//	1 01 000 001  =  1010 0000 1  ->  0xa0 0x80  (padded with zeros)
func TestHuffmanDecoderReadsSpecBitstream(t *testing.T) {
	lengths := []int{1, 2, 3, 3}
	stream := []byte{0xa0, 0x80}
	want := []uint16{0, 1, 2, 3}

	d := NewHuffmanDecoder(lengths)
	got, err := d.Decode(stream, len(want))
	if err != nil {
		t.Fatalf("Decode(% x) failed: %v", stream, err)
	}
	if len(got) != len(want) {
		t.Fatalf("decoded %d symbols, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("decoded % v from % x, want % v", got, stream, want)
		}
	}
}
