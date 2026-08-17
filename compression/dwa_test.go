package compression

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// The tests in this file check DWA's pieces against something outside this
// package: the transform against the JPEG iDCT/DCT definition DWA implements,
// the AC coding against the byte sequences the format specifies, and the
// classification rules against the encoding described in
// internal_dwa_classifier.h. The end-to-end check that this library reads what
// the OpenEXR reference implementation writes lives in
// exr/conformance_test.go, against files that implementation produced.

// specIDct8x8 is the two-dimensional inverse DCT DWA implements, written out
// from its definition rather than from the reference's factored form:
//
//	x[m][n] = 1/4 * sum_u sum_v C(u) C(v) X[u][v]
//	                  cos((2m+1) u pi / 16) cos((2n+1) v pi / 16)
//
// with C(0) = 1/sqrt(2) and C(k) = 1 otherwise. This is the JPEG iDCT, and the
// 1/4 C(0) C(0) = 1/8 scale on a DC-only block is exactly the factor
// dwaDctInverse8x8DcOnly applies.
func specIDct8x8(in [64]float64) [64]float64 {
	c := func(k int) float64 {
		if k == 0 {
			return 1 / math.Sqrt2
		}
		return 1
	}
	var out [64]float64
	for m := 0; m < 8; m++ {
		for n := 0; n < 8; n++ {
			sum := 0.0
			for u := 0; u < 8; u++ {
				for v := 0; v < 8; v++ {
					sum += c(u) * c(v) * in[u*8+v] *
						math.Cos(float64(2*m+1)*float64(u)*math.Pi/16) *
						math.Cos(float64(2*n+1)*float64(v)*math.Pi/16)
				}
			}
			out[m*8+n] = sum / 4
		}
	}
	return out
}

// specDct8x8 is the corresponding forward transform.
func specDct8x8(in [64]float64) [64]float64 {
	c := func(k int) float64 {
		if k == 0 {
			return 1 / math.Sqrt2
		}
		return 1
	}
	var out [64]float64
	for u := 0; u < 8; u++ {
		for v := 0; v < 8; v++ {
			sum := 0.0
			for m := 0; m < 8; m++ {
				for n := 0; n < 8; n++ {
					sum += in[m*8+n] *
						math.Cos(float64(2*m+1)*float64(u)*math.Pi/16) *
						math.Cos(float64(2*n+1)*float64(v)*math.Pi/16)
				}
			}
			out[u*8+v] = c(u) * c(v) * sum / 4
		}
	}
	return out
}

func testBlocks() [][64]float32 {
	var blocks [][64]float32

	var ramp [64]float32
	for i := range ramp {
		ramp[i] = float32(i) / 64
	}
	blocks = append(blocks, ramp)

	var dcOnly [64]float32
	dcOnly[0] = 12.5
	blocks = append(blocks, dcOnly)

	var single [64]float32
	single[35] = -3.25
	blocks = append(blocks, single)

	// A deterministic pseudo-random block, so the comparison is not confined
	// to smooth data the transform might get right by accident.
	var noise [64]float32
	state := uint32(12345)
	for i := range noise {
		state = state*1664525 + 1013904223
		noise[i] = float32(int32(state>>8)%2001-1000) / 100
	}
	blocks = append(blocks, noise)

	return blocks
}

// TestDwaDctInverseMatchesDefinition checks the reference's factored inverse
// DCT against the transform it is a factoring of. They cannot agree exactly:
// the reference builds its constants from a truncated literal for pi, which is
// itself part of the format, so the tolerance is what that truncation costs.
func TestDwaDctInverseMatchesDefinition(t *testing.T) {
	const tol = 2e-4

	for bi, block := range testBlocks() {
		var in [64]float64
		for i, v := range block {
			in[i] = float64(v)
		}
		want := specIDct8x8(in)

		got := block
		dwaDctInverse8x8(&got, 0)

		for i := range got {
			if diff := math.Abs(float64(got[i]) - want[i]); diff > tol*math.Max(1, math.Abs(want[i])) {
				t.Errorf("block %d coefficient %d: inverse DCT gave %v, definition gives %v",
					bi, i, got[i], want[i])
			}
		}
	}
}

func TestDwaDctForwardMatchesDefinition(t *testing.T) {
	const tol = 2e-4

	for bi, block := range testBlocks() {
		var in [64]float64
		for i, v := range block {
			in[i] = float64(v)
		}
		want := specDct8x8(in)

		got := block
		dwaDctForward8x8(&got)

		for i := range got {
			if diff := math.Abs(float64(got[i]) - want[i]); diff > tol*math.Max(1, math.Abs(want[i])) {
				t.Errorf("block %d coefficient %d: forward DCT gave %v, definition gives %v",
					bi, i, got[i], want[i])
			}
		}
	}
}

// TestDwaDctInverseZeroedRows checks the optimisation that skips rows known to
// be zero: it must produce exactly what a full inverse produces.
func TestDwaDctInverseZeroedRows(t *testing.T) {
	for zeroed := 1; zeroed <= 7; zeroed++ {
		var block [64]float32
		state := uint32(7)
		for row := 0; row < 8-zeroed; row++ {
			for col := 0; col < 8; col++ {
				state = state*1664525 + 1013904223
				block[row*8+col] = float32(int32(state>>8)%2001-1000) / 100
			}
		}

		full := block
		dwaDctInverse8x8(&full, 0)
		fast := block
		dwaDctInverse8x8(&fast, zeroed)

		if full != fast {
			t.Errorf("zeroedRows=%d: fast path disagrees with the full inverse", zeroed)
		}
	}
}

// TestDwaDcOnlyMatchesFullInverse checks the constant-block shortcut against
// the general inverse.
func TestDwaDcOnlyMatchesFullInverse(t *testing.T) {
	for _, dc := range []float32{0, 1, -7.5, 1024} {
		var block [64]float32
		block[0] = dc

		full := block
		dwaDctInverse8x8(&full, 7)
		fast := block
		dwaDctInverse8x8DcOnly(&fast)

		for i := range full {
			if diff := math.Abs(float64(full[i] - fast[i])); diff > 1e-5*math.Max(1, math.Abs(float64(dc))) {
				t.Fatalf("dc=%v index %d: DC-only gave %v, full inverse gave %v",
					dc, i, fast[i], full[i])
			}
		}
	}
}

// TestDwaCsc709RoundTrip checks the colour space conversion pair.
func TestDwaCsc709RoundTrip(t *testing.T) {
	var r, g, b [64]float32
	state := uint32(99)
	for i := 0; i < 64; i++ {
		state = state*1664525 + 1013904223
		r[i] = float32(state>>16) / 65536
		state = state*1664525 + 1013904223
		g[i] = float32(state>>16) / 65536
		state = state*1664525 + 1013904223
		b[i] = float32(state>>16) / 65536
	}
	origR, origG, origB := r, g, b

	dwaCsc709Forward64(&r, &g, &b)
	dwaCsc709Inverse64(&r, &g, &b)

	const tol = 1e-4
	for i := 0; i < 64; i++ {
		if math.Abs(float64(r[i]-origR[i])) > tol ||
			math.Abs(float64(g[i]-origG[i])) > tol ||
			math.Abs(float64(b[i]-origB[i])) > tol {
			t.Fatalf("index %d: got (%v %v %v), want (%v %v %v)",
				i, r[i], g[i], b[i], origR[i], origG[i], origB[i])
		}
	}
}

// TestDwaCsc709InverseMatchesBlock checks that the single-sample inverse used
// for constant blocks agrees with the 64-sample one.
func TestDwaCsc709InverseMatchesBlock(t *testing.T) {
	var y, cb, cr [64]float32
	for i := range y {
		y[i], cb[i], cr[i] = 0.5, -0.25, 0.125
	}
	block := [3][64]float32{y, cb, cr}
	dwaCsc709Inverse64(&block[0], &block[1], &block[2])

	one := [3]float32{0.5, -0.25, 0.125}
	dwaCsc709Inverse(&one[0], &one[1], &one[2])

	if block[0][0] != one[0] || block[1][0] != one[1] || block[2][0] != one[2] {
		t.Fatalf("single-sample inverse %v disagrees with block inverse (%v %v %v)",
			one, block[0][0], block[1][0], block[2][0])
	}
}

// TestDwaZigZag checks that the un-zig-zag mapping is the inverse of the
// forward zig-zag order given in the format.
func TestDwaZigZag(t *testing.T) {
	// The zig-zag scan order of an 8x8 block: the natural index visited at
	// each step, from the table quoted in quantizeCoeffAndZigXDR.
	zigzag := [64]int{
		0, 1, 8, 16, 9, 2, 3, 10,
		17, 24, 32, 25, 18, 11, 4, 5,
		12, 19, 26, 33, 40, 48, 41, 34,
		27, 20, 13, 6, 7, 14, 21, 28,
		35, 42, 49, 56, 57, 50, 43, 36,
		29, 22, 15, 23, 30, 37, 44, 51,
		58, 59, 52, 45, 38, 31, 39, 46,
		53, 60, 61, 54, 47, 55, 62, 63,
	}
	for step, natural := range zigzag {
		if dwaInvZigZag[natural] != step {
			t.Errorf("natural index %d is zig-zag step %d, but dwaInvZigZag says %d",
				natural, step, dwaInvZigZag[natural])
		}
	}
}

// TestDwaAcRleFormat pins the AC run-length coding to the byte sequences the
// format defines, rather than only to its own inverse.
func TestDwaAcRleFormat(t *testing.T) {
	tests := []struct {
		name  string
		block func(*[64]uint16)
		want  []uint16
	}{
		{
			// Nothing but the DC term: the whole 63-coefficient run is
			// signalled with the end-of-block symbol.
			name:  "all zero",
			block: func(b *[64]uint16) {},
			want:  []uint16{0xff00},
		},
		{
			// A single zero is cheaper written out than run-coded.
			name: "isolated zero",
			block: func(b *[64]uint16) {
				for i := 1; i < 64; i++ {
					b[i] = uint16(i)
				}
				b[5] = 0
			},
			want: func() []uint16 {
				var w []uint16
				for i := 1; i < 64; i++ {
					if i == 5 {
						w = append(w, 0)
						continue
					}
					w = append(w, uint16(i))
				}
				return w
			}(),
		},
		{
			// A run of three zeros in the middle becomes one run symbol.
			name: "interior run",
			block: func(b *[64]uint16) {
				for i := 1; i < 64; i++ {
					b[i] = 7
				}
				b[10], b[11], b[12] = 0, 0, 0
			},
			want: func() []uint16 {
				w := make([]uint16, 0, 62)
				for i := 1; i < 10; i++ {
					w = append(w, 7)
				}
				w = append(w, 0xff03)
				for i := 13; i < 64; i++ {
					w = append(w, 7)
				}
				return w
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var block [64]uint16
			block[0] = 0x1234 // DC, never part of the AC stream
			tt.block(&block)

			got := dwaRleAc(&block, nil)
			if len(got) != len(tt.want) {
				t.Fatalf("coded %d symbols, want %d\n got %v\nwant %v",
					len(got), len(tt.want), got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("symbol %d = %#04x, want %#04x", i, got[i], tt.want[i])
				}
			}

			// And the decoder must recover the block from those symbols.
			var back [64]uint16
			back[0] = block[0]
			pos := 0
			lastNonZero, err := dwaUnRleAc(got, &pos, &back)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if pos != len(got) {
				t.Errorf("decode consumed %d symbols, %d were written", pos, len(got))
			}
			if back != block {
				t.Errorf("decoded block %v, want %v", back, block)
			}
			wantLast := 0
			for i := 1; i < 64; i++ {
				if block[i] != 0 {
					wantLast = i
				}
			}
			if lastNonZero != wantLast {
				t.Errorf("lastNonZero = %d, want %d", lastNonZero, wantLast)
			}
		})
	}
}

// TestDwaUnRleAcTruncated checks that a stream that ends mid-block is an error
// rather than a silently zero-filled block.
func TestDwaUnRleAcTruncated(t *testing.T) {
	var block [64]uint16
	pos := 0
	if _, err := dwaUnRleAc([]uint16{1, 2, 3}, &pos, &block); err == nil {
		t.Fatal("truncated AC stream decoded without error")
	}
}

// TestDwaClassifierWireFormat pins the on-disk encoding of a classification
// rule: a NUL-terminated suffix, then a byte packing the colour index, the
// scheme and the case-insensitivity flag, then the pixel type.
func TestDwaClassifierWireFormat(t *testing.T) {
	rule := dwaClassifier{
		suffix:          "G",
		scheme:          compressorLossyDCT,
		pixelType:       DwaPixelTypeHalf,
		cscIdx:          1,
		caseInsensitive: false,
	}
	// cscIdx+1 = 2 in the high nibble, scheme 1 in bits 2..3, flag clear.
	want := []byte{'G', 0, 2<<4 | 1<<2, DwaPixelTypeHalf}
	got := rule.appendTo(nil)
	if len(got) != len(want) {
		t.Fatalf("serialised %d bytes, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("byte %d = %#02x, want %#02x", i, got[i], want[i])
		}
	}
	if rule.size() != len(want) {
		t.Errorf("size() = %d, want %d", rule.size(), len(want))
	}

	block := make([]byte, 2, 2+len(got))
	block = append(block, got...)
	binary.LittleEndian.PutUint16(block, uint16(len(block)))

	rules, n, err := dwaReadChannelRules(block)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if n != len(block) {
		t.Errorf("consumed %d bytes, block is %d", n, len(block))
	}
	if len(rules) != 1 || rules[0] != rule {
		t.Fatalf("parsed %v, want [%v]", rules, rule)
	}
}

func TestDwaReadChannelRulesRejectsMalformed(t *testing.T) {
	tests := []struct {
		name  string
		block []byte
	}{
		{"empty", nil},
		{"size only", []byte{2, 0}},
		{"size smaller than prefix", []byte{1, 0, 0, 0}},
		{"size past end", []byte{99, 0, 'R', 0, 0, 1}},
		{"unterminated suffix", []byte{8, 0, 'R', 'G', 'B', 'A', 'X', 'Y'}},
		{"scheme out of range", []byte{6, 0, 'R', 0, 3 << 2, 1}},
		{"pixel type out of range", []byte{6, 0, 'R', 0, 0, 9}},
		{"colour index out of range", []byte{6, 0, 'R', 0, 15 << 4, 1}},
		{"trailing partial rule", []byte{9, 0, 'R', 0, 1 << 2, 1, 'G', 0, 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := dwaReadChannelRules(tt.block); err == nil {
				t.Fatal("malformed rule block parsed without error")
			}
		})
	}
}

// TestDwaClassify checks that channels land in the schemes the default rules
// name, and that a layer's R, G and B are grouped for colour conversion while
// channels from different layers are not.
func TestDwaClassify(t *testing.T) {
	channels := []DwaChannel{
		{Name: "A", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "B", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "G", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "R", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "Z", PixelType: DwaPixelTypeFloat, XSampling: 1, YSampling: 1},
		{Name: "id", PixelType: DwaPixelTypeUint, XSampling: 1, YSampling: 1},
		{Name: "diffuse.B", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "diffuse.G", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "diffuse.R", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
	}
	chans, _, err := dwaBuildChannels(channels, 0, 7, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	sets := dwaClassify(chans, dwaDefaultRules)

	want := map[string]int{
		"A": compressorRLE, "B": compressorLossyDCT, "G": compressorLossyDCT,
		"R": compressorLossyDCT, "Z": compressorUnknown, "id": compressorUnknown,
		"diffuse.B": compressorLossyDCT, "diffuse.G": compressorLossyDCT,
		"diffuse.R": compressorLossyDCT,
	}
	for i := range chans {
		if chans[i].scheme != want[chans[i].ch.Name] {
			t.Errorf("channel %q classified %d, want %d",
				chans[i].ch.Name, chans[i].scheme, want[chans[i].ch.Name])
		}
	}

	if len(sets) != 2 {
		t.Fatalf("found %d colour triples, want 2 (the root layer and diffuse)", len(sets))
	}
	names := func(s dwaCscSet) [3]string {
		return [3]string{chans[s.idx[0]].ch.Name, chans[s.idx[1]].ch.Name, chans[s.idx[2]].ch.Name}
	}
	if got := names(sets[0]); got != [3]string{"R", "G", "B"} {
		t.Errorf("first triple is %v, want R G B", got)
	}
	if got := names(sets[1]); got != [3]string{"diffuse.R", "diffuse.G", "diffuse.B"} {
		t.Errorf("second triple is %v, want diffuse.R G B", got)
	}
}

// TestDwaClassifyLegacyRulesAreCaseInsensitive checks the rule set that
// version 0 and 1 chunks imply, which matches names ignoring case and knows
// spelled-out colour names.
func TestDwaClassifyLegacyRules(t *testing.T) {
	channels := []DwaChannel{
		{Name: "blue", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "GREEN", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "Red", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "a", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
	}
	chans, _, err := dwaBuildChannels(channels, 0, 7, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	sets := dwaClassify(chans, dwaLegacyRules)
	for i := range chans {
		want := compressorLossyDCT
		if chans[i].ch.Name == "a" {
			want = compressorRLE
		}
		if chans[i].scheme != want {
			t.Errorf("channel %q classified %d, want %d", chans[i].ch.Name, chans[i].scheme, want)
		}
	}
	if len(sets) != 1 {
		t.Fatalf("found %d colour triples, want 1", len(sets))
	}

	// The default rules are case sensitive, so the same channels must not be
	// DCT coded under them.
	chans2, _, _ := dwaBuildChannels(channels, 0, 7, 0, 7)
	dwaClassify(chans2, dwaDefaultRules)
	for i := range chans2 {
		if chans2[i].scheme != compressorUnknown {
			t.Errorf("channel %q matched a case-sensitive rule", chans2[i].ch.Name)
		}
	}
}

// dwaTestChannels is a four-channel layout covering all three schemes: R, G
// and B are DCT coded as a colour triple, A is run-length coded, and Z is
// stored losslessly.
func dwaTestChannels(zType int) []DwaChannel {
	return []DwaChannel{
		{Name: "A", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "B", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "G", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "R", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
		{Name: "Z", PixelType: zType, XSampling: 1, YSampling: 1},
	}
}

// dwaTestImage builds one uncompressed chunk in the layout OpenEXR uses:
// scanline by scanline, and within a scanline each channel's samples
// contiguously, channels in the order given.
func dwaTestImage(channels []DwaChannel, width, height int) []byte {
	rowStride, offsets := dwaTestLayout(channels, width)
	buf := make([]byte, rowStride*height)
	for y := 0; y < height; y++ {
		for c, ch := range channels {
			pos := y*rowStride + offsets[c]
			for x := 0; x < width; x++ {
				v := float32(x)/float32(width) + float32(y)/float32(2*height)
				switch ch.Name {
				case "G":
					v = float32(y) / float32(height)
				case "B":
					v = 0.25
				case "A":
					v = float32((x*7+y*3)%16) / 16
				case "Z":
					v = float32(x*height+y) / 8
				}
				switch ch.PixelType {
				case DwaPixelTypeHalf:
					binary.LittleEndian.PutUint16(buf[pos:], half.FromFloat32(v).Bits())
					pos += 2
				case DwaPixelTypeFloat:
					binary.LittleEndian.PutUint32(buf[pos:], math.Float32bits(v))
					pos += 4
				case DwaPixelTypeUint:
					binary.LittleEndian.PutUint32(buf[pos:], uint32(x*height+y))
					pos += 4
				}
			}
		}
	}
	return buf
}

// dwaTestLayout returns the size of one scanline and each channel's offset
// within it.
func dwaTestLayout(channels []DwaChannel, width int) (int, []int) {
	offsets := make([]int, len(channels))
	stride := 0
	for i, ch := range channels {
		offsets[i] = stride
		stride += width * dwaBytesPerElement(ch.PixelType)
	}
	return stride, offsets
}

// TestDwaLosslessChannelsSurviveExactly checks that the schemes DWA describes
// as lossless really are. Alpha is run-length coded and Z is deflated, so both
// must come back bit for bit; only the DCT channels may change.
func TestDwaLosslessChannelsSurviveExactly(t *testing.T) {
	for _, zType := range []int{DwaPixelTypeFloat, DwaPixelTypeUint, DwaPixelTypeHalf} {
		channels := dwaTestChannels(zType)
		const width, height = 37, 20
		src := dwaTestImage(channels, width, height)

		packed, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
		if err != nil {
			t.Fatalf("compress: %v", err)
		}
		got := make([]byte, len(src))
		if err := DWADecompress(packed, channels, 0, width-1, 0, height-1, got); err != nil {
			t.Fatalf("decompress: %v", err)
		}

		rowStride, offsets := dwaTestLayout(channels, width)
		for i, ch := range channels {
			if ch.Name != "A" && ch.Name != "Z" {
				continue
			}
			n := width * dwaBytesPerElement(ch.PixelType)
			for y := 0; y < height; y++ {
				off := y*rowStride + offsets[i]
				for b := 0; b < n; b++ {
					if got[off+b] != src[off+b] {
						t.Fatalf("Z type %d: channel %s row %d byte %d changed: %#02x -> %#02x",
							zType, ch.Name, y, b, src[off+b], got[off+b])
					}
				}
			}
		}
	}
}

// TestDwaLossyChannelsStayClose checks that the DCT channels come back within
// the error DWA's default quality allows.
func TestDwaLossyChannelsStayClose(t *testing.T) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 37, 20
	src := dwaTestImage(channels, width, height)

	packed, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	got := make([]byte, len(src))
	if err := DWADecompress(packed, channels, 0, width-1, 0, height-1, got); err != nil {
		t.Fatalf("decompress: %v", err)
	}

	rowStride, offsets := dwaTestLayout(channels, width)
	for i, ch := range channels {
		if ch.PixelType != DwaPixelTypeHalf || ch.Name == "A" {
			continue
		}
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				off := y*rowStride + offsets[i] + x*2
				want := half.FromBits(binary.LittleEndian.Uint16(src[off:])).Float32()
				have := half.FromBits(binary.LittleEndian.Uint16(got[off:])).Float32()
				if math.Abs(float64(have-want)) > 0.02 {
					t.Fatalf("channel %s pixel (%d,%d): %v became %v", ch.Name, x, y, want, have)
				}
			}
		}
	}
}

// TestDwaRejectsBadInput checks that damaged or unsupported chunks produce an
// error. A decoder that returns a zeroed buffer instead cannot be told apart
// from one that worked.
func TestDwaRejectsBadInput(t *testing.T) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 24, 16
	src := dwaTestImage(channels, width, height)
	good, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatal(err)
	}
	dst := make([]byte, len(src))

	corrupt := func(fn func([]byte) []byte) []byte {
		c := make([]byte, len(good))
		copy(c, good)
		return fn(c)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"header only", good[:dwaHeaderSize]},
		{"truncated header", good[:dwaHeaderSize-1]},
		{"truncated payload", good[:len(good)-1]},
		{"future version", corrupt(func(c []byte) []byte {
			binary.LittleEndian.PutUint64(c[dwaHdrVersion*8:], 3)
			return c
		})},
		{"unknown AC compression", corrupt(func(c []byte) []byte {
			binary.LittleEndian.PutUint64(c[dwaHdrAcCompression*8:], 7)
			return c
		})},
		{"AC count beyond what the geometry allows", corrupt(func(c []byte) []byte {
			binary.LittleEndian.PutUint64(c[dwaHdrAcUncompressedCount*8:], 1<<20)
			return c
		})},
		{"DC count claimed without DC data", corrupt(func(c []byte) []byte {
			binary.LittleEndian.PutUint64(c[dwaHdrDcCompressedSize*8:], 0)
			return c
		})},
		{"section size past end of chunk", corrupt(func(c []byte) []byte {
			binary.LittleEndian.PutUint64(c[dwaHdrUnknownCompressedSize*8:], 1<<20)
			return c
		})},
		{"garbage deflate stream", corrupt(func(c []byte) []byte {
			for i := dwaHeaderSize + 8; i < len(c); i++ {
				c[i] ^= 0xff
			}
			return c
		})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i := range dst {
				dst[i] = 0xcd
			}
			if err := DWADecompress(tt.data, channels, 0, width-1, 0, height-1, dst); err == nil {
				t.Fatal("damaged chunk decoded without error")
			}
		})
	}
}

// TestDwaRejectsWrongDestinationSize checks that a caller who sizes the output
// buffer for the wrong geometry is told so rather than getting a partial
// decode.
func TestDwaRejectsWrongDestinationSize(t *testing.T) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 24, 16
	src := dwaTestImage(channels, width, height)
	good, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatal(err)
	}
	if err := DWADecompress(good, channels, 0, width-1, 0, height-1, make([]byte, len(src)-2)); err == nil {
		t.Fatal("undersized destination accepted")
	}
	if _, err := DWACompress(src[:len(src)-2], channels, 0, width-1, 0, height-1, 45); err == nil {
		t.Fatal("undersized source accepted")
	}
}

// TestDwaRejectsUintLossyDct checks that a chunk whose rules ask for DCT
// coding of an integer channel is an error. The reference would write half
// values into a 32-bit channel and leave the rest of each sample untouched;
// there is no reading of that which is right.
func TestDwaRejectsUintLossyDct(t *testing.T) {
	channels := []DwaChannel{
		{Name: "R", PixelType: DwaPixelTypeUint, XSampling: 1, YSampling: 1},
	}
	chans, total, err := dwaBuildChannels(channels, 0, 15, 0, 15)
	if err != nil {
		t.Fatal(err)
	}
	chans[0].scheme = compressorLossyDCT
	if err := dwaSetRows(chans, make([]byte, total), 0, 15); err != nil {
		t.Fatal(err)
	}
	dc := make([]uint16, 4)
	ac := make([]uint16, 4)
	pos := 0
	dcPos := 0
	if err := dwaDecodeLossyDct([]*dwaChannelState{&chans[0]}, ac, &pos, dc, &dcPos, true); err == nil {
		t.Fatal("uint channel decoded as lossy DCT without error")
	}
}

// TestDwaHorizontalSubsampling checks a luminance-chroma layout, where the
// chroma channels carry half as many samples per row as the luminance. Each
// channel is coded on its own geometry.
func TestDwaHorizontalSubsampling(t *testing.T) {
	channels := []DwaChannel{
		{Name: "BY", PixelType: DwaPixelTypeHalf, XSampling: 2, YSampling: 1},
		{Name: "RY", PixelType: DwaPixelTypeHalf, XSampling: 2, YSampling: 1},
		{Name: "Y", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
	}
	const width, height = 35, 20
	chans, total, err := dwaBuildChannels(channels, 0, width-1, 0, height-1)
	if err != nil {
		t.Fatal(err)
	}
	if chans[0].width != 18 || chans[2].width != 35 {
		t.Fatalf("channel widths are %d and %d, want 18 and 35", chans[0].width, chans[2].width)
	}

	src := make([]byte, total)
	rowStride := (18 + 18 + 35) * 2
	for y := 0; y < height; y++ {
		pos := y * rowStride
		for c := range channels {
			for x := 0; x < chans[c].width; x++ {
				v := float32(x+c*3)/40 + float32(y)/60
				binary.LittleEndian.PutUint16(src[pos:], half.FromFloat32(v).Bits())
				pos += 2
			}
		}
	}

	packed, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatalf("compress: %v", err)
	}
	got := make([]byte, total)
	if err := DWADecompress(packed, channels, 0, width-1, 0, height-1, got); err != nil {
		t.Fatalf("decompress: %v", err)
	}
	for i := 0; i+1 < total; i += 2 {
		want := half.FromBits(binary.LittleEndian.Uint16(src[i:])).Float32()
		have := half.FromBits(binary.LittleEndian.Uint16(got[i:])).Float32()
		if math.Abs(float64(have-want)) > 0.02 {
			t.Fatalf("sample at byte %d: %v became %v", i, want, have)
		}
	}
}

// TestDwaVerticalSubsamplingIsSized checks that a vertically subsampled chunk
// is measured by the rows each channel actually stores, and that a caller who
// sized the buffer as if nothing were subsampled is told so.
//
// The rest of this library computes chunk sizes without accounting for
// vertical subsampling, so such a file cannot be read end to end. An error is
// the only honest outcome; returning a differently-shaped image would not be.
func TestDwaVerticalSubsamplingIsSized(t *testing.T) {
	channels := []DwaChannel{
		{Name: "BY", PixelType: DwaPixelTypeHalf, XSampling: 2, YSampling: 2},
		{Name: "RY", PixelType: DwaPixelTypeHalf, XSampling: 2, YSampling: 2},
		{Name: "Y", PixelType: DwaPixelTypeHalf, XSampling: 1, YSampling: 1},
	}
	chans, total, err := dwaBuildChannels(channels, 0, 15, 0, 15)
	if err != nil {
		t.Fatal(err)
	}
	if chans[0].height != 8 {
		t.Errorf("a channel with YSampling 2 stores %d of 16 rows, want 8", chans[0].height)
	}
	full := 16 * 16 * 3 * 2
	if total == full {
		t.Fatalf("subsampled chunk sized as %d bytes, same as unsubsampled", total)
	}
	if err := DWADecompress(make([]byte, dwaHeaderSize+2), channels,
		0, 15, 0, 15, make([]byte, full)); err == nil {
		t.Fatal("mis-sized destination accepted for a subsampled chunk")
	}
}

// TestDwaQuantizeStaysWithinTolerance checks the encoder's quantiser: it may
// move a coefficient only as far as the tolerance it is given, and it should
// take the chance to clear one when it can.
func TestDwaQuantizeStaysWithinTolerance(t *testing.T) {
	for _, tol := range []float32{0, 0.001, 0.01, 0.5} {
		for bits := 0; bits < 65536; bits += 7 {
			src := uint16(bits)
			f := half.FromBits(src).Float32()
			if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
				continue
			}
			got := dwaQuantize(src, tol)
			g := half.FromBits(got).Float32()
			if math.IsNaN(float64(g)) || math.IsInf(float64(g), 0) {
				t.Fatalf("tol=%v: quantising %v produced %v", tol, f, g)
			}
			if d := math.Abs(float64(g - f)); d > float64(tol) {
				t.Fatalf("tol=%v: quantising %v to %v moved it by %v", tol, f, g, d)
			}
		}
	}
	if got := dwaQuantize(half.FromFloat32(0.0005).Bits(), 0.001); got != 0 {
		t.Errorf("a coefficient smaller than the tolerance was kept as %#04x", got)
	}
	if got := dwaQuantize(half.FromFloat32(1).Bits(), 0); got != half.FromFloat32(1).Bits() {
		t.Errorf("a zero tolerance changed a coefficient to %#04x", got)
	}
}

// TestDwaSizesAreConsistent checks the sizes DWACompress records against the
// payloads it writes, since the decoder navigates the chunk entirely by them.
func TestDwaSizesAreConsistent(t *testing.T) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 40, 33
	src := dwaTestImage(channels, width, height)
	packed, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatal(err)
	}

	var counters [dwaNumSizesSingle]uint64
	for i := range counters {
		counters[i] = binary.LittleEndian.Uint64(packed[i*8:])
	}
	if counters[dwaHdrVersion] != dwaVersion {
		t.Errorf("version %d, want %d", counters[dwaHdrVersion], dwaVersion)
	}
	ruleSize := int(binary.LittleEndian.Uint16(packed[dwaHeaderSize:]))
	total := dwaHeaderSize + ruleSize +
		int(counters[dwaHdrUnknownCompressedSize]) +
		int(counters[dwaHdrAcCompressedSize]) +
		int(counters[dwaHdrDcCompressedSize]) +
		int(counters[dwaHdrRleCompressedSize])
	if total != len(packed) {
		t.Errorf("header accounts for %d bytes, chunk is %d", total, len(packed))
	}
	if counters[dwaHdrAcCompression] != acCompressionDeflate {
		t.Errorf("AC compression recorded as %d", counters[dwaHdrAcCompression])
	}
}

// TestDwaHufAcRoundTrip checks the Huffman AC path, which is what the
// reference implementation writes by default. The AC coefficients are coded
// with the shared Huffman coder and read back through the decoder's
// STATIC_HUFFMAN branch.
func TestDwaHufAcRoundTrip(t *testing.T) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 40, 24
	src := dwaTestImage(channels, width, height)

	deflated, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		t.Fatal(err)
	}
	viaDeflate := make([]byte, len(src))
	if err := DWADecompress(deflated, channels, 0, width-1, 0, height-1, viaDeflate); err != nil {
		t.Fatal(err)
	}

	// Recode the same AC coefficients with the static Huffman coder and check
	// the decoder gets the same image out.
	huffed, err := dwaRecodeAcAsHuffman(deflated)
	if err != nil {
		t.Fatalf("recode: %v", err)
	}
	viaHuffman := make([]byte, len(src))
	if err := DWADecompress(huffed, channels, 0, width-1, 0, height-1, viaHuffman); err != nil {
		t.Fatalf("decode Huffman-coded AC: %v", err)
	}

	for i := range viaDeflate {
		if viaDeflate[i] != viaHuffman[i] {
			t.Fatalf("byte %d differs between the deflate and Huffman AC paths: %#02x vs %#02x",
				i, viaDeflate[i], viaHuffman[i])
		}
	}
}

// dwaRecodeAcAsHuffman rewrites a chunk's AC section with OpenEXR's static
// Huffman coder, leaving everything else alone.
func dwaRecodeAcAsHuffman(chunk []byte) ([]byte, error) {
	var counters [dwaNumSizesSingle]uint64
	for i := range counters {
		counters[i] = binary.LittleEndian.Uint64(chunk[i*8:])
	}
	ruleSize := int(binary.LittleEndian.Uint16(chunk[dwaHeaderSize:]))
	pos := dwaHeaderSize + ruleSize
	unknownSize := int(counters[dwaHdrUnknownCompressedSize])
	acSize := int(counters[dwaHdrAcCompressedSize])
	acCount := int(counters[dwaHdrAcUncompressedCount])
	if acCount == 0 {
		return nil, errors.New("chunk has no AC coefficients to recode")
	}

	raw := make([]byte, acCount*2)
	if err := ZIPDecompressTo(raw, chunk[pos+unknownSize:pos+unknownSize+acSize]); err != nil {
		return nil, err
	}
	values := make([]uint16, acCount)
	for i := range values {
		values[i] = binary.LittleEndian.Uint16(raw[i*2:])
	}

	freqs := make([]uint64, hufEncSize)
	for _, v := range values {
		freqs[v]++
	}
	im := 0
	for im < 65536 && freqs[im] == 0 {
		im++
	}
	iM := 65535
	for iM > im && freqs[iM] == 0 {
		iM--
	}
	// The run-length pseudo-symbol sits one past the largest real symbol and
	// must have a code even though nothing here emits a run.
	iM++
	freqs[iM] = 1

	enc := NewHuffmanEncoder(freqs)
	bitstream := enc.Encode(values)
	lengths := enc.GetLengths()
	nBits := 0
	for _, v := range values {
		nBits += lengths[v]
	}

	table := packHufTableRange(nil, lengths, im, iM)
	block := make([]byte, hufBlockHeaderSize)
	binary.LittleEndian.PutUint32(block[0:], uint32(im))
	binary.LittleEndian.PutUint32(block[4:], uint32(iM))
	binary.LittleEndian.PutUint32(block[8:], uint32(len(table)))
	binary.LittleEndian.PutUint32(block[12:], uint32(nBits))
	block = append(block, table...)
	block = append(block, bitstream...)

	out := make([]byte, 0, len(chunk)+len(block))
	out = append(out, chunk[:pos+unknownSize]...)
	out = append(out, block...)
	out = append(out, chunk[pos+unknownSize+acSize:]...)
	binary.LittleEndian.PutUint64(out[dwaHdrAcCompressedSize*8:], uint64(len(block)))
	binary.LittleEndian.PutUint64(out[dwaHdrAcCompression*8:], acCompressionStaticHuffman)
	return out, nil
}

func BenchmarkDwaDecompress(b *testing.B) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 128, 32
	src := dwaTestImage(channels, width, height)
	packed, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45)
	if err != nil {
		b.Fatal(err)
	}
	dst := make([]byte, len(src))
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := DWADecompress(packed, channels, 0, width-1, 0, height-1, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDwaCompress(b *testing.B) {
	channels := dwaTestChannels(DwaPixelTypeFloat)
	const width, height = 128, 32
	src := dwaTestImage(channels, width, height)
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DWACompress(src, channels, 0, width-1, 0, height-1, 45); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDwaDctInverse(b *testing.B) {
	var data [64]float32
	for i := range data {
		data[i] = float32(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dwaDctInverse8x8(&data, 0)
	}
}

func BenchmarkDwaDctForward(b *testing.B) {
	var data [64]float32
	for i := range data {
		data[i] = float32(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dwaDctForward8x8(&data)
	}
}
