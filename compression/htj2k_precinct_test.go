package compression

import (
	"bytes"
	"encoding/binary"
	"image"
	"math"
	"testing"
)

func precinctChunk(t *testing.T, w, h int, opts *HTJ2KEncodeOptions) ([]byte, []HTJ2KChannelInfo) {
	t.Helper()
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: w, Height: h, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, w*h*4)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			v := 0.25 + float32(x)*0.001 + float32(y)*0.002 +
				0.05*float32(math.Sin(float64(x)*0.3)*math.Cos(float64(y)*0.2))
			binary.LittleEndian.PutUint32(src[(y*w+x)*4:], math.Float32bits(v))
		}
	}
	chunk, err := HTJ2KCompressOptions(src, h, channels, 128, opts)
	if err != nil {
		t.Fatalf("HTJ2KCompressOptions: %v", err)
	}
	return chunk, channels
}

// TestHTJ2KDefaultOutputIsUnchangedByTheOptionsType is the invariant that makes
// the opt-in safe to have at all.
//
// This package's bargain is that a chunk it writes is one any other
// implementation would have written. An options struct that shifted the default
// output by a byte would break that for every caller who never asked for
// anything, so the two paths are compared directly rather than assumed equal.
func TestHTJ2KDefaultOutputIsUnchangedByTheOptionsType(t *testing.T) {
	const w, h = 128, 64
	withNil, _ := precinctChunk(t, w, h, nil)
	withZero, _ := precinctChunk(t, w, h, &HTJ2KEncodeOptions{})

	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: w, Height: h, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, w*h*4)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			v := 0.25 + float32(x)*0.001 + float32(y)*0.002 +
				0.05*float32(math.Sin(float64(x)*0.3)*math.Cos(float64(y)*0.2))
			binary.LittleEndian.PutUint32(src[(y*w+x)*4:], math.Float32bits(v))
		}
	}
	legacy, err := HTJ2KCompress(src, h, channels, 128)
	if err != nil {
		t.Fatalf("HTJ2KCompress: %v", err)
	}

	if !bytes.Equal(legacy, withNil) {
		t.Errorf("HTJ2KCompress and HTJ2KCompressOptions(nil) differ: %d vs %d bytes",
			len(legacy), len(withNil))
	}
	if !bytes.Equal(legacy, withZero) {
		t.Errorf("a zero HTJ2KEncodeOptions changed the output: %d vs %d bytes",
			len(legacy), len(withZero))
	}

	// The three comparisons above are all relative, and that is not enough on
	// its own: a change to what "default" means moves all three together and
	// they still agree. The first version of this test did exactly that, and a
	// mutation forcing precincts on by default survived it.
	//
	// So the default is also pinned against something that cannot move with
	// it. Scod bit 0 of the COD marker is the codestream's own statement that
	// it carries a precinct partition (ISO/IEC 15444-1 A.6.1, Table A.13), and
	// the reference writes it clear.
	for name, chunk := range map[string][]byte{
		"HTJ2KCompress":              legacy,
		"HTJ2KCompressOptions(nil)":  withNil,
		"HTJ2KCompressOptions(zero)": withZero,
	} {
		if codDefinesPrecincts(t, chunk) {
			t.Errorf("%s wrote a precinct partition; the default must be what the "+
				"reference implementation writes, which has none", name)
		}
	}

	// The control: when precincts ARE asked for, the same bit must be set, or
	// this check is satisfied by a reader that always answers no.
	fine, _ := precinctChunk(t, w, h, &HTJ2KEncodeOptions{PrecinctSizeLog2: 5})
	if !codDefinesPrecincts(t, fine) {
		t.Error("a chunk written with precincts does not signal them in Scod; " +
			"the check above proves nothing")
	}
}

// codDefinesPrecincts reports whether the chunk's codestream signals a precinct
// partition, by reading Scod bit 0 of the COD marker segment.
func codDefinesPrecincts(t *testing.T, chunk []byte) bool {
	t.Helper()
	cs, _, err := HTJ2KExtractCodestream(chunk)
	if err != nil {
		t.Fatalf("HTJ2KExtractCodestream: %v", err)
	}
	// Walk the main header's marker segments to COD (0xFF52) rather than
	// scanning for the bytes, which could match inside another segment's body.
	for p := 2; p+4 <= len(cs); {
		if cs[p] != 0xFF {
			t.Fatalf("expected a marker at offset %d, got 0x%02x", p, cs[p])
		}
		marker := uint16(cs[p])<<8 | uint16(cs[p+1])
		if marker == 0xFF93 || marker == 0xFF90 { // SOD or SOT: past the main header
			break
		}
		segLen := int(cs[p+2])<<8 | int(cs[p+3])
		if segLen < 2 || p+2+segLen > len(cs) {
			t.Fatalf("marker 0x%04x at %d declares %d bytes", marker, p, segLen)
		}
		if marker == 0xFF52 { // COD
			return cs[p+4]&0x01 != 0
		}
		p += 2 + segLen
	}
	t.Fatal("no COD marker in the codestream")
	return false
}

// TestHTJ2KPrecinctsBuyAddressability is the other half: the opt-in has to be
// worth what it costs, and what it costs is a file the reference would not have
// written.
//
// Without a precinct partition a resolution is one packet covering the whole
// chunk, so the packet index can only ever return all of it however small the
// region. That is the thing being bought, and it is measured rather than
// asserted.
func TestHTJ2KPrecinctsBuyAddressability(t *testing.T) {
	const w, h = 512, 512

	plain, channels := precinctChunk(t, w, h, nil)
	fine, _ := precinctChunk(t, w, h, &HTJ2KEncodeOptions{PrecinctSizeLog2: 5})

	if bytes.Equal(plain, fine) {
		t.Fatal("the precinct request produced an identical chunk; it was ignored")
	}

	// The samples must survive the change. A partition that altered the pixels
	// would not be a trade, it would be a defect.
	wantWhole, err := HTJ2KDecompressPartial(plain, channels, nil)
	if err != nil {
		t.Fatalf("plain decode: %v", err)
	}
	gotWhole, err := HTJ2KDecompressPartial(fine, channels, nil)
	if err != nil {
		t.Fatalf("precinct decode: %v", err)
	}
	if !bytes.Equal(wantWhole.Data, gotWhole.Data) {
		t.Fatal("a precinct-partitioned chunk decoded to different samples")
	}

	// The saving: a region decode skips more.
	region := image.Rect(0, 0, 128, 128)
	plainRegion, err := HTJ2KDecompressPartial(plain, channels, &HTJ2KDecodeOptions{Region: &region})
	if err != nil {
		t.Fatalf("plain region: %v", err)
	}
	fineRegion, err := HTJ2KDecompressPartial(fine, channels, &HTJ2KDecodeOptions{Region: &region})
	if err != nil {
		t.Fatalf("precinct region: %v", err)
	}
	if fineRegion.DecodedBytes >= plainRegion.DecodedBytes {
		t.Errorf("with precincts the region decoded %d code-block bytes, without them %d; "+
			"the partition must reduce it", fineRegion.DecodedBytes, plainRegion.DecodedBytes)
	}
	// And the samples of the region must still be right.
	if fineRegion.Width != region.Dx() || fineRegion.Height != region.Dy() {
		t.Fatalf("precinct region produced %dx%d, want %dx%d",
			fineRegion.Width, fineRegion.Height, region.Dx(), region.Dy())
	}
	if !bytes.Equal(plainRegion.Data, fineRegion.Data) {
		t.Error("the same region decoded differently with and without a precinct partition")
	}

	cost := 100 * float64(len(fine)-len(plain)) / float64(len(plain))
	t.Logf("512x512 chunk: %d bytes plain, %d with 32x32 precincts (%+.2f%%); "+
		"a 128x128 region decodes %d code-block bytes instead of %d",
		len(plain), len(fine), cost, fineRegion.DecodedBytes, plainRegion.DecodedBytes)
}

// TestHTJ2KPrecinctRequestIsValidated keeps the option from appearing to accept
// more than it supports. Without this the checks above are satisfied by a
// function that takes any number and ignores most of them.
func TestHTJ2KPrecinctRequestIsValidated(t *testing.T) {
	const w, h = 64, 64
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeFloat, Width: w, Height: h, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, w*h*4)

	for _, bad := range []int{1, 4, 16, -1} {
		if _, err := HTJ2KCompressOptions(src, h, channels, 128,
			&HTJ2KEncodeOptions{PrecinctSizeLog2: bad}); err == nil {
			t.Errorf("precinct size 2^%d was accepted", bad)
		}
	}
	// The control: a supported size must work, or this is satisfied by a
	// validator that rejects everything.
	if _, err := HTJ2KCompressOptions(src, h, channels, 128,
		&HTJ2KEncodeOptions{PrecinctSizeLog2: 5}); err != nil {
		t.Errorf("precinct size 2^5 was refused: %v", err)
	}
}
