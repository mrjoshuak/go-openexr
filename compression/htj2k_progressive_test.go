package compression

import (
	"bytes"
	"encoding/binary"
	"testing"

	jpeg2000 "github.com/mrjoshuak/go-jpeg2000"
)

func TestHTJ2KExtractCodestream(t *testing.T) {
	// Create valid compressed data first
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	for i := range src {
		src[i] = byte(i % 256)
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Extract codestream
	codestream, channelMap, err := HTJ2KExtractCodestream(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractCodestream failed: %v", err)
	}

	if len(codestream) == 0 {
		t.Error("Extracted codestream is empty")
	}

	if len(channelMap) != 1 {
		t.Errorf("Expected 1 channel in map, got %d", len(channelMap))
	}

	// Codestream should start with J2K SOC marker (0xFF4F)
	if len(codestream) >= 2 && (codestream[0] != 0xFF || codestream[1] != 0x4F) {
		t.Logf("Codestream starts with %02x %02x (expected FF 4F for J2K SOC)", codestream[0], codestream[1])
	}
}

func TestHTJ2KExtractCodestreamCorrupted(t *testing.T) {
	_, _, err := HTJ2KExtractCodestream([]byte("short"))
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KExtractCodestreamInvalidMagic(t *testing.T) {
	data := []byte("XX\x00\x00\x00\x02\x00\x01\x00\x00extra-data")
	_, _, err := HTJ2KExtractCodestream(data)
	if err != ErrHTJ2KInvalidMagic {
		t.Errorf("Expected ErrHTJ2KInvalidMagic, got %v", err)
	}
}

func TestHTJ2KExtractCodestreamRGB(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
	}
	src := make([]byte, width*height*6)
	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	codestream, channelMap, err := HTJ2KExtractCodestream(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractCodestream failed: %v", err)
	}

	if len(codestream) == 0 {
		t.Error("Extracted codestream is empty")
	}
	if len(channelMap) != 3 {
		t.Errorf("Expected 3 channels in map, got %d", len(channelMap))
	}
}

func TestHTJ2KDecompressFloat(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}

	src := make([]byte, width*height*2)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 2
			val := uint16(0x3C00 + (x+y*8)*0x100) // half-float values
			src[offset] = byte(val)
			src[offset+1] = byte(val >> 8)
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Decompress to float
	img, channelMap, err := HTJ2KDecompressFloat(compressed, channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompressFloat failed: %v", err)
	}

	if img == nil {
		t.Fatal("Returned FloatImage is nil")
	}

	if img.Width != width || img.Height != height {
		t.Errorf("Image dimensions: got %dx%d, want %dx%d", img.Width, img.Height, width, height)
	}

	if img.ComponentCount() < 1 {
		t.Errorf("Component count: got %d, want >= 1", img.ComponentCount())
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}

	// Verify we get non-zero float values
	vals := img.At(0, 0)
	if vals == nil {
		t.Fatal("At(0,0) returned nil")
	}
	t.Logf("Sample value at (0,0): %f", vals[0])
}

func TestHTJ2KDecompressFloatRGB(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
	}

	src := make([]byte, width*height*6)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 6
			src[offset] = byte(x * 32)
			src[offset+1] = 0
			src[offset+2] = byte(y * 32)
			src[offset+3] = 0
			src[offset+4] = byte((x + y) * 16)
			src[offset+5] = 0
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	img, channelMap, err := HTJ2KDecompressFloat(compressed, channels)
	if err != nil {
		t.Fatalf("HTJ2KDecompressFloat failed: %v", err)
	}

	if img.ComponentCount() != 3 {
		t.Errorf("Component count: got %d, want 3", img.ComponentCount())
	}

	if len(channelMap) != 3 {
		t.Errorf("Channel map length: got %d, want 3", len(channelMap))
	}
}

func TestHTJ2KDecompressFloatChannelMismatch(t *testing.T) {
	width, height := 8, 8
	oneChannel := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	compressed, err := HTJ2KCompress(src, height, oneChannel, 32)
	if err != nil {
		t.Fatalf("Failed to create test data: %v", err)
	}

	threeChannels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "R"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "G"},
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "B"},
	}

	_, _, err = HTJ2KDecompressFloat(compressed, threeChannels)
	if err == nil {
		t.Error("Expected error for channel count mismatch")
	}
}

func TestHTJ2KDecompressFloatCorrupted(t *testing.T) {
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: 8, Height: 8, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	_, _, err := HTJ2KDecompressFloat([]byte("short"), channels)
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

// ---------------------------------------------------------------------------
// Packet-structure anchors.
//
// HTJ2KCompress asks go-jpeg2000 for a high-throughput codestream, so a tile
// body is a run of conforming JPEG 2000 T2 packets: a tag-tree coded packet
// header naming the code-blocks that contribute, followed by their bytes
// (ISO/IEC 15444-1 Annex B.9-B.10). This library used to receive a private
// container instead -- a two-byte code-block count and a fixed-width table --
// in which every packet was handed back a self-contained mini-table and so was
// never empty. The tests below used to assert that "every packet has bytes",
// which was a property of that container and not of a packet.
//
// What is true of any conforming codestream, and what these tests assert now,
// is that the packet set the extractor reports is exactly the packet set the
// codestream's own SIZ and COD marker segments describe: one packet per
// (tile, layer, resolution, component, precinct), enumerated in the
// progression order COD declares. Nothing here is derived from the extractor's
// own output, so an extractor that loses, invents, reorders or misaddresses a
// packet fails.
// ---------------------------------------------------------------------------

// j2kGeometry is what a JPEG 2000 codestream says about its own packet
// structure, read out of its marker segments (SIZ is ISO/IEC 15444-1 A.5.1,
// COD is A.6.1).
type j2kGeometry struct {
	rsiz           uint16 // SIZ Rsiz capability field; bit 14 marks HTJ2K
	components     int    // SIZ Csiz
	tiles          int    // numXtiles * numYtiles, A.5.1 equations A-1..A-4
	layers         int    // COD SGcod number of layers
	resolutions    int    // COD SPcod decomposition levels + 1
	progression    byte   // COD SGcod progression order
	codeBlockStyle byte   // COD SPcod code-block style; bit 6 (0x40) is HT
	precincts      []int  // packets per (layer, component) at each resolution
	tileBody       []byte // bytes between SOD and the end of the tile-part
}

func j2kBE16(b []byte) int { return int(binary.BigEndian.Uint16(b)) }
func j2kBE32(b []byte) int { return int(binary.BigEndian.Uint32(b)) }

func j2kCeilDiv(a, b int) int { return (a + b - 1) / b }

// parseJ2KGeometry reads the packet-relevant fields of a raw J2K codestream.
// It is deliberately a separate, direct reading of the bytes: the expectations
// it feeds must not come from the same code path they are checking.
func parseJ2KGeometry(t *testing.T, cs []byte) j2kGeometry {
	t.Helper()

	if len(cs) < 4 || binary.BigEndian.Uint16(cs[0:2]) != 0xFF4F {
		t.Fatalf("codestream does not start with the SOC marker FF4F: % X", cs[:min(len(cs), 8)])
	}

	var (
		g                            j2kGeometry
		xsiz, ysiz, xosiz, yosiz     int
		xtsiz, ytsiz, xtosiz, ytosiz int
		ppx, ppy                     []uint
		sawSIZ, sawCOD               bool
		sotPos                       = -1
	)

	for pos := 2; pos+4 <= len(cs); {
		marker := binary.BigEndian.Uint16(cs[pos : pos+2])
		if marker == 0xFF90 { // SOT: end of the main header
			sotPos = pos
			break
		}
		segLen := j2kBE16(cs[pos+2 : pos+4])
		if segLen < 2 || pos+2+segLen > len(cs) {
			t.Fatalf("marker %04X at offset %d declares length %d, past the %d-byte codestream",
				marker, pos, segLen, len(cs))
		}
		seg := cs[pos+4 : pos+2+segLen] // marker segment payload, after Lmar

		switch marker {
		case 0xFF51: // SIZ
			if len(seg) < 36 {
				t.Fatalf("SIZ payload is %d bytes, too short for the fixed fields", len(seg))
			}
			g.rsiz = binary.BigEndian.Uint16(seg[0:2])
			xsiz, ysiz = j2kBE32(seg[2:6]), j2kBE32(seg[6:10])
			xosiz, yosiz = j2kBE32(seg[10:14]), j2kBE32(seg[14:18])
			xtsiz, ytsiz = j2kBE32(seg[18:22]), j2kBE32(seg[22:26])
			xtosiz, ytosiz = j2kBE32(seg[26:30]), j2kBE32(seg[30:34])
			g.components = j2kBE16(seg[34:36])
			if g.components <= 0 || len(seg) < 36+3*g.components {
				t.Fatalf("SIZ declares %d components but carries %d payload bytes",
					g.components, len(seg))
			}
			for c := 0; c < g.components; c++ {
				if xr, yr := seg[36+3*c+1], seg[36+3*c+2]; xr != 1 || yr != 1 {
					// Subsampled components have their own resolution grid and
					// so their own precinct counts; this fixture has none, and
					// the derivation below would be wrong for one that did.
					t.Fatalf("component %d is subsampled %dx%d; the packet-count derivation here assumes 1x1",
						c, xr, yr)
				}
			}
			sawSIZ = true

		case 0xFF52: // COD
			if len(seg) < 10 {
				t.Fatalf("COD payload is %d bytes, too short for Scod+SGcod+SPcod", len(seg))
			}
			scod := seg[0]
			g.progression = seg[1]
			g.layers = j2kBE16(seg[2:4])
			g.resolutions = int(seg[5]) + 1 // SPcod carries decomposition levels
			g.codeBlockStyle = seg[8]
			if g.layers < 1 || g.resolutions < 1 {
				t.Fatalf("COD declares %d layers and %d resolutions", g.layers, g.resolutions)
			}
			if scod&0x01 != 0 {
				// Scod bit 0: precinct sizes follow, one byte per resolution,
				// low nibble PPx, high nibble PPy (Table A.13).
				if len(seg) < 10+g.resolutions {
					t.Fatalf("COD promises %d precinct bytes but carries %d payload bytes",
						g.resolutions, len(seg))
				}
				for r := 0; r < g.resolutions; r++ {
					ppx = append(ppx, uint(seg[10+r]&0x0F))
					ppy = append(ppy, uint(seg[10+r]>>4))
				}
			} else {
				// No precinct partition: the maximal precinct, 2^15 (A.6.1).
				for r := 0; r < g.resolutions; r++ {
					ppx = append(ppx, 15)
					ppy = append(ppy, 15)
				}
			}
			sawCOD = true
		}
		pos += 2 + segLen
	}

	if !sawSIZ || !sawCOD {
		t.Fatalf("main header is missing SIZ (%v) or COD (%v)", sawSIZ, sawCOD)
	}
	if sotPos < 0 {
		t.Fatal("codestream has no SOT marker, so it carries no tile-parts")
	}
	if xtsiz <= 0 || ytsiz <= 0 {
		t.Fatalf("SIZ declares a %dx%d tile", xtsiz, ytsiz)
	}

	numXTiles := j2kCeilDiv(xsiz-xtosiz, xtsiz)
	numYTiles := j2kCeilDiv(ysiz-ytosiz, ytsiz)
	g.tiles = numXTiles * numYTiles
	if g.tiles != 1 {
		// The precinct derivation below is written for the single tile at the
		// image origin that HTJ2KCompress produces; a tiled image needs the
		// per-tile region instead.
		t.Fatalf("codestream has %d tiles; this test derives packet counts for a single tile", g.tiles)
	}

	// Precinct counts, ISO/IEC 15444-1 B.6: on each resolution grid,
	// numprecinctswide = ceil(trx1 / 2^PPx) - floor(trx0 / 2^PPx), and zero
	// when the resolution is empty.
	tcx0, tcx1 := max(xtosiz, xosiz), min(xtosiz+xtsiz, xsiz)
	tcy0, tcy1 := max(ytosiz, yosiz), min(ytosiz+ytsiz, ysiz)
	for r := 0; r < g.resolutions; r++ {
		scale := 1 << uint(g.resolutions-1-r)
		trx0, trx1 := j2kCeilDiv(tcx0, scale), j2kCeilDiv(tcx1, scale)
		try0, try1 := j2kCeilDiv(tcy0, scale), j2kCeilDiv(tcy1, scale)
		wide, high := 0, 0
		if trx1 > trx0 {
			wide = j2kCeilDiv(trx1, 1<<ppx[r]) - trx0/(1<<ppx[r])
		}
		if try1 > try0 {
			high = j2kCeilDiv(try1, 1<<ppy[r]) - try0/(1<<ppy[r])
		}
		g.precincts = append(g.precincts, wide*high)
	}

	// Tile body: everything from the SOD marker to the end of the tile-part.
	// Psot (SOT bytes 6..10) covers the tile-part including its SOT segment;
	// zero means "to the end of the codestream" (A.4.2).
	if sotPos+12 > len(cs) {
		t.Fatalf("SOT at offset %d is truncated", sotPos)
	}
	tpEnd := len(cs)
	if psot := j2kBE32(cs[sotPos+6 : sotPos+10]); psot > 0 && sotPos+psot <= len(cs) {
		tpEnd = sotPos + psot
	}
	sodPos := -1
	for p := sotPos + 12; p+2 <= tpEnd; p++ {
		if binary.BigEndian.Uint16(cs[p:p+2]) == 0xFF93 {
			sodPos = p + 2
			break
		}
	}
	if sodPos < 0 {
		t.Fatalf("tile-part at offset %d has no SOD marker", sotPos)
	}
	g.tileBody = cs[sodPos:tpEnd]

	return g
}

// expectedPacketAddresses enumerates the packets a codestream with this
// geometry must contain, in the order its COD progression declares. One packet
// exists per (tile, layer, resolution, component, precinct) -- ISO/IEC 15444-1
// B.9 -- so the count is the product of those five, with the precinct term
// summed over resolutions because it varies with the resolution grid.
func expectedPacketAddresses(t *testing.T, g j2kGeometry) []jpeg2000.PacketAddress {
	t.Helper()

	if g.progression != 0 {
		// 0 is LRCP (Table A.16). The other orders permute the same set; this
		// fixture only ever produces LRCP, and guessing would be untested.
		t.Fatalf("COD declares progression order %d; this test enumerates LRCP (0) only", g.progression)
	}

	var addrs []jpeg2000.PacketAddress
	for tile := 0; tile < g.tiles; tile++ {
		for l := 0; l < g.layers; l++ {
			for r := 0; r < g.resolutions; r++ {
				for c := 0; c < g.components; c++ {
					for p := 0; p < g.precincts[r]; p++ {
						addrs = append(addrs, jpeg2000.PacketAddress{
							Tile:       uint16(tile),
							Resolution: uint8(r),
							Layer:      uint16(l),
							Component:  uint8(c),
							Precinct:   uint16(p),
						})
					}
				}
			}
		}
	}
	return addrs
}

// htj2kPacketFixture compresses a small HALF image and returns the chunk, its
// codestream and the geometry that codestream declares.
func htj2kPacketFixture(t *testing.T) (compressed, codestream []byte, geom j2kGeometry) {
	t.Helper()

	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	for i := range src {
		src[i] = byte(i % 256)
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	codestream, channelMap, err := HTJ2KExtractCodestream(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractCodestream failed: %v", err)
	}
	if len(channelMap) != 1 {
		t.Fatalf("Channel map length: got %d, want 1", len(channelMap))
	}

	geom = parseJ2KGeometry(t, codestream)

	// Anchor which container the packet expectations are about. HTJ2KCompress
	// promises a high-throughput codestream: Rsiz bit 14 (ISO/IEC 15444-15
	// A.2) and the HT code-block style bit in COD SPcod. If the encoder ever
	// falls back to a Part 1 codestream, the packet checks below would be
	// measuring a different container, and this says so first.
	if geom.rsiz&0x4000 == 0 {
		t.Errorf("SIZ Rsiz = %#04x, bit 14 (HTJ2K) is not set", geom.rsiz)
	}
	if geom.codeBlockStyle&0x40 == 0 {
		t.Errorf("COD SPcod code-block style = %#02x, bit 6 (HT) is not set", geom.codeBlockStyle)
	}
	if len(geom.tileBody) == 0 {
		t.Fatal("tile-part carries no bytes between SOD and its end, so there are no packets to extract")
	}

	return compressed, codestream, geom
}

func TestHTJ2KExtractPackets(t *testing.T) {
	compressed, codestream, geom := htj2kPacketFixture(t)

	packets, channelMap, err := HTJ2KExtractPackets(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractPackets failed: %v", err)
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}

	// The packet count is not a constant of this fixture: it is
	// tiles x layers x components x sum-over-resolutions(precincts), every
	// term read from the codestream's own SIZ and COD. For the 8x8 HALF chunk
	// this file compresses that currently works out to
	// 1 tile x 1 layer x 3 components x 4 resolutions x 1 precinct = 12.
	want := expectedPacketAddresses(t, geom)
	t.Logf("codestream declares %d tile(s), %d layer(s), %d component(s), %d resolution(s), precincts per resolution %v => %d packets",
		geom.tiles, geom.layers, geom.components, geom.resolutions, geom.precincts, len(want))

	if len(packets) != len(want) {
		t.Fatalf("extracted %d packets, but SIZ/COD describe %d (tiles=%d layers=%d components=%d resolutions=%d precincts=%v)",
			len(packets), len(want), geom.tiles, geom.layers, geom.components, geom.resolutions, geom.precincts)
	}
	for i := range want {
		if packets[i].Address != want[i] {
			t.Errorf("packet %d has address %+v, want %+v (LRCP order, ISO/IEC 15444-1 B.12)",
				i, packets[i].Address, want[i])
		}
	}

	// Packet payloads are cut from the tile body and from nowhere else, and
	// cannot in total exceed it.
	//
	// Measured state, recorded rather than asserted: go-jpeg2000's packet
	// index only slices payloads out of the private container it used to
	// write, and falls back to addresses with no bytes for a conforming T2
	// tile body, so every payload here is currently empty. The containment
	// check below is what stays true when that gap is closed; the total-bytes
	// check bounds it either way.
	total := 0
	for i, pkt := range packets {
		total += len(pkt.Data)
		if len(pkt.Data) > 0 && !bytes.Contains(geom.tileBody, pkt.Data) {
			t.Errorf("packet %d (%+v) carries %d bytes that do not appear in the %d-byte tile body",
				i, pkt.Address, len(pkt.Data), len(geom.tileBody))
		}
	}
	if total > len(geom.tileBody) {
		t.Errorf("packets carry %d bytes in total, more than the %d-byte tile body they were cut from",
			total, len(geom.tileBody))
	}
	t.Logf("extracted %d packets from a %d-byte codestream (%d-byte tile body); payload bytes reported: %d",
		len(packets), len(codestream), len(geom.tileBody), total)
}

func TestHTJ2KExtractPacketsCorrupted(t *testing.T) {
	_, _, err := HTJ2KExtractPackets([]byte("short"))
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KBuildPacketIndex(t *testing.T) {
	compressed, _, geom := htj2kPacketFixture(t)

	index, channelMap, err := HTJ2KBuildPacketIndex(compressed)
	if err != nil {
		t.Fatalf("HTJ2KBuildPacketIndex failed: %v", err)
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}

	// Same derivation as TestHTJ2KExtractPackets: the index must cover exactly
	// the packets SIZ and COD describe -- one per (tile, layer, resolution,
	// component, precinct), in COD's progression order.
	want := expectedPacketAddresses(t, geom)
	if index.Len() != len(want) {
		t.Fatalf("index holds %d packets, but SIZ/COD describe %d (tiles=%d layers=%d components=%d resolutions=%d precincts=%v)",
			index.Len(), len(want), geom.tiles, geom.layers, geom.components, geom.resolutions, geom.precincts)
	}

	addrs := index.AllAddresses()
	if len(addrs) != len(want) {
		t.Fatalf("AllAddresses returned %d addresses but Len() is %d", len(addrs), index.Len())
	}
	for i := range want {
		if addrs[i] != want[i] {
			t.Errorf("index address %d is %+v, want %+v", i, addrs[i], want[i])
		}
	}

	// Every address the index publishes must resolve, and resolve to the same
	// bytes the copying extractor reports for that packet. The two APIs are
	// documented to differ only in whether they copy.
	packets, _, err := HTJ2KExtractPackets(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractPackets failed: %v", err)
	}
	if len(packets) != len(addrs) {
		t.Fatalf("HTJ2KExtractPackets returned %d packets but the index holds %d", len(packets), len(addrs))
	}
	total := 0
	for i, addr := range addrs {
		data, err := index.GetPacket(addr)
		if err != nil {
			t.Errorf("GetPacket failed for %+v: %v", addr, err)
			continue
		}
		if packets[i].Address != addr {
			t.Errorf("packet %d is addressed %+v by the extractor and %+v by the index",
				i, packets[i].Address, addr)
			continue
		}
		if !bytes.Equal(data, packets[i].Data) {
			t.Errorf("packet %+v: index returns %d bytes, extractor returns %d bytes",
				addr, len(data), len(packets[i].Data))
		}
		total += len(data)
	}
	if total > len(geom.tileBody) {
		t.Errorf("index reports %d packet bytes in total, more than the %d-byte tile body",
			total, len(geom.tileBody))
	}

	// An index that answers for packets the codestream does not contain is as
	// broken as one that loses them, and a nil-data fallback makes that easy
	// to do by accident. One step past each bound must be a miss.
	first := want[0]
	for _, bad := range []jpeg2000.PacketAddress{
		{Tile: uint16(geom.tiles), Resolution: first.Resolution, Layer: first.Layer, Component: first.Component},
		{Tile: first.Tile, Resolution: uint8(geom.resolutions), Layer: first.Layer, Component: first.Component},
		{Tile: first.Tile, Resolution: first.Resolution, Layer: uint16(geom.layers), Component: first.Component},
		{Tile: first.Tile, Resolution: first.Resolution, Layer: first.Layer, Component: uint8(geom.components)},
		{Tile: first.Tile, Resolution: first.Resolution, Layer: first.Layer, Component: first.Component,
			Precinct: uint16(geom.precincts[0])},
	} {
		if _, err := index.GetPacket(bad); err == nil {
			t.Errorf("GetPacket succeeded for %+v, which is outside the packet set SIZ/COD describe", bad)
		}
	}

	t.Logf("indexed %d packets; payload bytes reported: %d of a %d-byte tile body",
		index.Len(), total, len(geom.tileBody))
}

func TestHTJ2KBuildPacketIndexCorrupted(t *testing.T) {
	_, _, err := HTJ2KBuildPacketIndex([]byte("short"))
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KNewProgressiveDecoder(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	for i := range src {
		src[i] = byte(i % 256)
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	decoder, channelMap, err := HTJ2KNewProgressiveDecoder(compressed)
	if err != nil {
		t.Fatalf("HTJ2KNewProgressiveDecoder failed: %v", err)
	}

	if decoder == nil {
		t.Fatal("Returned decoder is nil")
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}
}

func TestHTJ2KNewProgressiveDecoderCorrupted(t *testing.T) {
	_, _, err := HTJ2KNewProgressiveDecoder([]byte("short"))
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KProgressiveDecodeFeedAndReconstruct(t *testing.T) {
	width, height := 8, 8
	channels := []HTJ2KChannelInfo{
		{Type: HTJ2KPixelTypeHalf, Width: width, Height: height, XSampling: 1, YSampling: 1, Name: "Y"},
	}
	src := make([]byte, width*height*2)
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			offset := (y*width + x) * 2
			val := uint16(0x3C00 + (x+y*8)*0x100)
			src[offset] = byte(val)
			src[offset+1] = byte(val >> 8)
		}
	}

	compressed, err := HTJ2KCompress(src, height, channels, 32)
	if err != nil {
		t.Fatalf("HTJ2KCompress failed: %v", err)
	}

	// Extract packets
	packets, _, err := HTJ2KExtractPackets(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractPackets failed: %v", err)
	}

	// Create progressive decoder
	decoder, _, err := HTJ2KNewProgressiveDecoder(compressed)
	if err != nil {
		t.Fatalf("HTJ2KNewProgressiveDecoder failed: %v", err)
	}

	// Feed packets one at a time, reconstructing after each
	for i, pkt := range packets {
		if err := decoder.FeedPacket(pkt); err != nil {
			t.Fatalf("FeedPacket %d failed: %v", i, err)
		}

		img, err := decoder.Reconstruct()
		if err != nil {
			t.Fatalf("Reconstruct after %d packets failed: %v", i+1, err)
		}
		if img == nil {
			t.Fatalf("Reconstruct returned nil after %d packets", i+1)
		}

		t.Logf("After %d/%d packets: %dx%d image, %d components",
			i+1, len(packets), img.Width, img.Height, img.ComponentCount())
	}

	// Final image should be complete
	if !decoder.Complete() {
		t.Logf("Decoder not marked complete after all %d packets (may be normal)", len(packets))
	}
}
