// Package compression provides DWA (DreamWorks Animation) compression for
// OpenEXR.
//
// A DWA chunk splits its channels into three groups, decided by a table of
// classification rules carried in the chunk itself:
//
//   - LOSSY_DCT: 8x8 DCT in a perceptually flat space, with R'G'B' triples
//     first converted to Y'CbCr. Quantised coefficients are split into a DC
//     plane (deflate) and run-length-coded AC coefficients (OpenEXR's static
//     Huffman coder, the same one PIZ uses, or deflate).
//   - RLE: byte-planarised, run-length coded, then deflated. Lossless.
//   - UNKNOWN: planarised and deflated. Lossless.
//
// DWAA and DWAB differ only in chunk height: 32 scanlines against 256.
//
// This implementation is a transcription of OpenEXR 3.4's
// src/lib/OpenEXRCore/internal_dwa_*.h. Decoding must agree with that
// implementation bit for bit where the format is lossless and to within the
// codec's own rounding where it is not, so the transforms, lookup tables and
// buffer layouts follow it rather than being re-derived.
package compression

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"

	"github.com/mrjoshuak/go-openexr/half"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
)

// DWA chunk header fields. Each is a little-endian uint64, in this order, at
// the head of every DWA chunk.
const (
	dwaHdrVersion = iota
	dwaHdrUnknownUncompressedSize
	dwaHdrUnknownCompressedSize
	dwaHdrAcCompressedSize
	dwaHdrDcCompressedSize
	dwaHdrRleCompressedSize
	dwaHdrRleUncompressedSize
	dwaHdrRleRawSize
	dwaHdrAcUncompressedCount
	dwaHdrDcUncompressedCount
	dwaHdrAcCompression

	dwaNumSizesSingle
)

// dwaHeaderSize is the size in bytes of the counter block above.
const dwaHeaderSize = dwaNumSizesSingle * 8

// dwaVersion is the format version this package writes. Version 1 added the
// end-of-block symbol to the AC run-length coding and version 2 added the
// channel classification rules; both are read as well.
const dwaVersion = 2

// AC coefficient compression methods, as recorded in the chunk header.
const (
	acCompressionStaticHuffman = 0
	acCompressionDeflate       = 1
)

// Channel compression schemes.
const (
	compressorUnknown  = 0
	compressorLossyDCT = 1
	compressorRLE      = 2

	numCompressorSchemes = 3
)

// Pixel types, using OpenEXR's exr_pixel_type_t numbering. The classification
// rules in a chunk are keyed on these values, so they are part of the format.
const (
	DwaPixelTypeUint  = 0
	DwaPixelTypeHalf  = 1
	DwaPixelTypeFloat = 2

	dwaNumPixelTypes = 3
)

// DWA errors.
var (
	ErrDwaCorruptData   = errors.New("dwa: corrupt compressed data")
	ErrDwaUnsupported   = errors.New("dwa: unsupported version")
	ErrDwaInvalidHeader = errors.New("dwa: invalid header")
)

// DwaChannel describes one channel of a DWA chunk.
//
// The channels must be given in the order their samples are interleaved in the
// uncompressed chunk, which for an OpenEXR file is the channel list's order:
// sorted by name.
type DwaChannel struct {
	// Name is the full channel name, including any layer prefix. DWA
	// classifies on the part after the last '.', so the prefix matters only
	// for grouping R, G and B of the same layer into a colour-space-converted
	// triple.
	Name string
	// PixelType is one of DwaPixelTypeUint, DwaPixelTypeHalf or
	// DwaPixelTypeFloat.
	PixelType int
	// XSampling and YSampling are the channel's subsampling factors.
	XSampling int
	// YSampling is the vertical subsampling factor.
	YSampling int
	// PLinear reports whether the channel already holds perceptually linear
	// data. When it does, DWA skips the linear/nonlinear lookup.
	PLinear bool
}

// dwaBytesPerElement returns the stored size of one sample.
func dwaBytesPerElement(pixelType int) int {
	if pixelType == DwaPixelTypeHalf {
		return 2
	}
	return 4
}

// dwaClassifier is one channel classification rule. Rules are matched against
// the suffix of a channel name and its pixel type.
type dwaClassifier struct {
	suffix    string
	scheme    int
	pixelType int
	// cscIdx is 0, 1 or 2 for the red, green and blue members of a
	// colour-space-converted triple, and -1 for a channel that stands alone.
	cscIdx          int
	caseInsensitive bool
}

// dwaDefaultRules is the rule set written into every chunk this package
// produces, from sDefaultChannelRules in internal_dwa_classifier.h.
var dwaDefaultRules = []dwaClassifier{
	{"R", compressorLossyDCT, DwaPixelTypeHalf, 0, false},
	{"R", compressorLossyDCT, DwaPixelTypeFloat, 0, false},
	{"G", compressorLossyDCT, DwaPixelTypeHalf, 1, false},
	{"G", compressorLossyDCT, DwaPixelTypeFloat, 1, false},
	{"B", compressorLossyDCT, DwaPixelTypeHalf, 2, false},
	{"B", compressorLossyDCT, DwaPixelTypeFloat, 2, false},
	{"Y", compressorLossyDCT, DwaPixelTypeHalf, -1, false},
	{"Y", compressorLossyDCT, DwaPixelTypeFloat, -1, false},
	{"BY", compressorLossyDCT, DwaPixelTypeHalf, -1, false},
	{"BY", compressorLossyDCT, DwaPixelTypeFloat, -1, false},
	{"RY", compressorLossyDCT, DwaPixelTypeHalf, -1, false},
	{"RY", compressorLossyDCT, DwaPixelTypeFloat, -1, false},
	{"A", compressorRLE, DwaPixelTypeUint, -1, false},
	{"A", compressorRLE, DwaPixelTypeHalf, -1, false},
	{"A", compressorRLE, DwaPixelTypeFloat, -1, false},
}

// dwaLegacyRules is what a version 0 or 1 chunk means, from
// sLegacyChannelRules. Those versions carry no rules of their own.
var dwaLegacyRules = []dwaClassifier{
	{"r", compressorLossyDCT, DwaPixelTypeHalf, 0, true},
	{"r", compressorLossyDCT, DwaPixelTypeFloat, 0, true},
	{"red", compressorLossyDCT, DwaPixelTypeHalf, 0, true},
	{"red", compressorLossyDCT, DwaPixelTypeFloat, 0, true},
	{"g", compressorLossyDCT, DwaPixelTypeHalf, 1, true},
	{"g", compressorLossyDCT, DwaPixelTypeFloat, 1, true},
	{"grn", compressorLossyDCT, DwaPixelTypeHalf, 1, true},
	{"grn", compressorLossyDCT, DwaPixelTypeFloat, 1, true},
	{"green", compressorLossyDCT, DwaPixelTypeHalf, 1, true},
	{"green", compressorLossyDCT, DwaPixelTypeFloat, 1, true},
	{"b", compressorLossyDCT, DwaPixelTypeHalf, 2, true},
	{"b", compressorLossyDCT, DwaPixelTypeFloat, 2, true},
	{"blu", compressorLossyDCT, DwaPixelTypeHalf, 2, true},
	{"blu", compressorLossyDCT, DwaPixelTypeFloat, 2, true},
	{"blue", compressorLossyDCT, DwaPixelTypeHalf, 2, true},
	{"blue", compressorLossyDCT, DwaPixelTypeFloat, 2, true},
	{"y", compressorLossyDCT, DwaPixelTypeHalf, -1, true},
	{"y", compressorLossyDCT, DwaPixelTypeFloat, -1, true},
	{"by", compressorLossyDCT, DwaPixelTypeHalf, -1, true},
	{"by", compressorLossyDCT, DwaPixelTypeFloat, -1, true},
	{"ry", compressorLossyDCT, DwaPixelTypeHalf, -1, true},
	{"ry", compressorLossyDCT, DwaPixelTypeFloat, -1, true},
	{"a", compressorRLE, DwaPixelTypeUint, -1, true},
	{"a", compressorRLE, DwaPixelTypeHalf, -1, true},
	{"a", compressorRLE, DwaPixelTypeFloat, -1, true},
}

// dwaChannelSuffix returns the part of a channel name after the last '.', the
// part DWA's rules are matched against.
func dwaChannelSuffix(name string) string {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '.' {
			return name[i+1:]
		}
	}
	return name
}

// asciiEqualFold reports whether a and b are equal ignoring ASCII case. The
// reference uses strcasecmp, which folds ASCII only; folding more than that
// would classify channels a conforming implementation does not.
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// match reports whether the rule applies to a channel.
func (r *dwaClassifier) match(suffix string, pixelType int) bool {
	if r.pixelType != pixelType {
		return false
	}
	if r.caseInsensitive {
		return asciiEqualFold(suffix, r.suffix)
	}
	return suffix == r.suffix
}

// size returns the number of bytes the rule occupies on disk.
func (r *dwaClassifier) size() int {
	return len(r.suffix) + 1 + 2
}

// appendTo serialises the rule.
func (r *dwaClassifier) appendTo(dst []byte) []byte {
	dst = append(dst, r.suffix...)
	dst = append(dst, 0)
	value := byte(r.cscIdx+1)&15<<4 | byte(r.scheme)&3<<2
	if r.caseInsensitive {
		value |= 1
	}
	return append(dst, value, byte(r.pixelType))
}

// dwaMaxRuleSuffix is the longest channel-name suffix a rule may carry, from
// Classifier_read.
const dwaMaxRuleSuffix = 128

// dwaReadChannelRules parses the rule block that opens a version 2 chunk. It
// returns the rules and the size of the whole block, which includes its own
// two-byte length prefix.
func dwaReadChannelRules(data []byte) ([]dwaClassifier, int, error) {
	if len(data) <= 2 {
		return nil, 0, ErrDwaCorruptData
	}
	ruleSize := int(binary.LittleEndian.Uint16(data))
	if ruleSize < 2 || ruleSize > len(data) {
		return nil, 0, ErrDwaCorruptData
	}

	body := data[2:ruleSize]
	var rules []dwaClassifier
	for len(body) > 0 {
		// A rule is a NUL-terminated suffix followed by two bytes, so fewer
		// than four bytes left cannot hold one.
		if len(body) <= 3 {
			return nil, 0, ErrDwaCorruptData
		}
		end := -1
		for i := 0; i <= len(body)-3 && i < dwaMaxRuleSuffix+1; i++ {
			if body[i] == 0 {
				end = i
				break
			}
		}
		if end < 0 {
			return nil, 0, ErrDwaCorruptData
		}
		suffix := string(body[:end])
		if len(body) < end+3 {
			return nil, 0, ErrDwaCorruptData
		}
		value := body[end+1]
		pixelType := int(body[end+2])
		body = body[end+3:]

		rule := dwaClassifier{
			suffix:          suffix,
			cscIdx:          int(value>>4) - 1,
			scheme:          int(value>>2) & 3,
			caseInsensitive: value&1 != 0,
			pixelType:       pixelType,
		}
		if rule.cscIdx < -1 || rule.cscIdx >= 3 {
			return nil, 0, ErrDwaCorruptData
		}
		if rule.scheme >= numCompressorSchemes {
			return nil, 0, ErrDwaCorruptData
		}
		if rule.pixelType >= dwaNumPixelTypes {
			return nil, 0, ErrDwaCorruptData
		}
		rules = append(rules, rule)
	}
	return rules, ruleSize, nil
}

// dwaChannelState is the decoder's and encoder's working view of one channel.
type dwaChannelState struct {
	ch     DwaChannel
	scheme int

	// width and height are the channel's sample counts within the chunk,
	// after subsampling.
	width, height   int
	bytesPerElement int

	// rows points at each of the channel's scanlines inside the interleaved
	// uncompressed buffer.
	rows [][]byte

	// planar is the channel's slice of the shared UNKNOWN planar buffer.
	planar []byte
	// rlePlanes is the channel's slice of the shared RLE planar buffer, split
	// into one plane per byte of the pixel.
	rlePlanes [][]byte
}

// dwaCscSet names the three channels of one colour-space-converted triple.
type dwaCscSet struct {
	idx [3]int
}

// dwaClassify assigns a compression scheme to every channel and finds the
// R'G'B' triples that share a layer prefix and sampling, following
// DwaCompressor_classifyChannels.
func dwaClassify(chans []dwaChannelState, rules []dwaClassifier) []dwaCscSet {
	type prefixEntry struct {
		prefix string
		idx    [3]int
	}
	// Insertion-ordered, as the reference's linear-probed array is; the order
	// decides the order colour triples consume DCT coefficients.
	var prefixes []prefixEntry
	find := func(prefix string) *prefixEntry {
		for i := range prefixes {
			if prefixes[i].prefix == prefix {
				return &prefixes[i]
			}
		}
		prefixes = append(prefixes, prefixEntry{prefix: prefix, idx: [3]int{-1, -1, -1}})
		return &prefixes[len(prefixes)-1]
	}

	for c := range chans {
		name := chans[c].ch.Name
		suffix := dwaChannelSuffix(name)
		entry := find(name[:len(name)-len(suffix)])
		// Every matching rule is applied, so the last one wins; that is what
		// the reference does and files rely on it.
		for i := range rules {
			if rules[i].match(suffix, chans[c].ch.PixelType) {
				chans[c].scheme = rules[i].scheme
				if rules[i].cscIdx >= 0 {
					entry.idx[rules[i].cscIdx] = c
				}
			}
		}
	}

	var sets []dwaCscSet
	for _, e := range prefixes {
		r, g, b := e.idx[0], e.idx[1], e.idx[2]
		if r < 0 || g < 0 || b < 0 {
			continue
		}
		rc, gc, bc := chans[r].ch, chans[g].ch, chans[b].ch
		if rc.XSampling != gc.XSampling || rc.XSampling != bc.XSampling ||
			rc.YSampling != gc.YSampling || rc.YSampling != bc.YSampling {
			continue
		}
		sets = append(sets, dwaCscSet{idx: [3]int{r, g, b}})
	}
	return sets
}

// dwaBuildChannels computes each channel's geometry within a chunk covering
// x in [minX, maxX] and y in [minY, maxY], and the total size of the
// interleaved uncompressed buffer.
func dwaBuildChannels(channels []DwaChannel, minX, maxX, minY, maxY int) ([]dwaChannelState, int, error) {
	if maxX < minX || maxY < minY {
		return nil, 0, ErrDwaInvalidHeader
	}
	fullWidth := maxX - minX + 1

	chans := make([]dwaChannelState, len(channels))
	total := 0
	for i, ch := range channels {
		if ch.XSampling < 1 || ch.YSampling < 1 {
			return nil, 0, fmt.Errorf("dwa: channel %q has invalid sampling %dx%d",
				ch.Name, ch.XSampling, ch.YSampling)
		}
		if ch.PixelType < 0 || ch.PixelType >= dwaNumPixelTypes {
			return nil, 0, fmt.Errorf("dwa: channel %q has invalid pixel type %d", ch.Name, ch.PixelType)
		}
		height := 0
		for y := minY; y <= maxY; y++ {
			if y%ch.YSampling == 0 {
				height++
			}
		}
		chans[i] = dwaChannelState{
			ch:              ch,
			scheme:          compressorUnknown,
			width:           (fullWidth + ch.XSampling - 1) / ch.XSampling,
			height:          height,
			bytesPerElement: dwaBytesPerElement(ch.PixelType),
		}
		total += chans[i].width * chans[i].height * chans[i].bytesPerElement
	}
	return chans, total, nil
}

// dwaSetRows records where each channel's scanlines live inside the
// interleaved buffer buf.
func dwaSetRows(chans []dwaChannelState, buf []byte, minY, maxY int) error {
	for i := range chans {
		chans[i].rows = make([][]byte, 0, chans[i].height)
	}
	pos := 0
	for y := minY; y <= maxY; y++ {
		for i := range chans {
			c := &chans[i]
			if y%c.ch.YSampling != 0 {
				continue
			}
			n := c.width * c.bytesPerElement
			if pos+n > len(buf) {
				return ErrDwaCorruptData
			}
			c.rows = append(c.rows, buf[pos:pos+n:pos+n])
			pos += n
		}
	}
	return nil
}

// dwaPlanarSizes returns the sizes of the shared planar buffers, matching
// DwaCompressor_initializeBuffers. They bound what a chunk header is allowed
// to claim.
func dwaPlanarSizes(chans []dwaChannelState, numScanLines, fullWidth int) (unknown, rle int) {
	pixelCount := numScanLines * fullWidth
	for i := range chans {
		c := &chans[i]
		switch c.scheme {
		case compressorRLE:
			// RLE can in principle double its input, so the reference
			// reserves twice the pixel count.
			rle += 2 * pixelCount * c.bytesPerElement
		case compressorUnknown:
			unknown += pixelCount * c.bytesPerElement
		}
	}
	return unknown, rle
}

// dwaAssignPlanar hands each non-DCT channel its slice of the shared planar
// buffers, following DwaCompressor_setupChannelData.
func dwaAssignPlanar(chans []dwaChannelState, unknownBuf, rleBuf []byte) error {
	unknownPos, rlePos := 0, 0
	for i := range chans {
		c := &chans[i]
		size := c.width * c.height * c.bytesPerElement
		switch c.scheme {
		case compressorUnknown:
			if unknownPos+size > len(unknownBuf) {
				return ErrDwaCorruptData
			}
			c.planar = unknownBuf[unknownPos : unknownPos+size : unknownPos+size]
			unknownPos += size
		case compressorRLE:
			if rlePos+size > len(rleBuf) {
				return ErrDwaCorruptData
			}
			plane := c.width * c.height
			c.rlePlanes = make([][]byte, c.bytesPerElement)
			for b := 0; b < c.bytesPerElement; b++ {
				off := rlePos + b*plane
				c.rlePlanes[b] = rleBuf[off : off+plane : off+plane]
			}
			rlePos += size
		}
	}
	return nil
}

// DWADecompress decodes one DWAA or DWAB chunk into dst.
//
// channels lists the chunk's channels in interleaved order. minX and maxX are
// the inclusive horizontal bounds of the chunk and minY and maxY its inclusive
// vertical bounds, in image coordinates; the absolute y values matter because
// a channel with YSampling n stores only the rows where y is a multiple of n.
//
// dst must be exactly the size of the interleaved uncompressed chunk.
func DWADecompress(src []byte, channels []DwaChannel, minX, maxX, minY, maxY int, dst []byte) error {
	chans, total, err := dwaBuildChannels(channels, minX, maxX, minY, maxY)
	if err != nil {
		return err
	}
	if total != len(dst) {
		return fmt.Errorf("dwa: destination is %d bytes, chunk holds %d", len(dst), total)
	}
	if len(src) < dwaHeaderSize {
		return ErrDwaCorruptData
	}

	var counters [dwaNumSizesSingle]uint64
	for i := range counters {
		counters[i] = binary.LittleEndian.Uint64(src[i*8:])
	}
	// The reference rejects any counter that would be negative as a signed
	// 64-bit value; on a 64-bit platform that is the same as rejecting
	// anything that will not fit an int, and it is what keeps the arithmetic
	// below from wrapping.
	for _, v := range counters {
		if v > math.MaxInt32 {
			return ErrDwaCorruptData
		}
	}

	version := int(counters[dwaHdrVersion])
	if version > dwaVersion {
		return fmt.Errorf("%w: version %d", ErrDwaUnsupported, version)
	}
	unknownUncompressedSize := int(counters[dwaHdrUnknownUncompressedSize])
	unknownCompressedSize := int(counters[dwaHdrUnknownCompressedSize])
	acCompressedSize := int(counters[dwaHdrAcCompressedSize])
	dcCompressedSize := int(counters[dwaHdrDcCompressedSize])
	rleCompressedSize := int(counters[dwaHdrRleCompressedSize])
	rleUncompressedSize := int(counters[dwaHdrRleUncompressedSize])
	rleRawSize := int(counters[dwaHdrRleRawSize])
	acCount := int(counters[dwaHdrAcUncompressedCount])
	dcCount := int(counters[dwaHdrDcUncompressedCount])
	acCompression := int(counters[dwaHdrAcCompression])

	// A conforming reader starts from a zeroed output buffer; a channel that
	// the chunk does not describe must not be left holding whatever the
	// caller's buffer had in it.
	for i := range dst {
		dst[i] = 0
	}

	pos := dwaHeaderSize
	rules := dwaLegacyRules
	if version >= 2 {
		parsed, ruleBytes, err := dwaReadChannelRules(src[pos:])
		if err != nil {
			return err
		}
		rules = parsed
		pos += ruleBytes
	}

	// Locate the four payloads. Each bound is checked on its own so that a
	// size that overflows the sum cannot slip past.
	sections := [...]int{unknownCompressedSize, acCompressedSize, dcCompressedSize, rleCompressedSize}
	var bufs [4][]byte
	for i, n := range sections {
		if n > len(src)-pos {
			return ErrDwaCorruptData
		}
		bufs[i] = src[pos : pos+n]
		pos += n
	}
	compressedUnknown, compressedAc, compressedDc, compressedRle := bufs[0], bufs[1], bufs[2], bufs[3]

	cscSets := dwaClassify(chans, rules)

	numScanLines := maxY - minY + 1
	fullWidth := maxX - minX + 1
	unknownSize, rleSize := dwaPlanarSizes(chans, numScanLines, fullWidth)

	numBlocks := ((numScanLines + 7) / 8) * ((fullWidth + 7) / 8)
	numLossyDct := 0
	for i := range chans {
		if chans[i].scheme == compressorLossyDCT {
			numLossyDct++
		}
	}
	maxAcCount := numBlocks * 63 * numLossyDct
	maxDcCount := numBlocks * numLossyDct

	if err := dwaSetRows(chans, dst, minY, maxY); err != nil {
		return err
	}

	// UNKNOWN: deflated planar samples.
	unknownBuf := make([]byte, unknownSize)
	if unknownCompressedSize > 0 {
		if unknownUncompressedSize > unknownSize {
			return ErrDwaCorruptData
		}
		if err := ZIPDecompressTo(unknownBuf[:unknownUncompressedSize], compressedUnknown); err != nil {
			return ErrDwaCorruptData
		}
	}

	// AC: run-length coded DCT coefficients, Huffman or deflate coded.
	var acValues []uint16
	if acCompressedSize > 0 {
		if acCount > maxAcCount {
			return ErrDwaCorruptData
		}
		acValues = make([]uint16, acCount)
		switch acCompression {
		case acCompressionStaticHuffman:
			if err := hufDecompressInto(compressedAc, acValues); err != nil {
				return fmt.Errorf("dwa: AC coefficients: %w", err)
			}
		case acCompressionDeflate:
			raw := make([]byte, acCount*2)
			if err := ZIPDecompressTo(raw, compressedAc); err != nil {
				return ErrDwaCorruptData
			}
			for i := range acValues {
				acValues[i] = binary.LittleEndian.Uint16(raw[i*2:])
			}
		default:
			return fmt.Errorf("dwa: unknown AC compression method %d", acCompression)
		}
	} else if acCount != 0 {
		return ErrDwaCorruptData
	}

	// DC: one coefficient per block, deflated after the ZIP byte reorder and
	// predictor.
	var dcValues []uint16
	if dcCompressedSize > 0 {
		if dcCount > maxDcCount {
			return ErrDwaCorruptData
		}
		scratch := make([]byte, dcCount*2)
		if err := ZIPDecompressTo(scratch, compressedDc); err != nil {
			return ErrDwaCorruptData
		}
		raw := make([]byte, dcCount*2)
		predictor.ReconstructBytes(raw, scratch)
		dcValues = make([]uint16, dcCount)
		for i := range dcValues {
			dcValues[i] = binary.LittleEndian.Uint16(raw[i*2:])
		}
	} else if dcCount != 0 {
		return ErrDwaCorruptData
	}

	// RLE: deflated run-length coded byte planes.
	rleBuf := make([]byte, rleSize)
	if rleRawSize > 0 {
		if rleUncompressedSize > rleSize || rleRawSize > rleSize {
			return ErrDwaCorruptData
		}
		packed := make([]byte, rleUncompressedSize)
		if err := ZIPDecompressTo(packed, compressedRle); err != nil {
			return ErrDwaCorruptData
		}
		if err := RLEDecompressTo(packed, rleBuf[:rleRawSize]); err != nil {
			return ErrDwaCorruptData
		}
	}

	if err := dwaAssignPlanar(chans, unknownBuf, rleBuf); err != nil {
		return err
	}

	processed := make([]bool, len(chans))
	acPos, dcPos := 0, 0

	// Colour triples decode together, and consume their coefficients first.
	for _, set := range cscSets {
		r, g, b := set.idx[0], set.idx[1], set.idx[2]
		if chans[r].scheme != compressorLossyDCT ||
			chans[g].scheme != compressorLossyDCT ||
			chans[b].scheme != compressorLossyDCT {
			return ErrDwaCorruptData
		}
		comps := []*dwaChannelState{&chans[r], &chans[g], &chans[b]}
		if err := dwaDecodeLossyDct(comps, acValues, &acPos, dcValues, &dcPos, true); err != nil {
			return err
		}
		processed[r], processed[g], processed[b] = true, true, true
	}

	for i := range chans {
		if processed[i] {
			continue
		}
		c := &chans[i]
		if c.width == 0 || c.height == 0 {
			continue
		}
		switch c.scheme {
		case compressorLossyDCT:
			comps := []*dwaChannelState{c}
			if err := dwaDecodeLossyDct(comps, acValues, &acPos, dcValues, &dcPos, !c.ch.PLinear); err != nil {
				return err
			}
		case compressorRLE:
			// The samples arrive split into byte planes; put them back
			// together in the output buffer.
			offset := 0
			for _, row := range c.rows {
				for x := 0; x < c.width; x++ {
					for b := 0; b < c.bytesPerElement; b++ {
						row[x*c.bytesPerElement+b] = c.rlePlanes[b][offset+x]
					}
				}
				offset += c.width
			}
		case compressorUnknown:
			scanline := c.width * c.bytesPerElement
			offset := 0
			for _, row := range c.rows {
				if offset+scanline > len(c.planar) {
					return ErrDwaCorruptData
				}
				copy(row, c.planar[offset:offset+scanline])
				offset += scanline
			}
		default:
			return ErrDwaCorruptData
		}
	}

	return nil
}

// dwaUnRleAc expands one block's run-length coded AC coefficients into
// halfZig, which must arrive zeroed. It returns the zig-zag index of the last
// non-zero coefficient, which is 0 when the block has none.
//
// A value whose high byte is 0xff is a run of zeros of the length given by its
// low byte; a low byte of 0 means "zero to the end of the block".
func dwaUnRleAc(ac []uint16, pos *int, halfZig *[64]uint16) (int, error) {
	dctComp := 1
	lastNonZero := 0
	p := *pos
	for dctComp < 64 {
		if p >= len(ac) {
			return 0, ErrDwaCorruptData
		}
		val := ac[p]
		p++
		if val&0xff00 == 0xff00 {
			count := int(val & 0xff)
			if count == 0 {
				dctComp += 64
			} else {
				dctComp += count
			}
			continue
		}
		lastNonZero = dctComp
		halfZig[dctComp] = val
		dctComp++
	}
	*pos = p
	return lastNonZero, nil
}

// dwaZeroedRows maps the last non-zero zig-zag index to the number of trailing
// rows of the un-zig-zagged block that must be zero, so the inverse DCT can
// skip them. From the unrolled ladder in LossyDctDecoder_execute.
func dwaZeroedRows(lastNonZero int) int {
	switch {
	case lastNonZero < 2:
		return 7
	case lastNonZero < 3:
		return 6
	case lastNonZero < 9:
		return 5
	case lastNonZero < 10:
		return 4
	case lastNonZero < 20:
		return 3
	case lastNonZero < 21:
		return 2
	case lastNonZero < 35:
		return 1
	default:
		return 0
	}
}

// dwaDecodeLossyDct decodes one or three DCT-coded channels, consuming AC and
// DC coefficients from the shared planes. toLinear says whether the decoded
// samples still need the nonlinear-to-linear lookup applied.
//
// This is LossyDctDecoder_execute. All the comps share one geometry: three
// channels only ever decode together when their sampling matches.
func dwaDecodeLossyDct(comps []*dwaChannelState, ac []uint16, acPos *int, dc []uint16, dcPos *int, toLinear bool) error {
	numComp := len(comps)
	width, height := comps[0].width, comps[0].height
	if width == 0 || height == 0 {
		return nil
	}
	for _, c := range comps {
		if c.ch.PixelType == DwaPixelTypeUint {
			return fmt.Errorf("dwa: channel %q is uint but classified as lossy DCT", c.ch.Name)
		}
		if len(c.rows) < height {
			return ErrDwaCorruptData
		}
	}

	numBlocksX := (width + 7) / 8
	numBlocksY := (height + 7) / 8
	leftoverX := width - (numBlocksX-1)*8
	leftoverY := height - (numBlocksY-1)*8
	numFullBlocksX := width / 8

	// DC coefficients are stored one plane per component, so each component's
	// plane can be predicted from its own neighbours.
	needDc := numComp * numBlocksX * numBlocksY
	if len(dc)-*dcPos < needDc {
		return ErrDwaCorruptData
	}
	dcBase := *dcPos
	*dcPos += needDc

	rowBlock := make([][]uint16, numComp)
	for i := range rowBlock {
		rowBlock[i] = make([]uint16, numBlocksX*64)
	}

	var halfZig [64]uint16
	dctData := make([][64]float32, numComp)

	for blocky := 0; blocky < numBlocksY; blocky++ {
		maxY, maxX := 8, 8
		if blocky == numBlocksY-1 {
			maxY = leftoverY
		}

		for blockx := 0; blockx < numBlocksX; blockx++ {
			blockIsConstant := true
			if blockx == numBlocksX-1 {
				maxX = leftoverX
			}

			for comp := 0; comp < numComp; comp++ {
				halfZig = [64]uint16{}
				halfZig[0] = dc[dcBase+comp*numBlocksX*numBlocksY+blocky*numBlocksX+blockx]

				lastNonZero, err := dwaUnRleAc(ac, acPos, &halfZig)
				if err != nil {
					return err
				}

				if lastNonZero == 0 {
					dctData[comp][0] = half.FromBits(halfZig[0]).Float32()
					dwaDctInverse8x8DcOnly(&dctData[comp])
					continue
				}
				blockIsConstant = false
				for i := 0; i < 64; i++ {
					dctData[comp][i] = half.FromBits(halfZig[dwaInvZigZag[i]]).Float32()
				}
				dwaDctInverse8x8(&dctData[comp], dwaZeroedRows(lastNonZero))
			}

			if numComp == 3 {
				if blockIsConstant {
					// Every sample of the block is the same, so converting
					// the first one is enough.
					dwaCsc709Inverse(&dctData[0][0], &dctData[1][0], &dctData[2][0])
				} else {
					dwaCsc709Inverse64(&dctData[0], &dctData[1], &dctData[2])
				}
			}

			for comp := 0; comp < numComp; comp++ {
				out := rowBlock[comp][blockx*64 : blockx*64+64]
				if blockIsConstant {
					v := half.FromFloat32(dctData[comp][0]).Bits()
					for i := range out {
						out[i] = v
					}
					continue
				}
				for i := 0; i < 64; i++ {
					out[i] = half.FromFloat32(dctData[comp][i]).Bits()
				}
			}
		}

		// Unblock this row of blocks into the output scanlines.
		// Samples are always written as half here, packed two bytes apart
		// even in a FLOAT channel's four-byte-per-sample row; the loop below
		// expands those rows afterwards.
		for comp := 0; comp < numComp; comp++ {
			c := comps[comp]
			for y := 8 * blocky; y < 8*blocky+maxY; y++ {
				row := c.rows[y]
				src := rowBlock[comp][(y&7)*8:]
				for bx := 0; bx < numFullBlocksX; bx++ {
					s := src[bx*64 : bx*64+8]
					for x := 0; x < 8; x++ {
						v := s[x]
						if toLinear {
							v = dwaToLinear[v]
						}
						binary.LittleEndian.PutUint16(row[(bx*8+x)*2:], v)
					}
				}
				if numFullBlocksX != numBlocksX {
					s := src[numFullBlocksX*64:]
					for x := 0; x < maxX; x++ {
						v := s[x]
						if toLinear {
							v = dwaToLinear[v]
						}
						binary.LittleEndian.PutUint16(row[(numFullBlocksX*8+x)*2:], v)
					}
				}
			}
		}
	}

	// FLOAT channels were decoded as half into the low half of each sample;
	// expand them in place, back to front so the two never overlap.
	for _, c := range comps {
		if c.ch.PixelType != DwaPixelTypeFloat {
			continue
		}
		for _, row := range c.rows {
			for x := c.width - 1; x >= 0; x-- {
				v := binary.LittleEndian.Uint16(row[x*2:])
				f := half.FromBits(v).Float32()
				binary.LittleEndian.PutUint32(row[x*4:], math.Float32bits(f))
			}
		}
	}

	return nil
}
