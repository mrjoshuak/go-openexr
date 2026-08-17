package compression

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/mrjoshuak/go-openexr/half"
	"github.com/mrjoshuak/go-openexr/internal/predictor"
)

// dwaDefaultCompressionLevel is the level OpenEXR uses when a file does not
// set dwaCompressionLevel.
const dwaDefaultCompressionLevel = 45.0

// The JPEG quantisation tables DWA derives its per-coefficient error
// tolerances from, in natural (not zig-zag) order, from
// LossyDctEncoder_base_construct. Each is normalised by its own smallest entry,
// so the tolerance for coefficient i is quantBaseError * table[i] / tableMin.
var (
	dwaJpegQuantY = [64]float32{
		16, 11, 10, 16, 24, 40, 51, 61,
		12, 12, 14, 19, 26, 58, 60, 55,
		14, 13, 16, 24, 40, 57, 69, 56,
		14, 17, 22, 29, 51, 87, 80, 62,
		18, 22, 37, 56, 68, 109, 103, 77,
		24, 35, 55, 64, 81, 104, 113, 92,
		49, 64, 78, 87, 103, 121, 120, 101,
		72, 92, 95, 98, 112, 100, 103, 99,
	}
	dwaJpegQuantYMin = float32(10)

	dwaJpegQuantCbCr = [64]float32{
		17, 18, 24, 47, 99, 99, 99, 99,
		18, 21, 26, 66, 99, 99, 99, 99,
		24, 26, 56, 99, 99, 99, 99, 99,
		47, 66, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
		99, 99, 99, 99, 99, 99, 99, 99,
	}
	dwaJpegQuantCbCrMin = float32(17)
)

// dwaQuantize replaces a DCT coefficient with the nearby half value that has
// the fewest bits set, staying within errTol of the original.
//
// Fewer set bits means a shorter Huffman code and longer runs of zeros, which
// is where DWA's compression comes from; the coefficient's exact value is not
// otherwise meaningful. The reference (algoQuantize in internal_dwa_encoder.h)
// does the same search with exponent arithmetic instead of a loop. The two
// need not agree: quantisation is the encoder's free choice, and the decoder
// is told nothing about it. What must agree is the result of decoding, which
// is why the search is bounded by an error tolerance rather than by a table.
func dwaQuantize(src uint16, errTol float32) uint16 {
	if src == 0 {
		return 0
	}
	f := half.FromBits(src).Float32()
	if math.IsNaN(float64(f)) || math.IsInf(float64(f), 0) {
		return src
	}
	// Zero has no bits set at all, so take it whenever it is close enough.
	if abs32(f) <= errTol {
		return 0
	}

	best := src
	bestBits := bits.OnesCount16(src)
	bestErr := float32(0)

	consider := func(cand uint16) {
		c := half.FromBits(cand)
		cf := c.Float32()
		if math.IsNaN(float64(cf)) || math.IsInf(float64(cf), 0) {
			return
		}
		err := abs32(cf - f)
		if err > errTol {
			return
		}
		n := bits.OnesCount16(cand)
		if n < bestBits || (n == bestBits && err < bestErr) {
			best, bestBits, bestErr = cand, n, err
		}
	}

	// Drop low mantissa bits, rounding both down and up. Rounding up can carry
	// into the exponent, which is fine as long as the value stays finite.
	for shift := 1; shift <= 10; shift++ {
		mask := uint16(0xffff) << uint(shift)
		consider(src & mask)
		if up := src + 1<<uint(shift-1); up > src {
			consider(up & mask)
		}
	}
	return best
}

func abs32(f float32) float32 {
	if f < 0 {
		return -f
	}
	return f
}

// dwaRleAc run-length codes a block's 63 AC coefficients, appending to ac.
//
// A run of two or more zeros becomes 0xff00 | runLength, and a run that
// reaches the end of the block becomes 0xff00 exactly. A single zero is
// emitted verbatim, because a run token would cost the same and confuse the
// end-of-block case. From LossyDctEncoder_rleAc.
func dwaRleAc(block *[64]uint16, ac []uint16) []uint16 {
	dctComp := 1
	for dctComp < 64 {
		if block[dctComp] != 0 {
			ac = append(ac, block[dctComp])
			dctComp++
			continue
		}
		runLen := 1
		for dctComp+runLen < 64 && block[dctComp+runLen] == 0 {
			runLen++
		}
		switch {
		case runLen == 1:
			ac = append(ac, block[dctComp])
		case runLen+dctComp == 64:
			ac = append(ac, 0xff00)
		default:
			ac = append(ac, uint16(0xff00|runLen))
		}
		dctComp += runLen
	}
	return ac
}

// dwaChannelHalfPlane reads a channel's samples out of the interleaved input
// as a dense plane of half values. FLOAT channels are clamped into half's
// range first, as the reference does, so that a large float does not become an
// infinity that later rounds to zero.
func dwaChannelHalfPlane(c *dwaChannelState) []uint16 {
	plane := make([]uint16, c.width*c.height)
	for y, row := range c.rows {
		out := plane[y*c.width : (y+1)*c.width]
		switch c.ch.PixelType {
		case DwaPixelTypeHalf:
			for x := range out {
				out[x] = binary.LittleEndian.Uint16(row[x*2:])
			}
		default: // float
			for x := range out {
				f := math.Float32frombits(binary.LittleEndian.Uint32(row[x*4:]))
				if f > 65504 {
					f = 65504
				} else if f < -65504 {
					f = -65504
				}
				out[x] = half.FromFloat32(f).Bits()
			}
		}
	}
	return plane
}

// dwaEncodeLossyDct DCT-codes one channel, or three that share a colour space
// conversion, appending to the AC and DC planes. It mirrors
// LossyDctEncoder_execute.
func dwaEncodeLossyDct(comps []*dwaChannelState, quantBase float32, toNonlinear bool, ac, dc []uint16) ([]uint16, []uint16, error) {
	numComp := len(comps)
	width, height := comps[0].width, comps[0].height
	if width == 0 || height == 0 {
		return ac, dc, nil
	}
	for _, c := range comps {
		if c.ch.PixelType == DwaPixelTypeUint {
			return nil, nil, fmt.Errorf("dwa: channel %q is uint but classified as lossy DCT", c.ch.Name)
		}
	}

	var quantY, quantCbCr [64]float32
	for i := 0; i < 64; i++ {
		quantY[i] = quantBase * dwaJpegQuantY[i] / dwaJpegQuantYMin
		quantCbCr[i] = quantBase * dwaJpegQuantCbCr[i] / dwaJpegQuantCbCrMin
	}

	planes := make([][]uint16, numComp)
	for i, c := range comps {
		planes[i] = dwaChannelHalfPlane(c)
	}

	numBlocksX := (width + 7) / 8
	numBlocksY := (height + 7) / 8

	// DC coefficients are grouped by component so that the ZIP predictor sees
	// one smooth plane per component rather than an interleaving of three.
	dcPlanes := make([][]uint16, numComp)
	for i := range dcPlanes {
		dcPlanes[i] = make([]uint16, 0, numBlocksX*numBlocksY)
	}

	dctData := make([][64]float32, numComp)
	var halfZig [64]uint16

	for blocky := 0; blocky < numBlocksY; blocky++ {
		for blockx := 0; blockx < numBlocksX; blockx++ {
			for comp := 0; comp < numComp; comp++ {
				plane := planes[comp]
				for y := 0; y < 8; y++ {
					vy := 8*blocky + y
					// Blocks that run off the edge are filled by mirroring,
					// which keeps the DCT from seeing a step at the border.
					if vy >= height {
						vy = height - (vy - (height - 1))
					}
					if vy < 0 {
						vy = height - 1
					}
					for x := 0; x < 8; x++ {
						vx := 8*blockx + x
						if vx >= width {
							vx = width - (vx - (width - 1))
						}
						if vx < 0 {
							vx = width - 1
						}
						h := plane[vy*width+vx]
						if toNonlinear {
							h = dwaToNonlinear[h]
						}
						dctData[comp][y*8+x] = half.FromBits(h).Float32()
					}
				}
			}

			if numComp == 3 {
				dwaCsc709Forward64(&dctData[0], &dctData[1], &dctData[2])
			}

			for comp := 0; comp < numComp; comp++ {
				dwaDctForward8x8(&dctData[comp])

				quant := &quantY
				if comp > 0 {
					quant = &quantCbCr
				}
				for i := 0; i < 64; i++ {
					v := half.FromFloat32(dctData[comp][i]).Bits()
					halfZig[dwaInvZigZag[i]] = dwaQuantize(v, quant[i])
				}

				dcPlanes[comp] = append(dcPlanes[comp], halfZig[0])
				ac = dwaRleAc(&halfZig, ac)
			}
		}
	}

	for _, p := range dcPlanes {
		dc = append(dc, p...)
	}
	return ac, dc, nil
}

// DWACompress encodes one DWAA or DWAB chunk from interleaved samples.
//
// The arguments describe the same chunk geometry DWADecompress takes. level is
// the value of the file's dwaCompressionLevel attribute; a negative level
// means the default.
//
// The AC coefficients are deflated rather than Huffman coded. Both are
// permitted by the format and recorded in the chunk header, and the reference
// implementation reads either.
func DWACompress(src []byte, channels []DwaChannel, minX, maxX, minY, maxY int, level float32) ([]byte, error) {
	chans, total, err := dwaBuildChannels(channels, minX, maxX, minY, maxY)
	if err != nil {
		return nil, err
	}
	if total != len(src) {
		return nil, fmt.Errorf("dwa: source is %d bytes, chunk holds %d", len(src), total)
	}
	if level < 0 {
		level = dwaDefaultCompressionLevel
	}

	cscSets := dwaClassify(chans, dwaDefaultRules)
	if err := dwaSetRows(chans, src, minY, maxY); err != nil {
		return nil, err
	}

	numScanLines := maxY - minY + 1
	fullWidth := maxX - minX + 1
	unknownSize, rleSize := dwaPlanarSizes(chans, numScanLines, fullWidth)
	unknownBuf := make([]byte, unknownSize)
	rleBuf := make([]byte, rleSize)
	if err := dwaAssignPlanar(chans, unknownBuf, rleBuf); err != nil {
		return nil, err
	}

	quantBase := level / 100000.0
	var acValues, dcValues []uint16
	unknownUsed, rleRawSize := 0, 0

	processed := make([]bool, len(chans))
	for _, set := range cscSets {
		r, g, b := set.idx[0], set.idx[1], set.idx[2]
		comps := []*dwaChannelState{&chans[r], &chans[g], &chans[b]}
		acValues, dcValues, err = dwaEncodeLossyDct(comps, quantBase, true, acValues, dcValues)
		if err != nil {
			return nil, err
		}
		processed[r], processed[g], processed[b] = true, true, true
	}

	for i := range chans {
		if processed[i] {
			continue
		}
		c := &chans[i]
		switch c.scheme {
		case compressorLossyDCT:
			acValues, dcValues, err = dwaEncodeLossyDct(
				[]*dwaChannelState{c}, quantBase, !c.ch.PLinear, acValues, dcValues)
			if err != nil {
				return nil, err
			}
		case compressorRLE:
			// Split each pixel's bytes into separate planes; adjacent bytes
			// of the same significance run-length code far better.
			offset := 0
			for _, row := range c.rows {
				for x := 0; x < c.width; x++ {
					for b := 0; b < c.bytesPerElement; b++ {
						c.rlePlanes[b][offset+x] = row[x*c.bytesPerElement+b]
					}
				}
				offset += c.width
				rleRawSize += c.width * c.bytesPerElement
			}
		case compressorUnknown:
			scanline := c.width * c.bytesPerElement
			offset := 0
			for _, row := range c.rows {
				copy(c.planar[offset:offset+scanline], row)
				offset += scanline
			}
			unknownUsed += len(c.planar)
		}
	}

	var unknownCompressed, acCompressed, dcCompressed, rleCompressed []byte
	if unknownUsed > 0 {
		unknownCompressed, err = ZIPCompress(unknownBuf[:unknownUsed])
		if err != nil {
			return nil, err
		}
	}
	if len(acValues) > 0 {
		raw := make([]byte, len(acValues)*2)
		for i, v := range acValues {
			binary.LittleEndian.PutUint16(raw[i*2:], v)
		}
		acCompressed, err = ZIPCompress(raw)
		if err != nil {
			return nil, err
		}
	}
	if len(dcValues) > 0 {
		raw := make([]byte, len(dcValues)*2)
		for i, v := range dcValues {
			binary.LittleEndian.PutUint16(raw[i*2:], v)
		}
		scratch := make([]byte, len(raw))
		predictor.DeconstructBytes(scratch, raw)
		dcCompressed, err = ZIPCompress(scratch)
		if err != nil {
			return nil, err
		}
	}
	rleUncompressedSize := 0
	if rleRawSize > 0 {
		packed := RLECompress(rleBuf[:rleRawSize])
		rleUncompressedSize = len(packed)
		rleCompressed, err = ZIPCompress(packed)
		if err != nil {
			return nil, err
		}
	}

	rules := dwaRelevantRules(chans)
	ruleBytes := make([]byte, 2, 64)
	for i := range rules {
		ruleBytes = rules[i].appendTo(ruleBytes)
	}
	if len(ruleBytes) > math.MaxUint16 {
		return nil, fmt.Errorf("dwa: channel rules do not fit in %d bytes", math.MaxUint16)
	}
	binary.LittleEndian.PutUint16(ruleBytes, uint16(len(ruleBytes)))

	out := make([]byte, dwaHeaderSize, dwaHeaderSize+len(ruleBytes)+
		len(unknownCompressed)+len(acCompressed)+len(dcCompressed)+len(rleCompressed))
	counters := [dwaNumSizesSingle]uint64{
		dwaHdrVersion:                 dwaVersion,
		dwaHdrUnknownUncompressedSize: uint64(unknownUsed),
		dwaHdrUnknownCompressedSize:   uint64(len(unknownCompressed)),
		dwaHdrAcCompressedSize:        uint64(len(acCompressed)),
		dwaHdrDcCompressedSize:        uint64(len(dcCompressed)),
		dwaHdrRleCompressedSize:       uint64(len(rleCompressed)),
		dwaHdrRleUncompressedSize:     uint64(rleUncompressedSize),
		dwaHdrRleRawSize:              uint64(rleRawSize),
		dwaHdrAcUncompressedCount:     uint64(len(acValues)),
		dwaHdrDcUncompressedCount:     uint64(len(dcValues)),
		dwaHdrAcCompression:           acCompressionDeflate,
	}
	for i, v := range counters {
		binary.LittleEndian.PutUint64(out[i*8:], v)
	}

	out = append(out, ruleBytes...)
	out = append(out, unknownCompressed...)
	out = append(out, acCompressed...)
	out = append(out, dcCompressed...)
	out = append(out, rleCompressed...)
	return out, nil
}

// dwaRelevantRules returns the classification rules that apply to at least one
// of the chunk's channels; those are the only ones written into the chunk.
// From DwaCompressor_writeRelevantChannelRules.
func dwaRelevantRules(chans []dwaChannelState) []dwaClassifier {
	var out []dwaClassifier
	for i := range dwaDefaultRules {
		for c := range chans {
			suffix := dwaChannelSuffix(chans[c].ch.Name)
			if dwaDefaultRules[i].match(suffix, chans[c].ch.PixelType) {
				out = append(out, dwaDefaultRules[i])
				break
			}
		}
	}
	return out
}
