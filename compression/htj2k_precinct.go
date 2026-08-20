package compression

import (
	"errors"
	"fmt"

	"github.com/mrjoshuak/go-jpeg2000"
)

// HTJ2KEncodeOptions asks for a codestream that differs from the one the
// reference implementation writes.
//
// Everything here is off by default, and the default output stays byte-identical
// to the reference's, because that is the bargain this package makes: a file it
// writes is one any other implementation would have written. What is offered is
// a way to spend that deliberately, with the cost stated, rather than a knob
// that quietly changes what goes on disk.
type HTJ2KEncodeOptions struct {
	// PrecinctSizeLog2 partitions each resolution into precincts of
	// 2^PrecinctSizeLog2 samples on a side, from 5 (32x32) upwards. Zero
	// writes no precinct partition, which is what the reference does.
	//
	// **This changes the bytes.** A precinct-partitioned chunk is not what
	// libOpenEXR's compressor would have produced for the same pixels. It is
	// still a conforming HTJ2K codestream and libOpenEXR 3.4.14 reads one
	// exactly — the gate asserts that — but a reader comparing a file against
	// a reference encode will see a difference, and nothing here can make that
	// untrue.
	//
	// What it buys is addressability inside a chunk. Without a precinct
	// partition a resolution is a single packet covering the whole chunk, so
	// HTJ2KBuildPacketIndex can only ever return all of it: measured on a
	// 1024x1024 chunk, 18 of 18 packets and 100% of the bytes for any region.
	// With 32x32 precincts the same region resolves to 8.5% of the packet
	// bytes, and a region decode's code-block skip rises from 78.5% to 88.2%,
	// for about 4.3% more file.
	PrecinctSizeLog2 int

	// QualityLayers is **refused**, and is here to say so with the measurement
	// rather than leave the idea open.
	//
	// Writing a chunk in several quality layers would let a reader decode a
	// prefix of them and get a lower-bitrate version of the same file — no
	// proxy to generate, store or keep in sync. The mechanism works: decoding
	// a rate-allocated three-layer codestream of half-float content, the first
	// layer alone is 23.5% of the code-block data and its worst error is 0.8%
	// of the true value, with no pixel more than 10% off. Truncation perturbs
	// each coefficient by a bounded amount, and a float's bit pattern is
	// roughly logarithmic in its value, so a bounded error there is a bounded
	// *relative* error in the sample — the right behaviour for HDR, and not at
	// all the reduced-resolution case, which averages bit patterns across
	// discontinuities and fails badly.
	//
	// What stops it is the reference. libOpenEXR's HTJ2K support is OpenJPH,
	// and OpenJPH refuses the file outright:
	//
	//	ojph error 0x00030053: The current implementation supports 1 quality
	//	layer only. This codestream has 4 quality layers
	//
	// So a multi-layer chunk is not a trade like a precinct partition, which
	// the reference reads exactly. It is a file nothing else can open, and
	// this package will not write one silently. Setting it is an error until
	// OpenJPH supports more than one layer.
	QualityLayers int
}

// htj2kPrecinctMin is the smallest precinct this accepts. Below 32x32 the
// partition costs more in packet headers than the addressing is worth, and
// ISO/IEC 15444-1 B.6 forbids a precinct smaller than the code-block anyway.
const htj2kPrecinctMin = 5

func (o *HTJ2KEncodeOptions) validate() error {
	if o == nil {
		return nil
	}
	if o.QualityLayers != 0 {
		return fmt.Errorf("htj2k: quality layers are not supported: libOpenEXR's HTJ2K "+
			"support is OpenJPH, which refuses any codestream with more than one layer "+
			"(\"The current implementation supports 1 quality layer only\"), so a "+
			"%d-layer chunk would be unreadable by the reference implementation",
			o.QualityLayers)
	}
	if o.PrecinctSizeLog2 == 0 {
		return nil
	}
	if o.PrecinctSizeLog2 < htj2kPrecinctMin || o.PrecinctSizeLog2 > 15 {
		return fmt.Errorf("htj2k: precinct size 2^%d is outside the supported range 2^%d to 2^15",
			o.PrecinctSizeLog2, htj2kPrecinctMin)
	}
	return nil
}

// apply returns the encoder options with this request folded in. A nil or zero
// HTJ2KEncodeOptions returns the reference's own parameters untouched, which is
// what keeps the default byte-identical.
func (o *HTJ2KEncodeOptions) apply(opts *jpeg2000.Options) *jpeg2000.Options {
	if o == nil || o.PrecinctSizeLog2 == 0 {
		return opts
	}
	sizes := make([]jpeg2000.PrecinctSize, htj2kNumResolutions)
	for i := range sizes {
		sizes[i] = jpeg2000.PrecinctSize{
			WidthExp: uint8(o.PrecinctSizeLog2), HeightExp: uint8(o.PrecinctSizeLog2),
		}
	}
	opts.PrecinctSizes = sizes
	return opts
}

// HTJ2KCompressOptions compresses a chunk with encoder options.
//
// HTJ2KCompress is this with nil options and stays the way to write a chunk any
// other implementation would have written.
func HTJ2KCompressOptions(src []byte, numLines int, channels []HTJ2KChannelInfo,
	blockWidth int, opts *HTJ2KEncodeOptions) ([]byte, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return nil, errors.New("htj2k: no channels specified")
	}
	return htj2kCompress(src, numLines, channels, blockWidth, opts)
}
