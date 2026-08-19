package compression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"math"

	"github.com/mrjoshuak/go-jpeg2000"
)

// HTJ2KDecodeOptions asks for less than the whole chunk.
//
// This is an extension beyond the OpenEXR compressor interface, not a format
// feature. The reference implementation's compressors take no options at all —
// a chunk decompresses whole or not at all, and `dwaCompressionLevel` is the
// format's only quality knob anywhere. Nothing written here changes the file,
// and a file decoded this way is the same file every other reader sees.
//
// What makes it possible is that an HTJ2K chunk is a JPEG 2000 codestream, and
// a codestream keeps its own resolution levels, regions and quality layers
// whatever wraps it. A reader that knows this can pull a viewport, at a chosen
// resolution, out of an ordinary conforming EXR — no mipmaps, no proxy pyramid.
type HTJ2KDecodeOptions struct {
	// ReduceResolution skips this many resolution levels: 0 is full
	// resolution, 1 is half, 2 is quarter. The result is smaller than the
	// chunk's declared size, which is why the partial decoders return the
	// dimensions they produced rather than writing into a caller's buffer.
	ReduceResolution int

	// Region limits the decode to a rectangle, in the chunk's own full
	// resolution coordinates. Nil decodes the whole chunk.
	Region *image.Rectangle

	// QualityLayers decodes only the first N quality layers. 0 decodes all of
	// them. A codestream this package writes has one layer, so this matters
	// for chunks other tools wrote.
	QualityLayers int
}

func (o *HTJ2KDecodeOptions) config() *jpeg2000.Config {
	if o == nil {
		return nil
	}
	return &jpeg2000.Config{
		ReduceResolution: o.ReduceResolution,
		DecodeArea:       o.Region,
		QualityLayers:    o.QualityLayers,
	}
}

// HTJ2KPartialResult is what a partial decode produced: the packed samples in
// the chunk's own interleaved layout, and the dimensions they cover.
//
// Width and Height are the decoded extent, which is the chunk's size reduced by
// ReduceResolution and clipped to Region. BytesPerLine describes the packing,
// so a caller can walk the samples without re-deriving the channel layout.
type HTJ2KPartialResult struct {
	Data          []byte
	Width, Height int
	BytesPerLine  int
}

// HTJ2KDecompressPartial decompresses part of an HTJ2K chunk: a chosen
// resolution, a chosen region, or a prefix of the quality layers.
//
// The channels describe the chunk as it was written, at full resolution; the
// result describes what was actually produced. Passing nil options is the whole
// chunk and is equivalent to HTJ2KDecompress.
//
// This is an extension beyond the reference compressor interface — see
// HTJ2KDecodeOptions. It is the inner half of a byte-range read: File.ChunkRange
// locates the chunk without decompressing it, HTJ2KBuildPacketIndex locates the
// packets inside it, and this turns the bytes into pixels at the resolution
// asked for rather than at the one the chunk was written at.
func HTJ2KDecompressPartial(src []byte, channels []HTJ2KChannelInfo, opts *HTJ2KDecodeOptions) (*HTJ2KPartialResult, error) {
	if len(src) < htj2kHeaderSize {
		return nil, ErrHTJ2KCorrupted
	}
	headerSize, channelMap, err := readHTJ2KHeader(src)
	if err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return nil, errors.New("htj2k: no channels specified")
	}
	if len(channelMap) != len(channels) {
		return nil, fmt.Errorf("htj2k: channel count mismatch: expected %d, got %d",
			len(channels), len(channelMap))
	}
	for _, c := range channelMap {
		if int(c) >= len(channels) {
			return nil, ErrHTJ2KChannelMap
		}
	}

	fullWidth := channels[0].Width
	if err := htj2kValidateChannels(channels, fullWidth); err != nil {
		return nil, err
	}
	allHalf, anyHalf := htj2kAllHalf(channels)
	if anyHalf && !allHalf {
		return nil, errors.New("htj2k: mixed HALF and 32-bit channels not supported")
	}

	codestream := src[headerSize:]
	cfg := opts.config()
	n := len(channels)

	// The packing is derived from the decoded width, not the declared one: a
	// reduced-resolution decode produces fewer samples per line, and packing
	// them at the full stride would interleave the channels wrongly.
	var (
		width, height int
		bytesPerLine  int
		offsets       []int
		out           []byte
	)

	if allHalf {
		img, err := jpeg2000.DecodeHalfConfig(bytes.NewReader(codestream), cfg)
		if err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 half decode failed: %w", err)
		}
		if img.ComponentCount() != n {
			return nil, fmt.Errorf("htj2k: codestream has %d components, the chunk declares %d channels",
				img.ComponentCount(), n)
		}
		width, height = img.Width, img.Height
		offsets, bytesPerLine = htj2kLineLayoutAt(channels, width)
		out = make([]byte, bytesPerLine*height)
		for y := 0; y < height; y++ {
			line := out[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				srcRow := img.Components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					binary.LittleEndian.PutUint16(row[x*2:], srcRow[x])
				}
			}
		}
	} else {
		img, err := jpeg2000.DecodeFloatConfig(bytes.NewReader(codestream), cfg)
		if err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 float decode failed: %w", err)
		}
		if img.ComponentCount() != n {
			return nil, fmt.Errorf("htj2k: codestream has %d components, the chunk declares %d channels",
				img.ComponentCount(), n)
		}
		width, height = img.Width, img.Height
		offsets, bytesPerLine = htj2kLineLayoutAt(channels, width)
		out = make([]byte, bytesPerLine*height)
		for y := 0; y < height; y++ {
			line := out[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				srcRow := img.Components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					binary.LittleEndian.PutUint32(row[x*4:], math.Float32bits(srcRow[x]))
				}
			}
		}
	}

	return &HTJ2KPartialResult{
		Data: out, Width: width, Height: height, BytesPerLine: bytesPerLine,
	}, nil
}

// htj2kLineLayoutAt returns the per-channel byte offsets within one packed line
// and the line's length, for a decode that produced the given width.
//
// htj2kLineLayout derives the same thing from the channels' declared widths,
// which is right for a whole-chunk decode and wrong for a reduced one.
func htj2kLineLayoutAt(channels []HTJ2KChannelInfo, width int) ([]int, int) {
	offsets := make([]int, len(channels))
	pos := 0
	for i, ch := range channels {
		offsets[i] = pos
		size := 4
		if ch.Type == HTJ2KPixelTypeHalf {
			size = 2
		}
		pos += width * size
	}
	return offsets, pos
}
