// Package compression provides compression algorithms for OpenEXR files.
package compression

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"image"
	"image/color"
	"io"
	"math"

	"github.com/mrjoshuak/go-jpeg2000"
)

// HTJ2K compression errors
var (
	ErrHTJ2KCorrupted    = errors.New("compression: corrupted HTJ2K data")
	ErrHTJ2KInvalidMagic = errors.New("compression: invalid HTJ2K magic number")
	ErrHTJ2KChannelMap   = errors.New("compression: invalid HTJ2K channel map")
)

// HTJ2K chunk header constants
const (
	htj2kMagic      uint16 = 0x4854 // "HT" in big-endian
	htj2kHeaderSize int    = 6      // Magic (2) + PLEN (4)
)

// HTJ2KChannelInfo describes a channel for HTJ2K compression
type HTJ2KChannelInfo struct {
	Type      int // 0=UINT, 1=HALF, 2=FLOAT
	Width     int
	Height    int
	XSampling int
	YSampling int
	Name      string // Channel name for RGB detection
}

// HTJ2K pixel type constants (matching OpenEXR)
const (
	HTJ2KPixelTypeUint  = 0
	HTJ2KPixelTypeHalf  = 1
	HTJ2KPixelTypeFloat = 2
)

// writeHTJ2KHeader writes the OpenEXR HTJ2K chunk header
func writeHTJ2KHeader(w io.Writer, channelMap []uint16) error {
	// Calculate payload size: 2 bytes for count + 2 bytes per channel
	payloadLen := uint32(2 + len(channelMap)*2)

	// Write magic number (big-endian)
	if err := binary.Write(w, binary.BigEndian, htj2kMagic); err != nil {
		return err
	}

	// Write payload length (big-endian)
	if err := binary.Write(w, binary.BigEndian, payloadLen); err != nil {
		return err
	}

	// Write channel count (big-endian)
	if err := binary.Write(w, binary.BigEndian, uint16(len(channelMap))); err != nil {
		return err
	}

	// Write channel map (big-endian)
	for _, ch := range channelMap {
		if err := binary.Write(w, binary.BigEndian, ch); err != nil {
			return err
		}
	}

	return nil
}

// readHTJ2KHeader reads and validates the OpenEXR HTJ2K chunk header
func readHTJ2KHeader(data []byte) (headerSize int, channelMap []uint16, err error) {
	if len(data) < htj2kHeaderSize {
		return 0, nil, ErrHTJ2KCorrupted
	}

	// Read magic number (big-endian)
	magic := binary.BigEndian.Uint16(data[0:2])
	if magic != htj2kMagic {
		return 0, nil, ErrHTJ2KInvalidMagic
	}

	// Read payload length (big-endian)
	payloadLen := binary.BigEndian.Uint32(data[2:6])
	if int(payloadLen) > len(data)-htj2kHeaderSize {
		return 0, nil, ErrHTJ2KCorrupted
	}

	// Read channel count (big-endian)
	if payloadLen < 2 {
		return 0, nil, ErrHTJ2KChannelMap
	}
	channelCount := binary.BigEndian.Uint16(data[6:8])

	// Validate payload size matches channel count
	expectedPayload := 2 + int(channelCount)*2
	if int(payloadLen) < expectedPayload {
		return 0, nil, ErrHTJ2KChannelMap
	}

	// Read channel map
	channelMap = make([]uint16, channelCount)
	offset := 8
	for i := 0; i < int(channelCount); i++ {
		channelMap[i] = binary.BigEndian.Uint16(data[offset : offset+2])
		offset += 2
	}

	headerSize = htj2kHeaderSize + int(payloadLen)
	return headerSize, channelMap, nil
}

// makeChannelMap creates a channel map, detecting RGB channels for RCT optimization
// Returns the channel map and whether RGB was detected
func makeChannelMap(channels []HTJ2KChannelInfo) ([]uint16, bool) {
	n := len(channels)
	channelMap := make([]uint16, n)

	// Default: identity mapping
	for i := 0; i < n; i++ {
		channelMap[i] = uint16(i)
	}

	// Detect RGB channels for RCT optimization
	rIdx, gIdx, bIdx := -1, -1, -1

	for i, ch := range channels {
		name := ch.Name
		// Extract suffix after last '.'
		suffix := name
		if idx := lastIndexByte(name, '.'); idx >= 0 {
			suffix = name[idx+1:]
		}

		// Check for R/G/B or red/green/blue (case-insensitive)
		switch toLower(suffix) {
		case "r", "red":
			if rIdx < 0 {
				rIdx = i
			}
		case "g", "green":
			if gIdx < 0 {
				gIdx = i
			}
		case "b", "blue":
			if bIdx < 0 {
				bIdx = i
			}
		}
	}

	// Check if we found a valid RGB triplet with matching types/sampling
	isRGB := rIdx >= 0 && gIdx >= 0 && bIdx >= 0
	if isRGB {
		r, g, b := channels[rIdx], channels[gIdx], channels[bIdx]
		isRGB = r.Type == g.Type && r.Type == b.Type &&
			r.XSampling == g.XSampling && r.XSampling == b.XSampling &&
			r.YSampling == g.YSampling && r.YSampling == b.YSampling
	}

	if isRGB {
		// Reorder: R=0, G=1, B=2, then remaining channels
		channelMap[0] = uint16(rIdx)
		channelMap[1] = uint16(gIdx)
		channelMap[2] = uint16(bIdx)

		nextIdx := 3
		for i := 0; i < n; i++ {
			if i != rIdx && i != gIdx && i != bIdx {
				channelMap[nextIdx] = uint16(i)
				nextIdx++
			}
		}
	}

	return channelMap, isRGB
}

// Helper functions
func lastIndexByte(s string, c byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == c {
			return i
		}
	}
	return -1
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

// exrImage wraps OpenEXR raw pixel data as an image.Image for JPEG 2000 encoding
type exrImage struct {
	width, height int
	channels      []HTJ2KChannelInfo
	data          []byte // Interleaved scanline data
	bytesPerPixel int
	channelMap    []uint16 // Maps J2K component to data channel
	isRGB         bool
}

// newEXRImage creates an image wrapper for EXR data
func newEXRImage(width, height int, channels []HTJ2KChannelInfo, data []byte) *exrImage {
	bytesPerPixel := 0
	for _, ch := range channels {
		switch ch.Type {
		case HTJ2KPixelTypeHalf:
			bytesPerPixel += 2
		case HTJ2KPixelTypeUint, HTJ2KPixelTypeFloat:
			bytesPerPixel += 4
		}
	}

	channelMap, isRGB := makeChannelMap(channels)

	return &exrImage{
		width:         width,
		height:        height,
		channels:      channels,
		data:          data,
		bytesPerPixel: bytesPerPixel,
		channelMap:    channelMap,
		isRGB:         isRGB,
	}
}

func (img *exrImage) ColorModel() color.Model {
	return color.Gray16Model // We use 16-bit per component
}

func (img *exrImage) Bounds() image.Rectangle {
	return image.Rect(0, 0, img.width, img.height)
}

func (img *exrImage) At(x, y int) color.Color {
	// This is used for general image access but JPEG 2000 encoder
	// should use the component-based access
	if len(img.channels) == 1 {
		// Single channel: return as gray
		offset := (y*img.width + x) * img.bytesPerPixel
		if img.channels[0].Type == HTJ2KPixelTypeHalf {
			v := binary.LittleEndian.Uint16(img.data[offset:])
			return color.Gray16{Y: v}
		}
	}
	return color.Gray16{Y: 0}
}

// Sample geometry of an OpenEXR chunk.
//
// A compressed scanline chunk holds its pixels the way OpenEXR packs them: for
// each scanline in the chunk, for each channel in name-sorted order, that
// channel's whole row. It is *not* pixel-interleaved. The reference codec calls
// each channel's position within a line its raster_line_offset
// (internal_ht_common.cpp), and both its encoder and its decoder index the
// packed buffer as line_pixels + raster_line_offset, advancing line_pixels by
// one line's worth of bytes per scanline (internal_ht.cpp).
//
// Reading this buffer as though it were pixel-interleaved transposes every
// sample, which no round trip can see because the same transposition is applied
// on the way back out.

// htj2kSampleSize returns the byte width of one sample of the given HTJ2K
// pixel type.
func htj2kSampleSize(pixelType int) int {
	if pixelType == HTJ2KPixelTypeHalf {
		return 2
	}
	return 4
}

// htj2kLineLayout returns each channel's byte offset within one packed
// scanline, and the total number of bytes a scanline occupies. Channels are
// given in EXR (name-sorted) order, which is the order they are packed in.
func htj2kLineLayout(channels []HTJ2KChannelInfo) (offsets []int, bytesPerLine int) {
	offsets = make([]int, len(channels))
	for i, ch := range channels {
		offsets[i] = bytesPerLine
		bytesPerLine += ch.Width * htj2kSampleSize(ch.Type)
	}
	return offsets, bytesPerLine
}

// htj2kValidateChannels rejects the channel configurations this codec cannot
// represent, rather than encoding them wrongly.
func htj2kValidateChannels(channels []HTJ2KChannelInfo, width int) error {
	if len(channels) == 0 {
		return errors.New("htj2k: no channels specified")
	}
	for _, ch := range channels {
		if ch.XSampling != 1 || ch.YSampling != 1 {
			// The reference switches OpenJPH to planar mode for subsampled
			// channels; go-jpeg2000 has no planar entry point, so a subsampled
			// channel would silently be written at the wrong size.
			return fmt.Errorf("htj2k: channel %q is subsampled (%dx%d); only 1x1 sampling is supported",
				ch.Name, ch.XSampling, ch.YSampling)
		}
		if ch.Width != width {
			return fmt.Errorf("htj2k: channel %q is %d wide, but channel %q is %d wide",
				ch.Name, ch.Width, channels[0].Name, width)
		}
	}
	return nil
}

// htj2kAllHalf reports whether every channel is HALF. HALF samples are 16 bits
// wide and UINT and FLOAT samples are 32; go-jpeg2000 encodes a whole image at
// one sample width, so the two cannot be mixed in one codestream.
func htj2kAllHalf(channels []HTJ2KChannelInfo) (allHalf bool, anyHalf bool) {
	allHalf = true
	for _, ch := range channels {
		if ch.Type == HTJ2KPixelTypeHalf {
			anyHalf = true
		} else {
			allHalf = false
		}
	}
	return allHalf, anyHalf
}

// htj2kWrap prepends the OpenEXR HTJ2K chunk header to a codestream.
func htj2kWrap(channelMap []uint16, codestream []byte) ([]byte, error) {
	var headerBuf bytes.Buffer
	if err := writeHTJ2KHeader(&headerBuf, channelMap); err != nil {
		return nil, err
	}
	output := make([]byte, 0, headerBuf.Len()+len(codestream))
	output = append(output, headerBuf.Bytes()...)
	output = append(output, codestream...)
	return output, nil
}

// htj2kOptions returns the encoder options for an OpenEXR HTJ2K chunk.
//
// These mirror what the reference codec asks OpenJPH for in internal_ht.cpp:
// reversible (lossless) coding, five decomposition levels, and 128x32
// code-blocks for both HTJ2K codecs — the 256 and 32 in the codec names are
// the scanline grouping, not the code-block size. HighThroughput is what makes
// the codestream HTJ2K rather than baseline Part 1: it selects the FBCS block
// coder, writes the CAP marker and sets Rsiz bit 14, without which OpenJPH
// refuses the file with "Rsiz bit 14 is not set (this is not a JPH file)".
func htj2kOptions(blockWidth int) *jpeg2000.Options {
	if blockWidth <= 0 {
		blockWidth = htj2kCodeBlockWidth
	}
	return &jpeg2000.Options{
		Format:         jpeg2000.FormatJ2K, // raw codestream, no JP2 wrapper
		Lossless:       true,
		HighThroughput: true,
		HTBlockWidth:   blockWidth,
		HTBlockHeight:  htj2kCodeBlockHeight,
		NumResolutions: htj2kNumResolutions,
	}
}

// Codestream parameters the reference codec uses for every HTJ2K chunk
// (internal_ht.cpp: set_block_dims(128, 32), set_num_decomposition(5)).
const (
	htj2kCodeBlockWidth  = 128
	htj2kCodeBlockHeight = 32
	htj2kNumResolutions  = 6 // 5 decomposition levels plus the base
)

// HTJ2KCompress compresses one packed OpenEXR scanline chunk into an HTJ2K
// chunk: the OpenEXR chunk header followed by a JPEG 2000 codestream.
//
// src is packed in OpenEXR's scanline order (per line, each channel's whole row
// in name-sorted order). numLines is the chunk height. blockWidth is the
// code-block width; the reference always uses 128, and the height is fixed at
// the reference's 32.
//
// Samples are carried as raw bit patterns, never converted: a HALF channel is
// coded as a signed 16-bit component and a FLOAT or UINT channel as a signed
// 32-bit component, each with the NLT Type 3 (binary complement) point
// transform that maps the sign-magnitude encoding onto two's complement. That
// is the transform the reference applies to HALF and FLOAT; it applies none to
// UINT, declaring the component unsigned instead. Both forms are reversible and
// both are read correctly by the reference decoder, which takes whatever
// OpenJPH reconstructs and stores the 32 bits verbatim (internal_ht.cpp).
func HTJ2KCompress(src []byte, numLines int, channels []HTJ2KChannelInfo, blockWidth int) ([]byte, error) {
	if len(channels) == 0 {
		return nil, errors.New("htj2k: no channels specified")
	}
	width, height := channels[0].Width, numLines
	if err := htj2kValidateChannels(channels, width); err != nil {
		return nil, err
	}
	if width <= 0 || height <= 0 {
		return nil, fmt.Errorf("htj2k: empty chunk %dx%d", width, height)
	}

	offsets, bytesPerLine := htj2kLineLayout(channels)
	if need := bytesPerLine * height; len(src) < need {
		return nil, fmt.Errorf("htj2k: chunk data is %d bytes, need %d for %d lines of %d bytes",
			len(src), need, height, bytesPerLine)
	}

	// The channel map records which EXR channel each codestream component
	// carries; it puts R, G and B in components 0, 1 and 2 so the reversible
	// colour transform decorrelates them.
	channelMap, _ := makeChannelMap(channels)

	allHalf, anyHalf := htj2kAllHalf(channels)
	if anyHalf && !allHalf {
		return nil, errors.New("htj2k: mixed HALF and 32-bit channels not supported")
	}

	n := len(channels)
	var codestreamBuf bytes.Buffer

	if allHalf {
		components := make([][]uint16, n)
		for c := range components {
			components[c] = make([]uint16, width*height)
		}
		for y := 0; y < height; y++ {
			line := src[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				dst := components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					dst[x] = binary.LittleEndian.Uint16(row[x*2:])
				}
			}
		}
		img := &jpeg2000.HalfImage{Width: width, Height: height, Components: components}
		if err := jpeg2000.EncodeHalf(&codestreamBuf, img, htj2kOptions(blockWidth)); err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 half encode failed: %w", err)
		}
	} else {
		components := make([][]float32, n)
		for c := range components {
			components[c] = make([]float32, width*height)
		}
		for y := 0; y < height; y++ {
			line := src[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				dst := components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					// Float32frombits is a reinterpretation, not a conversion:
					// a UINT channel's 32 bits survive it unchanged and come
					// back out of Float32bits on the other side.
					dst[x] = math.Float32frombits(binary.LittleEndian.Uint32(row[x*4:]))
				}
			}
		}
		img := &jpeg2000.FloatImage{
			Width: width, Height: height, Components: components,
			BitDepth: 32, Signed: true,
		}
		if err := jpeg2000.EncodeFloat(&codestreamBuf, img, htj2kOptions(blockWidth)); err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 float encode failed: %w", err)
		}
	}

	return htj2kWrap(channelMap, codestreamBuf.Bytes())
}

// HTJ2KDecompress decompresses an HTJ2K chunk back into OpenEXR's packed
// scanline layout. expectedSize is the size the caller expects, and is checked
// rather than assumed; pass 0 to skip the check.
func HTJ2KDecompress(src []byte, expectedSize int, channels []HTJ2KChannelInfo) ([]byte, error) {
	if len(src) < htj2kHeaderSize {
		return nil, ErrHTJ2KCorrupted
	}
	headerSize, channelMap, err := readHTJ2KHeader(src)
	if err != nil {
		return nil, err
	}
	if len(channelMap) != len(channels) {
		return nil, fmt.Errorf("htj2k: channel count mismatch: expected %d, got %d",
			len(channels), len(channelMap))
	}
	if len(channels) == 0 {
		return nil, errors.New("htj2k: no channels specified")
	}
	for _, c := range channelMap {
		if int(c) >= len(channels) {
			return nil, ErrHTJ2KChannelMap
		}
	}

	width := channels[0].Width
	if err := htj2kValidateChannels(channels, width); err != nil {
		return nil, err
	}
	offsets, bytesPerLine := htj2kLineLayout(channels)
	codestream := src[headerSize:]

	allHalf, anyHalf := htj2kAllHalf(channels)
	if anyHalf && !allHalf {
		return nil, errors.New("htj2k: mixed HALF and 32-bit channels not supported")
	}

	n := len(channels)
	var output []byte

	if allHalf {
		img, err := jpeg2000.DecodeHalf(bytes.NewReader(codestream))
		if err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 half decode failed: %w", err)
		}
		if img.Width != width || img.ComponentCount() != n {
			return nil, fmt.Errorf("htj2k: codestream is %dx%d with %d components; the chunk declares %d wide with %d channels",
				img.Width, img.Height, img.ComponentCount(), width, n)
		}
		output = make([]byte, bytesPerLine*img.Height)
		for y := 0; y < img.Height; y++ {
			line := output[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				srcRow := img.Components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					binary.LittleEndian.PutUint16(row[x*2:], srcRow[x])
				}
			}
		}
	} else {
		img, err := jpeg2000.DecodeFloat(bytes.NewReader(codestream))
		if err != nil {
			return nil, fmt.Errorf("htj2k: jpeg2000 float decode failed: %w", err)
		}
		if img.Width != width || img.ComponentCount() != n {
			return nil, fmt.Errorf("htj2k: codestream is %dx%d with %d components; the chunk declares %d wide with %d channels",
				img.Width, img.Height, img.ComponentCount(), width, n)
		}
		output = make([]byte, bytesPerLine*img.Height)
		for y := 0; y < img.Height; y++ {
			line := output[y*bytesPerLine:]
			for comp := 0; comp < n; comp++ {
				row := line[offsets[channelMap[comp]]:]
				srcRow := img.Components[comp][y*width : (y+1)*width]
				for x := 0; x < width; x++ {
					binary.LittleEndian.PutUint32(row[x*4:], math.Float32bits(srcRow[x]))
				}
			}
		}
	}

	if expectedSize > 0 && len(output) != expectedSize {
		return nil, fmt.Errorf("htj2k: decoded %d bytes, the chunk declares %d", len(output), expectedSize)
	}
	return output, nil
}

// HTJ2KDecompressTo decompresses into a pre-allocated buffer
func HTJ2KDecompressTo(src []byte, dst []byte, channels []HTJ2KChannelInfo) error {
	result, err := HTJ2KDecompress(src, len(dst), channels)
	if err != nil {
		return err
	}
	if len(result) != len(dst) {
		return fmt.Errorf("htj2k: size mismatch: expected %d, got %d", len(dst), len(result))
	}
	copy(dst, result)
	return nil
}
