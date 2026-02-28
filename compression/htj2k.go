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
	htj2kHeaderSize        = 6      // Magic (2) + PLEN (4)
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

// htj2kChunkHeader represents the OpenEXR HTJ2K chunk header
type htj2kChunkHeader struct {
	Magic      uint16   // "HT" (0x4854)
	PayloadLen uint32   // Length of header payload
	ChannelMap []uint16 // Maps J2K component index to EXR channel index
}

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

// HTJ2KCompress compresses scanline data using HTJ2K compression
func HTJ2KCompress(src []byte, numLines int, channels []HTJ2KChannelInfo, blockSize int) ([]byte, error) {
	if len(channels) == 0 {
		return nil, errors.New("htj2k: no channels specified")
	}

	// Calculate dimensions
	width := channels[0].Width
	height := numLines

	// Validate all channels have consistent dimensions for JPEG 2000
	for _, ch := range channels {
		if ch.Type == HTJ2KPixelTypeFloat {
			return nil, errors.New("htj2k: FLOAT (32-bit) pixel type not supported for compression; use HALF (16-bit) channels")
		}
	}

	// Create channel map for RCT optimization
	channelMap, isRGB := makeChannelMap(channels)

	// Create JPEG 2000 encoder options
	opts := &jpeg2000.Options{
		Format:         jpeg2000.FormatJ2K, // Raw codestream, no JP2 wrapper
		Lossless:       true,               // OpenEXR HTJ2K is always lossless
		HighThroughput: true,               // Enable HTJ2K mode
		HTBlockWidth:   blockSize,
		HTBlockHeight:  blockSize,
		NumResolutions: 6, // 5 decomposition levels + base
	}

	// MCT (RCT for lossless) is automatically applied by go-jpeg2000
	// when there are 3+ components, so no explicit option needed
	_ = isRGB

	// Create image wrapper
	img := newEXRImage(width, height, channels, src)

	// Encode to JPEG 2000
	var codestreamBuf bytes.Buffer
	if err := jpeg2000.Encode(&codestreamBuf, img, opts); err != nil {
		return nil, fmt.Errorf("htj2k: jpeg2000 encode failed: %w", err)
	}

	// Build output: header + codestream
	headerSize := htj2kHeaderSize + 2 + len(channelMap)*2
	output := make([]byte, 0, headerSize+codestreamBuf.Len())

	var headerBuf bytes.Buffer
	if err := writeHTJ2KHeader(&headerBuf, channelMap); err != nil {
		return nil, err
	}
	output = append(output, headerBuf.Bytes()...)
	output = append(output, codestreamBuf.Bytes()...)

	return output, nil
}

// HTJ2KDecompress decompresses HTJ2K-compressed data
func HTJ2KDecompress(src []byte, expectedSize int, channels []HTJ2KChannelInfo) ([]byte, error) {
	if len(src) < htj2kHeaderSize {
		return nil, ErrHTJ2KCorrupted
	}

	// Parse header
	headerSize, channelMap, err := readHTJ2KHeader(src)
	if err != nil {
		return nil, err
	}

	// Validate channel map
	if len(channelMap) != len(channels) {
		return nil, fmt.Errorf("htj2k: channel count mismatch: expected %d, got %d",
			len(channels), len(channelMap))
	}

	// Decode JPEG 2000 codestream
	codestream := src[headerSize:]
	img, err := jpeg2000.Decode(bytes.NewReader(codestream))
	if err != nil {
		return nil, fmt.Errorf("htj2k: jpeg2000 decode failed: %w", err)
	}

	// Extract pixel data and reorder according to channel map
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()

	// Calculate output size
	bytesPerPixel := 0
	for _, ch := range channels {
		switch ch.Type {
		case HTJ2KPixelTypeHalf:
			bytesPerPixel += 2
		case HTJ2KPixelTypeUint, HTJ2KPixelTypeFloat:
			bytesPerPixel += 4
		}
	}

	output := make([]byte, width*height*bytesPerPixel)

	// Convert decoded image to EXR format
	// This needs to handle the channel reordering from channelMap
	if err := extractPixelData(img, output, channels, channelMap); err != nil {
		return nil, err
	}

	return output, nil
}

// extractPixelData extracts pixel data from a decoded JPEG 2000 image
func extractPixelData(img image.Image, dst []byte, channels []HTJ2KChannelInfo, channelMap []uint16) error {
	bounds := img.Bounds()
	width, height := bounds.Dx(), bounds.Dy()

	// Calculate byte offsets for each channel in the output
	channelOffsets := make([]int, len(channels))
	offset := 0
	for i, ch := range channels {
		channelOffsets[i] = offset
		switch ch.Type {
		case HTJ2KPixelTypeHalf:
			offset += 2
		case HTJ2KPixelTypeUint:
			offset += 4
		}
	}
	bytesPerPixel := offset

	// Create inverse channel map (J2K component -> output channel)
	inverseMap := make([]int, len(channelMap))
	for outCh, j2kComp := range channelMap {
		if int(j2kComp) < len(inverseMap) {
			inverseMap[j2kComp] = outCh
		}
	}

	// Extract based on image type
	switch src := img.(type) {
	case *image.Gray16:
		if len(channels) != 1 {
			return errors.New("htj2k: Gray16 image but multiple channels expected")
		}
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				gray := src.Gray16At(x, y)
				dstOffset := (y*width + x) * bytesPerPixel
				binary.LittleEndian.PutUint16(dst[dstOffset:], gray.Y)
			}
		}

	case *image.NRGBA64:
		// Handle multi-channel images
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				c := src.NRGBA64At(x, y)
				dstOffset := (y*width + x) * bytesPerPixel

				// Write channels in correct order using inverse map
				for j2kComp := 0; j2kComp < len(channelMap); j2kComp++ {
					outCh := inverseMap[j2kComp]
					chOffset := channelOffsets[outCh]

					var val uint16
					switch j2kComp {
					case 0:
						val = c.R
					case 1:
						val = c.G
					case 2:
						val = c.B
					case 3:
						val = c.A
					}

					if channels[outCh].Type == HTJ2KPixelTypeHalf {
						binary.LittleEndian.PutUint16(dst[dstOffset+chOffset:], val)
					}
				}
			}
		}

	default:
		// Generic fallback using color.Model
		for y := 0; y < height; y++ {
			for x := 0; x < width; x++ {
				c := img.At(x, y)
				gray := color.Gray16Model.Convert(c).(color.Gray16)
				dstOffset := (y*width + x) * bytesPerPixel
				binary.LittleEndian.PutUint16(dst[dstOffset:], gray.Y)
			}
		}
	}

	return nil
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
