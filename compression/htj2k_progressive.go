package compression

import (
	"bytes"
	"fmt"

	jpeg2000 "github.com/mrjoshuak/go-jpeg2000"
)

// HTJ2KExtractCodestream strips the OpenEXR HTJ2K chunk header and returns
// the raw JPEG 2000 codestream along with the channel mapping.
// This is useful for direct access to the J2K data for advanced processing
// such as packet extraction or progressive decoding.
func HTJ2KExtractCodestream(src []byte) (codestream []byte, channelMap []uint16, err error) {
	if len(src) < htj2kHeaderSize {
		return nil, nil, ErrHTJ2KCorrupted
	}

	headerSize, channelMap, err := readHTJ2KHeader(src)
	if err != nil {
		return nil, nil, err
	}

	return src[headerSize:], channelMap, nil
}

// HTJ2KExtractPackets parses HTJ2K-compressed data and returns all wavelet
// packets with their addresses. Each packet represents one quality layer of
// one resolution level of one component in one tile — the atomic unit for
// progressive quality improvement.
//
// Packets can be delivered in any order to a ProgressiveDecoder to produce
// progressively improving images. Lower resolution packets produce a coarse
// image quickly; higher resolution and quality layer packets refine detail.
func HTJ2KExtractPackets(src []byte) ([]jpeg2000.Packet, []uint16, error) {
	codestream, channelMap, err := HTJ2KExtractCodestream(src)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: %w", err)
	}

	packets, err := jpeg2000.ExtractPackets(codestream)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: packet extraction failed: %w", err)
	}

	return packets, channelMap, nil
}

// HTJ2KBuildPacketIndex builds a memory-efficient packet index from
// HTJ2K-compressed data. Unlike ExtractPackets, the index references byte
// ranges in the original codestream without copying packet data, making it
// suitable for large images where memory is a concern.
//
// The returned PacketIndex supports random access by PacketAddress and can
// enumerate all available packets via AllAddresses().
func HTJ2KBuildPacketIndex(src []byte) (*jpeg2000.PacketIndex, []uint16, error) {
	codestream, channelMap, err := HTJ2KExtractCodestream(src)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: %w", err)
	}

	index, err := jpeg2000.BuildPacketIndex(codestream)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: packet index failed: %w", err)
	}

	return index, channelMap, nil
}

// HTJ2KNewProgressiveDecoder creates a progressive decoder from
// HTJ2K-compressed data. The decoder accepts wavelet packets incrementally
// via FeedPacket() and produces continuously improving float32 images via
// Reconstruct().
//
// This enables progressive rendering workflows where a coarse image appears
// immediately and refines as more data arrives. The decoder handles packets
// in any order — resolution, quality layer, and component packets can be
// prioritized based on application needs.
func HTJ2KNewProgressiveDecoder(src []byte) (*jpeg2000.ProgressiveDecoder, []uint16, error) {
	codestream, channelMap, err := HTJ2KExtractCodestream(src)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: %w", err)
	}

	decoder, err := jpeg2000.NewProgressiveDecoderFromCodestream(codestream)
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: progressive decoder init failed: %w", err)
	}

	return decoder, channelMap, nil
}

// HTJ2KDecompressFloat decompresses HTJ2K-compressed data and returns a
// FloatImage with float32 component values. This is useful for processing
// workflows that operate on floating-point data rather than raw byte buffers.
//
// For HALF (float16) channels, the returned float32 values exactly represent
// the original half-float values. Component ordering follows the channel map
// embedded in the HTJ2K header.
func HTJ2KDecompressFloat(src []byte, channels []HTJ2KChannelInfo) (*jpeg2000.FloatImage, []uint16, error) {
	if len(src) < htj2kHeaderSize {
		return nil, nil, ErrHTJ2KCorrupted
	}

	headerSize, channelMap, err := readHTJ2KHeader(src)
	if err != nil {
		return nil, nil, err
	}

	if len(channelMap) != len(channels) {
		return nil, nil, fmt.Errorf("htj2k: channel count mismatch: expected %d, got %d",
			len(channels), len(channelMap))
	}

	codestream := src[headerSize:]
	img, err := jpeg2000.DecodeFloat(bytes.NewReader(codestream))
	if err != nil {
		return nil, nil, fmt.Errorf("htj2k: jpeg2000 float decode failed: %w", err)
	}

	return img, channelMap, nil
}
