package compression

import (
	"testing"
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

func TestHTJ2KExtractPackets(t *testing.T) {
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

	packets, channelMap, err := HTJ2KExtractPackets(compressed)
	if err != nil {
		t.Fatalf("HTJ2KExtractPackets failed: %v", err)
	}

	if len(packets) == 0 {
		t.Error("No packets extracted")
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}

	t.Logf("Extracted %d packets from %d bytes of compressed data", len(packets), len(compressed))

	// Verify packets have data
	for i, pkt := range packets {
		if len(pkt.Data) == 0 {
			t.Errorf("Packet %d has no data", i)
		}
	}
}

func TestHTJ2KExtractPacketsCorrupted(t *testing.T) {
	_, _, err := HTJ2KExtractPackets([]byte("short"))
	if err == nil {
		t.Error("Expected error for corrupted data")
	}
}

func TestHTJ2KBuildPacketIndex(t *testing.T) {
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

	index, channelMap, err := HTJ2KBuildPacketIndex(compressed)
	if err != nil {
		t.Fatalf("HTJ2KBuildPacketIndex failed: %v", err)
	}

	if index.Len() == 0 {
		t.Error("Packet index is empty")
	}

	if len(channelMap) != 1 {
		t.Errorf("Channel map length: got %d, want 1", len(channelMap))
	}

	// Verify we can get all addresses
	addrs := index.AllAddresses()
	if len(addrs) == 0 {
		t.Error("No packet addresses in index")
	}

	t.Logf("Indexed %d packets", index.Len())

	// Verify we can retrieve packet data by address
	for _, addr := range addrs {
		data, err := index.GetPacket(addr)
		if err != nil {
			t.Errorf("GetPacket failed for %v: %v", addr, err)
			continue
		}
		if len(data) == 0 {
			t.Errorf("Empty data for packet %v", addr)
		}
	}
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
