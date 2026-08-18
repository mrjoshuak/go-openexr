package compression

// Deprecated compressor/decompressor objects from before v1.4.0.
//
// These wrap the stateless DWACompress and DWADecompress entry points. The
// original implementations produced codestreams no conforming DWA reader could
// decode; these do not, so behaviour changes even though the API does not.

// DwaChannelData describes one channel to the deprecated DWA objects.
//
// Deprecated: use DwaChannel with DWACompress or DWADecompress.
type DwaChannelData struct {
	Name       string
	PixelType  int
	XSampling  int
	YSampling  int
	Scheme     int
	PlanarData []uint16
}

// toDwaChannels converts the deprecated descriptor to the current one. A zero
// sampling factor means the caller left it unset; DWA requires at least 1.
func toDwaChannels(in []DwaChannelData) []DwaChannel {
	out := make([]DwaChannel, len(in))
	for i, c := range in {
		x, y := c.XSampling, c.YSampling
		if x < 1 {
			x = 1
		}
		if y < 1 {
			y = 1
		}
		out[i] = DwaChannel{Name: c.Name, PixelType: c.PixelType, XSampling: x, YSampling: y}
	}
	return out
}

// DwaCompressor encodes DWA chunks.
//
// Deprecated: use DWACompress.
type DwaCompressor struct {
	width, height int
	level         float32
	channels      []DwaChannel
}

// NewDwaCompressor returns a compressor for width x height images.
//
// Deprecated: use DWACompress.
func NewDwaCompressor(width, height int, level float32) *DwaCompressor {
	return &DwaCompressor{width: width, height: height, level: level, channels: singleHalfChannel()}
}

// SetChannels sets the channel list.
//
// Deprecated: use DWACompress.
func (c *DwaCompressor) SetChannels(channels []DwaChannelData) {
	if len(channels) > 0 {
		c.channels = toDwaChannels(channels)
	}
}

// Compress encodes one chunk.
//
// Deprecated: use DWACompress.
func (c *DwaCompressor) Compress(src []byte) ([]byte, error) {
	return DWACompress(src, c.channels, 0, c.width-1, 0, c.height-1, c.level)
}

// DwaDecompressor decodes DWA chunks.
//
// Deprecated: use DWADecompress.
type DwaDecompressor struct {
	width, height int
	channels      []DwaChannel
}

// NewDwaDecompressor returns a decompressor for width x height images.
//
// Deprecated: use DWADecompress.
func NewDwaDecompressor(width, height int) *DwaDecompressor {
	return &DwaDecompressor{width: width, height: height, channels: singleHalfChannel()}
}

// SetChannels sets the channel list.
//
// Deprecated: use DWADecompress.
func (d *DwaDecompressor) SetChannels(channels []DwaChannelData) {
	if len(channels) > 0 {
		d.channels = toDwaChannels(channels)
	}
}

// Decompress decodes one chunk into dst.
//
// Deprecated: use DWADecompress.
func (d *DwaDecompressor) Decompress(src []byte, dst []byte) error {
	return DWADecompress(src, d.channels, 0, d.width-1, 0, d.height-1, dst)
}
