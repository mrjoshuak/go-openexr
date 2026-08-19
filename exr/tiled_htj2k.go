package exr

import (
	"github.com/mrjoshuak/go-openexr/compression"
)

// HTJ2K over tiles.
//
// A tile is a chunk like any other, so the codec is the same one the scanline
// path uses; only the block's shape differs. It was missing from the tiled
// switch entirely, which meant a tiled header declaring htj2k256 or htj2k32
// produced "compression not yet implemented" from the writer — the one
// compression in the format that a tiled cloud workflow most wants, since an
// HTJ2K chunk is a JPEG 2000 codestream whose packets are individually
// addressable.

// htj2kTileChannels builds the per-channel description an HTJ2K chunk needs for
// a tile of the given size.
//
// The dimensions are the tile's, not the image's: an edge tile is smaller than
// the tile size, and a codec told the wrong extent reads past its input.
func htj2kTileChannels(cl *ChannelList, tileWidth, tileHeight int) []compression.HTJ2KChannelInfo {
	sorted := cl.SortedByName()
	channels := make([]compression.HTJ2KChannelInfo, len(sorted))
	for i, ch := range sorted {
		xs, ys := int(ch.XSampling), int(ch.YSampling)
		if xs < 1 {
			xs = 1
		}
		if ys < 1 {
			ys = 1
		}
		var htType int
		switch ch.Type {
		case PixelTypeUint:
			htType = compression.HTJ2KPixelTypeUint
		case PixelTypeHalf:
			htType = compression.HTJ2KPixelTypeHalf
		case PixelTypeFloat:
			htType = compression.HTJ2KPixelTypeFloat
		}
		channels[i] = compression.HTJ2KChannelInfo{
			Type:      htType,
			Width:     (tileWidth + xs - 1) / xs,
			Height:    (tileHeight + ys - 1) / ys,
			XSampling: xs,
			YSampling: ys,
			Name:      ch.Name,
		}
	}
	return channels
}

// compressTileHTJ2K compresses one tile's packed samples into an HTJ2K chunk.
func (w *TiledWriter) compressTileHTJ2K(data []byte, tileWidth, tileHeight, blockWidth int) ([]byte, error) {
	channels := htj2kTileChannels(w.channelList, tileWidth, tileHeight)
	return compression.HTJ2KCompress(data, tileHeight, channels, blockWidth)
}

// decompressTileHTJ2K expands one HTJ2K tile chunk back to packed samples.
func (r *TiledReader) decompressTileHTJ2K(data []byte, tileWidth, tileHeight int) ([]byte, error) {
	channels := htj2kTileChannels(r.channelList, tileWidth, tileHeight)
	return compression.HTJ2KDecompress(data, r.calculateTileSize(tileWidth, tileHeight), channels)
}

// htj2kBlockWidth is the code-block width for a compression identifier. The two
// HTJ2K compressions differ in how many scanlines a chunk holds; the block
// width follows the same split, as it does on the scanline path.
func htj2kBlockWidth(c Compression) int {
	if c == CompressionHTJ2K32 {
		return 32
	}
	return 128
}
