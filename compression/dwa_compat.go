package compression

// Deprecated wrappers for the pre-v1.4.0 DWA entry points.
//
// The old signatures took only a width, a height and a level. DWA needs the
// channel list — it classifies each channel by name into a colour-space-
// converted triple, an RLE-coded alpha or a lossless passthrough, and the
// classification changes the codestream. Carrying no channel information, the
// old API could only ever describe the single-HALF-channel case, and in
// practice it did not produce a codestream any conforming implementation could
// read.
//
// These wrappers keep callers compiling and now produce conforming output for
// the single-HALF-channel image the old signature implies. Anything else needs
// DWACompress or DWADecompress, which take the channel list.

// singleHalfChannel describes the one-channel image the legacy signatures imply.
func singleHalfChannel() []DwaChannel {
	return []DwaChannel{{
		Name:      "Y",
		PixelType: DwaPixelTypeHalf,
		XSampling: 1,
		YSampling: 1,
	}}
}

// CompressDWAA compresses a single HALF channel using DWAA.
//
// Deprecated: use DWACompress, which takes the channel list DWA requires.
func CompressDWAA(src []byte, width, height int, level float32) ([]byte, error) {
	return DWACompress(src, singleHalfChannel(), 0, width-1, 0, height-1, level)
}

// CompressDWAB compresses a single HALF channel using DWAB.
//
// Deprecated: use DWACompress, which takes the channel list DWA requires.
func CompressDWAB(src []byte, width, height int, level float32) ([]byte, error) {
	return DWACompress(src, singleHalfChannel(), 0, width-1, 0, height-1, level)
}

// DecompressDWAA decompresses a single HALF channel encoded with DWAA.
//
// Deprecated: use DWADecompress, which takes the channel list DWA requires.
func DecompressDWAA(src []byte, dst []byte, width, height int) error {
	return DWADecompress(src, singleHalfChannel(), 0, width-1, 0, height-1, dst)
}

// DecompressDWAB decompresses a single HALF channel encoded with DWAB.
//
// Deprecated: use DWADecompress, which takes the channel list DWA requires.
func DecompressDWAB(src []byte, dst []byte, width, height int) error {
	return DWADecompress(src, singleHalfChannel(), 0, width-1, 0, height-1, dst)
}
