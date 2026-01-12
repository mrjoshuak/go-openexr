package exr

import (
	"errors"
	"fmt"
	"sort"

	"github.com/mrjoshuak/go-openexr/compression"
	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Standard header attribute names
const (
	AttrNameChannels            = "channels"
	AttrNameCompression         = "compression"
	AttrNameDataWindow          = "dataWindow"
	AttrNameDisplayWindow       = "displayWindow"
	AttrNameLineOrder           = "lineOrder"
	AttrNamePixelAspectRatio    = "pixelAspectRatio"
	AttrNameScreenWindowCenter  = "screenWindowCenter"
	AttrNameScreenWindowWidth   = "screenWindowWidth"
	AttrNameTiles               = "tiles"
	AttrNameType                = "type"
	AttrNameName                = "name"
	AttrNameVersion             = "version"
	AttrNameChunkCount          = "chunkCount"
	AttrNameDWACompressionLevel = "dwaCompressionLevel"
)

// DefaultDWACompressionLevel is the default compression level for DWA/DWAB compression.
// The value 45.0 is "visually lossless" according to the reference implementation.
// Lower values = less compression, higher quality. 0 = minimal quantization.
// Higher values = more compression, more loss.
const DefaultDWACompressionLevel = float32(45.0)

// Part types for multi-part files
const (
	PartTypeScanline     = "scanlineimage"
	PartTypeTiled        = "tiledimage"
	PartTypeDeepScanline = "deepscanline"
	PartTypeDeepTiled    = "deeptile"
)

// Header errors
var (
	ErrMissingRequiredAttribute = errors.New("exr: missing required attribute")
	ErrInvalidHeader            = errors.New("exr: invalid header")
	ErrEmptyDataWindow          = errors.New("exr: data window is empty")
	ErrNoChannels               = errors.New("exr: no channels defined")
)

// CompressionOptions contains compression-related settings that are not
// stored as standard EXR attributes. These are used for deterministic
// round-trip support.
type CompressionOptions struct {
	// ZIPLevel is the zlib compression level (0-9, or -1 for default).
	// When reading files, this is detected from the zlib header's FLEVEL field.
	// When writing, this level is used for ZIP/ZIPS compression.
	ZIPLevel compression.CompressionLevel

	// DetectedFLevel is the FLEVEL category detected from the source file.
	// This is set during reading and can be used to determine a compatible
	// compression level for writing.
	DetectedFLevel compression.FLevel

	// FLevelDetected indicates whether DetectedFLevel was set from reading.
	FLevelDetected bool
}

// DefaultCompressionOptions returns compression options with default values.
func DefaultCompressionOptions() CompressionOptions {
	return CompressionOptions{
		ZIPLevel:       compression.CompressionLevelDefault,
		DetectedFLevel: compression.FLevelDefault,
		FLevelDetected: false,
	}
}

// Header represents the header of an OpenEXR file or part.
type Header struct {
	attrs map[string]*Attribute

	// CompOpts contains compression-related options not stored as attributes.
	// These are used for deterministic round-trip support.
	CompOpts CompressionOptions
}

// NewHeader creates a new empty header.
func NewHeader() *Header {
	return &Header{
		attrs:    make(map[string]*Attribute),
		CompOpts: DefaultCompressionOptions(),
	}
}

// SetZIPLevel sets the zlib compression level for ZIP/ZIPS compression.
// Level should be -2 to 9 (see compression.CompressionLevel constants).
// For deterministic round-trip, use the level from the source file.
func (h *Header) SetZIPLevel(level compression.CompressionLevel) {
	h.CompOpts.ZIPLevel = level
}

// ZIPLevel returns the configured zlib compression level.
func (h *Header) ZIPLevel() compression.CompressionLevel {
	return h.CompOpts.ZIPLevel
}

// SetDetectedFLevel records the FLEVEL detected from compressed data.
// This is typically called by readers when decompressing ZIP data.
func (h *Header) SetDetectedFLevel(flevel compression.FLevel) {
	h.CompOpts.DetectedFLevel = flevel
	h.CompOpts.FLevelDetected = true
	// Also set the ZIP level to a compatible value
	h.CompOpts.ZIPLevel = compression.FLevelToLevel(flevel)
}

// DetectedFLevel returns the FLEVEL detected from the source file.
// Returns FLevelDefault and false if no FLEVEL was detected.
func (h *Header) DetectedFLevel() (compression.FLevel, bool) {
	return h.CompOpts.DetectedFLevel, h.CompOpts.FLevelDetected
}

// CompressionOptions returns the compression options for this header.
func (h *Header) CompressionOptions() CompressionOptions {
	return h.CompOpts
}

// SetCompressionOptions sets the compression options for this header.
func (h *Header) SetCompressionOptions(opts CompressionOptions) {
	h.CompOpts = opts
}

// NewScanlineHeader creates a header for a scanline image with default values.
func NewScanlineHeader(width, height int) *Header {
	h := NewHeader()

	dataWindow := Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}}
	h.SetDataWindow(dataWindow)
	h.SetDisplayWindow(dataWindow)
	h.SetCompression(CompressionZIP)
	h.SetLineOrder(LineOrderIncreasing)
	h.SetPixelAspectRatio(1.0)
	h.SetScreenWindowCenter(V2f{0, 0})
	h.SetScreenWindowWidth(1.0)

	// Add default RGB channels
	cl := NewChannelList()
	cl.Add(NewChannel("R", PixelTypeHalf))
	cl.Add(NewChannel("G", PixelTypeHalf))
	cl.Add(NewChannel("B", PixelTypeHalf))
	h.SetChannels(cl)

	return h
}

// Set sets an attribute value.
func (h *Header) Set(attr *Attribute) {
	h.attrs[attr.Name] = attr
}

// Get returns an attribute by name, or nil if not found.
func (h *Header) Get(name string) *Attribute {
	return h.attrs[name]
}

// Has returns true if the header has an attribute with the given name.
func (h *Header) Has(name string) bool {
	_, ok := h.attrs[name]
	return ok
}

// Remove removes an attribute by name.
func (h *Header) Remove(name string) {
	delete(h.attrs, name)
}

// Attributes returns all attributes as a slice, sorted by name for deterministic iteration.
func (h *Header) Attributes() []*Attribute {
	names := h.sortedAttributeNames()
	result := make([]*Attribute, len(names))
	for i, name := range names {
		result[i] = h.attrs[name]
	}
	return result
}

// sortedAttributeNames returns attribute names in sorted order for deterministic serialization.
func (h *Header) sortedAttributeNames() []string {
	names := make([]string, 0, len(h.attrs))
	for name := range h.attrs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Required attribute accessors

// Channels returns the channel list.
func (h *Header) Channels() *ChannelList {
	attr := h.attrs[AttrNameChannels]
	if attr == nil {
		return nil
	}
	if cl, ok := attr.Value.(*ChannelList); ok {
		return cl
	}
	return nil
}

// SetChannels sets the channel list.
func (h *Header) SetChannels(cl *ChannelList) {
	h.Set(&Attribute{Name: AttrNameChannels, Type: AttrTypeChlist, Value: cl})
}

// Compression returns the compression method.
func (h *Header) Compression() Compression {
	attr := h.attrs[AttrNameCompression]
	if attr == nil {
		return CompressionNone
	}
	if c, ok := attr.Value.(Compression); ok {
		return c
	}
	return CompressionNone
}

// SetCompression sets the compression method.
func (h *Header) SetCompression(c Compression) {
	h.Set(&Attribute{Name: AttrNameCompression, Type: AttrTypeCompression, Value: c})
}

// DataWindow returns the data window (bounding box of actual pixel data).
func (h *Header) DataWindow() Box2i {
	attr := h.attrs[AttrNameDataWindow]
	if attr == nil {
		return Box2i{}
	}
	if b, ok := attr.Value.(Box2i); ok {
		return b
	}
	return Box2i{}
}

// SetDataWindow sets the data window.
func (h *Header) SetDataWindow(b Box2i) {
	h.Set(&Attribute{Name: AttrNameDataWindow, Type: AttrTypeBox2i, Value: b})
}

// DisplayWindow returns the display window (full image dimensions).
func (h *Header) DisplayWindow() Box2i {
	attr := h.attrs[AttrNameDisplayWindow]
	if attr == nil {
		return Box2i{}
	}
	if b, ok := attr.Value.(Box2i); ok {
		return b
	}
	return Box2i{}
}

// SetDisplayWindow sets the display window.
func (h *Header) SetDisplayWindow(b Box2i) {
	h.Set(&Attribute{Name: AttrNameDisplayWindow, Type: AttrTypeBox2i, Value: b})
}

// LineOrder returns the scanline ordering.
func (h *Header) LineOrder() LineOrder {
	attr := h.attrs[AttrNameLineOrder]
	if attr == nil {
		return LineOrderIncreasing
	}
	if lo, ok := attr.Value.(LineOrder); ok {
		return lo
	}
	return LineOrderIncreasing
}

// SetLineOrder sets the scanline ordering.
func (h *Header) SetLineOrder(lo LineOrder) {
	h.Set(&Attribute{Name: AttrNameLineOrder, Type: AttrTypeLineOrder, Value: lo})
}

// PixelAspectRatio returns the pixel aspect ratio.
func (h *Header) PixelAspectRatio() float32 {
	attr := h.attrs[AttrNamePixelAspectRatio]
	if attr == nil {
		return 1.0
	}
	if ratio, ok := attr.Value.(float32); ok {
		return ratio
	}
	return 1.0
}

// SetPixelAspectRatio sets the pixel aspect ratio.
func (h *Header) SetPixelAspectRatio(ratio float32) {
	h.Set(&Attribute{Name: AttrNamePixelAspectRatio, Type: AttrTypeFloat, Value: ratio})
}

// ScreenWindowCenter returns the screen window center.
func (h *Header) ScreenWindowCenter() V2f {
	attr := h.attrs[AttrNameScreenWindowCenter]
	if attr == nil {
		return V2f{0, 0}
	}
	if v, ok := attr.Value.(V2f); ok {
		return v
	}
	return V2f{0, 0}
}

// SetScreenWindowCenter sets the screen window center.
func (h *Header) SetScreenWindowCenter(center V2f) {
	h.Set(&Attribute{Name: AttrNameScreenWindowCenter, Type: AttrTypeV2f, Value: center})
}

// ScreenWindowWidth returns the screen window width.
func (h *Header) ScreenWindowWidth() float32 {
	attr := h.attrs[AttrNameScreenWindowWidth]
	if attr == nil {
		return 1.0
	}
	if w, ok := attr.Value.(float32); ok {
		return w
	}
	return 1.0
}

// SetScreenWindowWidth sets the screen window width.
func (h *Header) SetScreenWindowWidth(width float32) {
	h.Set(&Attribute{Name: AttrNameScreenWindowWidth, Type: AttrTypeFloat, Value: width})
}

// Optional attribute accessors

// Preview returns the preview image, or nil if none exists.
func (h *Header) Preview() *Preview {
	attr := h.attrs["preview"]
	if attr == nil {
		return nil
	}
	if p, ok := attr.Value.(Preview); ok {
		return &p
	}
	return nil
}

// SetPreview sets the preview image.
func (h *Header) SetPreview(p Preview) {
	h.Set(&Attribute{Name: "preview", Type: AttrTypePreview, Value: p})
}

// HasPreview returns true if the header has a preview image.
func (h *Header) HasPreview() bool {
	return h.attrs["preview"] != nil
}

// TileDescription returns the tile description, or nil for scanline images.
func (h *Header) TileDescription() *TileDescription {
	attr := h.attrs[AttrNameTiles]
	if attr == nil {
		return nil
	}
	if td, ok := attr.Value.(TileDescription); ok {
		return &td
	}
	return nil
}

// SetTileDescription sets the tile description.
func (h *Header) SetTileDescription(td TileDescription) {
	h.Set(&Attribute{Name: AttrNameTiles, Type: AttrTypeTileDesc, Value: td})
}

// SetDWACompressionLevel sets the DWA/DWAB compression level.
// The default value is 45.0 which provides "visually lossless" compression.
// Lower values = less compression, higher quality. 0 = minimal quantization.
// Higher values = more compression, more loss.
func (h *Header) SetDWACompressionLevel(level float32) {
	h.Set(&Attribute{Name: AttrNameDWACompressionLevel, Type: AttrTypeFloat, Value: level})
}

// DWACompressionLevel returns the DWA/DWAB compression level.
// Returns DefaultDWACompressionLevel (45.0) if not set.
func (h *Header) DWACompressionLevel() float32 {
	attr := h.attrs[AttrNameDWACompressionLevel]
	if attr != nil {
		if v, ok := attr.Value.(float32); ok {
			return v
		}
	}
	return DefaultDWACompressionLevel
}

// IsTiled returns true if this is a tiled image.
func (h *Header) IsTiled() bool {
	return h.attrs[AttrNameTiles] != nil
}

// Helper methods

// Width returns the width of the data window.
func (h *Header) Width() int {
	return int(h.DataWindow().Width())
}

// Height returns the height of the data window.
func (h *Header) Height() int {
	return int(h.DataWindow().Height())
}

// Validate checks that all required attributes are present and valid.
func (h *Header) Validate() error {
	required := []string{
		AttrNameChannels,
		AttrNameCompression,
		AttrNameDataWindow,
		AttrNameDisplayWindow,
		AttrNameLineOrder,
		AttrNamePixelAspectRatio,
		AttrNameScreenWindowCenter,
		AttrNameScreenWindowWidth,
	}

	for _, name := range required {
		if !h.Has(name) {
			return fmt.Errorf("%w: %s", ErrMissingRequiredAttribute, name)
		}
	}

	// Validate data window
	dw := h.DataWindow()
	if dw.IsEmpty() {
		return ErrEmptyDataWindow
	}

	// Validate channels
	cl := h.Channels()
	if cl == nil || cl.Len() == 0 {
		return ErrNoChannels
	}

	return nil
}

// ReadHeader reads a header from the reader.
func ReadHeader(r *xdr.Reader) (*Header, error) {
	h := NewHeader()

	for {
		attr, err := ReadAttribute(r)
		if err != nil {
			return nil, err
		}
		if attr == nil {
			// End of header
			break
		}
		h.Set(attr)
	}

	return h, nil
}

// WriteHeader writes a header to the writer.
// The header is terminated with a null byte (empty name).
// Attributes are written in sorted order by name to ensure deterministic output.
func WriteHeader(w *xdr.BufferWriter, h *Header) error {
	// Get sorted attribute names for deterministic output
	names := h.sortedAttributeNames()
	for _, name := range names {
		attr := h.attrs[name]
		if err := WriteAttribute(w, attr); err != nil {
			return err
		}
	}
	// Header terminator
	w.WriteByte(0)
	return nil
}

// SerializeForTest serializes the header to bytes for testing.
func (h *Header) SerializeForTest() []byte {
	w := xdr.NewBufferWriter(4096)
	WriteHeader(w, h)
	return w.Bytes()
}

// ReadHeaderFromBytes parses a header from bytes for testing.
func ReadHeaderFromBytes(data []byte) (*Header, error) {
	r := xdr.NewReader(data)
	return ReadHeader(r)
}

// NumXLevels returns the number of resolution levels in the X direction.
// For LevelModeOne, this is always 1.
// For LevelModeMipmap, this is the same as NumYLevels.
// For LevelModeRipmap, X and Y levels are independent.
func (h *Header) NumXLevels() int {
	td := h.TileDescription()
	if td == nil {
		return 1
	}

	switch td.Mode {
	case LevelModeOne:
		return 1
	case LevelModeMipmap:
		return h.numMipmapLevels()
	case LevelModeRipmap:
		dw := h.DataWindow()
		w := int(dw.Width())
		return numLevels(w, td.RoundingMode)
	}
	return 1
}

// NumYLevels returns the number of resolution levels in the Y direction.
// For LevelModeOne, this is always 1.
// For LevelModeMipmap, this is the same as NumXLevels.
// For LevelModeRipmap, X and Y levels are independent.
func (h *Header) NumYLevels() int {
	td := h.TileDescription()
	if td == nil {
		return 1
	}

	switch td.Mode {
	case LevelModeOne:
		return 1
	case LevelModeMipmap:
		return h.numMipmapLevels()
	case LevelModeRipmap:
		dw := h.DataWindow()
		ht := int(dw.Height())
		return numLevels(ht, td.RoundingMode)
	}
	return 1
}

// numMipmapLevels calculates the number of mipmap levels for both dimensions.
func (h *Header) numMipmapLevels() int {
	td := h.TileDescription()
	dw := h.DataWindow()
	w := int(dw.Width())
	ht := int(dw.Height())

	// For mipmap, both dimensions have the same number of levels
	// The number of levels is based on the larger dimension
	maxDim := w
	if ht > maxDim {
		maxDim = ht
	}
	return numLevels(maxDim, td.RoundingMode)
}

// numLevels calculates the number of resolution levels for a given dimension.
func numLevels(size int, roundingMode LevelRoundingMode) int {
	if size <= 0 {
		return 0
	}
	levels := 1
	for size > 1 {
		if roundingMode == LevelRoundDown {
			size /= 2
		} else {
			size = (size + 1) / 2
		}
		levels++
	}
	return levels
}

// LevelWidth returns the width of the image at resolution level lx.
// Level 0 is the full resolution.
func (h *Header) LevelWidth(lx int) int {
	td := h.TileDescription()
	dw := h.DataWindow()
	w := int(dw.Width())

	if td == nil || lx < 0 {
		return w
	}

	for i := 0; i < lx && w > 1; i++ {
		if td.RoundingMode == LevelRoundDown {
			w /= 2
		} else {
			w = (w + 1) / 2
		}
	}
	if w < 1 {
		w = 1
	}
	return w
}

// LevelHeight returns the height of the image at resolution level ly.
// Level 0 is the full resolution.
func (h *Header) LevelHeight(ly int) int {
	td := h.TileDescription()
	dw := h.DataWindow()
	ht := int(dw.Height())

	if td == nil || ly < 0 {
		return ht
	}

	for i := 0; i < ly && ht > 1; i++ {
		if td.RoundingMode == LevelRoundDown {
			ht /= 2
		} else {
			ht = (ht + 1) / 2
		}
	}
	if ht < 1 {
		ht = 1
	}
	return ht
}

// NumXTiles returns the number of tiles in the X direction at level lx.
func (h *Header) NumXTiles(lx int) int {
	td := h.TileDescription()
	if td == nil || td.XSize == 0 {
		return 0
	}
	w := h.LevelWidth(lx)
	return (w + int(td.XSize) - 1) / int(td.XSize)
}

// NumYTiles returns the number of tiles in the Y direction at level ly.
func (h *Header) NumYTiles(ly int) int {
	td := h.TileDescription()
	if td == nil || td.YSize == 0 {
		return 0
	}
	ht := h.LevelHeight(ly)
	return (ht + int(td.YSize) - 1) / int(td.YSize)
}

// ChunksInFile calculates the number of chunks needed for this header.
func (h *Header) ChunksInFile() int {
	dw := h.DataWindow()
	height := int(dw.Height())

	if h.IsTiled() {
		td := h.TileDescription()
		if td == nil {
			return 0 // Invalid tiled file without tile description
		}

		switch td.Mode {
		case LevelModeOne:
			return h.NumXTiles(0) * h.NumYTiles(0)

		case LevelModeMipmap:
			total := 0
			numLevels := h.numMipmapLevels()
			for level := 0; level < numLevels; level++ {
				total += h.NumXTiles(level) * h.NumYTiles(level)
			}
			return total

		case LevelModeRipmap:
			// Ripmap has independent X and Y levels
			total := 0
			numXLev := h.NumXLevels()
			numYLev := h.NumYLevels()
			for ly := 0; ly < numYLev; ly++ {
				for lx := 0; lx < numXLev; lx++ {
					total += h.NumXTiles(lx) * h.NumYTiles(ly)
				}
			}
			return total
		}
		return h.NumXTiles(0) * h.NumYTiles(0)
	}

	// Scanline file
	linesPerChunk := h.Compression().ScanlinesPerChunk()
	return (height + linesPerChunk - 1) / linesPerChunk
}
