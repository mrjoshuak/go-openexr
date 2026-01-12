package exr

import (
	"sort"
	"strings"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// PixelType defines the data type for pixel channel values.
type PixelType uint32

const (
	// PixelTypeUint is a 32-bit unsigned integer.
	PixelTypeUint PixelType = 0
	// PixelTypeHalf is a 16-bit IEEE 754 half-precision float.
	PixelTypeHalf PixelType = 1
	// PixelTypeFloat is a 32-bit IEEE 754 single-precision float.
	PixelTypeFloat PixelType = 2
)

// String returns a string representation of the pixel type.
func (pt PixelType) String() string {
	switch pt {
	case PixelTypeUint:
		return "uint"
	case PixelTypeHalf:
		return "half"
	case PixelTypeFloat:
		return "float"
	default:
		return "unknown"
	}
}

// Size returns the size in bytes of one pixel value.
func (pt PixelType) Size() int {
	switch pt {
	case PixelTypeUint:
		return 4
	case PixelTypeHalf:
		return 2
	case PixelTypeFloat:
		return 4
	default:
		return 0
	}
}

// Channel describes a single image channel.
type Channel struct {
	// Name is the channel name (e.g., "R", "G", "B", "A", "Z").
	Name string
	// Type is the pixel data type.
	Type PixelType
	// XSampling is the horizontal subsampling factor (1 = full resolution).
	XSampling int32
	// YSampling is the vertical subsampling factor (1 = full resolution).
	YSampling int32
	// PLinear indicates if the channel stores perceptually linear data.
	// This is a hint for display applications.
	PLinear bool
}

// NewChannel creates a new channel with the given name and type.
// XSampling and YSampling default to 1 (full resolution).
func NewChannel(name string, pixelType PixelType) Channel {
	return Channel{
		Name:      name,
		Type:      pixelType,
		XSampling: 1,
		YSampling: 1,
		PLinear:   false,
	}
}

// Layer returns the layer name for a channel, or empty string if none.
// Layers use dot notation: "layer.channel" -> "layer"
// "layer.sublayer.channel" -> "layer.sublayer"
func (c Channel) Layer() string {
	idx := strings.LastIndex(c.Name, ".")
	if idx < 0 {
		return ""
	}
	return c.Name[:idx]
}

// BaseName returns the channel name without the layer prefix.
// "layer.R" -> "R", "R" -> "R"
func (c Channel) BaseName() string {
	idx := strings.LastIndex(c.Name, ".")
	if idx < 0 {
		return c.Name
	}
	return c.Name[idx+1:]
}

// ChannelList represents an ordered collection of channels.
type ChannelList struct {
	channels []Channel
	byName   map[string]int // Index by name
}

// NewChannelList creates an empty channel list.
func NewChannelList() *ChannelList {
	return &ChannelList{
		channels: make([]Channel, 0),
		byName:   make(map[string]int),
	}
}

// Add adds a channel to the list. Returns false if a channel
// with the same name already exists.
func (cl *ChannelList) Add(c Channel) bool {
	if _, exists := cl.byName[c.Name]; exists {
		return false
	}
	cl.byName[c.Name] = len(cl.channels)
	cl.channels = append(cl.channels, c)
	return true
}

// Get returns a channel by name, or nil if not found.
func (cl *ChannelList) Get(name string) *Channel {
	idx, exists := cl.byName[name]
	if !exists {
		return nil
	}
	return &cl.channels[idx]
}

// Len returns the number of channels.
func (cl *ChannelList) Len() int {
	return len(cl.channels)
}

// At returns the channel at the given index.
func (cl *ChannelList) At(i int) Channel {
	return cl.channels[i]
}

// Channels returns a slice of all channels.
func (cl *ChannelList) Channels() []Channel {
	result := make([]Channel, len(cl.channels))
	copy(result, cl.channels)
	return result
}

// Names returns a slice of all channel names.
func (cl *ChannelList) Names() []string {
	names := make([]string, len(cl.channels))
	for i, c := range cl.channels {
		names[i] = c.Name
	}
	return names
}

// SortedByName returns a copy of all channels sorted alphabetically by name.
// This is the canonical order for OpenEXR compression algorithms.
func (cl *ChannelList) SortedByName() []Channel {
	result := make([]Channel, len(cl.channels))
	copy(result, cl.channels)
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Layers returns unique layer names from all channels.
func (cl *ChannelList) Layers() []string {
	layerSet := make(map[string]bool)
	for _, c := range cl.channels {
		layer := c.Layer()
		if layer != "" {
			layerSet[layer] = true
		}
	}
	layers := make([]string, 0, len(layerSet))
	for layer := range layerSet {
		layers = append(layers, layer)
	}
	sort.Strings(layers)
	return layers
}

// ChannelsInLayer returns all channels belonging to a specific layer.
// Pass empty string to get channels without a layer prefix.
func (cl *ChannelList) ChannelsInLayer(layer string) []Channel {
	result := make([]Channel, 0)
	for _, c := range cl.channels {
		if c.Layer() == layer {
			result = append(result, c)
		}
	}
	return result
}

// SortByName sorts channels alphabetically by name.
func (cl *ChannelList) SortByName() {
	sort.Slice(cl.channels, func(i, j int) bool {
		return cl.channels[i].Name < cl.channels[j].Name
	})
	cl.rebuildIndex()
}

// SortForCompression sorts channels optimally for compression.
// Channels are sorted by pixel type (for better compression),
// then by name within each type group.
func (cl *ChannelList) SortForCompression() {
	sort.Slice(cl.channels, func(i, j int) bool {
		// Primary: sort by pixel type (smaller types first for interleaving)
		if cl.channels[i].Type != cl.channels[j].Type {
			return cl.channels[i].Type < cl.channels[j].Type
		}
		// Secondary: sort by name
		return cl.channels[i].Name < cl.channels[j].Name
	})
	cl.rebuildIndex()
}

func (cl *ChannelList) rebuildIndex() {
	cl.byName = make(map[string]int)
	for i, c := range cl.channels {
		cl.byName[c.Name] = i
	}
}

// HasRGB returns true if the channel list contains R, G, and B channels.
func (cl *ChannelList) HasRGB() bool {
	_, hasR := cl.byName["R"]
	_, hasG := cl.byName["G"]
	_, hasB := cl.byName["B"]
	return hasR && hasG && hasB
}

// HasAlpha returns true if the channel list contains an A channel.
func (cl *ChannelList) HasAlpha() bool {
	_, hasA := cl.byName["A"]
	return hasA
}

// HasRGBA returns true if the channel list contains R, G, B, and A channels.
func (cl *ChannelList) HasRGBA() bool {
	return cl.HasRGB() && cl.HasAlpha()
}

// ReadChannelList reads a channel list from the reader.
// The format is: channel entries followed by a null byte.
// Each channel entry is: name\0, type (4 bytes), pLinear (1 byte),
// reserved (3 bytes), xSampling (4 bytes), ySampling (4 bytes).
func ReadChannelList(r *xdr.Reader) (*ChannelList, error) {
	cl := NewChannelList()

	for {
		// Read channel name
		name, err := r.ReadString()
		if err != nil {
			return nil, err
		}

		// Empty name marks end of channel list
		if name == "" {
			break
		}

		// Read channel properties
		pixelType, err := r.ReadUint32()
		if err != nil {
			return nil, err
		}

		pLinear, err := r.ReadByte()
		if err != nil {
			return nil, err
		}

		// Skip 3 reserved bytes
		if err := r.Skip(3); err != nil {
			return nil, err
		}

		xSampling, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}

		ySampling, err := r.ReadInt32()
		if err != nil {
			return nil, err
		}

		cl.Add(Channel{
			Name:      name,
			Type:      PixelType(pixelType),
			PLinear:   pLinear != 0,
			XSampling: xSampling,
			YSampling: ySampling,
		})
	}

	return cl, nil
}

// WriteChannelList writes a channel list to the writer.
func WriteChannelList(w *xdr.BufferWriter, cl *ChannelList) {
	for _, c := range cl.channels {
		w.WriteString(c.Name)
		w.WriteUint32(uint32(c.Type))
		if c.PLinear {
			w.WriteUint8(1)
		} else {
			w.WriteUint8(0)
		}
		// 3 reserved bytes
		w.WriteUint8(0)
		w.WriteUint8(0)
		w.WriteUint8(0)
		w.WriteInt32(c.XSampling)
		w.WriteInt32(c.YSampling)
	}
	// Terminating null byte
	w.WriteByte(0)
}

// BytesPerPixel returns the total bytes needed per pixel for all channels.
// This accounts for subsampling.
func (cl *ChannelList) BytesPerPixel() int {
	total := 0
	for _, c := range cl.channels {
		total += c.Type.Size()
	}
	return total
}

// BytesPerScanline returns bytes needed for one scanline of the given width.
func (cl *ChannelList) BytesPerScanline(width int) int {
	total := 0
	for _, c := range cl.channels {
		// Account for subsampling
		sampledWidth := (width + int(c.XSampling) - 1) / int(c.XSampling)
		total += sampledWidth * c.Type.Size()
	}
	return total
}
