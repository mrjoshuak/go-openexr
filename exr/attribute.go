package exr

import (
	"errors"
	"fmt"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// Compression defines the compression method for pixel data.
type Compression uint8

const (
	// CompressionNone stores uncompressed data.
	CompressionNone Compression = 0
	// CompressionRLE uses run-length encoding.
	CompressionRLE Compression = 1
	// CompressionZIPS uses zlib compression on single scanlines.
	CompressionZIPS Compression = 2
	// CompressionZIP uses zlib compression on 16 scanlines.
	CompressionZIP Compression = 3
	// CompressionPIZ uses wavelet compression.
	CompressionPIZ Compression = 4
	// CompressionPXR24 uses 24-bit float conversion with zlib.
	CompressionPXR24 Compression = 5
	// CompressionB44 uses 4x4 block lossy compression.
	CompressionB44 Compression = 6
	// CompressionB44A uses B44 with flat area detection.
	CompressionB44A Compression = 7
	// CompressionDWAA uses DCT-based lossy compression (32 scanlines).
	CompressionDWAA Compression = 8
	// CompressionDWAB uses DCT-based lossy compression (256 scanlines).
	CompressionDWAB Compression = 9
	// CompressionHTJ2K256 uses lossless High-Throughput JPEG 2000, 256 scanlines
	// per chunk. The code-block size is 128x32 for both HTJ2K codecs; only the
	// scanline grouping differs.
	CompressionHTJ2K256 Compression = 10
	// CompressionHTJ2K32 uses lossless High-Throughput JPEG 2000, 32 scanlines
	// per chunk. More efficient for partial buffer access.
	CompressionHTJ2K32 Compression = 11
)

// String returns a string representation of the compression type.
func (c Compression) String() string {
	switch c {
	case CompressionNone:
		return "none"
	case CompressionRLE:
		return "rle"
	case CompressionZIPS:
		return "zips"
	case CompressionZIP:
		return "zip"
	case CompressionPIZ:
		return "piz"
	case CompressionPXR24:
		return "pxr24"
	case CompressionB44:
		return "b44"
	case CompressionB44A:
		return "b44a"
	case CompressionDWAA:
		return "dwaa"
	case CompressionDWAB:
		return "dwab"
	case CompressionHTJ2K256:
		return "htj2k256"
	case CompressionHTJ2K32:
		return "htj2k32"
	default:
		return "unknown"
	}
}

// ScanlinesPerChunk returns the number of scanlines grouped together
// for this compression type.
func (c Compression) ScanlinesPerChunk() int {
	switch c {
	case CompressionNone, CompressionRLE, CompressionZIPS:
		return 1
	case CompressionZIP, CompressionPXR24:
		return 16
	case CompressionPIZ, CompressionB44, CompressionB44A, CompressionDWAA:
		return 32
	case CompressionDWAB:
		return 256
	case CompressionHTJ2K256:
		return 256
	case CompressionHTJ2K32:
		// The 256 and 32 in these codec names are the scanline grouping, not
		// the code-block size: OpenEXR's CompressionDesc table gives
		// htj2k256 numScanlines 256 and htj2k32 numScanlines 32
		// (ImfCompression.cpp). Reporting 256 here wrote a htj2k32 file as one
		// chunk of up to 256 lines, which the reference reads as a chunk of the
		// wrong height and rejects.
		return 32
	default:
		return 1
	}
}

// IsLossy returns true if the compression is lossy.
func (c Compression) IsLossy() bool {
	return c == CompressionPXR24 || c == CompressionB44 ||
		c == CompressionB44A || c == CompressionDWAA || c == CompressionDWAB
}

// LineOrder defines the order of scanlines in the file.
type LineOrder uint8

const (
	// LineOrderIncreasing stores scanlines from top to bottom (y=0 first).
	LineOrderIncreasing LineOrder = 0
	// LineOrderDecreasing stores scanlines from bottom to top (y=max first).
	LineOrderDecreasing LineOrder = 1
	// LineOrderRandom allows scanlines in any order (for tiled images).
	LineOrderRandom LineOrder = 2
)

// String returns a string representation of the line order.
func (lo LineOrder) String() string {
	switch lo {
	case LineOrderIncreasing:
		return "increasing_y"
	case LineOrderDecreasing:
		return "decreasing_y"
	case LineOrderRandom:
		return "random_y"
	default:
		return "unknown"
	}
}

// EnvMap defines environment map types.
type EnvMap uint8

const (
	// EnvMapLatLong is a latitude-longitude environment map.
	EnvMapLatLong EnvMap = 0
	// EnvMapCube is a cube map.
	EnvMapCube EnvMap = 1
)

// TileDescription describes tile dimensions and level modes.
type TileDescription struct {
	XSize        uint32
	YSize        uint32
	Mode         LevelMode
	RoundingMode LevelRoundingMode
}

// LevelMode defines how multi-resolution levels are stored.
type LevelMode uint8

const (
	// LevelModeOne stores a single resolution level.
	LevelModeOne LevelMode = 0
	// LevelModeMipmap stores power-of-2 mipmap levels.
	LevelModeMipmap LevelMode = 1
	// LevelModeRipmap stores independent X and Y resolution levels.
	LevelModeRipmap LevelMode = 2
)

// LevelRoundingMode defines how level sizes are rounded.
type LevelRoundingMode uint8

const (
	// LevelRoundDown rounds level sizes down.
	LevelRoundDown LevelRoundingMode = 0
	// LevelRoundUp rounds level sizes up.
	LevelRoundUp LevelRoundingMode = 1
)

// Attribute errors
var (
	ErrUnknownAttributeType = errors.New("exr: unknown attribute type")
	ErrAttributeNotFound    = errors.New("exr: attribute not found")
	ErrInvalidAttribute     = errors.New("exr: invalid attribute value")
)

// AttributeType identifies the type of an attribute.
type AttributeType string

// Standard attribute types
const (
	AttrTypeBox2i          AttributeType = "box2i"
	AttrTypeBox2f          AttributeType = "box2f"
	AttrTypeChlist         AttributeType = "chlist"
	AttrTypeChromaticities AttributeType = "chromaticities"
	AttrTypeCompression    AttributeType = "compression"
	AttrTypeDouble         AttributeType = "double"
	AttrTypeEnvmap         AttributeType = "envmap"
	AttrTypeFloat          AttributeType = "float"
	AttrTypeFloatVector    AttributeType = "floatvector"
	AttrTypeInt            AttributeType = "int"
	AttrTypeKeycode        AttributeType = "keycode"
	AttrTypeLineOrder      AttributeType = "lineOrder"
	AttrTypeDeepImageState AttributeType = "deepImageState"
	AttrTypeM33d           AttributeType = "m33d"
	AttrTypeM33f           AttributeType = "m33f"
	AttrTypeM44d           AttributeType = "m44d"
	AttrTypeM44f           AttributeType = "m44f"
	AttrTypePreview        AttributeType = "preview"
	AttrTypeRational       AttributeType = "rational"
	AttrTypeString         AttributeType = "string"
	AttrTypeStringVector   AttributeType = "stringvector"
	AttrTypeTileDesc       AttributeType = "tiledesc"
	AttrTypeTimecode       AttributeType = "timecode"
	AttrTypeV2d            AttributeType = "v2d"
	AttrTypeV2f            AttributeType = "v2f"
	AttrTypeV2i            AttributeType = "v2i"
	AttrTypeV3d            AttributeType = "v3d"
	AttrTypeV3f            AttributeType = "v3f"
	AttrTypeV3i            AttributeType = "v3i"
)

// Attribute represents a single header attribute.
type Attribute struct {
	Name  string
	Type  AttributeType
	Value interface{}
}

// ReadAttribute reads a single attribute from the reader.
// Returns nil when the header terminator (empty name) is reached.
func ReadAttribute(r *xdr.Reader) (*Attribute, error) {
	// Read attribute name
	name, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	// Empty name marks end of header
	if name == "" {
		return nil, nil
	}

	// Read attribute type
	typeName, err := r.ReadString()
	if err != nil {
		return nil, err
	}

	// Read attribute size
	size, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}

	attr := &Attribute{
		Name: name,
		Type: AttributeType(typeName),
	}

	// Read attribute value based on type
	switch attr.Type {
	case AttrTypeBox2i:
		attr.Value, err = ReadBox2i(r)
	case AttrTypeBox2f:
		attr.Value, err = ReadBox2f(r)
	case AttrTypeChlist:
		attr.Value, err = ReadChannelList(r)
	case AttrTypeChromaticities:
		attr.Value, err = ReadChromaticities(r)
	case AttrTypeCompression:
		b, e := r.ReadByte()
		attr.Value, err = Compression(b), e
	case AttrTypeDouble:
		attr.Value, err = r.ReadFloat64()
	case AttrTypeEnvmap:
		b, e := r.ReadByte()
		attr.Value, err = EnvMap(b), e
	case AttrTypeFloat:
		attr.Value, err = r.ReadFloat32()
	case AttrTypeInt:
		attr.Value, err = r.ReadInt32()
	case AttrTypeKeycode:
		attr.Value, err = ReadKeyCode(r)
	case AttrTypeLineOrder:
		b, e := r.ReadByte()
		attr.Value, err = LineOrder(b), e
	case AttrTypeDeepImageState:
		b, e := r.ReadByte()
		attr.Value, err = DeepImageState(b), e
	case AttrTypeM33f:
		attr.Value, err = ReadM33f(r)
	case AttrTypeM44f:
		attr.Value, err = ReadM44f(r)
	case AttrTypePreview:
		attr.Value, err = ReadPreview(r)
	case AttrTypeRational:
		attr.Value, err = ReadRational(r)
	case AttrTypeString:
		// String attribute: read size bytes as string (no null terminator)
		b, e := r.ReadBytes(int(size))
		if e == nil {
			attr.Value = string(b)
		}
		err = e
	case AttrTypeStringVector:
		attr.Value, err = readStringVector(r, int(size))
	case AttrTypeTileDesc:
		attr.Value, err = readTileDescription(r)
	case AttrTypeTimecode:
		attr.Value, err = ReadTimeCode(r)
	case AttrTypeV2i:
		attr.Value, err = ReadV2i(r)
	case AttrTypeV2f:
		attr.Value, err = ReadV2f(r)
	case AttrTypeV2d:
		attr.Value, err = ReadV2d(r)
	case AttrTypeV3i:
		attr.Value, err = ReadV3i(r)
	case AttrTypeV3f:
		attr.Value, err = ReadV3f(r)
	case AttrTypeV3d:
		attr.Value, err = ReadV3d(r)
	case AttrTypeM33d:
		attr.Value, err = ReadM33d(r)
	case AttrTypeM44d:
		attr.Value, err = ReadM44d(r)
	case AttrTypeFloatVector:
		attr.Value, err = ReadFloatVector(r, int(size))
	default:
		// Unknown attribute type: read raw bytes
		rawBytes, e := r.ReadBytes(int(size))
		if e == nil {
			attr.Value = rawBytes
		}
		err = e
	}

	if err != nil {
		return nil, err
	}

	return attr, nil
}

// WriteAttribute writes an attribute to the writer.
func WriteAttribute(w *xdr.BufferWriter, attr *Attribute) error {
	// Write name and type
	w.WriteString(attr.Name)
	w.WriteString(string(attr.Type))

	// Write value to temporary buffer to get size
	valueWriter := xdr.NewBufferWriter(256)
	if err := writeAttributeValue(valueWriter, attr); err != nil {
		return err
	}

	// Write size and value
	w.WriteInt32(int32(valueWriter.Len()))
	w.WriteBytes(valueWriter.Bytes())

	return nil
}

// writeAttributeValue writes the value portion of an attribute to the buffer.
// The type-specific encoding is determined by the attribute's Type field.
func writeAttributeValue(w *xdr.BufferWriter, attr *Attribute) error {
	switch attr.Type {
	case AttrTypeBox2i:
		WriteBox2i(w, attr.Value.(Box2i))
	case AttrTypeBox2f:
		WriteBox2f(w, attr.Value.(Box2f))
	case AttrTypeChlist:
		WriteChannelList(w, attr.Value.(*ChannelList))
	case AttrTypeChromaticities:
		WriteChromaticities(w, attr.Value.(Chromaticities))
	case AttrTypeCompression:
		w.WriteByte(byte(attr.Value.(Compression)))
	case AttrTypeDouble:
		w.WriteFloat64(attr.Value.(float64))
	case AttrTypeEnvmap:
		w.WriteByte(byte(attr.Value.(EnvMap)))
	case AttrTypeFloat:
		w.WriteFloat32(attr.Value.(float32))
	case AttrTypeInt:
		w.WriteInt32(attr.Value.(int32))
	case AttrTypeKeycode:
		WriteKeyCode(w, attr.Value.(KeyCode))
	case AttrTypeLineOrder:
		w.WriteByte(byte(attr.Value.(LineOrder)))
	case AttrTypeDeepImageState:
		w.WriteByte(byte(attr.Value.(DeepImageState)))
	case AttrTypeM33f:
		WriteM33f(w, attr.Value.(M33f))
	case AttrTypeM44f:
		WriteM44f(w, attr.Value.(M44f))
	case AttrTypePreview:
		WritePreview(w, attr.Value.(Preview))
	case AttrTypeRational:
		WriteRational(w, attr.Value.(Rational))
	case AttrTypeString:
		s := attr.Value.(string)
		w.WriteBytes([]byte(s))
	case AttrTypeStringVector:
		writeStringVector(w, attr.Value.([]string))
	case AttrTypeTileDesc:
		writeTileDescription(w, attr.Value.(TileDescription))
	case AttrTypeTimecode:
		WriteTimeCode(w, attr.Value.(TimeCode))
	case AttrTypeV2i:
		WriteV2i(w, attr.Value.(V2i))
	case AttrTypeV2f:
		WriteV2f(w, attr.Value.(V2f))
	case AttrTypeV2d:
		WriteV2d(w, attr.Value.(V2d))
	case AttrTypeV3i:
		WriteV3i(w, attr.Value.(V3i))
	case AttrTypeV3f:
		WriteV3f(w, attr.Value.(V3f))
	case AttrTypeV3d:
		WriteV3d(w, attr.Value.(V3d))
	case AttrTypeM33d:
		WriteM33d(w, attr.Value.(M33d))
	case AttrTypeM44d:
		WriteM44d(w, attr.Value.(M44d))
	case AttrTypeFloatVector:
		WriteFloatVector(w, attr.Value.(FloatVector))
	default:
		// Raw bytes for unknown types
		if bytes, ok := attr.Value.([]byte); ok {
			w.WriteBytes(bytes)
		} else {
			return fmt.Errorf("%w: %s", ErrUnknownAttributeType, attr.Type)
		}
	}
	return nil
}

// readStringVector reads a string vector attribute from the XDR reader.
// Each string is encoded as a 4-byte length followed by the string bytes.
func readStringVector(r *xdr.Reader, size int) ([]string, error) {
	if size == 0 {
		return []string{}, nil
	}

	// Read raw bytes
	data, err := r.ReadBytes(size)
	if err != nil {
		return nil, err
	}

	// Parse string vector format:
	// Each string is: length (4 bytes) + string bytes
	result := make([]string, 0)
	reader := xdr.NewReader(data)

	for reader.Len() > 0 {
		strLen, err := reader.ReadInt32()
		if err != nil {
			return nil, err
		}
		strBytes, err := reader.ReadBytes(int(strLen))
		if err != nil {
			return nil, err
		}
		result = append(result, string(strBytes))
	}

	return result, nil
}

// writeStringVector writes a string vector to the buffer.
// Each string is written as a 4-byte length followed by the string bytes.
func writeStringVector(w *xdr.BufferWriter, strings []string) {
	for _, s := range strings {
		w.WriteInt32(int32(len(s)))
		w.WriteBytes([]byte(s))
	}
}

// readTileDescription reads a tile description from the XDR reader.
// The format is: xSize (4), ySize (4), mode (1 byte with level and rounding).
func readTileDescription(r *xdr.Reader) (TileDescription, error) {
	var td TileDescription
	var err error

	td.XSize, err = r.ReadUint32()
	if err != nil {
		return td, err
	}

	td.YSize, err = r.ReadUint32()
	if err != nil {
		return td, err
	}

	mode, err := r.ReadByte()
	if err != nil {
		return td, err
	}

	td.Mode = LevelMode(mode & 0x0F)
	td.RoundingMode = LevelRoundingMode((mode >> 4) & 0x0F)

	return td, nil
}

// writeTileDescription writes a tile description to the buffer.
// The format is: xSize (4), ySize (4), mode (1 byte with level and rounding).
func writeTileDescription(w *xdr.BufferWriter, td TileDescription) {
	w.WriteUint32(td.XSize)
	w.WriteUint32(td.YSize)
	mode := byte(td.Mode) | (byte(td.RoundingMode) << 4)
	w.WriteByte(mode)
}
