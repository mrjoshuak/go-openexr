// Package exr provides ACES (Academy Color Encoding System) file support.
//
// The Academy Image Interchange Framework (ACES) defines a subset of OpenEXR
// with specific constraints for color-accurate image interchange:
//   - Images are stored as scanlines (no tiles)
//   - Only R,G,B or Y,RY,BY channels (plus optional alpha)
//   - Only None, PIZ, or B44A compression
//   - Must have ACES chromaticities in the header
//
// AcesOutputFile writes ACES-compliant EXR files with these restrictions.
// AcesInputFile reads EXR files and converts pixel data to ACES RGB color space,
// performing chromatic adaptation when necessary.
package exr

import (
	"errors"
	"io"
	"math"
)

// ACES chromaticity constants
// These define the ACES RGB primaries and D60 white point.
var (
	// ACESRedPrimary is the ACES red primary in CIE xy coordinates.
	ACESRedPrimary = V2f{X: 0.73470, Y: 0.26530}

	// ACESGreenPrimary is the ACES green primary in CIE xy coordinates.
	ACESGreenPrimary = V2f{X: 0.00000, Y: 1.00000}

	// ACESBluePrimary is the ACES blue primary in CIE xy coordinates.
	ACESBluePrimary = V2f{X: 0.00010, Y: -0.07700}

	// ACESWhitePoint is the ACES white point (D60) in CIE xy coordinates.
	ACESWhitePoint = V2f{X: 0.32168, Y: 0.33767}
)

// ACESChromaticities returns the standard ACES chromaticities.
func ACESChromaticities() Chromaticities {
	return Chromaticities{
		RedX: ACESRedPrimary.X, RedY: ACESRedPrimary.Y,
		GreenX: ACESGreenPrimary.X, GreenY: ACESGreenPrimary.Y,
		BlueX: ACESBluePrimary.X, BlueY: ACESBluePrimary.Y,
		WhiteX: ACESWhitePoint.X, WhiteY: ACESWhitePoint.Y,
	}
}

// ACES-specific errors
var (
	ErrInvalidACESCompression = errors.New("exr: invalid compression type for ACES file (must be None, PIZ, or B44A)")
	ErrInvalidACESChannels    = errors.New("exr: invalid channels for ACES file (must be R,G,B or Y,RY,BY with optional A)")
	ErrACESTiledNotAllowed    = errors.New("exr: tiled images are not allowed in ACES files")
	ErrACESNoFrameBuffer      = errors.New("exr: no frame buffer set")
)

// Header attribute name for adopted neutral (white point adaptation).
const AttrNameAdoptedNeutral = "adoptedNeutral"

// chromaticitiesEqual checks if two chromaticities are equal within tolerance.
func chromaticitiesEqual(a, b Chromaticities) bool {
	const epsilon = 1e-5
	return math.Abs(float64(a.RedX-b.RedX)) < epsilon &&
		math.Abs(float64(a.RedY-b.RedY)) < epsilon &&
		math.Abs(float64(a.GreenX-b.GreenX)) < epsilon &&
		math.Abs(float64(a.GreenY-b.GreenY)) < epsilon &&
		math.Abs(float64(a.BlueX-b.BlueX)) < epsilon &&
		math.Abs(float64(a.BlueY-b.BlueY)) < epsilon &&
		math.Abs(float64(a.WhiteX-b.WhiteX)) < epsilon &&
		math.Abs(float64(a.WhiteY-b.WhiteY)) < epsilon
}

// v2fEqual checks if two V2f are equal within tolerance.
func v2fEqual(a, b V2f) bool {
	const epsilon = 1e-5
	return math.Abs(float64(a.X-b.X)) < epsilon &&
		math.Abs(float64(a.Y-b.Y)) < epsilon
}

// ValidateACESCompression checks if a compression method is allowed for ACES files.
func ValidateACESCompression(c Compression) error {
	switch c {
	case CompressionNone, CompressionPIZ, CompressionB44A:
		return nil
	default:
		return ErrInvalidACESCompression
	}
}

// ValidateACESChannels checks if a channel list is valid for ACES files.
// Valid configurations are:
//   - R, G, B (with optional A)
//   - Y, RY, BY (with optional A)
func ValidateACESChannels(cl *ChannelList) error {
	if cl == nil {
		return ErrInvalidACESChannels
	}

	hasR := cl.Get("R") != nil
	hasG := cl.Get("G") != nil
	hasB := cl.Get("B") != nil
	hasY := cl.Get("Y") != nil
	hasRY := cl.Get("RY") != nil
	hasBY := cl.Get("BY") != nil
	hasA := cl.Get("A") != nil

	// Check for RGB mode
	rgbMode := hasR && hasG && hasB

	// Check for YC mode (luminance + chroma)
	ycMode := hasY && hasRY && hasBY

	if !rgbMode && !ycMode {
		return ErrInvalidACESChannels
	}

	// Count total channels
	expectedCount := 3
	if hasA {
		expectedCount = 4
	}

	if cl.Len() != expectedCount {
		return ErrInvalidACESChannels
	}

	return nil
}

// RGBtoXYZ computes the 4x4 matrix that converts RGB values to CIE XYZ
// for the given chromaticities. The Y value of the white point equals 1.
func RGBtoXYZ(c Chromaticities) M44f {
	// Convert chromaticity xy to XYZ (assuming Y=1 for each primary)
	redX := float64(c.RedX)
	redY := float64(c.RedY)
	greenX := float64(c.GreenX)
	greenY := float64(c.GreenY)
	blueX := float64(c.BlueX)
	blueY := float64(c.BlueY)
	whiteX := float64(c.WhiteX)
	whiteY := float64(c.WhiteY)

	// Compute XYZ coordinates of primaries (assuming Y=1)
	// X = x/y, Y = 1, Z = (1-x-y)/y
	rX := redX / redY
	rY := 1.0
	rZ := (1.0 - redX - redY) / redY

	gX := greenX / greenY
	gY := 1.0
	gZ := (1.0 - greenX - greenY) / greenY

	bX := blueX / blueY
	bY := 1.0
	bZ := (1.0 - blueX - blueY) / blueY

	// Compute XYZ of white point
	wX := whiteX / whiteY
	wY := 1.0
	wZ := (1.0 - whiteX - whiteY) / whiteY

	// Build the matrix of primary XYZ values
	// [rX gX bX]   [Sr]   [wX]
	// [rY gY bY] * [Sg] = [wY]
	// [rZ gZ bZ]   [Sb]   [wZ]
	// We need to solve for [Sr, Sg, Sb]

	// Invert the primary matrix to get scale factors
	// Using Cramer's rule for 3x3 matrix inversion
	det := rX*(gY*bZ-bY*gZ) - gX*(rY*bZ-bY*rZ) + bX*(rY*gZ-gY*rZ)

	if math.Abs(det) < 1e-10 {
		return Identity44()
	}

	invDet := 1.0 / det

	// Compute inverse matrix elements
	i00 := (gY*bZ - bY*gZ) * invDet
	i01 := (bX*gZ - gX*bZ) * invDet
	i02 := (gX*bY - bX*gY) * invDet
	i10 := (bY*rZ - rY*bZ) * invDet
	i11 := (rX*bZ - bX*rZ) * invDet
	i12 := (bX*rY - rX*bY) * invDet
	i20 := (rY*gZ - gY*rZ) * invDet
	i21 := (gX*rZ - rX*gZ) * invDet
	i22 := (rX*gY - gX*rY) * invDet

	// Scale factors
	sr := i00*wX + i01*wY + i02*wZ
	sg := i10*wX + i11*wY + i12*wZ
	sb := i20*wX + i21*wY + i22*wZ

	// Build the final RGB to XYZ matrix
	return M44f{
		float32(sr * rX), float32(sg * gX), float32(sb * bX), 0,
		float32(sr * rY), float32(sg * gY), float32(sb * bY), 0,
		float32(sr * rZ), float32(sg * gZ), float32(sb * bZ), 0,
		0, 0, 0, 1,
	}
}

// XYZtoRGB computes the 4x4 matrix that converts CIE XYZ values to RGB
// for the given chromaticities. This is the inverse of RGBtoXYZ.
func XYZtoRGB(c Chromaticities) M44f {
	rgbToXYZ := RGBtoXYZ(c)
	return inverse44(rgbToXYZ)
}

// ChromaticAdaptation computes the Bradford chromatic adaptation transform matrix
// that converts colors from a source white point to a destination white point.
func ChromaticAdaptation(srcWhite, dstWhite V2f) M44f {
	// Bradford cone primary matrix, XYZ to LMS.
	//
	// The literals are in the convention this file multiplies in — row-major,
	// applied as M*v, so row 0 produces L from X, Y and Z. Imath writes the
	// transpose of this because it composes row vectors as v*M, and until
	// v1.4.14 these constants were copied in that form and then applied as
	// M*v. The result was a plausible-looking file with the wrong colours:
	// Rec.709 (0.8, 0.2, 0.1) came out as (0.3179, 0.1019, 0.0796) where the
	// reference exr2aces produces (0.4460, 0.2441, 0.1234), an error of up to
	// 58% per channel with nothing reported.
	bradfordCPM := M44f{
		0.895100, 0.266400, -0.161400, 0,
		-0.750200, 1.713500, 0.036700, 0,
		0.038900, -0.068500, 1.029600, 0,
		0, 0, 0, 1,
	}

	// Inverse Bradford matrix, LMS back to XYZ, in the same convention.
	inverseBradfordCPM := M44f{
		0.986993, -0.147054, 0.159963, 0,
		0.432305, 0.518360, 0.049291, 0,
		-0.008529, 0.040043, 0.968487, 0,
		0, 0, 0, 1,
	}

	// Convert white points to XYZ (Y=1)
	srcX := float64(srcWhite.X) / float64(srcWhite.Y)
	srcY := 1.0
	srcZ := (1.0 - float64(srcWhite.X) - float64(srcWhite.Y)) / float64(srcWhite.Y)

	dstX := float64(dstWhite.X) / float64(dstWhite.Y)
	dstY := 1.0
	dstZ := (1.0 - float64(dstWhite.X) - float64(dstWhite.Y)) / float64(dstWhite.Y)

	// Transform to cone response space
	srcLMS := transformV3(bradfordCPM, srcX, srcY, srcZ)
	dstLMS := transformV3(bradfordCPM, dstX, dstY, dstZ)

	// Compute ratios
	ratioL := dstLMS[0] / srcLMS[0]
	ratioM := dstLMS[1] / srcLMS[1]
	ratioS := dstLMS[2] / srcLMS[2]

	// Build diagonal scaling matrix
	ratioMat := M44f{
		float32(ratioL), 0, 0, 0,
		0, float32(ratioM), 0, 0,
		0, 0, float32(ratioS), 0,
		0, 0, 0, 1,
	}

	// Final adaptation matrix. Applied as M*v the rightmost factor acts first,
	// so it reads right to left: take XYZ to LMS, scale by the white-point
	// ratio, come back. Imath composes this the other way round because it
	// multiplies row vectors, and that order was copied here too.
	temp := multiply44(inverseBradfordCPM, ratioMat)
	return multiply44(temp, bradfordCPM)
}

// Matrix utility functions

// multiply44 multiplies two 4x4 matrices.
func multiply44(a, b M44f) M44f {
	var result M44f
	for i := 0; i < 4; i++ {
		for j := 0; j < 4; j++ {
			var sum float32
			for k := 0; k < 4; k++ {
				sum += a[i*4+k] * b[k*4+j]
			}
			result[i*4+j] = sum
		}
	}
	return result
}

// inverse44 computes the inverse of a 4x4 matrix.
func inverse44(m M44f) M44f {
	// Using the classical adjugate method
	// For a 4x4 matrix, compute cofactors and divide by determinant

	// This is a general 4x4 inverse, but since we're dealing with
	// 3x3 transformation matrices embedded in 4x4 (with [0,0,0,1] last row/col),
	// we can use a simpler approach

	// Extract the 3x3 part
	a := float64(m[0])
	b := float64(m[1])
	c := float64(m[2])
	d := float64(m[4])
	e := float64(m[5])
	f := float64(m[6])
	g := float64(m[8])
	h := float64(m[9])
	i := float64(m[10])

	// Compute determinant of 3x3 part
	det := a*(e*i-f*h) - b*(d*i-f*g) + c*(d*h-e*g)

	if math.Abs(det) < 1e-10 {
		return Identity44()
	}

	invDet := 1.0 / det

	// Compute inverse of 3x3 part
	return M44f{
		float32((e*i - f*h) * invDet), float32((c*h - b*i) * invDet), float32((b*f - c*e) * invDet), 0,
		float32((f*g - d*i) * invDet), float32((a*i - c*g) * invDet), float32((c*d - a*f) * invDet), 0,
		float32((d*h - e*g) * invDet), float32((b*g - a*h) * invDet), float32((a*e - b*d) * invDet), 0,
		0, 0, 0, 1,
	}
}

// transformV3 transforms a 3D vector by a 4x4 matrix (treating it as 3x3).
func transformV3(m M44f, x, y, z float64) [3]float64 {
	return [3]float64{
		float64(m[0])*x + float64(m[1])*y + float64(m[2])*z,
		float64(m[4])*x + float64(m[5])*y + float64(m[6])*z,
		float64(m[8])*x + float64(m[9])*y + float64(m[10])*z,
	}
}

// AcesInputFile reads EXR files and converts pixels to ACES RGB color space.
type AcesInputFile struct {
	file           *File
	scanlineReader *ScanlineReader
	tiledReader    *TiledReader
	header         *Header
	frameBuffer    *FrameBuffer
	dataWindow     Box2i
	isTiled        bool

	// Color conversion state
	mustConvertColor bool
	fileToACES       M44f
}

// OpenAcesInputFile opens an EXR file for reading as ACES.
// The file's pixels will be converted to ACES RGB color space if necessary.
// Both scanline and tiled EXR files are supported.
func OpenAcesInputFile(r io.ReaderAt, size int64) (*AcesInputFile, error) {
	file, err := OpenReader(r, size)
	if err != nil {
		return nil, err
	}

	header := file.Header(0)
	if header == nil {
		return nil, ErrInvalidHeader
	}

	af := &AcesInputFile{
		file:       file,
		header:     header,
		dataWindow: header.DataWindow(),
		isTiled:    header.IsTiled(),
	}

	// Create the appropriate reader
	if af.isTiled {
		af.tiledReader, err = NewTiledReader(file)
		if err != nil {
			return nil, err
		}
	} else {
		af.scanlineReader, err = NewScanlineReader(file)
		if err != nil {
			return nil, err
		}
	}

	af.initColorConversion()
	return af, nil
}

// initColorConversion sets up the color space conversion matrix if needed.
func (af *AcesInputFile) initColorConversion() {
	header := af.header
	acesChr := ACESChromaticities()

	// Get file chromaticities (default to Rec.709 if not specified)
	fileChr := DefaultChromaticities()
	if header.Has("chromaticities") {
		attr := header.Get("chromaticities")
		if attr != nil {
			fileChr = attr.Value.(Chromaticities)
		}
	}

	// Get adopted neutral (white point) if present
	fileNeutral := V2f{X: fileChr.WhiteX, Y: fileChr.WhiteY}
	if header.Has(AttrNameAdoptedNeutral) {
		attr := header.Get(AttrNameAdoptedNeutral)
		if attr != nil {
			fileNeutral = attr.Value.(V2f)
			// Update chromaticities white for RGBtoXYZ computation
			fileChr.WhiteX = fileNeutral.X
			fileChr.WhiteY = fileNeutral.Y
		}
	}

	acesNeutral := V2f{X: acesChr.WhiteX, Y: acesChr.WhiteY}

	// Check if file is already in ACES color space
	acesPrimaries := ACESChromaticities()
	fileMatchesACES := v2fEqual(V2f{fileChr.RedX, fileChr.RedY}, V2f{acesPrimaries.RedX, acesPrimaries.RedY}) &&
		v2fEqual(V2f{fileChr.GreenX, fileChr.GreenY}, V2f{acesPrimaries.GreenX, acesPrimaries.GreenY}) &&
		v2fEqual(V2f{fileChr.BlueX, fileChr.BlueY}, V2f{acesPrimaries.BlueX, acesPrimaries.BlueY}) &&
		v2fEqual(V2f{fileChr.WhiteX, fileChr.WhiteY}, acesNeutral)

	if fileMatchesACES {
		// File already contains ACES data, no conversion necessary
		return
	}

	af.mustConvertColor = true

	// Build conversion matrix: file RGB -> XYZ -> Bradford adaptation -> ACES RGB
	fileToXYZ := RGBtoXYZ(fileChr)
	bradfordTrans := ChromaticAdaptation(fileNeutral, acesNeutral)
	xyzToACES := XYZtoRGB(acesChr)

	// Combine, right to left as M*v requires: the file's RGB becomes XYZ, the
	// white point is adapted, and the result becomes ACES RGB. Composing these
	// left to right — the row-vector order — was the third of the three places
	// this file mixed conventions.
	temp := multiply44(xyzToACES, bradfordTrans)
	af.fileToACES = multiply44(temp, fileToXYZ)
}

// Header returns the header of the file.
func (af *AcesInputFile) Header() *Header {
	return af.header
}

// DataWindow returns the data window.
func (af *AcesInputFile) DataWindow() Box2i {
	return af.dataWindow
}

// DisplayWindow returns the display window.
func (af *AcesInputFile) DisplayWindow() Box2i {
	return af.header.DisplayWindow()
}

// SetFrameBuffer sets the frame buffer to read pixels into.
func (af *AcesInputFile) SetFrameBuffer(fb *FrameBuffer) {
	af.frameBuffer = fb
	if af.isTiled {
		af.tiledReader.SetFrameBuffer(fb)
	} else {
		af.scanlineReader.SetFrameBuffer(fb)
	}
}

// IsTiled returns true if the input file is tiled.
func (af *AcesInputFile) IsTiled() bool {
	return af.isTiled
}

// ReadPixels reads scanlines from y1 to y2 (inclusive) into the frame buffer.
// The pixel data is converted to ACES RGB color space if necessary.
// For tiled files, this reads all tiles that overlap with the specified scanline range.
func (af *AcesInputFile) ReadPixels(y1, y2 int) error {
	if af.frameBuffer == nil {
		return ErrACESNoFrameBuffer
	}

	// Read pixels using the appropriate reader
	if af.isTiled {
		// For tiled files, read all tiles that overlap with the requested scanlines
		td := af.tiledReader.TileDescription()
		tileHeight := int(td.YSize)
		tileWidth := int(td.XSize)

		// Calculate which tiles we need
		firstTileY := y1 / tileHeight
		lastTileY := y2 / tileHeight
		numTilesX := af.tiledReader.NumTilesX()

		// Read all required tiles
		for ty := firstTileY; ty <= lastTileY; ty++ {
			for tx := 0; tx < numTilesX; tx++ {
				if err := af.tiledReader.ReadTile(tx, ty); err != nil {
					return err
				}
			}
		}

		// Adjust y1/y2 to actual tile boundaries for color conversion
		y1 = firstTileY * tileHeight
		y2 = (lastTileY+1)*tileHeight - 1
		if y2 > int(af.dataWindow.Max.Y) {
			y2 = int(af.dataWindow.Max.Y)
		}
		_ = tileWidth // suppress unused warning
	} else {
		if err := af.scanlineReader.ReadPixels(y1, y2); err != nil {
			return err
		}
	}

	// If no conversion needed, we're done
	if !af.mustConvertColor {
		return nil
	}

	// Convert pixels to ACES color space
	minX := int(af.dataWindow.Min.X)
	maxX := int(af.dataWindow.Max.X)

	rSlice := af.frameBuffer.Get("R")
	gSlice := af.frameBuffer.Get("G")
	bSlice := af.frameBuffer.Get("B")

	if rSlice == nil || gSlice == nil || bSlice == nil {
		// No RGB channels to convert
		return nil
	}

	for y := y1; y <= y2; y++ {
		for x := minX; x <= maxX; x++ {
			// Get original RGB values
			r := rSlice.GetFloat32(x, y)
			g := gSlice.GetFloat32(x, y)
			b := bSlice.GetFloat32(x, y)

			// Apply color conversion matrix
			acesR := af.fileToACES[0]*r + af.fileToACES[1]*g + af.fileToACES[2]*b
			acesG := af.fileToACES[4]*r + af.fileToACES[5]*g + af.fileToACES[6]*b
			acesB := af.fileToACES[8]*r + af.fileToACES[9]*g + af.fileToACES[10]*b

			// Write converted values
			rSlice.SetFloat32(x, y, acesR)
			gSlice.SetFloat32(x, y, acesG)
			bSlice.SetFloat32(x, y, acesB)
		}
	}

	return nil
}

// NeedsColorConversion returns true if the file requires color space conversion.
func (af *AcesInputFile) NeedsColorConversion() bool {
	return af.mustConvertColor
}

// AcesOutputFile writes ACES-compliant EXR files.
type AcesOutputFile struct {
	writer      *ScanlineWriter
	header      *Header
	frameBuffer *FrameBuffer
	dataWindow  Box2i
}

// AcesOutputOptions specifies options for creating an ACES output file.
type AcesOutputOptions struct {
	// Compression specifies the compression method.
	// Must be CompressionNone, CompressionPIZ, or CompressionB44A.
	// Defaults to CompressionPIZ if not set.
	Compression Compression

	// PixelAspectRatio defaults to 1.0.
	PixelAspectRatio float32

	// ScreenWindowCenter defaults to (0, 0).
	ScreenWindowCenter V2f

	// ScreenWindowWidth defaults to 1.0.
	ScreenWindowWidth float32

	// LineOrder defaults to LineOrderIncreasing.
	LineOrder LineOrder

	// WriteAlpha specifies whether to include an alpha channel.
	WriteAlpha bool
}

// NewAcesOutputFile creates a new ACES-compliant EXR file for writing.
func NewAcesOutputFile(w io.WriteSeeker, width, height int, opts *AcesOutputOptions) (*AcesOutputFile, error) {
	if opts == nil {
		opts = &AcesOutputOptions{}
	}

	// Set defaults
	compression := opts.Compression
	if compression == 0 {
		compression = CompressionPIZ
	}

	// Validate compression
	if err := ValidateACESCompression(compression); err != nil {
		return nil, err
	}

	pixelAspectRatio := opts.PixelAspectRatio
	if pixelAspectRatio == 0 {
		pixelAspectRatio = 1.0
	}

	screenWindowWidth := opts.ScreenWindowWidth
	if screenWindowWidth == 0 {
		screenWindowWidth = 1.0
	}

	// Create header
	header := NewHeader()

	dataWindow := Box2i{Min: V2i{0, 0}, Max: V2i{int32(width - 1), int32(height - 1)}}
	header.SetDataWindow(dataWindow)
	header.SetDisplayWindow(dataWindow)
	header.SetCompression(compression)
	header.SetLineOrder(opts.LineOrder)
	header.SetPixelAspectRatio(pixelAspectRatio)
	header.SetScreenWindowCenter(opts.ScreenWindowCenter)
	header.SetScreenWindowWidth(screenWindowWidth)

	// Add ACES chromaticities
	acesChr := ACESChromaticities()
	header.Set(&Attribute{
		Name:  "chromaticities",
		Type:  AttrTypeChromaticities,
		Value: acesChr,
	})

	// Add adopted neutral (white point)
	header.Set(&Attribute{
		Name:  AttrNameAdoptedNeutral,
		Type:  AttrTypeV2f,
		Value: ACESWhitePoint,
	})

	// Add channels (RGB, with optional A)
	cl := NewChannelList()
	cl.Add(NewChannel("R", PixelTypeHalf))
	cl.Add(NewChannel("G", PixelTypeHalf))
	cl.Add(NewChannel("B", PixelTypeHalf))
	if opts.WriteAlpha {
		cl.Add(NewChannel("A", PixelTypeHalf))
	}
	header.SetChannels(cl)

	// Create the scanline writer
	writer, err := NewScanlineWriter(w, header)
	if err != nil {
		return nil, err
	}

	return &AcesOutputFile{
		writer:     writer,
		header:     header,
		dataWindow: dataWindow,
	}, nil
}

// NewAcesOutputFileFromHeader creates a new ACES output file with a custom header.
// The header is validated and modified to ensure ACES compliance.
func NewAcesOutputFileFromHeader(w io.WriteSeeker, header *Header) (*AcesOutputFile, error) {
	// Validate and enforce ACES restrictions

	// Check for tiles (not allowed)
	if header.IsTiled() {
		return nil, ErrACESTiledNotAllowed
	}

	// Validate compression
	if err := ValidateACESCompression(header.Compression()); err != nil {
		return nil, err
	}

	// Validate channels
	if err := ValidateACESChannels(header.Channels()); err != nil {
		return nil, err
	}

	// Ensure ACES chromaticities are set
	acesChr := ACESChromaticities()
	header.Set(&Attribute{
		Name:  "chromaticities",
		Type:  AttrTypeChromaticities,
		Value: acesChr,
	})

	// Ensure adopted neutral is set
	header.Set(&Attribute{
		Name:  AttrNameAdoptedNeutral,
		Type:  AttrTypeV2f,
		Value: ACESWhitePoint,
	})

	// Create the scanline writer
	writer, err := NewScanlineWriter(w, header)
	if err != nil {
		return nil, err
	}

	return &AcesOutputFile{
		writer:     writer,
		header:     header,
		dataWindow: header.DataWindow(),
	}, nil
}

// Header returns the header of the file.
func (af *AcesOutputFile) Header() *Header {
	return af.header
}

// DataWindow returns the data window.
func (af *AcesOutputFile) DataWindow() Box2i {
	return af.dataWindow
}

// SetFrameBuffer sets the frame buffer to write pixels from.
// The pixel data must already be in ACES RGB color space.
func (af *AcesOutputFile) SetFrameBuffer(fb *FrameBuffer) {
	af.frameBuffer = fb
	af.writer.SetFrameBuffer(fb)
}

// WritePixels writes scanlines from y1 to y2 (inclusive) from the frame buffer.
func (af *AcesOutputFile) WritePixels(y1, y2 int) error {
	if af.frameBuffer == nil {
		return ErrACESNoFrameBuffer
	}
	return af.writer.WritePixels(y1, y2)
}

// Close finalizes the file.
func (af *AcesOutputFile) Close() error {
	return af.writer.Close()
}

// Convenience functions for header manipulation

// SetChromaticities sets the chromaticities attribute on a header.
func SetChromaticities(h *Header, c Chromaticities) {
	h.Set(&Attribute{
		Name:  "chromaticities",
		Type:  AttrTypeChromaticities,
		Value: c,
	})
}

// GetChromaticities returns the chromaticities from a header,
// or the default Rec.709 chromaticities if not present.
func GetChromaticities(h *Header) Chromaticities {
	if h.Has("chromaticities") {
		attr := h.Get("chromaticities")
		if attr != nil {
			return attr.Value.(Chromaticities)
		}
	}
	return DefaultChromaticities()
}

// SetAdoptedNeutral sets the adopted neutral (white point) attribute on a header.
func SetAdoptedNeutral(h *Header, white V2f) {
	h.Set(&Attribute{
		Name:  AttrNameAdoptedNeutral,
		Type:  AttrTypeV2f,
		Value: white,
	})
}

// GetAdoptedNeutral returns the adopted neutral from a header,
// or the chromaticities white point if not present.
func GetAdoptedNeutral(h *Header) V2f {
	if h.Has(AttrNameAdoptedNeutral) {
		attr := h.Get(AttrNameAdoptedNeutral)
		if attr != nil {
			return attr.Value.(V2f)
		}
	}
	chr := GetChromaticities(h)
	return V2f{X: chr.WhiteX, Y: chr.WhiteY}
}

// HasACESChromaticities checks if a header has ACES chromaticities.
func HasACESChromaticities(h *Header) bool {
	if !h.Has("chromaticities") {
		return false
	}
	chr := GetChromaticities(h)
	return chromaticitiesEqual(chr, ACESChromaticities())
}
