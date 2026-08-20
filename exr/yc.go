package exr

import (
	"io"
	"os"

	"github.com/mrjoshuak/go-openexr/half"
)

// YCMode specifies which channels to write in luminance/chroma format.
type YCMode int

const (
	// WriteRGBA writes full RGB + Alpha channels (default, no YC conversion)
	WriteRGBA YCMode = iota
	// WriteY writes luminance only (grayscale)
	WriteY
	// WriteYA writes luminance + alpha
	WriteYA
	// WriteYC writes luminance + chroma (Y, RY, BY) with 2x2 chroma subsampling
	WriteYC
	// WriteYCA writes luminance + chroma + alpha with 2x2 chroma subsampling
	WriteYCA
)

// ITU-R BT.709 coefficients for RGB to YCbCr conversion
// Y  = 0.2126*R + 0.7152*G + 0.0722*B
// Cb = (B - Y) / 1.8556
// Cr = (R - Y) / 1.5748
const (
	kr709 = 0.2126
	kg709 = 0.7152
	kb709 = 0.0722
)

// RGBtoYC converts linear RGB to OpenEXR luminance/chroma (Y, RY, BY).
// Uses ITU-R BT.709 primaries.
// Y is luminance, RY and BY are normalized chroma differences.
func RGBtoYC(r, g, b float32) (y, ry, by float32) {
	// Compute luminance using Rec. 709 coefficients
	y = float32(kr709)*r + float32(kg709)*g + float32(kb709)*b

	// The chroma channels are the differences divided by the luminance, which
	// is what the format stores and what every other reader assumes
	// (ImfRgbaYca RGBAtoYCA: ycaOut.r = (rgbaIn.r - Y) / Y).
	//
	// This used to store the plain differences. That is self-consistent — this
	// library's own round trip recovered the original pixels exactly — and it
	// means something different from what the file says, so the chroma of
	// every YC file written here was wrong for everyone else, and every YC
	// file written elsewhere was read wrongly here. A round trip cannot see a
	// convention that both ends share.
	//
	// Y of zero has no chroma to express, and the reference writes zero there
	// rather than dividing.
	if y == 0 {
		return 0, 0, 0
	}
	ry = (r - y) / y
	by = (b - y) / y

	return y, ry, by
}

// YCtoRGB converts OpenEXR luminance/chroma (Y, RY, BY) to linear RGB.
// Uses ITU-R BT.709 primaries.
func YCtoRGB(y, ry, by float32) (r, g, b float32) {
	// The inverse of RGBtoYC: the chroma channels carry (R-Y)/Y and (B-Y)/Y,
	// so recovering R and B is a multiply, not an add (ImfRgbaYca YCAtoRGBA:
	// rgbaOut.r = (ycaIn.r + 1) * Y).
	r = (ry + 1) * y
	b = (by + 1) * y

	// Reconstruct G from Y = kr*R + kg*G + kb*B
	// G = (Y - kr*R - kb*B) / kg
	g = (y - float32(kr709)*r - float32(kb709)*b) / float32(kg709)

	return r, g, b
}

// IsYCImage returns true if the header describes a luminance/chroma image.
// It checks for Y, RY, BY channels with appropriate subsampling.
func IsYCImage(h *Header) bool {
	if h == nil {
		return false
	}

	channels := h.Channels()
	if channels == nil {
		return false
	}

	hasY := false
	hasRY := false
	hasBY := false

	for i := 0; i < channels.Len(); i++ {
		ch := channels.At(i)
		switch ch.Name {
		case "Y":
			hasY = true
		case "RY":
			// Check for 2x2 subsampling
			if ch.XSampling == 2 && ch.YSampling == 2 {
				hasRY = true
			}
		case "BY":
			// Check for 2x2 subsampling
			if ch.XSampling == 2 && ch.YSampling == 2 {
				hasBY = true
			}
		}
	}

	return hasY && (hasRY || hasBY)
}

// IsLuminanceOnlyImage returns true if the header describes a luminance-only image (grayscale).
func IsLuminanceOnlyImage(h *Header) bool {
	if h == nil {
		return false
	}

	channels := h.Channels()
	if channels == nil {
		return false
	}

	hasY := false
	hasChroma := false

	for i := 0; i < channels.Len(); i++ {
		ch := channels.At(i)
		switch ch.Name {
		case "Y":
			hasY = true
		case "RY", "BY":
			hasChroma = true
		}
	}

	return hasY && !hasChroma
}

// YCOutputFile provides a simple interface for writing luminance/chroma images.
type YCOutputFile struct {
	path   string
	header *Header
	width  int
	height int
	mode   YCMode
}

// NewYCOutputFile creates a new YC output file.
func NewYCOutputFile(path string, width, height int, mode YCMode) (*YCOutputFile, error) {
	h := NewScanlineHeader(width, height)
	h.SetCompression(CompressionZIP)

	// Set up channels based on mode
	channels := NewChannelList()

	switch mode {
	case WriteY:
		channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	case WriteYA:
		channels.Add(Channel{Name: "A", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	case WriteYC:
		// Y at full resolution, RY/BY at half resolution (2x2 subsampling)
		channels.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
		channels.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
		channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	case WriteYCA:
		// A at full resolution, Y at full resolution, RY/BY at half resolution
		channels.Add(Channel{Name: "A", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		channels.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
		channels.Add(Channel{Name: "RY", Type: PixelTypeHalf, XSampling: 2, YSampling: 2})
		channels.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	default:
		// WriteRGBA - standard RGBA output
		channels.Add(Channel{Name: "A", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		channels.Add(Channel{Name: "B", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		channels.Add(Channel{Name: "G", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
		channels.Add(Channel{Name: "R", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	}

	h.SetChannels(channels)

	return &YCOutputFile{
		path:   path,
		header: h,
		width:  width,
		height: height,
		mode:   mode,
	}, nil
}

// Header returns the header for configuration.
func (w *YCOutputFile) Header() *Header {
	return w.header
}

// WriteRGBA writes an RGBAImage, converting to YC format as specified by the mode.
func (w *YCOutputFile) WriteRGBA(img *RGBAImage) error {
	f, err := os.Create(w.path)
	if err != nil {
		return err
	}
	defer f.Close()

	return w.writeRGBATo(f, img)
}

func (w *YCOutputFile) writeRGBATo(writer io.WriteSeeker, img *RGBAImage) error {
	fb := NewFrameBuffer()

	switch w.mode {
	case WriteY:
		yData := make([]byte, w.width*w.height*2)
		fb.Set("Y", NewSlice(PixelTypeHalf, yData, w.width, w.height))

		// Convert RGB to Y
		for y := 0; y < w.height; y++ {
			for x := 0; x < w.width; x++ {
				r, g, b, _ := img.RGBA(x+img.Rect.Min.X, y+img.Rect.Min.Y)
				yVal, _, _ := RGBtoYC(r, g, b)
				fb.Get("Y").SetHalf(x, y, half.FromFloat32(yVal))
			}
		}

	case WriteYA:
		yData := make([]byte, w.width*w.height*2)
		aData := make([]byte, w.width*w.height*2)
		fb.Set("Y", NewSlice(PixelTypeHalf, yData, w.width, w.height))
		fb.Set("A", NewSlice(PixelTypeHalf, aData, w.width, w.height))

		for y := 0; y < w.height; y++ {
			for x := 0; x < w.width; x++ {
				r, g, b, a := img.RGBA(x+img.Rect.Min.X, y+img.Rect.Min.Y)
				yVal, _, _ := RGBtoYC(r, g, b)
				fb.Get("Y").SetHalf(x, y, half.FromFloat32(yVal))
				fb.Get("A").SetHalf(x, y, half.FromFloat32(a))
			}
		}

	case WriteYC:
		yData := make([]byte, w.width*w.height*2)
		// Chroma at half resolution
		chromaW := (w.width + 1) / 2
		chromaH := (w.height + 1) / 2
		ryData := make([]byte, chromaW*chromaH*2)
		byData := make([]byte, chromaW*chromaH*2)

		fb.Set("Y", NewSlice(PixelTypeHalf, yData, w.width, w.height))
		rySlice := NewSlice(PixelTypeHalf, ryData, chromaW, chromaH)
		bySlice := NewSlice(PixelTypeHalf, byData, chromaW, chromaH)
		rySlice.XSampling = 2
		rySlice.YSampling = 2
		bySlice.XSampling = 2
		bySlice.YSampling = 2
		fb.Set("RY", rySlice)
		fb.Set("BY", bySlice)

		// Convert RGB to YC with chroma subsampling
		w.convertToYC(img, fb, false)

	case WriteYCA:
		yData := make([]byte, w.width*w.height*2)
		aData := make([]byte, w.width*w.height*2)
		chromaW := (w.width + 1) / 2
		chromaH := (w.height + 1) / 2
		ryData := make([]byte, chromaW*chromaH*2)
		byData := make([]byte, chromaW*chromaH*2)

		fb.Set("Y", NewSlice(PixelTypeHalf, yData, w.width, w.height))
		fb.Set("A", NewSlice(PixelTypeHalf, aData, w.width, w.height))
		rySlice := NewSlice(PixelTypeHalf, ryData, chromaW, chromaH)
		bySlice := NewSlice(PixelTypeHalf, byData, chromaW, chromaH)
		rySlice.XSampling = 2
		rySlice.YSampling = 2
		bySlice.XSampling = 2
		bySlice.YSampling = 2
		fb.Set("RY", rySlice)
		fb.Set("BY", bySlice)

		// Convert RGB to YCA with chroma subsampling
		w.convertToYC(img, fb, true)

	default:
		// WriteRGBA - standard path
		rData := make([]byte, w.width*w.height*2)
		gData := make([]byte, w.width*w.height*2)
		bData := make([]byte, w.width*w.height*2)
		aData := make([]byte, w.width*w.height*2)

		fb.Set("R", NewSlice(PixelTypeHalf, rData, w.width, w.height))
		fb.Set("G", NewSlice(PixelTypeHalf, gData, w.width, w.height))
		fb.Set("B", NewSlice(PixelTypeHalf, bData, w.width, w.height))
		fb.Set("A", NewSlice(PixelTypeHalf, aData, w.width, w.height))

		for y := 0; y < w.height; y++ {
			for x := 0; x < w.width; x++ {
				r, g, b, a := img.RGBA(x+img.Rect.Min.X, y+img.Rect.Min.Y)
				fb.Get("R").SetHalf(x, y, half.FromFloat32(r))
				fb.Get("G").SetHalf(x, y, half.FromFloat32(g))
				fb.Get("B").SetHalf(x, y, half.FromFloat32(b))
				fb.Get("A").SetHalf(x, y, half.FromFloat32(a))
			}
		}
	}

	sw, err := NewScanlineWriter(writer, w.header)
	if err != nil {
		return err
	}
	sw.SetFrameBuffer(fb)

	yMin := int(w.header.DataWindow().Min.Y)
	yMax := int(w.header.DataWindow().Max.Y)
	if err := sw.WritePixels(yMin, yMax); err != nil {
		return err
	}

	return sw.Close()
}

// convertToYC converts RGB to YC with 2x2 chroma subsampling.
func (w *YCOutputFile) convertToYC(img *RGBAImage, fb *FrameBuffer, withAlpha bool) {
	chromaW := (w.width + 1) / 2
	chromaH := (w.height + 1) / 2

	// First pass: compute Y for all pixels and optionally A
	for y := 0; y < w.height; y++ {
		for x := 0; x < w.width; x++ {
			r, g, b, a := img.RGBA(x+img.Rect.Min.X, y+img.Rect.Min.Y)
			yVal, _, _ := RGBtoYC(r, g, b)
			fb.Get("Y").SetHalf(x, y, half.FromFloat32(yVal))
			if withAlpha {
				fb.Get("A").SetHalf(x, y, half.FromFloat32(a))
			}
		}
	}

	// Second pass: compute subsampled chroma using box filter
	for cy := 0; cy < chromaH; cy++ {
		for cx := 0; cx < chromaW; cx++ {
			// Average the 2x2 block
			var sumRY, sumBY float32
			var count float32

			for dy := 0; dy < 2; dy++ {
				for dx := 0; dx < 2; dx++ {
					px := cx*2 + dx
					py := cy*2 + dy
					if px < w.width && py < w.height {
						r, g, b, _ := img.RGBA(px+img.Rect.Min.X, py+img.Rect.Min.Y)
						_, ry, by := RGBtoYC(r, g, b)
						sumRY += ry
						sumBY += by
						count++
					}
				}
			}

			if count > 0 {
				avgRY := sumRY / count
				avgBY := sumBY / count
				// SetHalf takes window-absolute coordinates and divides by the
				// channel's sampling, so a chroma plane column must be given
				// as the image column it stands for. Passing the plane index
				// straight in wrote each value at a quarter of its intended
				// position and left three quarters of the chroma untouched.
				ryS, byS := fb.Get("RY"), fb.Get("BY")
				ryS.SetHalf(cx*ryS.XSampling, cy*ryS.YSampling, half.FromFloat32(avgRY))
				byS.SetHalf(cx*byS.XSampling, cy*byS.YSampling, half.FromFloat32(avgBY))
			}
		}
	}
}

// YCInputFile provides a simple interface for reading YC images as RGBA.
type YCInputFile struct {
	file   *File
	header *Header
	dw     Box2i
}

// OpenYCInputFile opens an EXR file for reading YC data.
// The returned YCInputFile must be closed to release the file handle.
func OpenYCInputFile(path string) (*YCInputFile, error) {
	f, err := OpenFile(path)
	if err != nil {
		return nil, err
	}
	yc, err := NewYCInputFile(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	return yc, nil
}

// NewYCInputFile creates a YC input file from an existing File.
func NewYCInputFile(f *File) (*YCInputFile, error) {
	if f == nil {
		return nil, ErrInvalidFile
	}
	h := f.Header(0)
	if h == nil {
		return nil, ErrInvalidHeader
	}
	return &YCInputFile{
		file:   f,
		header: h,
		dw:     h.DataWindow(),
	}, nil
}

// Header returns the file header.
func (r *YCInputFile) Header() *Header {
	return r.header
}

// Close closes the underlying file.
func (r *YCInputFile) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// IsYC returns true if this file contains YC data.
func (r *YCInputFile) IsYC() bool {
	return IsYCImage(r.header)
}

// IsLuminanceOnly returns true if this file contains luminance-only data.
func (r *YCInputFile) IsLuminanceOnly() bool {
	return IsLuminanceOnlyImage(r.header)
}

// Width returns the image width.
func (r *YCInputFile) Width() int {
	return int(r.dw.Width())
}

// Height returns the image height.
func (r *YCInputFile) Height() int {
	return int(r.dw.Height())
}

// ReadRGBA reads the image as RGBA, automatically converting from YC if necessary.
func (r *YCInputFile) ReadRGBA() (*RGBAImage, error) {
	width := r.Width()
	height := r.Height()

	channels := r.header.Channels()
	if channels == nil {
		return nil, ErrInvalidHeader
	}

	// Detect image type
	isYC := IsYCImage(r.header)
	isLumOnly := IsLuminanceOnlyImage(r.header)

	if isYC {
		return r.readYCImage(width, height)
	} else if isLumOnly {
		return r.readLuminanceOnlyImage(width, height)
	}

	// Fall back to standard RGBA reading
	rgba, err := NewRGBAInputFile(r.file)
	if err != nil {
		return nil, err
	}
	return rgba.ReadRGBA()
}

func (r *YCInputFile) readYCImage(width, height int) (*RGBAImage, error) {
	img := NewRGBAImage(RectFromSize(width, height))

	fb := NewFrameBuffer()

	// Y at full resolution
	yData := make([]byte, width*height*4)
	fb.Set("Y", NewSlice(PixelTypeFloat, yData, width, height))

	// Chroma at half resolution
	chromaW := (width + 1) / 2
	chromaH := (height + 1) / 2
	ryData := make([]byte, chromaW*chromaH*4)
	byData := make([]byte, chromaW*chromaH*4)

	rySlice := NewSlice(PixelTypeFloat, ryData, chromaW, chromaH)
	bySlice := NewSlice(PixelTypeFloat, byData, chromaW, chromaH)
	rySlice.XSampling = 2
	rySlice.YSampling = 2
	bySlice.XSampling = 2
	bySlice.YSampling = 2
	fb.Set("RY", rySlice)
	fb.Set("BY", bySlice)

	// Check for alpha
	hasAlpha := false
	channels := r.header.Channels()
	for i := 0; i < channels.Len(); i++ {
		if channels.At(i).Name == "A" {
			hasAlpha = true
			break
		}
	}

	aData := make([]byte, width*height*4)
	if hasAlpha {
		fb.Set("A", NewSlice(PixelTypeFloat, aData, width, height))
	}

	// Read using scanline reader
	sr, err := NewScanlineReader(r.file)
	if err != nil {
		return nil, err
	}
	sr.SetFrameBuffer(fb)

	yMin := int(r.dw.Min.Y)
	yMax := int(r.dw.Max.Y)
	if err := sr.ReadPixels(yMin, yMax); err != nil {
		return nil, err
	}

	// Convert YC to RGB with bilinear chroma upsampling
	for py := 0; py < height; py++ {
		for px := 0; px < width; px++ {
			yVal := fb.Get("Y").GetFloat32(px, py)

			// Bilinear interpolation of chroma
			ry := bilinearSample(fb.Get("RY"), px, py, width, height, chromaW, chromaH)
			by := bilinearSample(fb.Get("BY"), px, py, width, height, chromaW, chromaH)

			r, g, b := YCtoRGB(yVal, ry, by)

			var a float32 = 1.0
			if hasAlpha {
				a = fb.Get("A").GetFloat32(px, py)
			}

			img.SetRGBA(px, py, r, g, b, a)
		}
	}

	return img, nil
}

// bilinearSample samples a subsampled chroma channel with bilinear interpolation.
func bilinearSample(slice *Slice, px, py, fullW, fullH, chromaW, chromaH int) float32 {
	// Convert full-res coordinates to chroma coordinates
	fx := float32(px) / 2.0
	fy := float32(py) / 2.0

	// Get integer coordinates and fractions
	x0 := int(fx)
	y0 := int(fy)
	x1 := x0 + 1
	y1 := y0 + 1

	// Clamp to chroma dimensions
	if x0 < 0 {
		x0 = 0
	}
	if y0 < 0 {
		y0 = 0
	}
	if x1 >= chromaW {
		x1 = chromaW - 1
	}
	if y1 >= chromaH {
		y1 = chromaH - 1
	}

	fracX := fx - float32(x0)
	fracY := fy - float32(y0)

	// Sample four corners.
	//
	// x0..y1 are chroma plane coordinates and GetFloat32 takes window-absolute
	// ones, dividing by the channel's sampling itself — so the plane index has
	// to be scaled back up by that same sampling. Passing it straight in
	// divided twice and read a quarter of the plane over and over, which is
	// the same mistake the row functions and the chroma writer both made.
	// Scaling by the slice's own factor rather than by a literal 2 keeps this
	// right for a caller whose slice is already plane-native.
	sx, sy := slice.XSampling, slice.YSampling
	if sx < 1 {
		sx = 1
	}
	if sy < 1 {
		sy = 1
	}
	v00 := slice.GetFloat32(x0*sx, y0*sy)
	v10 := slice.GetFloat32(x1*sx, y0*sy)
	v01 := slice.GetFloat32(x0*sx, y1*sy)
	v11 := slice.GetFloat32(x1*sx, y1*sy)

	// Bilinear interpolation
	v0 := v00*(1-fracX) + v10*fracX
	v1 := v01*(1-fracX) + v11*fracX
	return v0*(1-fracY) + v1*fracY
}

func (r *YCInputFile) readLuminanceOnlyImage(width, height int) (*RGBAImage, error) {
	img := NewRGBAImage(RectFromSize(width, height))

	fb := NewFrameBuffer()
	yData := make([]byte, width*height*4)
	fb.Set("Y", NewSlice(PixelTypeFloat, yData, width, height))

	// Check for alpha
	hasAlpha := false
	channels := r.header.Channels()
	for i := 0; i < channels.Len(); i++ {
		if channels.At(i).Name == "A" {
			hasAlpha = true
			break
		}
	}

	aData := make([]byte, width*height*4)
	if hasAlpha {
		fb.Set("A", NewSlice(PixelTypeFloat, aData, width, height))
	}

	sr, err := NewScanlineReader(r.file)
	if err != nil {
		return nil, err
	}
	sr.SetFrameBuffer(fb)

	yMin := int(r.dw.Min.Y)
	yMax := int(r.dw.Max.Y)
	if err := sr.ReadPixels(yMin, yMax); err != nil {
		return nil, err
	}

	// Convert Y to RGB (grayscale: R=G=B=Y)
	for py := 0; py < height; py++ {
		for px := 0; px < width; px++ {
			yVal := fb.Get("Y").GetFloat32(px, py)

			var a float32 = 1.0
			if hasAlpha {
				a = fb.Get("A").GetFloat32(px, py)
			}

			img.SetRGBA(px, py, yVal, yVal, yVal, a)
		}
	}

	return img, nil
}
