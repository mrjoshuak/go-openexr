// exrenvmap converts OpenEXR environment maps between latitude-longitude
// and cube-face formats.
//
// Usage:
//
//	exrenvmap [options] infile outfile
//
// Options:
//
//	-li           interpret input as latitude-longitude map
//	-ci           interpret input as cube map
//	-lo           output latitude-longitude map
//	-co           output cube map (default)
//	-w width      set output width in pixels (default 256)
//	-f r n        set filter radius r and sample rate n (default 1.0, 5)
//	-b            apply diffuse blur (180-degree filter)
//	-c type       set compression: none, rle, zips, zip, piz, pxr24, b44, b44a, dwaa, dwab
//	-t x y        set tile size (default 64 64)
//	-m            generate mipmap levels
//	-d            round down mipmap level sizes (default)
//	-u            round up mipmap level sizes
//	-p t b        pad top and bottom of input with t and b lines
//	-v            verbose output
//	-h, --help    show help
//	--version     show version
//
// Six-file I/O:
//
//	If the input or output filename contains a '%' character, exrenvmap
//	reads/writes six separate files for the cube faces. The '%' is replaced
//	with +X, -X, +Y, -Y, +Z, -Z for each face file.
package main

import (
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

// envMapString converts EnvMap to string.
func envMapString(e exr.EnvMap) string {
	switch e {
	case exr.EnvMapLatLong:
		return "latlong"
	case exr.EnvMapCube:
		return "cube"
	default:
		return "unknown"
	}
}

const version = "1.1.0"

// Cube face names for six-file I/O
var cubeFaceNames = []string{"+X", "-X", "+Y", "-Y", "+Z", "-Z"}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "exrenvmap: %v\n", err)
		os.Exit(1)
	}
}

type config struct {
	inputLatLong  bool
	inputCube     bool
	outputLatLong bool
	outputCube    bool
	outputWidth   int
	filterRadius  float64
	numSamples    int
	diffuseBlur   bool
	compression   string
	tileWidth     int
	tileHeight    int
	mipmap        bool
	roundUp       bool
	roundDown     bool
	padTop        int
	padBottom     int
	verbose       bool
	inFile        string
	outFile       string
}

func run() error {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		return err
	}
	if cfg == nil {
		// Help or version was printed
		return nil
	}

	// Determine output type (default to cube map)
	outputType := exr.EnvMapCube
	if cfg.outputLatLong {
		outputType = exr.EnvMapLatLong
	}
	if cfg.outputCube {
		outputType = exr.EnvMapCube
	}

	// Parse compression type
	comp, err := parseCompression(cfg.compression)
	if err != nil {
		return err
	}

	// Determine rounding mode
	roundingMode := exr.LevelRoundDown
	if cfg.roundUp {
		roundingMode = exr.LevelRoundUp
	}

	// Check if using six-file I/O
	inputSixFile := strings.Contains(cfg.inFile, "%")
	outputSixFile := strings.Contains(cfg.outFile, "%")

	// Read input image
	if cfg.verbose {
		if inputSixFile {
			fmt.Printf("Reading 6 cube face files from %s\n", cfg.inFile)
		} else {
			fmt.Printf("Reading %s\n", cfg.inFile)
		}
	}

	var inputImage *exr.EnvMapImage
	var inputHeader *exr.Header

	if inputSixFile {
		inputImage, inputHeader, err = readSixCubeFaceFiles(cfg.inFile, cfg.padTop, cfg.padBottom, cfg.verbose)
	} else {
		inputImage, inputHeader, err = readEnvMapImage(cfg.inFile, cfg.inputLatLong, cfg.inputCube, cfg.padTop, cfg.padBottom, cfg.verbose)
	}
	if err != nil {
		return fmt.Errorf("failed to read input: %w", err)
	}

	// Apply diffuse blur if requested
	if cfg.diffuseBlur {
		if cfg.verbose {
			fmt.Println("Applying diffuse blur...")
		}
		inputImage = blurImage(inputImage, cfg.verbose)
	}

	// Convert/resize to output format
	if cfg.verbose {
		fmt.Printf("Converting to %s format (%dx%d)\n", envMapString(outputType), cfg.outputWidth, outputHeight(outputType, cfg.outputWidth))
	}

	outputImage := convertEnvMap(inputImage, outputType, cfg.outputWidth, float32(cfg.filterRadius), cfg.numSamples, cfg.verbose)

	// Write output file
	if cfg.verbose {
		if outputSixFile {
			fmt.Printf("Writing 6 cube face files to %s\n", cfg.outFile)
		} else {
			fmt.Printf("Writing %s\n", cfg.outFile)
		}
	}

	if outputSixFile {
		err = writeSixCubeFaceFiles(cfg.outFile, outputImage, inputHeader, comp, cfg.verbose)
	} else {
		err = writeEnvMapImage(cfg.outFile, outputImage, inputHeader, comp, cfg.tileWidth, cfg.tileHeight, cfg.mipmap, roundingMode, cfg.verbose)
	}
	if err != nil {
		return fmt.Errorf("failed to write output: %w", err)
	}

	if cfg.verbose {
		fmt.Println("Done.")
	}

	return nil
}

func parseArgs(args []string) (*config, error) {
	cfg := &config{
		outputWidth:  256,
		filterRadius: 1.0,
		numSamples:   5,
		compression:  "zip",
		tileWidth:    64,
		tileHeight:   64,
		roundDown:    true,
	}

	var positional []string

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch arg {
		case "-h", "--help":
			usageMessage(os.Stdout)
			return nil, nil

		case "--version":
			fmt.Printf("exrenvmap version %s\n", version)
			return nil, nil

		case "-li":
			cfg.inputLatLong = true

		case "-ci":
			cfg.inputCube = true

		case "-lo":
			cfg.outputLatLong = true

		case "-co":
			cfg.outputCube = true

		case "-w":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -w")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.outputWidth); err != nil {
				return nil, fmt.Errorf("invalid width: %s", args[i])
			}

		case "-f":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -f")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%f", &cfg.filterRadius); err != nil {
				return nil, fmt.Errorf("invalid filter radius: %s", args[i])
			}

		case "-n":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -n")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.numSamples); err != nil {
				return nil, fmt.Errorf("invalid sample count: %s", args[i])
			}

		case "-b":
			cfg.diffuseBlur = true

		case "-c":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -c")
			}
			i++
			cfg.compression = args[i]

		case "-tx":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -tx")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.tileWidth); err != nil {
				return nil, fmt.Errorf("invalid tile width: %s", args[i])
			}

		case "-ty":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing value for -ty")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.tileHeight); err != nil {
				return nil, fmt.Errorf("invalid tile height: %s", args[i])
			}

		case "-t":
			// -t x y syntax (two values)
			if i+2 >= len(args) {
				return nil, fmt.Errorf("missing values for -t (expected: -t width height)")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.tileWidth); err != nil {
				return nil, fmt.Errorf("invalid tile width: %s", args[i])
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.tileHeight); err != nil {
				return nil, fmt.Errorf("invalid tile height: %s", args[i])
			}

		case "-m":
			cfg.mipmap = true

		case "-d":
			cfg.roundDown = true
			cfg.roundUp = false

		case "-u":
			cfg.roundUp = true
			cfg.roundDown = false

		case "-p":
			// -p top bottom syntax
			if i+2 >= len(args) {
				return nil, fmt.Errorf("missing values for -p (expected: -p top bottom)")
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.padTop); err != nil {
				return nil, fmt.Errorf("invalid top padding: %s", args[i])
			}
			i++
			if _, err := fmt.Sscanf(args[i], "%d", &cfg.padBottom); err != nil {
				return nil, fmt.Errorf("invalid bottom padding: %s", args[i])
			}

		case "-v":
			cfg.verbose = true

		default:
			if strings.HasPrefix(arg, "-") {
				return nil, fmt.Errorf("unknown option: %s", arg)
			}
			positional = append(positional, arg)
		}
	}

	if len(positional) < 2 {
		usageMessage(os.Stderr)
		return nil, fmt.Errorf("expected 2 arguments (infile outfile), got %d", len(positional))
	}

	cfg.inFile = positional[0]
	cfg.outFile = positional[1]

	return cfg, nil
}

func usageMessage(w *os.File) {
	fmt.Fprintf(w, `Usage: exrenvmap [options] infile outfile

Convert an OpenEXR latitude-longitude environment map into a cube-face
environment map or vice versa. Reads an environment map image from infile,
converts it, and stores the result in outfile.

Options:
  -li           interpret input as latitude-longitude map
  -ci           interpret input as cube map
  -lo           output latitude-longitude map
  -co           output cube map (default)
  -w width      set output width in pixels (default 256)
  -f radius     set antialiasing filter radius (default 1.0)
  -n samples    set sampling rate: n by n samples (default 5)
  -b            apply diffuse blur (180-degree filter kernel)
  -c type       set compression: none, rle, zips, zip, piz, pxr24, b44, b44a, dwaa, dwab
  -t x y        set tile size (default 64 64)
  -tx width     set tile width (default 64)
  -ty height    set tile height (default 64)
  -m            generate mipmap levels (outputs tiled format)
  -d            round down mipmap level sizes (default)
  -u            round up mipmap level sizes
  -p t b        pad top and bottom of input with t and b lines
  -v            verbose output
  -h, --help    show this help message
  --version     show version information

Six-file I/O:
  If the input or output filename contains a '%%' character, exrenvmap
  reads/writes six separate files for the cube faces. The '%%' is replaced
  with +X, -X, +Y, -Y, +Z, -Z for each face file.

Latitude-longitude format:
  Full sphere in single image with 2:1 aspect ratio (360 x 180 degrees).
  Top is +Y (up), center is +Z (forward).

Cube map format:
  Six square faces stacked vertically (+X, -X, +Y, -Y, +Z, -Z).
  Image height is 6 times the width.

`)
}

// parseCompression converts a compression string to exr.Compression.
func parseCompression(s string) (exr.Compression, error) {
	switch strings.ToLower(s) {
	case "none":
		return exr.CompressionNone, nil
	case "rle":
		return exr.CompressionRLE, nil
	case "zips":
		return exr.CompressionZIPS, nil
	case "zip":
		return exr.CompressionZIP, nil
	case "piz":
		return exr.CompressionPIZ, nil
	case "pxr24":
		return exr.CompressionPXR24, nil
	case "b44":
		return exr.CompressionB44, nil
	case "b44a":
		return exr.CompressionB44A, nil
	case "dwaa":
		return exr.CompressionDWAA, nil
	case "dwab":
		return exr.CompressionDWAB, nil
	default:
		return exr.CompressionNone, fmt.Errorf("unknown compression type: %s", s)
	}
}

// outputHeight calculates the output height based on type and width.
func outputHeight(envType exr.EnvMap, width int) int {
	if envType == exr.EnvMapLatLong {
		return width / 2 // 2:1 aspect ratio
	}
	return width * 6 // 6 square faces stacked
}

// readEnvMapImage reads an EXR file and returns an EnvMapImage.
func readEnvMapImage(path string, forceLatLong, forceCube bool, padTop, padBottom int, verbose bool) (*exr.EnvMapImage, *exr.Header, error) {
	f, err := exr.OpenFile(path)
	if err != nil {
		return nil, nil, err
	}

	h := f.Header(0)
	if h == nil {
		return nil, nil, fmt.Errorf("no header found")
	}

	// Determine environment map type
	envType := exr.EnvMapLatLong
	if forceLatLong {
		envType = exr.EnvMapLatLong
	} else if forceCube {
		envType = exr.EnvMapCube
	} else if h.HasEnvmap() {
		envType = h.Envmap()
	} else {
		// Guess from aspect ratio
		dw := h.DataWindow()
		w := int(dw.Width())
		ht := int(dw.Height())
		if ht == w*6 {
			envType = exr.EnvMapCube
		} else if w == ht*2 {
			envType = exr.EnvMapLatLong
		}
	}

	if verbose {
		fmt.Printf("  Input type: %s\n", envMapString(envType))
		fmt.Printf("  Dimensions: %dx%d\n", h.Width(), h.Height())
	}

	// Read pixel data
	rgba, err := exr.NewRGBAInputFile(f)
	if err != nil {
		return nil, nil, err
	}

	img, err := rgba.ReadRGBA()
	if err != nil {
		return nil, nil, err
	}

	// Convert to EnvMapImage with optional padding
	w := img.Rect.Dx()
	ht := img.Rect.Dy()
	paddedHeight := ht + padTop + padBottom
	envImg := exr.NewEnvMapImage(envType, w, paddedHeight)

	// Fill with black (zeros) - already initialized

	// Copy image data with offset for top padding
	for y := 0; y < ht; y++ {
		for x := 0; x < w; x++ {
			r, g, b, a := img.RGBA(x, y)
			envImg.Set(x, y+padTop, exr.RGBA{R: r, G: g, B: b, A: a})
		}
	}

	return envImg, h, nil
}

// readSixCubeFaceFiles reads 6 cube face files with % placeholder in filename.
func readSixCubeFaceFiles(pathPattern string, padTop, padBottom int, verbose bool) (*exr.EnvMapImage, *exr.Header, error) {
	var faceImages [6]*exr.EnvMapImage
	var firstHeader *exr.Header
	var faceSize int

	for i, faceName := range cubeFaceNames {
		facePath := strings.Replace(pathPattern, "%", faceName, 1)
		if verbose {
			fmt.Printf("  Reading face %s from %s\n", faceName, facePath)
		}

		faceImg, header, err := readEnvMapImage(facePath, false, false, 0, 0, false)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read face %s: %w", faceName, err)
		}

		// Verify square face
		if faceImg.Width != faceImg.Height {
			return nil, nil, fmt.Errorf("cube face %s is not square: %dx%d", faceName, faceImg.Width, faceImg.Height)
		}

		if i == 0 {
			faceSize = faceImg.Width
			firstHeader = header
		} else if faceImg.Width != faceSize {
			return nil, nil, fmt.Errorf("cube face %s size %d doesn't match expected %d", faceName, faceImg.Width, faceSize)
		}

		faceImages[i] = faceImg
	}

	// Create combined cube map image
	cubeWidth := faceSize
	cubeHeight := faceSize*6 + padTop + padBottom
	cubeImg := exr.NewEnvMapImage(exr.EnvMapCube, cubeWidth, cubeHeight)

	// Copy faces into stacked cube map
	for face := 0; face < 6; face++ {
		faceImg := faceImages[face]
		yOffset := face*faceSize + padTop

		for y := 0; y < faceSize; y++ {
			for x := 0; x < faceSize; x++ {
				cubeImg.Set(x, y+yOffset, faceImg.At(x, y))
			}
		}
	}

	return cubeImg, firstHeader, nil
}

// writeSixCubeFaceFiles writes 6 cube face files with % placeholder in filename.
func writeSixCubeFaceFiles(pathPattern string, img *exr.EnvMapImage, srcHeader *exr.Header, comp exr.Compression, verbose bool) error {
	if img.Type != exr.EnvMapCube {
		return fmt.Errorf("six-file output requires cube map format")
	}

	faceSize := exr.CubeSizeOfFace(img.DataWindow())

	for i, faceName := range cubeFaceNames {
		facePath := strings.Replace(pathPattern, "%", faceName, 1)
		if verbose {
			fmt.Printf("  Writing face %s to %s\n", faceName, facePath)
		}

		// Extract face data
		faceImg := exr.NewEnvMapImage(exr.EnvMapCube, faceSize, faceSize)
		yOffset := i * faceSize

		for y := 0; y < faceSize; y++ {
			for x := 0; x < faceSize; x++ {
				faceImg.Set(x, y, img.At(x, y+yOffset))
			}
		}

		// Write as scanline image (no envmap attribute for individual faces)
		err := writeFaceImage(facePath, faceImg, srcHeader, comp)
		if err != nil {
			return fmt.Errorf("failed to write face %s: %w", faceName, err)
		}
	}

	return nil
}

// writeFaceImage writes a single cube face as a regular image.
func writeFaceImage(path string, img *exr.EnvMapImage, srcHeader *exr.Header, comp exr.Compression) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Create header
	h := exr.NewScanlineHeader(img.Width, img.Height)
	h.SetCompression(comp)

	// Copy relevant attributes from source header
	if srcHeader != nil {
		if srcHeader.Has("chromaticities") {
			h.Set(srcHeader.Get("chromaticities"))
		}
	}

	// Set up channel list with RGBA
	channels := exr.NewChannelList()
	channels.Add(exr.Channel{Name: "R", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "G", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "B", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "A", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	h.SetChannels(channels)

	// Create frame buffer
	fb := exr.NewFrameBuffer()
	rData := make([]byte, img.Width*img.Height*2)
	gData := make([]byte, img.Width*img.Height*2)
	bData := make([]byte, img.Width*img.Height*2)
	aData := make([]byte, img.Width*img.Height*2)

	fb.Set("R", exr.NewSlice(exr.PixelTypeHalf, rData, img.Width, img.Height))
	fb.Set("G", exr.NewSlice(exr.PixelTypeHalf, gData, img.Width, img.Height))
	fb.Set("B", exr.NewSlice(exr.PixelTypeHalf, bData, img.Width, img.Height))
	fb.Set("A", exr.NewSlice(exr.PixelTypeHalf, aData, img.Width, img.Height))

	// Fill frame buffer with pixel data
	for y := 0; y < img.Height; y++ {
		for x := 0; x < img.Width; x++ {
			c := img.At(x, y)
			fb.Get("R").SetHalf(x, y, half.FromFloat32(c.R))
			fb.Get("G").SetHalf(x, y, half.FromFloat32(c.G))
			fb.Get("B").SetHalf(x, y, half.FromFloat32(c.B))
			fb.Get("A").SetHalf(x, y, half.FromFloat32(c.A))
		}
	}

	// Write file
	sw, err := exr.NewScanlineWriter(f, h)
	if err != nil {
		return err
	}
	sw.SetFrameBuffer(fb)

	yMin := int(h.DataWindow().Min.Y)
	yMax := int(h.DataWindow().Max.Y)
	if err := sw.WritePixels(yMin, yMax); err != nil {
		return err
	}

	return sw.Close()
}

// writeEnvMapImage writes an EnvMapImage to an EXR file.
func writeEnvMapImage(path string, img *exr.EnvMapImage, srcHeader *exr.Header, comp exr.Compression, tileW, tileH int, mipmap bool, roundingMode exr.LevelRoundingMode, verbose bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Set up channel list with RGBA
	channels := exr.NewChannelList()
	channels.Add(exr.Channel{Name: "R", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "G", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "B", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
	channels.Add(exr.Channel{Name: "A", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})

	// Create frame buffer with source data
	fb := exr.NewFrameBuffer()
	rData := make([]byte, img.Width*img.Height*2)
	gData := make([]byte, img.Width*img.Height*2)
	bData := make([]byte, img.Width*img.Height*2)
	aData := make([]byte, img.Width*img.Height*2)

	fb.Set("R", exr.NewSlice(exr.PixelTypeHalf, rData, img.Width, img.Height))
	fb.Set("G", exr.NewSlice(exr.PixelTypeHalf, gData, img.Width, img.Height))
	fb.Set("B", exr.NewSlice(exr.PixelTypeHalf, bData, img.Width, img.Height))
	fb.Set("A", exr.NewSlice(exr.PixelTypeHalf, aData, img.Width, img.Height))

	// Fill frame buffer with pixel data
	for y := 0; y < img.Height; y++ {
		for x := 0; x < img.Width; x++ {
			c := img.At(x, y)
			fb.Get("R").SetHalf(x, y, half.FromFloat32(c.R))
			fb.Get("G").SetHalf(x, y, half.FromFloat32(c.G))
			fb.Get("B").SetHalf(x, y, half.FromFloat32(c.B))
			fb.Get("A").SetHalf(x, y, half.FromFloat32(c.A))
		}
	}

	// Write tiled format when mipmaps are requested
	if mipmap {
		return writeTiledEnvMap(f, img, srcHeader, fb, channels, comp, tileW, tileH, roundingMode, verbose)
	}

	// Write scanline format
	return writeScanlineEnvMap(f, img, srcHeader, fb, channels, comp)
}

// writeScanlineEnvMap writes a scanline format environment map.
func writeScanlineEnvMap(f *os.File, img *exr.EnvMapImage, srcHeader *exr.Header, fb *exr.FrameBuffer, channels *exr.ChannelList, comp exr.Compression) error {
	// Create header
	h := exr.NewScanlineHeader(img.Width, img.Height)
	h.SetCompression(comp)
	h.SetEnvmap(img.Type)
	h.SetChannels(channels)

	// Copy relevant attributes from source header
	if srcHeader != nil {
		if srcHeader.Has("chromaticities") {
			h.Set(srcHeader.Get("chromaticities"))
		}
	}

	// Write file
	sw, err := exr.NewScanlineWriter(f, h)
	if err != nil {
		return err
	}
	sw.SetFrameBuffer(fb)

	yMin := int(h.DataWindow().Min.Y)
	yMax := int(h.DataWindow().Max.Y)
	if err := sw.WritePixels(yMin, yMax); err != nil {
		return err
	}

	return sw.Close()
}

// writeTiledEnvMap writes a tiled format environment map with mipmap levels.
func writeTiledEnvMap(f *os.File, img *exr.EnvMapImage, srcHeader *exr.Header, fb *exr.FrameBuffer, channels *exr.ChannelList, comp exr.Compression, tileW, tileH int, roundingMode exr.LevelRoundingMode, verbose bool) error {
	// Create tiled header
	h := exr.NewHeader()

	dataWindow := exr.Box2i{Min: exr.V2i{X: 0, Y: 0}, Max: exr.V2i{X: int32(img.Width - 1), Y: int32(img.Height - 1)}}
	h.SetDataWindow(dataWindow)
	h.SetDisplayWindow(dataWindow)
	h.SetCompression(comp)
	h.SetLineOrder(exr.LineOrderIncreasing)
	h.SetPixelAspectRatio(1.0)
	h.SetScreenWindowCenter(exr.V2f{X: 0, Y: 0})
	h.SetScreenWindowWidth(1.0)
	h.SetEnvmap(img.Type)
	h.SetChannels(channels)

	// Set tile description with mipmap mode
	h.SetTileDescription(exr.TileDescription{
		XSize:        uint32(tileW),
		YSize:        uint32(tileH),
		Mode:         exr.LevelModeMipmap,
		RoundingMode: roundingMode,
	})

	// Copy relevant attributes from source header
	if srcHeader != nil {
		if srcHeader.Has("chromaticities") {
			h.Set(srcHeader.Get("chromaticities"))
		}
	}

	// Create tiled writer
	writer, err := exr.NewTiledWriter(f, h)
	if err != nil {
		return err
	}

	if verbose {
		numLevels := h.NumXLevels()
		fmt.Printf("  Generating %d mipmap levels\n", numLevels)
		fmt.Printf("  Tile size: %dx%d\n", tileW, tileH)
		roundStr := "down"
		if roundingMode == exr.LevelRoundUp {
			roundStr = "up"
		}
		fmt.Printf("  Level rounding: %s\n", roundStr)
	}

	// Generate mipmap levels
	levels, err := exr.GenerateMipmapsFromFrameBuffer(fb, img.Width, img.Height, h, exr.FilterBox)
	if err != nil {
		return fmt.Errorf("failed to generate mipmaps: %w", err)
	}

	numLevels := writer.NumLevels()

	// Write each mipmap level
	for level := 0; level < numLevels; level++ {
		if level >= len(levels) {
			break
		}
		levelData := levels[level]

		if verbose {
			fmt.Printf("  Writing level %d: %dx%d pixels\n", level, levelData.Width, levelData.Height)
		}

		writer.SetFrameBuffer(levelData.FrameBuffer)

		numXTiles := writer.NumXTilesAtLevel(level)
		numYTiles := writer.NumYTilesAtLevel(level)

		for tileY := 0; tileY < numYTiles; tileY++ {
			for tileX := 0; tileX < numXTiles; tileX++ {
				if err := writer.WriteTileLevel(tileX, tileY, level, level); err != nil {
					return fmt.Errorf("failed to write tile (%d, %d) at level %d: %w", tileX, tileY, level, err)
				}
			}
		}
	}

	return writer.Close()
}

// convertEnvMap converts an environment map to a different format/size.
func convertEnvMap(src *exr.EnvMapImage, dstType exr.EnvMap, dstWidth int, filterRadius float32, numSamples int, verbose bool) *exr.EnvMapImage {
	dstHeight := outputHeight(dstType, dstWidth)
	dst := exr.NewEnvMapImage(dstType, dstWidth, dstHeight)
	dw := dst.DataWindow()

	if dstType == exr.EnvMapLatLong {
		// Generate latitude-longitude map
		radius := 0.5 * 2 * math.Pi * float64(filterRadius) / float64(dstWidth)

		for y := 0; y < dstHeight; y++ {
			for x := 0; x < dstWidth; x++ {
				dir := exr.DirectionFromLatLongPixel(dw, float32(x), float32(y))
				c := src.FilteredLookup(dir, float32(radius), numSamples)
				dst.Set(x, y, c)
			}
		}
	} else {
		// Generate cube map
		sof := exr.CubeSizeOfFace(dw)
		radius := 1.5 * float64(filterRadius) / float64(sof)

		for face := exr.CubeFacePosX; face <= exr.CubeFaceNegZ; face++ {
			if verbose {
				fmt.Printf("  Processing face %d\n", face)
			}
			for fy := 0; fy < sof; fy++ {
				for fx := 0; fx < sof; fx++ {
					posInFace := exr.V2f{X: float32(fx), Y: float32(fy)}
					dir := exr.DirectionFromCubeFaceAndPosition(face, posInFace, dw)
					pos := exr.CubePixelPositionFromFacePosition(face, posInFace, dw)
					c := src.FilteredLookup(dir, float32(radius), numSamples)
					dst.Set(int(pos.X+0.5), int(pos.Y+0.5), c)
				}
			}
		}
	}

	return dst
}

// blurImage applies a 180-degree diffuse blur to the environment map.
// This simulates what a diffuse surface would see from the environment.
func blurImage(src *exr.EnvMapImage, verbose bool) *exr.EnvMapImage {
	// Convert to cube map for blurring (more uniform sampling)
	const maxInWidth = 40
	const outWidth = 100

	if verbose {
		fmt.Println("  Converting to cube map for blur...")
	}

	// First, shrink image to manageable size
	var blurSrc *exr.EnvMapImage
	if src.Type == exr.EnvMapLatLong {
		// Convert lat-long to cube
		w := src.Width / 4
		if w > maxInWidth {
			w = maxInWidth
		}
		blurSrc = convertEnvMap(src, exr.EnvMapCube, w, 1.0, 7, false)
	} else {
		// Already cube, possibly resize
		w := src.Width
		for w > maxInWidth {
			if w >= maxInWidth*2 {
				w /= 2
			} else {
				w = maxInWidth
			}
		}
		if w != src.Width {
			blurSrc = convertEnvMap(src, exr.EnvMapCube, w, 1.0, 7, false)
		} else {
			blurSrc = src
		}
	}

	if verbose {
		fmt.Println("  Computing pixel weights...")
	}

	// Compute solid angle weights for each pixel
	dw := blurSrc.DataWindow()
	sof := exr.CubeSizeOfFace(dw)
	w := blurSrc.Width
	h := blurSrc.Height

	// Weight each pixel by solid angle
	weightedPixels := make([]exr.RGBA, w*h)
	copy(weightedPixels, blurSrc.Pixels)
	var weightTotal float64

	for face := exr.CubeFacePosX; face <= exr.CubeFaceNegZ; face++ {
		var faceDir exr.V3f
		switch face {
		case exr.CubeFacePosX:
			faceDir = exr.V3f{X: 1, Y: 0, Z: 0}
		case exr.CubeFaceNegX:
			faceDir = exr.V3f{X: -1, Y: 0, Z: 0}
		case exr.CubeFacePosY:
			faceDir = exr.V3f{X: 0, Y: 1, Z: 0}
		case exr.CubeFaceNegY:
			faceDir = exr.V3f{X: 0, Y: -1, Z: 0}
		case exr.CubeFacePosZ:
			faceDir = exr.V3f{X: 0, Y: 0, Z: 1}
		case exr.CubeFaceNegZ:
			faceDir = exr.V3f{X: 0, Y: 0, Z: -1}
		}

		for fy := 0; fy < sof; fy++ {
			yEdge := fy == 0 || fy == sof-1
			for fx := 0; fx < sof; fx++ {
				xEdge := fx == 0 || fx == sof-1

				posInFace := exr.V2f{X: float32(fx), Y: float32(fy)}
				dir := exr.DirectionFromCubeFaceAndPosition(face, posInFace, dw)

				// Normalize
				length := float32(math.Sqrt(float64(dir.X*dir.X + dir.Y*dir.Y + dir.Z*dir.Z)))
				if length > 0 {
					dir.X /= length
					dir.Y /= length
					dir.Z /= length
				}

				// Solid angle weight
				dot := dir.X*faceDir.X + dir.Y*faceDir.Y + dir.Z*faceDir.Z
				weight := float64(dot)

				// Adjust for edge/corner duplication
				if xEdge && yEdge {
					weight /= 3
				} else if xEdge || yEdge {
					weight /= 2
				}

				pos := exr.CubePixelPositionFromFacePosition(face, posInFace, dw)
				px := int(pos.X + 0.5)
				py := int(pos.Y + 0.5)
				idx := py*w + px

				if idx >= 0 && idx < len(weightedPixels) {
					weightedPixels[idx] = weightedPixels[idx].Scale(float32(weight))
					weightTotal += weight
				}
			}
		}
	}

	// Normalize weights
	numPixels := len(weightedPixels)
	weightNorm := float64(numPixels) / weightTotal
	for i := range weightedPixels {
		weightedPixels[i] = weightedPixels[i].Scale(float32(weightNorm))
	}

	if verbose {
		fmt.Println("  Generating blurred image...")
	}

	// Create output cube map
	outHeight := outWidth * 6
	result := exr.NewEnvMapImage(exr.EnvMapCube, outWidth, outHeight)
	outDw := result.DataWindow()
	outSof := exr.CubeSizeOfFace(outDw)

	// For each output pixel, integrate over hemisphere
	for face := exr.CubeFacePosX; face <= exr.CubeFaceNegZ; face++ {
		if verbose {
			fmt.Printf("    Face %d\n", face)
		}

		for fy := 0; fy < outSof; fy++ {
			for fx := 0; fx < outSof; fx++ {
				posInFace := exr.V2f{X: float32(fx), Y: float32(fy)}
				dir2 := exr.DirectionFromCubeFaceAndPosition(face, posInFace, outDw)

				// Normalize dir2
				len2 := float32(math.Sqrt(float64(dir2.X*dir2.X + dir2.Y*dir2.Y + dir2.Z*dir2.Z)))
				if len2 > 0 {
					dir2.X /= len2
					dir2.Y /= len2
					dir2.Z /= len2
				}

				var weightSum float64
				var rSum, gSum, bSum, aSum float64

				// Integrate over all input pixels
				for inFace := exr.CubeFacePosX; inFace <= exr.CubeFaceNegZ; inFace++ {
					for ify := 0; ify < sof; ify++ {
						for ifx := 0; ifx < sof; ifx++ {
							inPosInFace := exr.V2f{X: float32(ifx), Y: float32(ify)}
							dir1 := exr.DirectionFromCubeFaceAndPosition(inFace, inPosInFace, dw)

							// Normalize dir1
							len1 := float32(math.Sqrt(float64(dir1.X*dir1.X + dir1.Y*dir1.Y + dir1.Z*dir1.Z)))
							if len1 > 0 {
								dir1.X /= len1
								dir1.Y /= len1
								dir1.Z /= len1
							}

							// Cosine weight (N dot L for diffuse)
							weight := float64(dir1.X*dir2.X + dir1.Y*dir2.Y + dir1.Z*dir2.Z)
							if weight <= 0 {
								continue
							}

							inPos := exr.CubePixelPositionFromFacePosition(inFace, inPosInFace, dw)
							px := int(inPos.X + 0.5)
							py := int(inPos.Y + 0.5)
							idx := py*w + px

							if idx >= 0 && idx < len(weightedPixels) {
								p := weightedPixels[idx]
								weightSum += weight
								rSum += float64(p.R) * weight
								gSum += float64(p.G) * weight
								bSum += float64(p.B) * weight
								aSum += float64(p.A) * weight
							}
						}
					}
				}

				if weightSum > 0 {
					outPos := exr.CubePixelPositionFromFacePosition(face, posInFace, outDw)
					result.Set(int(outPos.X+0.5), int(outPos.Y+0.5), exr.RGBA{
						R: float32(rSum / weightSum),
						G: float32(gSum / weightSum),
						B: float32(bSum / weightSum),
						A: float32(aSum / weightSum),
					})
				}
			}
		}
	}

	return result
}
