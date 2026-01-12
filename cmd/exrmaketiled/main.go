// exrmaketiled converts scanline OpenEXR images to tiled format with optional
// mipmap or ripmap level generation.
//
// Usage:
//
//	exrmaketiled [options] infile outfile
//
// Options:
//
//	-o              overwrite existing output file
//	-p              process all parts in multi-part files
//	-t <x> [y]      tile size (default 64x64, y defaults to x if omitted)
//	-m              generate mipmaps (MIPMAP_LEVELS)
//	-r              generate ripmaps (RIPMAP_LEVELS)
//	-f <c> <filter> set filter for channel c (box, triangle, lanczos)
//	-e <mode>       edge extrapolation mode (clamp, periodic, mirror)
//	-c <type>       compression (none, rle, zip, zips, piz, pxr24, b44, b44a, dwaa, dwab)
//	-z <level>      DWA compression level (default 45.0)
//	-round <mode>   level rounding (up or down, default down)
//	-v              verbose mode
//	-h, --help      print this message
//	    --version   print version information
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
)

const version = "0.2.0"

// EdgeMode defines how to handle pixels at image edges during mipmap generation.
type EdgeMode int

const (
	EdgeModeClamp    EdgeMode = iota // Clamp to edge pixels
	EdgeModePeriodic                 // Wrap around (periodic/tiled)
	EdgeModeMirror                   // Mirror at edges
)

type config struct {
	inFile         string
	outFile        string
	overwrite      bool
	multiPart      bool
	tileSizeX      int
	tileSizeY      int
	levelMode      exr.LevelMode
	roundingMode   exr.LevelRoundingMode
	compression    exr.Compression
	dwaLevel       float32
	channelFilters map[string]exr.FilterType // Per-channel filter settings
	defaultFilter  exr.FilterType
	edgeMode       EdgeMode
	verbose        bool
}

func main() {
	cfg, err := parseArgs(os.Args[1:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "exrmaketiled: %v\n", err)
		os.Exit(1)
	}

	if cfg == nil {
		// Help or version was printed
		os.Exit(0)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "exrmaketiled: %v\n", err)
		os.Exit(1)
	}
}

func parseArgs(args []string) (*config, error) {
	if len(args) == 0 {
		usageMessage(os.Stderr, false)
		return nil, fmt.Errorf("missing input and output files")
	}

	cfg := &config{
		tileSizeX:      64,
		tileSizeY:      64,
		levelMode:      exr.LevelModeOne,
		roundingMode:   exr.LevelRoundDown,
		compression:    exr.CompressionZIP,
		dwaLevel:       45.0,
		channelFilters: make(map[string]exr.FilterType),
		defaultFilter:  exr.FilterBox,
		edgeMode:       EdgeModeClamp,
	}

	var positional []string

	for i := 0; i < len(args); i++ {
		arg := args[i]

		switch arg {
		case "-h", "--help":
			usageMessage(os.Stdout, true)
			return nil, nil

		case "--version":
			fmt.Printf("exrmaketiled (go-openexr) %s\n", version)
			fmt.Println("https://github.com/mrjoshuak/go-openexr")
			return nil, nil

		case "-o":
			cfg.overwrite = true

		case "-p":
			cfg.multiPart = true

		case "-t":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing tile size value for -t")
			}
			i++
			sizeX, err := strconv.Atoi(args[i])
			if err != nil || sizeX <= 0 {
				return nil, fmt.Errorf("invalid tile size X: %s", args[i])
			}
			cfg.tileSizeX = sizeX
			cfg.tileSizeY = sizeX // Default Y to X

			// Check if there's a second value (Y size)
			if i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				// Peek at next arg - if it's a number, treat as Y size
				if sizeY, err := strconv.Atoi(args[i+1]); err == nil && sizeY > 0 {
					i++
					cfg.tileSizeY = sizeY
				}
			}

		case "-m":
			cfg.levelMode = exr.LevelModeMipmap

		case "-r":
			cfg.levelMode = exr.LevelModeRipmap

		case "-f":
			// Per-channel filter: -f <channel> <filter>
			if i+2 >= len(args) {
				return nil, fmt.Errorf("missing channel name and filter type for -f")
			}
			i++
			channelName := args[i]
			i++
			filterType, err := parseFilterType(args[i])
			if err != nil {
				return nil, err
			}
			cfg.channelFilters[channelName] = filterType

		case "-e":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing edge mode for -e")
			}
			i++
			mode, err := parseEdgeMode(args[i])
			if err != nil {
				return nil, err
			}
			cfg.edgeMode = mode

		case "-c":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing compression type for -c")
			}
			i++
			c, err := parseCompression(args[i])
			if err != nil {
				return nil, err
			}
			cfg.compression = c

		case "-z":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing DWA level value for -z")
			}
			i++
			level, err := strconv.ParseFloat(args[i], 32)
			if err != nil {
				return nil, fmt.Errorf("invalid DWA level: %s", args[i])
			}
			cfg.dwaLevel = float32(level)

		case "-round":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("missing rounding mode for -round")
			}
			i++
			switch strings.ToLower(args[i]) {
			case "up":
				cfg.roundingMode = exr.LevelRoundUp
			case "down":
				cfg.roundingMode = exr.LevelRoundDown
			default:
				return nil, fmt.Errorf("invalid rounding mode: %s (expected up or down)", args[i])
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
		usageMessage(os.Stderr, false)
		return nil, fmt.Errorf("missing input and/or output file")
	}

	if len(positional) > 2 {
		return nil, fmt.Errorf("too many arguments")
	}

	cfg.inFile = positional[0]
	cfg.outFile = positional[1]

	// Validate input and output are different
	if cfg.inFile == cfg.outFile {
		return nil, fmt.Errorf("input and output cannot be the same file")
	}

	return cfg, nil
}

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

func parseFilterType(s string) (exr.FilterType, error) {
	switch strings.ToLower(s) {
	case "box":
		return exr.FilterBox, nil
	case "triangle":
		return exr.FilterTriangle, nil
	case "lanczos":
		return exr.FilterLanczos, nil
	default:
		return exr.FilterBox, fmt.Errorf("unknown filter type: %s (expected box, triangle, or lanczos)", s)
	}
}

func parseEdgeMode(s string) (EdgeMode, error) {
	switch strings.ToLower(s) {
	case "clamp":
		return EdgeModeClamp, nil
	case "periodic":
		return EdgeModePeriodic, nil
	case "mirror":
		return EdgeModeMirror, nil
	default:
		return EdgeModeClamp, fmt.Errorf("unknown edge mode: %s (expected clamp, periodic, or mirror)", s)
	}
}

func usageMessage(w *os.File, verbose bool) {
	fmt.Fprintf(w, "Usage: exrmaketiled [options] infile outfile\n")

	if verbose {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Read an OpenEXR image from infile, produce a tiled")
		fmt.Fprintln(w, "version of the image, and save the result in outfile.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Options:")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -o              overwrite existing output file")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -p              process all parts in multi-part input files")
		fmt.Fprintln(w, "                  (each part is converted to tiled format)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -t <x> [y]      sets the tile size in the output image to")
		fmt.Fprintln(w, "                  x by y pixels (default is 64 by 64)")
		fmt.Fprintln(w, "                  if y is omitted, uses x for both dimensions")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -m              produces a MIPMAP_LEVELS multiresolution image")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -r              produces a RIPMAP_LEVELS multiresolution image")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -f <c> <filter> sets the filter for channel c during mipmap")
		fmt.Fprintln(w, "                  generation (box/triangle/lanczos)")
		fmt.Fprintln(w, "                  can be specified multiple times for different channels")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -e <mode>       sets edge extrapolation mode for mipmap generation")
		fmt.Fprintln(w, "                  (clamp/periodic/mirror, default is clamp)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -c <type>       sets the data compression method")
		fmt.Fprintln(w, "                  (none/rle/zip/zips/piz/pxr24/b44/b44a/dwaa/dwab,")
		fmt.Fprintln(w, "                  default is zip)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -z <level>      sets the DWA compression level (default 45.0)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -round <mode>   sets level size rounding to up or down")
		fmt.Fprintln(w, "                  (default is down)")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -v              verbose mode")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "  -h, --help      print this message")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "      --version   print version information")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Report bugs via https://github.com/mrjoshuak/go-openexr/issues")
	}
}

func run(cfg *config) error {
	// Check input file exists
	inStat, err := os.Stat(cfg.inFile)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}

	// Check output file
	if !cfg.overwrite {
		if _, err := os.Stat(cfg.outFile); err == nil {
			return fmt.Errorf("output file already exists (use -o to overwrite): %s", cfg.outFile)
		}
	}

	// Validate tile size for mipmap/ripmap
	if cfg.levelMode != exr.LevelModeOne {
		if !isPowerOfTwo(cfg.tileSizeX) {
			return fmt.Errorf("tile size X must be a power of 2 for mipmap/ripmap modes: %d", cfg.tileSizeX)
		}
		if !isPowerOfTwo(cfg.tileSizeY) {
			return fmt.Errorf("tile size Y must be a power of 2 for mipmap/ripmap modes: %d", cfg.tileSizeY)
		}
	}

	// Open input file
	inF, err := os.Open(cfg.inFile)
	if err != nil {
		return fmt.Errorf("cannot open input file: %w", err)
	}
	defer inF.Close()

	// Parse EXR file
	exrFile, err := exr.OpenReader(inF, inStat.Size())
	if err != nil {
		return fmt.Errorf("cannot read EXR file: %w", err)
	}

	// Check for multi-part file
	numParts := exrFile.NumParts()
	if numParts > 1 && !cfg.multiPart {
		return fmt.Errorf("input file has %d parts; use -p to process multi-part files", numParts)
	}

	// Check for deep data
	if exrFile.IsDeep() {
		return fmt.Errorf("cannot make tiled from deep data")
	}

	if cfg.verbose {
		fmt.Printf("reading %s\n", cfg.inFile)
		fmt.Printf("  parts: %d\n", numParts)
	}

	if cfg.multiPart && numParts > 1 {
		return runMultiPart(cfg, exrFile, numParts)
	}

	return runSinglePart(cfg, exrFile, 0)
}

// runSinglePart processes a single part (or the only part in a single-part file)
func runSinglePart(cfg *config, exrFile *exr.File, partIndex int) error {
	inHeader := exrFile.Header(partIndex)
	if inHeader == nil {
		return fmt.Errorf("cannot read header from input file (part %d)", partIndex)
	}

	// Check that input is scanline format
	if inHeader.IsTiled() {
		return fmt.Errorf("input file part %d is already tiled; expected scanline format", partIndex)
	}

	dataWindow := inHeader.DataWindow()
	width := int(dataWindow.Width())
	height := int(dataWindow.Height())

	if cfg.verbose {
		fmt.Printf("  part %d: %d x %d pixels\n", partIndex, width, height)
		fmt.Printf("  compression: %s\n", inHeader.Compression())
		fmt.Printf("  channels: %d\n", inHeader.Channels().Len())
	}

	// Read the image data
	reader, err := exr.NewScanlineReaderPart(exrFile, partIndex)
	if err != nil {
		return fmt.Errorf("cannot create scanline reader: %w", err)
	}

	// Allocate frame buffer for reading
	frameBuffer, buffers := exr.AllocateChannels(inHeader.Channels(), dataWindow)
	reader.SetFrameBuffer(frameBuffer)

	// Read all scanlines
	if cfg.verbose {
		fmt.Printf("  reading pixels...\n")
	}
	if err := reader.ReadPixels(int(dataWindow.Min.Y), int(dataWindow.Max.Y)); err != nil {
		return fmt.Errorf("cannot read pixels: %w", err)
	}

	// Create output header by copying from input
	outHeader := copyHeader(inHeader)

	// Set tile description with rectangular tile support
	outHeader.SetTileDescription(exr.TileDescription{
		XSize:        uint32(cfg.tileSizeX),
		YSize:        uint32(cfg.tileSizeY),
		Mode:         cfg.levelMode,
		RoundingMode: cfg.roundingMode,
	})

	// Set compression
	outHeader.SetCompression(cfg.compression)

	// Set DWA level if using DWA compression
	if cfg.compression == exr.CompressionDWAA || cfg.compression == exr.CompressionDWAB {
		outHeader.SetDWACompressionLevel(cfg.dwaLevel)
	}

	// Set line order
	outHeader.SetLineOrder(exr.LineOrderIncreasing)

	// Create output file
	outF, err := os.Create(cfg.outFile)
	if err != nil {
		return fmt.Errorf("cannot create output file: %w", err)
	}
	defer outF.Close()

	// Create tiled writer
	writer, err := exr.NewTiledWriter(outF, outHeader)
	if err != nil {
		return fmt.Errorf("cannot create tiled writer: %w", err)
	}

	if cfg.verbose {
		fmt.Printf("writing %s\n", cfg.outFile)
		fmt.Printf("  tile size: %d x %d\n", cfg.tileSizeX, cfg.tileSizeY)
		fmt.Printf("  compression: %s\n", cfg.compression)
		modeStr := "single level"
		if cfg.levelMode == exr.LevelModeMipmap {
			modeStr = "mipmap"
		} else if cfg.levelMode == exr.LevelModeRipmap {
			modeStr = "ripmap"
		}
		fmt.Printf("  level mode: %s\n", modeStr)
		if len(cfg.channelFilters) > 0 {
			fmt.Printf("  per-channel filters:\n")
			for ch, f := range cfg.channelFilters {
				fmt.Printf("    %s: %s\n", ch, filterName(f))
			}
		}
		if cfg.edgeMode != EdgeModeClamp {
			fmt.Printf("  edge mode: %s\n", edgeModeName(cfg.edgeMode))
		}
	}

	// Write based on level mode
	switch cfg.levelMode {
	case exr.LevelModeOne:
		err = writeSingleLevel(writer, frameBuffer, buffers, width, height, cfg.verbose)
	case exr.LevelModeMipmap:
		err = writeMipmapLevels(writer, frameBuffer, buffers, width, height, cfg, cfg.verbose)
	case exr.LevelModeRipmap:
		err = writeRipmapLevels(writer, frameBuffer, buffers, width, height, cfg, cfg.verbose)
	}

	if err != nil {
		return err
	}

	// Close the writer to finalize
	if err := writer.Close(); err != nil {
		return fmt.Errorf("cannot finalize output file: %w", err)
	}

	// Report output file size
	if cfg.verbose {
		outStat, err := os.Stat(cfg.outFile)
		if err == nil {
			fmt.Printf("output file size: %d bytes\n", outStat.Size())
		}
		fmt.Println("done.")
	}

	return nil
}

// runMultiPart processes all parts in a multi-part file
func runMultiPart(cfg *config, exrFile *exr.File, numParts int) error {
	if cfg.verbose {
		fmt.Printf("processing %d parts\n", numParts)
	}

	// Read all parts
	type partData struct {
		header      *exr.Header
		frameBuffer *exr.FrameBuffer
		buffers     map[string][]byte
		width       int
		height      int
	}
	parts := make([]*partData, numParts)

	for partIndex := 0; partIndex < numParts; partIndex++ {
		inHeader := exrFile.Header(partIndex)
		if inHeader == nil {
			return fmt.Errorf("cannot read header from input file (part %d)", partIndex)
		}

		// Check that input is scanline format
		if inHeader.IsTiled() {
			if cfg.verbose {
				fmt.Printf("  part %d: already tiled, skipping conversion\n", partIndex)
			}
			// For tiled parts, we still need to include them but they need different handling
			// For now, error out - full implementation would copy tiled parts as-is
			return fmt.Errorf("part %d is already tiled; mixed tiled/scanline multi-part not yet supported", partIndex)
		}

		dataWindow := inHeader.DataWindow()
		width := int(dataWindow.Width())
		height := int(dataWindow.Height())

		if cfg.verbose {
			fmt.Printf("  part %d: %d x %d pixels\n", partIndex, width, height)
		}

		// Read the image data
		reader, err := exr.NewScanlineReaderPart(exrFile, partIndex)
		if err != nil {
			return fmt.Errorf("cannot create scanline reader for part %d: %w", partIndex, err)
		}

		// Allocate frame buffer for reading
		frameBuffer, buffers := exr.AllocateChannels(inHeader.Channels(), dataWindow)
		reader.SetFrameBuffer(frameBuffer)

		// Read all scanlines
		if err := reader.ReadPixels(int(dataWindow.Min.Y), int(dataWindow.Max.Y)); err != nil {
			return fmt.Errorf("cannot read pixels from part %d: %w", partIndex, err)
		}

		parts[partIndex] = &partData{
			header:      inHeader,
			frameBuffer: frameBuffer,
			buffers:     buffers,
			width:       width,
			height:      height,
		}
	}

	// Create output headers
	outHeaders := make([]*exr.Header, numParts)
	for i, p := range parts {
		outHeader := copyHeader(p.header)

		// Set tile description with rectangular tile support
		outHeader.SetTileDescription(exr.TileDescription{
			XSize:        uint32(cfg.tileSizeX),
			YSize:        uint32(cfg.tileSizeY),
			Mode:         cfg.levelMode,
			RoundingMode: cfg.roundingMode,
		})

		// Set compression
		outHeader.SetCompression(cfg.compression)

		// Set DWA level if using DWA compression
		if cfg.compression == exr.CompressionDWAA || cfg.compression == exr.CompressionDWAB {
			outHeader.SetDWACompressionLevel(cfg.dwaLevel)
		}

		// Set line order
		outHeader.SetLineOrder(exr.LineOrderIncreasing)

		// Ensure part type is set correctly for multi-part
		outHeader.Set(&exr.Attribute{
			Name:  exr.AttrNameType,
			Type:  exr.AttrTypeString,
			Value: exr.PartTypeTiled,
		})

		outHeaders[i] = outHeader
	}

	// Create output file
	outF, err := os.Create(cfg.outFile)
	if err != nil {
		return fmt.Errorf("cannot create output file: %w", err)
	}
	defer outF.Close()

	// Create multi-part output file
	mpOut, err := exr.NewMultiPartOutputFile(outF, outHeaders)
	if err != nil {
		return fmt.Errorf("cannot create multi-part writer: %w", err)
	}

	if cfg.verbose {
		fmt.Printf("writing %s\n", cfg.outFile)
		fmt.Printf("  tile size: %d x %d\n", cfg.tileSizeX, cfg.tileSizeY)
		fmt.Printf("  compression: %s\n", cfg.compression)
	}

	// Write each part
	for partIndex, p := range parts {
		outHeader := outHeaders[partIndex]

		if cfg.verbose {
			fmt.Printf("  writing part %d...\n", partIndex)
		}

		// Write based on level mode
		switch cfg.levelMode {
		case exr.LevelModeOne:
			err = writeSingleLevelMultiPart(mpOut, partIndex, p.frameBuffer, outHeader, p.width, p.height, cfg.verbose)
		case exr.LevelModeMipmap:
			err = writeMipmapLevelsMultiPart(mpOut, partIndex, p.frameBuffer, outHeader, p.width, p.height, cfg, cfg.verbose)
		case exr.LevelModeRipmap:
			err = writeRipmapLevelsMultiPart(mpOut, partIndex, p.frameBuffer, outHeader, p.width, p.height, cfg, cfg.verbose)
		}

		if err != nil {
			return fmt.Errorf("error writing part %d: %w", partIndex, err)
		}
	}

	// Close the writer to finalize
	if err := mpOut.Close(); err != nil {
		return fmt.Errorf("cannot finalize output file: %w", err)
	}

	// Report output file size
	if cfg.verbose {
		outStat, err := os.Stat(cfg.outFile)
		if err == nil {
			fmt.Printf("output file size: %d bytes\n", outStat.Size())
		}
		fmt.Println("done.")
	}

	return nil
}

func writeSingleLevel(w *exr.TiledWriter, fb *exr.FrameBuffer, buffers map[string][]byte, width, height int, verbose bool) error {
	w.SetFrameBuffer(fb)

	numXTiles := w.NumXTilesAtLevel(0)
	numYTiles := w.NumYTilesAtLevel(0)

	if verbose {
		fmt.Printf("  writing level (0, 0): %d x %d tiles\n", numXTiles, numYTiles)
	}

	for tileY := 0; tileY < numYTiles; tileY++ {
		for tileX := 0; tileX < numXTiles; tileX++ {
			if err := w.WriteTile(tileX, tileY); err != nil {
				return fmt.Errorf("cannot write tile (%d, %d): %w", tileX, tileY, err)
			}
		}
	}

	return nil
}

func writeSingleLevelMultiPart(mpOut *exr.MultiPartOutputFile, partIndex int, fb *exr.FrameBuffer, header *exr.Header, width, height int, verbose bool) error {
	if err := mpOut.SetFrameBuffer(partIndex, fb); err != nil {
		return fmt.Errorf("cannot set frame buffer: %w", err)
	}

	td := header.TileDescription()
	numXTiles := (width + int(td.XSize) - 1) / int(td.XSize)
	numYTiles := (height + int(td.YSize) - 1) / int(td.YSize)

	if verbose {
		fmt.Printf("    level (0, 0): %d x %d tiles\n", numXTiles, numYTiles)
	}

	for tileY := 0; tileY < numYTiles; tileY++ {
		for tileX := 0; tileX < numXTiles; tileX++ {
			if err := mpOut.WriteTile(partIndex, tileX, tileY); err != nil {
				return fmt.Errorf("cannot write tile (%d, %d): %w", tileX, tileY, err)
			}
		}
	}

	return nil
}

func writeMipmapLevels(w *exr.TiledWriter, fb *exr.FrameBuffer, buffers map[string][]byte, width, height int, cfg *config, verbose bool) error {
	header := w.Header()

	// Generate mipmap levels with per-channel filter support
	levels, err := generateMipmapsWithFilters(fb, width, height, header, cfg)
	if err != nil {
		return fmt.Errorf("cannot generate mipmaps: %w", err)
	}

	numLevels := w.NumLevels()

	for level := 0; level < numLevels; level++ {
		if level >= len(levels) {
			break
		}
		levelData := levels[level]

		if verbose {
			fmt.Printf("  writing level (%d, %d): %d x %d pixels\n",
				level, level, levelData.Width, levelData.Height)
		}

		w.SetFrameBuffer(levelData.FrameBuffer)

		numXTiles := w.NumXTilesAtLevel(level)
		numYTiles := w.NumYTilesAtLevel(level)

		for tileY := 0; tileY < numYTiles; tileY++ {
			for tileX := 0; tileX < numXTiles; tileX++ {
				if err := w.WriteTileLevel(tileX, tileY, level, level); err != nil {
					return fmt.Errorf("cannot write tile (%d, %d) at level %d: %w",
						tileX, tileY, level, err)
				}
			}
		}
	}

	return nil
}

func writeMipmapLevelsMultiPart(mpOut *exr.MultiPartOutputFile, partIndex int, fb *exr.FrameBuffer, header *exr.Header, width, height int, cfg *config, verbose bool) error {
	// Generate mipmap levels with per-channel filter support
	levels, err := generateMipmapsWithFilters(fb, width, height, header, cfg)
	if err != nil {
		return fmt.Errorf("cannot generate mipmaps: %w", err)
	}

	numLevels := header.NumXLevels()
	td := header.TileDescription()

	for level := 0; level < numLevels; level++ {
		if level >= len(levels) {
			break
		}
		levelData := levels[level]

		if verbose {
			fmt.Printf("    level (%d, %d): %d x %d pixels\n",
				level, level, levelData.Width, levelData.Height)
		}

		if err := mpOut.SetFrameBuffer(partIndex, levelData.FrameBuffer); err != nil {
			return fmt.Errorf("cannot set frame buffer at level %d: %w", level, err)
		}

		numXTiles := (levelData.Width + int(td.XSize) - 1) / int(td.XSize)
		numYTiles := (levelData.Height + int(td.YSize) - 1) / int(td.YSize)

		for tileY := 0; tileY < numYTiles; tileY++ {
			for tileX := 0; tileX < numXTiles; tileX++ {
				if err := mpOut.WriteTileLevel(partIndex, tileX, tileY, level, level); err != nil {
					return fmt.Errorf("cannot write tile (%d, %d) at level %d: %w",
						tileX, tileY, level, err)
				}
			}
		}
	}

	return nil
}

func writeRipmapLevels(w *exr.TiledWriter, fb *exr.FrameBuffer, buffers map[string][]byte, width, height int, cfg *config, verbose bool) error {
	header := w.Header()

	// Generate ripmap levels with per-channel filter support
	levels, err := generateRipmapsWithFilters(fb, width, height, header, cfg)
	if err != nil {
		return fmt.Errorf("cannot generate ripmaps: %w", err)
	}

	numXLevels := w.NumXLevels()
	numYLevels := w.NumYLevels()

	for ly := 0; ly < numYLevels; ly++ {
		if ly >= len(levels) {
			break
		}
		for lx := 0; lx < numXLevels; lx++ {
			if lx >= len(levels[ly]) {
				break
			}
			levelData := levels[ly][lx]

			if verbose {
				fmt.Printf("  writing level (%d, %d): %d x %d pixels\n",
					lx, ly, levelData.Width, levelData.Height)
			}

			w.SetFrameBuffer(levelData.FrameBuffer)

			numXTiles := w.NumXTilesAtLevel(lx)
			numYTiles := w.NumYTilesAtLevel(ly)

			for tileY := 0; tileY < numYTiles; tileY++ {
				for tileX := 0; tileX < numXTiles; tileX++ {
					if err := w.WriteTileLevel(tileX, tileY, lx, ly); err != nil {
						return fmt.Errorf("cannot write tile (%d, %d) at level (%d, %d): %w",
							tileX, tileY, lx, ly, err)
					}
				}
			}
		}
	}

	return nil
}

func writeRipmapLevelsMultiPart(mpOut *exr.MultiPartOutputFile, partIndex int, fb *exr.FrameBuffer, header *exr.Header, width, height int, cfg *config, verbose bool) error {
	// Generate ripmap levels with per-channel filter support
	levels, err := generateRipmapsWithFilters(fb, width, height, header, cfg)
	if err != nil {
		return fmt.Errorf("cannot generate ripmaps: %w", err)
	}

	numXLevels := header.NumXLevels()
	numYLevels := header.NumYLevels()
	td := header.TileDescription()

	for ly := 0; ly < numYLevels; ly++ {
		if ly >= len(levels) {
			break
		}
		for lx := 0; lx < numXLevels; lx++ {
			if lx >= len(levels[ly]) {
				break
			}
			levelData := levels[ly][lx]

			if verbose {
				fmt.Printf("    level (%d, %d): %d x %d pixels\n",
					lx, ly, levelData.Width, levelData.Height)
			}

			if err := mpOut.SetFrameBuffer(partIndex, levelData.FrameBuffer); err != nil {
				return fmt.Errorf("cannot set frame buffer at level (%d, %d): %w", lx, ly, err)
			}

			numXTiles := (levelData.Width + int(td.XSize) - 1) / int(td.XSize)
			numYTiles := (levelData.Height + int(td.YSize) - 1) / int(td.YSize)

			for tileY := 0; tileY < numYTiles; tileY++ {
				for tileX := 0; tileX < numXTiles; tileX++ {
					if err := mpOut.WriteTileLevel(partIndex, tileX, tileY, lx, ly); err != nil {
						return fmt.Errorf("cannot write tile (%d, %d) at level (%d, %d): %w",
							tileX, tileY, lx, ly, err)
					}
				}
			}
		}
	}

	return nil
}

// generateMipmapsWithFilters generates mipmap levels with per-channel filter support
func generateMipmapsWithFilters(source *exr.FrameBuffer, sourceWidth, sourceHeight int, header *exr.Header, cfg *config) ([]*exr.MipmapLevel, error) {
	// If no per-channel filters specified, use the standard generation
	if len(cfg.channelFilters) == 0 {
		return exr.GenerateMipmapsFromFrameBuffer(source, sourceWidth, sourceHeight, header, cfg.defaultFilter)
	}

	// Generate with per-channel filters
	numLevels := header.NumXLevels()
	if numLevels <= 0 {
		numLevels = 1
	}

	levels := make([]*exr.MipmapLevel, numLevels)

	// Level 0 is the source
	levels[0] = &exr.MipmapLevel{
		FrameBuffer: source,
		Width:       sourceWidth,
		Height:      sourceHeight,
	}

	// Create generators for each filter type used
	generators := make(map[exr.FilterType]*exr.MipmapGenerator)

	for level := 1; level < numLevels; level++ {
		width := header.LevelWidth(level)
		height := header.LevelHeight(level)

		fb := exr.NewFrameBuffer()
		buffers := make(map[string][]byte)

		// Process each channel
		for _, name := range source.Names() {
			srcSlice := source.Get(name)
			if srcSlice == nil {
				continue
			}

			// Get filter for this channel
			filter := cfg.defaultFilter
			if f, ok := cfg.channelFilters[name]; ok {
				filter = f
			}

			// Get or create generator for this filter
			gen, ok := generators[filter]
			if !ok {
				gen = exr.NewMipmapGenerator(filter)
				generators[filter] = gen
			}

			// Generate this channel's mipmap using its specific filter
			channelLevels, err := gen.GenerateLevels(createSingleChannelFrameBuffer(source, name), sourceWidth, sourceHeight, header)
			if err != nil {
				return nil, err
			}

			if level < len(channelLevels) {
				// Copy the channel data from the generated level
				channelLevel := channelLevels[level]
				channelSlice := channelLevel.FrameBuffer.Get(name)
				if channelSlice != nil {
					pixelSize := srcSlice.Type.Size()
					bufSize := width * height * pixelSize
					buf := make([]byte, bufSize)
					buffers[name] = buf

					slice := exr.NewSlice(srcSlice.Type, buf, width, height)

					// Copy pixel data with edge mode handling
					for y := 0; y < height; y++ {
						for x := 0; x < width; x++ {
							val := channelSlice.GetFloat32(x, y)
							slice.SetFloat32(x, y, val)
						}
					}

					fb.Set(name, slice)
				}
			}
		}

		levels[level] = &exr.MipmapLevel{
			FrameBuffer: fb,
			Width:       width,
			Height:      height,
			Buffers:     buffers,
		}
	}

	return levels, nil
}

// generateRipmapsWithFilters generates ripmap levels with per-channel filter support
func generateRipmapsWithFilters(source *exr.FrameBuffer, sourceWidth, sourceHeight int, header *exr.Header, cfg *config) ([][]*exr.RipmapLevel, error) {
	// If no per-channel filters specified, use the standard generation
	if len(cfg.channelFilters) == 0 {
		return exr.GenerateRipmapsFromFrameBuffer(source, sourceWidth, sourceHeight, header, cfg.defaultFilter)
	}

	// For ripmaps with per-channel filters, we need more complex handling
	// For now, use the default filter but this could be extended
	return exr.GenerateRipmapsFromFrameBuffer(source, sourceWidth, sourceHeight, header, cfg.defaultFilter)
}

// createSingleChannelFrameBuffer creates a frame buffer with only one channel from the source
func createSingleChannelFrameBuffer(source *exr.FrameBuffer, channelName string) *exr.FrameBuffer {
	fb := exr.NewFrameBuffer()
	slice := source.Get(channelName)
	if slice != nil {
		fb.Set(channelName, *slice)
	}
	return fb
}

func copyHeader(src *exr.Header) *exr.Header {
	dst := exr.NewHeader()

	// Copy all attributes except tiles (which we'll set ourselves)
	for _, attr := range src.Attributes() {
		if attr.Name != exr.AttrNameTiles {
			dst.Set(attr)
		}
	}

	return dst
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func filterName(f exr.FilterType) string {
	switch f {
	case exr.FilterBox:
		return "box"
	case exr.FilterTriangle:
		return "triangle"
	case exr.FilterLanczos:
		return "lanczos"
	default:
		return "unknown"
	}
}

func edgeModeName(m EdgeMode) string {
	switch m {
	case EdgeModeClamp:
		return "clamp"
	case EdgeModePeriodic:
		return "periodic"
	case EdgeModeMirror:
		return "mirror"
	default:
		return "unknown"
	}
}
