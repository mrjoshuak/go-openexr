// exrinfo displays information about OpenEXR files.
//
// Usage:
//
//	exrinfo [-v|--verbose] [-a|--all-metadata] [-s|--strict] <filename> [<filename> ...]
//
// Use '-' as filename to read from stdin:
//
//	cat image.exr | exrinfo -
//
// Options:
//
//	-v, --verbose       Print detailed information including all header attributes
//	-a, --all-metadata  Print all metadata including custom attributes as hex
//	-s, --strict        Strict mode: validate file structure and report violations
//	-h, -?, --help      Print help message
//	    --version       Print version information
package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
)

const version = "1.0.0"

var (
	verbose  bool
	allMeta  bool
	strict   bool
	showHelp bool
	showVer  bool
)

func init() {
	flag.BoolVar(&verbose, "v", false, "verbose mode")
	flag.BoolVar(&verbose, "verbose", false, "verbose mode")
	flag.BoolVar(&allMeta, "a", false, "print all metadata")
	flag.BoolVar(&allMeta, "all-metadata", false, "print all metadata")
	flag.BoolVar(&strict, "s", false, "strict mode")
	flag.BoolVar(&strict, "strict", false, "strict mode")
	flag.BoolVar(&showHelp, "h", false, "print help message")
	flag.BoolVar(&showHelp, "help", false, "print help message")
	flag.BoolVar(&showHelp, "?", false, "print help message")
	flag.BoolVar(&showVer, "version", false, "print version information")
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [-v|--verbose] [-a|--all-metadata] [-s|--strict] <filename> [<filename> ...]\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "Read EXR files and print values of header attributes\n\n")
	fmt.Fprintf(os.Stderr, "Use '-' as filename to read from stdin.\n\n")
	fmt.Fprintf(os.Stderr, "Options:\n")
	fmt.Fprintf(os.Stderr, "  -s, --strict        strict mode\n")
	fmt.Fprintf(os.Stderr, "  -a, --all-metadata  print all metadata\n")
	fmt.Fprintf(os.Stderr, "  -v, --verbose       verbose mode\n")
	fmt.Fprintf(os.Stderr, "  -h, -?, --help      print this message\n")
	fmt.Fprintf(os.Stderr, "      --version       print version information\n")
	fmt.Fprintf(os.Stderr, "\nReport bugs via https://github.com/mrjoshuak/go-openexr/issues\n")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if showHelp {
		usage()
		os.Exit(0)
	}

	if showVer {
		fmt.Printf("exrinfo (go-openexr) %s\n", version)
		fmt.Println("Pure Go implementation")
		os.Exit(0)
	}

	args := flag.Args()
	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	failCount := 0
	for i, filename := range args {
		if i > 0 {
			fmt.Println() // Separate multiple file outputs
		}
		if err := processFile(filename); err != nil {
			fmt.Fprintf(os.Stderr, "ERROR '%s': %v\n", filename, err)
			failCount++
		}
	}

	os.Exit(failCount)
}

func processFile(filename string) error {
	var reader io.ReaderAt
	var size int64
	var displayName string

	if filename == "-" {
		// Read from stdin
		data, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("reading stdin: %w", err)
		}
		reader = bytes.NewReader(data)
		size = int64(len(data))
		displayName = "<stdin>"
	} else {
		f, err := os.Open(filename)
		if err != nil {
			return err
		}
		defer f.Close()

		fi, err := f.Stat()
		if err != nil {
			return err
		}
		reader = f
		size = fi.Size()
		displayName = filename
	}

	exrFile, err := exr.OpenReader(reader, size)
	if err != nil {
		return err
	}

	// Validate in strict mode
	var validationErrors []string
	if strict {
		validationErrors = validateFile(exrFile)
	}

	// Print file info
	printFileInfo(displayName, exrFile)

	// Print validation errors in strict mode
	if strict && len(validationErrors) > 0 {
		fmt.Println("\nValidation errors:")
		for _, e := range validationErrors {
			fmt.Printf("  - %s\n", e)
		}
		return fmt.Errorf("%d validation error(s)", len(validationErrors))
	}

	return nil
}

func printFileInfo(filename string, f *exr.File) {
	fmt.Printf("File: %s\n", filename)
	fmt.Printf("  Version: %d\n", f.Version())

	// Version flags
	var flags []string
	if f.IsTiled() {
		flags = append(flags, "tiled")
	}
	if f.IsDeep() {
		flags = append(flags, "deep")
	}
	if f.IsMultiPart() {
		flags = append(flags, "multipart")
	}
	if exr.HasLongNames(f.VersionField()) {
		flags = append(flags, "long_names")
	}
	if len(flags) > 0 {
		fmt.Printf("  Flags: %s\n", strings.Join(flags, ", "))
	}

	numParts := f.NumParts()
	if numParts > 1 {
		fmt.Printf("  Parts: %d\n", numParts)
	}

	// Print each part
	for i := 0; i < numParts; i++ {
		if numParts > 1 {
			fmt.Printf("\n  Part %d:\n", i)
		}
		printPartInfo(f, i, numParts > 1)
	}
}

func printPartInfo(f *exr.File, partIndex int, multiPart bool) {
	h := f.Header(partIndex)
	indent := "  "
	if multiPart {
		indent = "    "
	}

	// Part name and type (for multi-part files)
	if multiPart {
		if name := getStringAttr(h, "name"); name != "" {
			fmt.Printf("%sName: %s\n", indent, name)
		}
		if partType := getStringAttr(h, "type"); partType != "" {
			fmt.Printf("%sType: %s\n", indent, partType)
		}
	}

	// Data window (dimensions)
	dw := h.DataWindow()
	fmt.Printf("%sData window: (%d, %d) - (%d, %d)  [%d x %d]\n",
		indent,
		dw.Min.X, dw.Min.Y, dw.Max.X, dw.Max.Y,
		dw.Width(), dw.Height())

	// Display window
	displayW := h.DisplayWindow()
	if displayW != dw {
		fmt.Printf("%sDisplay window: (%d, %d) - (%d, %d)  [%d x %d]\n",
			indent,
			displayW.Min.X, displayW.Min.Y, displayW.Max.X, displayW.Max.Y,
			displayW.Width(), displayW.Height())
	}

	// Compression
	fmt.Printf("%sCompression: %s\n", indent, h.Compression().String())

	// Channels
	cl := h.Channels()
	if cl != nil && cl.Len() > 0 {
		fmt.Printf("%sChannels (%d):\n", indent, cl.Len())
		for i := 0; i < cl.Len(); i++ {
			ch := cl.At(i)
			samplingInfo := ""
			if ch.XSampling != 1 || ch.YSampling != 1 {
				samplingInfo = fmt.Sprintf(" (sampling: %d, %d)", ch.XSampling, ch.YSampling)
			}
			plinearInfo := ""
			if ch.PLinear {
				plinearInfo = " [plinear]"
			}
			fmt.Printf("%s  - %s: %s%s%s\n", indent, ch.Name, ch.Type.String(), samplingInfo, plinearInfo)
		}
	}

	// Tile info (if tiled)
	if td := h.TileDescription(); td != nil {
		fmt.Printf("%sTile size: %d x %d\n", indent, td.XSize, td.YSize)
		levelMode := "one level"
		switch td.Mode {
		case exr.LevelModeMipmap:
			levelMode = "mipmap"
		case exr.LevelModeRipmap:
			levelMode = "ripmap"
		}
		fmt.Printf("%sLevel mode: %s\n", indent, levelMode)
		roundingMode := "round down"
		if td.RoundingMode == exr.LevelRoundUp {
			roundingMode = "round up"
		}
		fmt.Printf("%sRounding mode: %s\n", indent, roundingMode)

		if verbose || allMeta {
			fmt.Printf("%sX levels: %d, Y levels: %d\n", indent, h.NumXLevels(), h.NumYLevels())
		}
	}

	// Verbose/all-metadata: show all attributes
	if verbose || allMeta {
		fmt.Printf("%sLine order: %s\n", indent, h.LineOrder().String())
		fmt.Printf("%sPixel aspect ratio: %g\n", indent, h.PixelAspectRatio())
		swc := h.ScreenWindowCenter()
		fmt.Printf("%sScreen window center: (%g, %g)\n", indent, swc.X, swc.Y)
		fmt.Printf("%sScreen window width: %g\n", indent, h.ScreenWindowWidth())

		// Chunk/offset info
		offsets := f.Offsets(partIndex)
		fmt.Printf("%sChunks: %d\n", indent, len(offsets))

		if allMeta && len(offsets) > 0 {
			fmt.Printf("%sOffset table:\n", indent)
			// Show first and last few offsets
			maxShow := 5
			if len(offsets) <= maxShow*2 {
				for i, off := range offsets {
					fmt.Printf("%s  [%d]: %d\n", indent, i, off)
				}
			} else {
				for i := 0; i < maxShow; i++ {
					fmt.Printf("%s  [%d]: %d\n", indent, i, offsets[i])
				}
				fmt.Printf("%s  ...\n", indent)
				for i := len(offsets) - maxShow; i < len(offsets); i++ {
					fmt.Printf("%s  [%d]: %d\n", indent, i, offsets[i])
				}
			}
		}

		// All attributes
		printAllAttributes(h, indent, allMeta)
	}
}

func printAllAttributes(h *exr.Header, indent string, showHex bool) {
	attrs := h.Attributes()
	if len(attrs) == 0 {
		return
	}

	// Sort attributes by name for consistent output
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Name < attrs[j].Name
	})

	fmt.Printf("%sAll attributes:\n", indent)

	// Skip the standard attributes we've already displayed
	displayed := map[string]bool{
		"channels":           true,
		"compression":        true,
		"dataWindow":         true,
		"displayWindow":      true,
		"lineOrder":          true,
		"pixelAspectRatio":   true,
		"screenWindowCenter": true,
		"screenWindowWidth":  true,
		"tiles":              true,
		"name":               true,
		"type":               true,
	}

	for _, attr := range attrs {
		if displayed[attr.Name] {
			continue
		}
		fmt.Printf("%s  %s (%s): %s\n", indent, attr.Name, attr.Type, formatAttributeValue(attr, showHex))
	}
}

func formatAttributeValue(attr *exr.Attribute, showHex bool) string {
	switch v := attr.Value.(type) {
	case string:
		return fmt.Sprintf("%q", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case exr.V2i:
		return fmt.Sprintf("(%d, %d)", v.X, v.Y)
	case exr.V2f:
		return fmt.Sprintf("(%g, %g)", v.X, v.Y)
	case exr.V3i:
		return fmt.Sprintf("(%d, %d, %d)", v.X, v.Y, v.Z)
	case exr.V3f:
		return fmt.Sprintf("(%g, %g, %g)", v.X, v.Y, v.Z)
	case exr.Box2i:
		return fmt.Sprintf("(%d, %d) - (%d, %d)", v.Min.X, v.Min.Y, v.Max.X, v.Max.Y)
	case exr.Box2f:
		return fmt.Sprintf("(%g, %g) - (%g, %g)", v.Min.X, v.Min.Y, v.Max.X, v.Max.Y)
	case exr.M33f:
		return fmt.Sprintf("[%g %g %g; %g %g %g; %g %g %g]",
			v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], v[8])
	case exr.M44f:
		return fmt.Sprintf("[%g %g %g %g; %g %g %g %g; %g %g %g %g; %g %g %g %g]",
			v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
			v[8], v[9], v[10], v[11], v[12], v[13], v[14], v[15])
	case exr.Chromaticities:
		return fmt.Sprintf("red(%g,%g) green(%g,%g) blue(%g,%g) white(%g,%g)",
			v.RedX, v.RedY, v.GreenX, v.GreenY, v.BlueX, v.BlueY, v.WhiteX, v.WhiteY)
	case exr.Rational:
		return fmt.Sprintf("%d/%d (%.6f)", v.Num, v.Denom, v.Float64())
	case exr.TimeCode:
		return fmt.Sprintf("%02d:%02d:%02d:%02d", v.Hours(), v.Minutes(), v.Seconds(), v.Frame())
	case exr.KeyCode:
		return fmt.Sprintf("mfc=%d type=%d prefix=%d count=%d perf_offset=%d perfs_per_frame=%d perfs_per_count=%d",
			v.FilmMfcCode, v.FilmType, v.Prefix, v.Count, v.PerfOffset, v.PerfsPerFrame, v.PerfsPerCount)
	case exr.Preview:
		return fmt.Sprintf("%dx%d preview image (%d bytes)", v.Width, v.Height, len(v.Pixels))
	case exr.EnvMap:
		switch v {
		case exr.EnvMapLatLong:
			return "latlong"
		case exr.EnvMapCube:
			return "cube"
		default:
			return fmt.Sprintf("unknown(%d)", v)
		}
	case []string:
		return fmt.Sprintf("[%s]", strings.Join(v, ", "))
	case []byte:
		if showHex {
			if len(v) <= 32 {
				return hex.EncodeToString(v)
			}
			return fmt.Sprintf("%s... (%d bytes)", hex.EncodeToString(v[:32]), len(v))
		}
		return fmt.Sprintf("<%d bytes>", len(v))
	default:
		return fmt.Sprintf("%v", v)
	}
}

func getStringAttr(h *exr.Header, name string) string {
	attr := h.Get(name)
	if attr == nil {
		return ""
	}
	if s, ok := attr.Value.(string); ok {
		return s
	}
	return ""
}

func validateFile(f *exr.File) []string {
	var errors []string

	// Check version
	if f.Version() != 2 {
		errors = append(errors, fmt.Sprintf("unexpected version: %d (expected 2)", f.Version()))
	}

	// Validate each part
	for i := 0; i < f.NumParts(); i++ {
		h := f.Header(i)
		partErrors := validatePart(h, i, f.NumParts() > 1)
		errors = append(errors, partErrors...)

		// Validate offset table
		offsets := f.Offsets(i)
		expectedChunks := h.ChunksInFile()
		if len(offsets) != expectedChunks {
			errors = append(errors, fmt.Sprintf("part %d: offset table size mismatch: got %d, expected %d",
				i, len(offsets), expectedChunks))
		}

		// Check for valid offsets (should be positive and increasing in most cases)
		for j, off := range offsets {
			if off <= 0 {
				errors = append(errors, fmt.Sprintf("part %d: invalid offset at index %d: %d", i, j, off))
				break // Don't spam with too many errors
			}
		}
	}

	return errors
}

func validatePart(h *exr.Header, partIndex int, isMultiPart bool) []string {
	var errors []string
	prefix := ""
	if isMultiPart {
		prefix = fmt.Sprintf("part %d: ", partIndex)
	}

	// Check required attributes
	if err := h.Validate(); err != nil {
		errors = append(errors, prefix+err.Error())
	}

	// Additional strict checks
	dw := h.DataWindow()
	if dw.Width() <= 0 || dw.Height() <= 0 {
		errors = append(errors, prefix+"data window has non-positive dimensions")
	}

	displayW := h.DisplayWindow()
	if displayW.Width() <= 0 || displayW.Height() <= 0 {
		errors = append(errors, prefix+"display window has non-positive dimensions")
	}

	// Check channels
	cl := h.Channels()
	if cl == nil || cl.Len() == 0 {
		errors = append(errors, prefix+"no channels defined")
	} else {
		for i := 0; i < cl.Len(); i++ {
			ch := cl.At(i)
			if ch.XSampling <= 0 || ch.YSampling <= 0 {
				errors = append(errors, fmt.Sprintf("%schannel '%s' has invalid sampling: (%d, %d)",
					prefix, ch.Name, ch.XSampling, ch.YSampling))
			}
		}
	}

	// Multi-part files must have name attribute
	if isMultiPart {
		if getStringAttr(h, "name") == "" {
			errors = append(errors, prefix+"missing 'name' attribute (required for multi-part files)")
		}
	}

	// Validate tile description if present
	if td := h.TileDescription(); td != nil {
		if td.XSize == 0 || td.YSize == 0 {
			errors = append(errors, prefix+"tile size is zero")
		}
		if td.Mode > exr.LevelModeRipmap {
			errors = append(errors, fmt.Sprintf("%sinvalid level mode: %d", prefix, td.Mode))
		}
		if td.RoundingMode > exr.LevelRoundUp {
			errors = append(errors, fmt.Sprintf("%sinvalid rounding mode: %d", prefix, td.RoundingMode))
		}
	}

	// Check compression type is valid
	comp := h.Compression()
	if comp > exr.CompressionDWAB {
		errors = append(errors, fmt.Sprintf("%sunknown compression type: %d", prefix, comp))
	}

	// Check line order is valid
	lo := h.LineOrder()
	if lo > exr.LineOrderRandom {
		errors = append(errors, fmt.Sprintf("%sunknown line order: %d", prefix, lo))
	}

	return errors
}
