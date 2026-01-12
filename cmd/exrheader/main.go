// exrheader prints header information from OpenEXR files.
//
// Usage:
//
//	exrheader <filename> [<filename> ...]
//
// Options:
//
//	-h, --help     print this message
//	--version      print version information
package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/mrjoshuak/go-openexr/exr"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		usageMessage(os.Stderr, false)
		os.Exit(1)
	}

	// Check for help or version flags
	for _, arg := range os.Args[1:] {
		if arg == "-h" || arg == "--help" {
			usageMessage(os.Stdout, true)
			os.Exit(0)
		}
		if arg == "--version" {
			fmt.Printf("exrheader (go-openexr) %s\n", version)
			fmt.Println("https://github.com/mrjoshuak/go-openexr")
			os.Exit(0)
		}
	}

	// Process each file
	exitCode := 0
	for i := 1; i < len(os.Args); i++ {
		if err := printInfo(os.Args[i]); err != nil {
			fmt.Fprintf(os.Stderr, "exrheader: %s: %v\n", os.Args[i], err)
			exitCode = 1
		}
	}

	os.Exit(exitCode)
}

func usageMessage(w *os.File, verbose bool) {
	fmt.Fprintf(w, "Usage: exrheader imagefile [imagefile ...]\n")

	if verbose {
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Read exr files and print the values of header attributes.")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Options:")
		fmt.Fprintln(w, "  -h, --help        print this message")
		fmt.Fprintln(w, "      --version     print version information")
		fmt.Fprintln(w, "")
		fmt.Fprintln(w, "Report bugs via https://github.com/mrjoshuak/go-openexr/issues")
	}
}

func printInfo(fileName string) error {
	// Open the file
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	if err != nil {
		return err
	}

	// Open as EXR file
	exrFile, err := exr.OpenReader(f, stat.Size())
	if err != nil {
		return err
	}

	numParts := exrFile.NumParts()

	// Print file name and version info
	fmt.Printf("\nfile %s:\n\n", fileName)

	versionField := exrFile.VersionField()
	versionNum := exr.Version(versionField)
	flags := versionField >> 8

	fmt.Printf("file format version: %d, flags 0x%x\n", versionNum, flags)

	// Print header for each part
	for p := 0; p < numParts; p++ {
		h := exrFile.Header(p)
		if h == nil {
			continue
		}

		if numParts > 1 {
			fmt.Printf("\n\n part %d:\n", p)
		}

		printHeader(h)
	}

	fmt.Println()
	return nil
}

func printHeader(h *exr.Header) {
	// Get all attributes and sort by name
	attrs := h.Attributes()
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Name < attrs[j].Name
	})

	for _, attr := range attrs {
		printAttribute(attr)
	}
}

func printAttribute(attr *exr.Attribute) {
	fmt.Printf("%s (type %s)", attr.Name, attr.Type)

	switch attr.Type {
	case exr.AttrTypeBox2i:
		b := attr.Value.(exr.Box2i)
		fmt.Printf(": (%d %d) - (%d %d)", b.Min.X, b.Min.Y, b.Max.X, b.Max.Y)

	case exr.AttrTypeBox2f:
		b := attr.Value.(exr.Box2f)
		fmt.Printf(": (%g %g) - (%g %g)", b.Min.X, b.Min.Y, b.Max.X, b.Max.Y)

	case exr.AttrTypeChlist:
		cl := attr.Value.(*exr.ChannelList)
		fmt.Print(":")
		printChannelList(cl)

	case exr.AttrTypeChromaticities:
		c := attr.Value.(exr.Chromaticities)
		fmt.Printf(":\n")
		fmt.Printf("    red   (%g, %g)\n", c.RedX, c.RedY)
		fmt.Printf("    green (%g, %g)\n", c.GreenX, c.GreenY)
		fmt.Printf("    blue  (%g, %g)\n", c.BlueX, c.BlueY)
		fmt.Printf("    white (%g, %g)", c.WhiteX, c.WhiteY)

	case exr.AttrTypeCompression:
		c := attr.Value.(exr.Compression)
		fmt.Printf(": %s", compressionDescription(c))

	case exr.AttrTypeDouble:
		v := attr.Value.(float64)
		fmt.Printf(": %g", v)

	case exr.AttrTypeEnvmap:
		e := attr.Value.(exr.EnvMap)
		fmt.Printf(": %s", envmapDescription(e))

	case exr.AttrTypeFloat:
		v := attr.Value.(float32)
		fmt.Printf(": %g", v)

	case exr.AttrTypeInt:
		v := attr.Value.(int32)
		fmt.Printf(": %d", v)

	case exr.AttrTypeKeycode:
		kc := attr.Value.(exr.KeyCode)
		fmt.Printf(":\n")
		fmt.Printf("    film manufacturer code %d\n", kc.FilmMfcCode)
		fmt.Printf("    film type code %d\n", kc.FilmType)
		fmt.Printf("    prefix %d\n", kc.Prefix)
		fmt.Printf("    count %d\n", kc.Count)
		fmt.Printf("    perf offset %d\n", kc.PerfOffset)
		fmt.Printf("    perfs per frame %d\n", kc.PerfsPerFrame)
		fmt.Printf("    perfs per count %d", kc.PerfsPerCount)

	case exr.AttrTypeLineOrder:
		lo := attr.Value.(exr.LineOrder)
		fmt.Printf(": %s", lineOrderDescription(lo))

	case exr.AttrTypeM33f:
		m := attr.Value.(exr.M33f)
		fmt.Printf(":\n")
		fmt.Printf("   (%g %g %g\n", m[0], m[1], m[2])
		fmt.Printf("    %g %g %g\n", m[3], m[4], m[5])
		fmt.Printf("    %g %g %g)", m[6], m[7], m[8])

	case exr.AttrTypeM44f:
		m := attr.Value.(exr.M44f)
		fmt.Printf(":\n")
		fmt.Printf("   (%g %g %g %g\n", m[0], m[1], m[2], m[3])
		fmt.Printf("    %g %g %g %g\n", m[4], m[5], m[6], m[7])
		fmt.Printf("    %g %g %g %g\n", m[8], m[9], m[10], m[11])
		fmt.Printf("    %g %g %g %g)", m[12], m[13], m[14], m[15])

	case exr.AttrTypePreview:
		p := attr.Value.(exr.Preview)
		fmt.Printf(": %d by %d pixels", p.Width, p.Height)

	case exr.AttrTypeString:
		s := attr.Value.(string)
		fmt.Printf(": \"%s\"", s)

	case exr.AttrTypeStringVector:
		sv := attr.Value.([]string)
		fmt.Print(":")
		for _, s := range sv {
			fmt.Printf("\n    \"%s\"", s)
		}

	case exr.AttrTypeRational:
		r := attr.Value.(exr.Rational)
		fmt.Printf(": %d/%d (%g)", r.Num, r.Denom, r.Float64())

	case exr.AttrTypeTileDesc:
		td := attr.Value.(exr.TileDescription)
		fmt.Printf(":\n")
		fmt.Printf("    %s\n", levelModeDescription(td.Mode))
		fmt.Printf("    tile size %d by %d pixels", td.XSize, td.YSize)
		if td.Mode != exr.LevelModeOne {
			fmt.Printf("\n    level sizes rounded %s", levelRoundingModeDescription(td.RoundingMode))
		}

	case exr.AttrTypeTimecode:
		tc := attr.Value.(exr.TimeCode)
		fmt.Printf(":\n")
		printTimeCode(tc)

	case exr.AttrTypeV2i:
		v := attr.Value.(exr.V2i)
		fmt.Printf(": (%d, %d)", v.X, v.Y)

	case exr.AttrTypeV2f:
		v := attr.Value.(exr.V2f)
		fmt.Printf(": (%g, %g)", v.X, v.Y)

	case exr.AttrTypeV3i:
		v := attr.Value.(exr.V3i)
		fmt.Printf(": (%d, %d, %d)", v.X, v.Y, v.Z)

	case exr.AttrTypeV3f:
		v := attr.Value.(exr.V3f)
		fmt.Printf(": (%g, %g, %g)", v.X, v.Y, v.Z)

	default:
		// Raw bytes for unknown types
		if b, ok := attr.Value.([]byte); ok {
			if len(b) <= 32 {
				fmt.Printf(": %s", formatBytes(b))
			} else {
				fmt.Printf(": (%d bytes)", len(b))
			}
		}
	}

	fmt.Println()
}

func printChannelList(cl *exr.ChannelList) {
	// Sort channels by name for consistent output
	channels := cl.Channels()
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].Name < channels[j].Name
	})

	for _, c := range channels {
		fmt.Printf("\n    %s, %s", c.Name, pixelTypeDescription(c.Type))
		fmt.Printf(", sampling %d %d", c.XSampling, c.YSampling)
		if c.PLinear {
			fmt.Print(", plinear")
		}
	}
}

func printTimeCode(tc exr.TimeCode) {
	fmt.Printf("    time %02d:%02d:%02d:%02d\n",
		tc.Hours(), tc.Minutes(), tc.Seconds(), tc.Frame())

	// Print flags using proper accessors
	fmt.Printf("    drop frame %d, color frame %d, field/phase %d\n",
		boolToInt(tc.DropFrame()), boolToInt(tc.ColorFrame()), boolToInt(tc.FieldPhase()))

	fmt.Printf("    bgf0 %d, bgf1 %d, bgf2 %d\n",
		boolToInt(tc.Bgf0()), boolToInt(tc.Bgf1()), boolToInt(tc.Bgf2()))

	fmt.Printf("    user data 0x%x", tc.UserData())
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func compressionDescription(c exr.Compression) string {
	switch c {
	case exr.CompressionNone:
		return "none"
	case exr.CompressionRLE:
		return "rle"
	case exr.CompressionZIPS:
		return "zip, individual scanlines"
	case exr.CompressionZIP:
		return "zip, multi-scanline blocks"
	case exr.CompressionPIZ:
		return "piz"
	case exr.CompressionPXR24:
		return "pxr24"
	case exr.CompressionB44:
		return "b44"
	case exr.CompressionB44A:
		return "b44a"
	case exr.CompressionDWAA:
		return "dwaa"
	case exr.CompressionDWAB:
		return "dwab"
	default:
		return fmt.Sprintf("unknown (%d)", int(c))
	}
}

func lineOrderDescription(lo exr.LineOrder) string {
	switch lo {
	case exr.LineOrderIncreasing:
		return "increasing y"
	case exr.LineOrderDecreasing:
		return "decreasing y"
	case exr.LineOrderRandom:
		return "random y"
	default:
		return fmt.Sprintf("%d", int(lo))
	}
}

func envmapDescription(e exr.EnvMap) string {
	switch e {
	case exr.EnvMapLatLong:
		return "latitude-longitude map"
	case exr.EnvMapCube:
		return "cube-face map"
	default:
		return fmt.Sprintf("map type %d", int(e))
	}
}

func levelModeDescription(lm exr.LevelMode) string {
	switch lm {
	case exr.LevelModeOne:
		return "single level"
	case exr.LevelModeMipmap:
		return "mip-map"
	case exr.LevelModeRipmap:
		return "rip-map"
	default:
		return fmt.Sprintf("level mode %d", int(lm))
	}
}

func levelRoundingModeDescription(rm exr.LevelRoundingMode) string {
	switch rm {
	case exr.LevelRoundDown:
		return "down"
	case exr.LevelRoundUp:
		return "up"
	default:
		return fmt.Sprintf("mode %d", int(rm))
	}
}

func pixelTypeDescription(pt exr.PixelType) string {
	switch pt {
	case exr.PixelTypeUint:
		return "32-bit unsigned integer"
	case exr.PixelTypeHalf:
		return "16-bit floating-point"
	case exr.PixelTypeFloat:
		return "32-bit floating-point"
	default:
		return fmt.Sprintf("type %d", int(pt))
	}
}

func formatBytes(b []byte) string {
	var buf bytes.Buffer
	for i, v := range b {
		if i > 0 {
			buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%02x", v)
	}
	return buf.String()
}
