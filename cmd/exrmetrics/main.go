// exrmetrics benchmarks OpenEXR read/write performance.
//
// This tool mirrors the C++ exrmetrics tool from the upstream OpenEXR project,
// enabling direct performance comparison between the Go and C++ implementations.
//
// Usage:
//
//	exrmetrics [options] inputfile
//
// Options:
//
//	--bench         Run comprehensive benchmarks (all compressions, half/float)
//	--passes N      Number of passes for timing (default: 10)
//	--compression   Compression method (none,rle,zips,zip,piz,pxr24,b44,b44a,dwaa,dwab,all)
//	--pixelmode     Pixel type (half,float,orig,all)
//	--csv           Output in CSV format (default for --bench)
//	--json          Output in JSON format
//	-t N            Number of threads (0=single, -1=auto)
//	-v              Verbose output
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

type pixelMode int

const (
	pixelModeOrig pixelMode = iota
	pixelModeHalf
	pixelModeFloat
)

func (m pixelMode) String() string {
	switch m {
	case pixelModeHalf:
		return "half"
	case pixelModeFloat:
		return "float"
	default:
		return "original"
	}
}

type runResult struct {
	FileName    string
	Compression exr.Compression
	PixelMode   pixelMode
	WriteTime   time.Duration
	RereadTime  time.Duration
}

func main() {
	var (
		bench       = flag.Bool("bench", false, "Run comprehensive benchmarks")
		passes      = flag.Int("passes", 10, "Number of passes for timing")
		compression = flag.String("compression", "orig", "Compression method(s)")
		pixelmode   = flag.String("pixelmode", "orig", "Pixel mode(s)")
		csvOutput   = flag.Bool("csv", false, "Output in CSV format")
		jsonOutput  = flag.Bool("json", false, "Output in JSON format")
		multiThread = flag.Bool("m", false, "Set to multi-threaded (system selected thread count)")
		threads     = flag.Int("t", 0, "Use a pool of n worker threads for processing")
		verbose     = flag.Bool("v", false, "Verbose output")
	)
	// Add -z as alias for --compression
	flag.StringVar(compression, "z", "orig", "Compression method(s) (alias for --compression)")
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Fprintln(os.Stderr, "Usage: exrmetrics [options] inputfile")
		flag.PrintDefaults()
		os.Exit(1)
	}

	inputFile := flag.Arg(0)

	// Set thread count and parallel config
	if *multiThread {
		runtime.GOMAXPROCS(runtime.NumCPU())
		exr.SetParallelConfig(exr.ParallelConfig{
			NumWorkers: 0, // 0 means use all CPUs
			GrainSize:  1, // At least 1 chunk per worker (parallelize aggressively)
		})
	} else if *threads > 0 {
		runtime.GOMAXPROCS(*threads)
		exr.SetParallelConfig(exr.ParallelConfig{
			NumWorkers: *threads,
			GrainSize:  1,
		})
	} else {
		// Single-threaded: disable parallel processing
		exr.SetParallelConfig(exr.ParallelConfig{
			NumWorkers: 1,
			GrainSize:  1000, // High grain size to force sequential
		})
	}

	// Parse compression list
	compressions := parseCompressions(*compression, *bench)

	// Parse pixel modes
	pixelModes := parsePixelModes(*pixelmode, *bench)

	// --bench implies CSV output
	if *bench {
		*csvOutput = true
	}

	// Run benchmarks
	results := runBenchmarks(inputFile, compressions, pixelModes, *passes, *verbose)

	// Output results
	if *jsonOutput {
		printJSON(results)
	} else if *csvOutput {
		printCSV(results)
	} else {
		printCSV(results)
	}
}

func parseCompressions(s string, bench bool) []exr.Compression {
	if bench || s == "all" {
		return []exr.Compression{
			exr.CompressionNone,
			exr.CompressionRLE,
			exr.CompressionZIPS,
			exr.CompressionZIP,
			exr.CompressionPIZ,
			exr.CompressionPXR24,
			exr.CompressionB44,
			exr.CompressionB44A,
			exr.CompressionDWAA,
			exr.CompressionDWAB,
		}
	}

	var result []exr.Compression
	for _, name := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "none":
			result = append(result, exr.CompressionNone)
		case "rle":
			result = append(result, exr.CompressionRLE)
		case "zips":
			result = append(result, exr.CompressionZIPS)
		case "zip":
			result = append(result, exr.CompressionZIP)
		case "piz":
			result = append(result, exr.CompressionPIZ)
		case "pxr24":
			result = append(result, exr.CompressionPXR24)
		case "b44":
			result = append(result, exr.CompressionB44)
		case "b44a":
			result = append(result, exr.CompressionB44A)
		case "dwaa":
			result = append(result, exr.CompressionDWAA)
		case "dwab":
			result = append(result, exr.CompressionDWAB)
		case "orig":
			// Will use original compression from file
			result = append(result, exr.Compression(255)) // sentinel
		}
	}
	if len(result) == 0 {
		result = append(result, exr.Compression(255)) // orig
	}
	return result
}

func parsePixelModes(s string, bench bool) []pixelMode {
	if bench || s == "all" {
		return []pixelMode{pixelModeHalf, pixelModeFloat}
	}

	var result []pixelMode
	for _, name := range strings.Split(s, ",") {
		switch strings.ToLower(strings.TrimSpace(name)) {
		case "half":
			result = append(result, pixelModeHalf)
		case "float":
			result = append(result, pixelModeFloat)
		case "orig":
			result = append(result, pixelModeOrig)
		}
	}
	if len(result) == 0 {
		result = append(result, pixelModeOrig)
	}
	return result
}

func compressionName(c exr.Compression) string {
	switch c {
	case exr.CompressionNone:
		return "none"
	case exr.CompressionRLE:
		return "rle"
	case exr.CompressionZIPS:
		return "zips"
	case exr.CompressionZIP:
		return "zip"
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
		return "original"
	}
}

func runBenchmarks(inputFile string, compressions []exr.Compression, modes []pixelMode, passes int, verbose bool) []runResult {
	var results []runResult

	for _, comp := range compressions {
		for _, mode := range modes {
			if verbose {
				fmt.Fprintf(os.Stderr, "Benchmarking %s %s...\n", compressionName(comp), mode)
			}

			result := benchmarkOne(inputFile, comp, mode, passes, verbose)
			results = append(results, result)
		}
	}

	return results
}

// seekableBuffer wraps bytes.Buffer to provide io.WriteSeeker
type seekableBuffer struct {
	buf []byte
	pos int64
}

func newSeekableBuffer() *seekableBuffer {
	return &seekableBuffer{buf: make([]byte, 0, 1<<20)} // 1MB initial
}

func (s *seekableBuffer) Write(p []byte) (n int, err error) {
	need := int(s.pos) + len(p)
	if need > len(s.buf) {
		if need > cap(s.buf) {
			newBuf := make([]byte, need, need*2)
			copy(newBuf, s.buf)
			s.buf = newBuf
		} else {
			s.buf = s.buf[:need]
		}
	}
	copy(s.buf[s.pos:], p)
	s.pos += int64(len(p))
	return len(p), nil
}

func (s *seekableBuffer) Seek(offset int64, whence int) (int64, error) {
	var newPos int64
	switch whence {
	case io.SeekStart:
		newPos = offset
	case io.SeekCurrent:
		newPos = s.pos + offset
	case io.SeekEnd:
		newPos = int64(len(s.buf)) + offset
	}
	if newPos < 0 {
		return 0, fmt.Errorf("negative position")
	}
	s.pos = newPos
	return s.pos, nil
}

func (s *seekableBuffer) Bytes() []byte {
	return s.buf
}

func (s *seekableBuffer) Reset() {
	s.buf = s.buf[:0]
	s.pos = 0
}

func benchmarkOne(inputFile string, targetComp exr.Compression, mode pixelMode, passes int, verbose bool) runResult {
	// Read source file using mmap for best performance
	srcFile, err := exr.OpenFileMmap(inputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening %s: %v\n", inputFile, err)
		os.Exit(1)
	}
	defer srcFile.Close()

	srcHeader := srcFile.Header(0)
	dw := srcHeader.DataWindow()
	width, height := int(dw.Width()), int(dw.Height())

	// Determine target pixel type
	targetType := exr.PixelTypeFloat
	if mode == pixelModeHalf {
		targetType = exr.PixelTypeHalf
	} else if mode == pixelModeOrig {
		// Use first channel's type
		if srcHeader.Channels().Len() > 0 {
			targetType = srcHeader.Channels().At(0).Type
		}
	}

	// Get compression from file if using orig
	comp := targetComp
	if targetComp == exr.Compression(255) {
		comp = srcHeader.Compression()
	}

	// Read source pixels into our target pixel type
	srcFb := exr.NewFrameBuffer()
	srcReader, err := exr.NewScanlineReader(srcFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating reader: %v\n", err)
		os.Exit(1)
	}

	channelNames := make([]string, 0, srcHeader.Channels().Len())
	for j := 0; j < srcHeader.Channels().Len(); j++ {
		ch := srcHeader.Channels().At(j)
		channelNames = append(channelNames, ch.Name)
		data := make([]byte, width*height*targetType.Size())
		if err := srcFb.Insert(ch.Name, exr.NewSlice(targetType, data, width, height)); err != nil {
			fmt.Fprintf(os.Stderr, "Error inserting channel %s: %v\n", ch.Name, err)
			os.Exit(1)
		}
	}
	srcReader.SetFrameBuffer(srcFb)
	if err := srcReader.ReadPixels(int(dw.Min.Y), int(dw.Max.Y)); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading pixels: %v\n", err)
		os.Exit(1)
	}

	// Create output header
	outHeader := exr.NewScanlineHeader(width, height)
	outHeader.SetCompression(comp)
	cl := exr.NewChannelList()
	for _, name := range channelNames {
		cl.Add(exr.Channel{Name: name, Type: targetType, XSampling: 1, YSampling: 1})
	}
	outHeader.SetChannels(cl)

	// Benchmark write+reread
	writeTimes := make([]time.Duration, passes)
	rereadTimes := make([]time.Duration, passes)
	buf := newSeekableBuffer()

	for i := 0; i < passes; i++ {
		buf.Reset()

		// Write
		start := time.Now()
		writer, err := exr.NewScanlineWriter(buf, outHeader)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating writer: %v\n", err)
			os.Exit(1)
		}
		writer.SetFrameBuffer(srcFb)
		if err := writer.WritePixels(0, height-1); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing pixels: %v\n", err)
			os.Exit(1)
		}
		writer.Close()
		writeTimes[i] = time.Since(start)

		// Reread
		data := buf.Bytes()
		if verbose && i == 0 {
			fmt.Fprintf(os.Stderr, "  Written %d bytes\n", len(data))
		}
		start = time.Now()
		r := bytes.NewReader(data)
		file, err := exr.Open(r, int64(len(data)))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reopening: %v\n", err)
			os.Exit(1)
		}
		reader, err := exr.NewScanlineReader(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error creating reader: %v\n", err)
			os.Exit(1)
		}
		readFb := exr.NewFrameBuffer()
		for _, name := range channelNames {
			if err := readFb.Insert(name, exr.NewSlice(targetType, make([]byte, width*height*targetType.Size()), width, height)); err != nil {
				fmt.Fprintf(os.Stderr, "Error inserting channel %s: %v\n", name, err)
				os.Exit(1)
			}
		}
		reader.SetFrameBuffer(readFb)
		if err := reader.ReadPixels(0, height-1); err != nil {
			fmt.Fprintf(os.Stderr, "Error rereading: %v\n", err)
			os.Exit(1)
		}
		rereadTimes[i] = time.Since(start)
	}

	return runResult{
		FileName:    inputFile,
		Compression: comp,
		PixelMode:   mode,
		WriteTime:   median(writeTimes),
		RereadTime:  median(rereadTimes),
	}
}

func median(times []time.Duration) time.Duration {
	if len(times) == 0 {
		return 0
	}
	if len(times) == 1 {
		return times[0]
	}
	sorted := make([]time.Duration, len(times))
	copy(sorted, times)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	mid := len(sorted) / 2
	if len(sorted)%2 == 0 {
		return (sorted[mid-1] + sorted[mid]) / 2
	}
	return sorted[mid]
}

func printCSV(results []runResult) {
	fmt.Println("file name,compression,pixel mode,write time,count reread time,reread time")
	for _, r := range results {
		fmt.Printf("%s,%s,%s,%g,---,%g\n",
			r.FileName,
			compressionName(r.Compression),
			r.PixelMode,
			r.WriteTime.Seconds(),
			r.RereadTime.Seconds())
	}
}

func printJSON(results []runResult) {
	fmt.Println("[")
	for i, r := range results {
		comma := ","
		if i == len(results)-1 {
			comma = ""
		}
		fmt.Printf(`  {"file": %q, "compression": %q, "pixel_mode": %q, "write_time": %g, "reread_time": %g}%s`+"\n",
			r.FileName, compressionName(r.Compression), r.PixelMode.String(),
			r.WriteTime.Seconds(), r.RereadTime.Seconds(), comma)
	}
	fmt.Println("]")
}

// Ensure half package is available for pixel type conversions
var _ = half.FromFloat32
