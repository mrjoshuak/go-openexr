# go-openexr

A pure Go implementation of the OpenEXR image file format.

[![CI](https://github.com/mrjoshuak/go-openexr/actions/workflows/ci.yml/badge.svg)](https://github.com/mrjoshuak/go-openexr/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mrjoshuak/go-openexr.svg)](https://pkg.go.dev/github.com/mrjoshuak/go-openexr)
[![Go Report Card](https://goreportcard.com/badge/github.com/mrjoshuak/go-openexr)](https://goreportcard.com/report/github.com/mrjoshuak/go-openexr)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Overview

go-openexr provides native Go support for reading and writing OpenEXR (.exr) files, the professional-grade HDR image format used in motion picture production, visual effects, and computer graphics.

### Why go-openexr?

**Zero CGO Dependencies** — This is a 100% pure Go implementation with no C/C++ bindings. This matters because:

- **Simple cross-compilation**: Build for any platform with a single `go build` command. No need to set up cross-compilers, install platform-specific libraries, or manage toolchains.
- **Easy deployment**: Ship a single static binary. No shared libraries to install, no dependency conflicts, no "works on my machine" issues.
- **Container-friendly**: Perfect for Docker, Kubernetes, and serverless environments where native dependencies complicate builds and bloat images.
- **Reproducible builds**: Go's toolchain ensures consistent builds across environments without native build system variability.

**Full Read/Write Support** — Unlike read-only alternatives, go-openexr provides complete write capabilities for generating EXR files in your pipelines.

**Production-Ready Feature Set** — Implements all major OpenEXR capabilities including deep data, multi-part files, tiled storage with mipmap/ripmap support, and all eleven compression codecs including HTJ2K with progressive decode.

### Features

- **100% Pure Go**: No CGO dependencies, fully portable across platforms
- **HDR Support**: Full high-dynamic-range imaging with half-float (float16) precision
- **All Compression Codecs**: None, RLE, ZIPS, ZIP, PIZ, PXR24, B44, B44A, DWAA, DWAB, HTJ2K
- **Tiled Images**: Efficient random access with mipmap and ripmap support
- **Multi-Part Files**: Multiple images in a single file
- **Deep Data**: Variable samples per pixel for compositing workflows
- **Multi-Channel**: Arbitrary channel layouts with layer support
- **Progressive HTJ2K Decoding**: Extract wavelet packets and decode progressively for fast preview workflows
- **Parallel Processing**: Configurable worker pools for efficient encoding/decoding

### OpenEXR Format Compatibility

go-openexr implements the complete [OpenEXR specification](https://openexr.com/):

| Category                              | Status      |
| ------------------------------------- | ----------- |
| Storage types (scanline, tiled, deep) | ✅ Complete |
| All compression codecs (11 types incl. HTJ2K) | ✅ Complete |
| All pixel types (UINT, HALF, FLOAT)   | ✅ Complete |
| Mipmap/Ripmap levels                  | ✅ Complete |
| Multi-part files                      | ✅ Complete |
| Deep scanline/tiled images            | ✅ Complete |
| Standard attributes                   | ✅ Complete |
| Preview images                        | ✅ Complete |
| Luminance/Chroma (YC)                 | ✅ Complete |
| Multi-view/Stereo                     | ✅ Complete |

Files produced by go-openexr are validated against the OpenEXR project's tools (`exrinfo`, `exrcheck`) to ensure full interoperability with the broader OpenEXR ecosystem.

## What's New in v1.1.0

### PIZ Float32 Channel Support

PIZ compression now fully supports float32 and uint32 channels. Previous versions were limited to half-float and uint16 channel types. The implementation uses proper Haar wavelet transforms and Huffman coding for all channel widths, with canonical code assignment matching the C++ OpenEXR reference.

### Progressive HTJ2K Decoding

The headline feature of v1.1.0: extract wavelet packets from HTJ2K-compressed EXR tiles and feed them to a progressive decoder that produces continuously improving float32 images.

Each wavelet packet represents one quality layer at one resolution level of one component -- the atomic unit for progressive quality improvement. Lower-resolution packets produce a coarse image instantly; higher-resolution and higher-quality-layer packets refine detail progressively. Packets can be delivered in any order, so applications can prioritize by resolution, component, or quality layer based on their needs.

This enables progressive rendering workflows in VFX and post-production pipelines where fast visual feedback matters more than waiting for a complete decode.

Key APIs in the `compression` package:

- `HTJ2KNewProgressiveDecoder()` -- create a decoder that accepts packets via `FeedPacket()` and produces images via `Reconstruct()`
- `HTJ2KExtractPackets()` -- extract all wavelet packets from HTJ2K data
- `HTJ2KBuildPacketIndex()` -- build a memory-efficient index referencing packet byte ranges without copying data
- `HTJ2KExtractCodestream()` -- extract the raw J2K codestream for advanced processing

```go
// Extract packets from HTJ2K-compressed EXR data
packets, channelMap, err := compression.HTJ2KExtractPackets(htj2kData)

// Create progressive decoder
decoder, _, err := compression.HTJ2KNewProgressiveDecoder(htj2kData)

// Feed packets — low resolution first for fast preview
for _, pkt := range packets {
    decoder.FeedPacket(pkt)
    img, _ := decoder.Reconstruct() // progressively improving image
    // Display or process img...
}
```

### Float Image Output

`HTJ2KDecompressFloat()` decompresses HTJ2K data directly to float32 component images instead of raw byte buffers. For HALF (float16) channels, the returned float32 values exactly represent the original half-float values. This simplifies processing workflows that operate on floating-point data.

### Packet-Level Access

`HTJ2KExtractPackets()` and `HTJ2KBuildPacketIndex()` provide random access to individual wavelet packets within HTJ2K-compressed data. The packet index variant references byte ranges in the original codestream without copying, making it suitable for large images where memory is a concern.

### Known Limitations

HTJ2K compression of FLOAT (32-bit) pixel type is not yet supported -- the underlying JPEG 2000 encoder is currently limited to 16-bit precision. HALF channels work fully with HTJ2K. This limitation is in the [go-jpeg2000](https://github.com/mrjoshuak/go-jpeg2000) encoder, not in go-openexr itself.

### Dependency

The progressive HTJ2K features are powered by [go-jpeg2000](https://github.com/mrjoshuak/go-jpeg2000) v1.1.0, which provides the progressive decode, float output, and packet extraction APIs that go-openexr builds on.

## Status

**Production Ready** — This project implements the complete OpenEXR specification:

- All 11 compression codecs (None, RLE, ZIPS, ZIP, PIZ, PXR24, B44, B44A, DWAA, DWAB, HTJ2K)
- Deep scanline and tiled images
- Multi-part files with mixed storage types
- Preview images and thumbnails
- Luminance/Chroma (YC) color space
- Multi-view/Stereo support
- All standard metadata attributes
- ID Manifest / Cryptomatte support

Test coverage averages 90%+ across all packages. See [PROGRESS.md](PROGRESS.md) for detailed implementation status.

## Security

Security is a priority for go-openexr. Image parsers are a common attack vector, and we take proactive steps to ensure robustness against malformed or malicious input.

### Continuous Fuzz Testing

We use Go's built-in fuzzing framework to continuously test all parsing code paths:

- **Compression codecs**: All decompressors are fuzz-tested (RLE, ZIP, PIZ, PXR24, B44, DWAA, HTJ2K, etc.)
- **File parsing**: Header parsing, attribute decoding, and offset table validation
- **Reader APIs**: ScanlineReader and TiledReader with arbitrary input

Fuzz tests run for extended periods (hours to days) to discover edge cases that unit tests miss.

### Input Validation

All data entering the system is validated at parsing boundaries:

- **Bounds checking**: Array indices, slice lengths, and buffer sizes are validated before use
- **Integer overflow protection**: Arithmetic operations that could overflow are checked
- **Resource limits**: Maximum dimensions (64K x 64K) and allocation sizes prevent DoS attacks
- **Malformed data rejection**: Invalid compression parameters, pixel types, and sampling values are rejected with clear errors

### Memory Safety

As a pure Go implementation, go-openexr benefits from Go's memory safety guarantees:

- No buffer overflows from unchecked pointer arithmetic
- No use-after-free or double-free vulnerabilities
- Automatic bounds checking on all slice and array accesses
- Garbage collection prevents memory leaks

### Reporting Security Issues

If you discover a security vulnerability, please report it privately by emailing the maintainers rather than opening a public issue. We take all reports seriously and will respond promptly.

## Installation

```bash
go get github.com/mrjoshuak/go-openexr
```

Requires Go 1.23 or later.

## Quick Start

### Reading an EXR File

```go
package main

import (
    "fmt"
    "log"

    "github.com/mrjoshuak/go-openexr/exr"
)

func main() {
    // Open the file
    file, err := exr.OpenFile("image.exr")
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    // Get image dimensions (part 0 for single-part files)
    header := file.Header(0)
    dataWindow := header.DataWindow()
    width := dataWindow.Max.X - dataWindow.Min.X + 1
    height := dataWindow.Max.Y - dataWindow.Min.Y + 1

    fmt.Printf("Image size: %dx%d\n", width, height)

    // List channels
    channels := header.Channels()
    for i := 0; i < channels.Len(); i++ {
        ch := channels.At(i)
        fmt.Printf("Channel: %s (%v)\n", ch.Name, ch.Type)
    }

    // Read pixels into RGBA buffer using the high-level API
    rgbaFile, err := exr.OpenRGBAInputFile("image.exr")
    if err != nil {
        log.Fatal(err)
    }
    defer rgbaFile.Close()

    pixels := make([]exr.RGBA, width*height)
    if err := rgbaFile.ReadPixels(pixels); err != nil {
        log.Fatal(err)
    }
}
```

### Writing an EXR File

```go
package main

import (
    "log"

    "github.com/mrjoshuak/go-openexr/exr"
)

func main() {
    width, height := 640, 480

    // Create pixel data
    pixels := make([]exr.RGBA, width*height)
    for y := 0; y < height; y++ {
        for x := 0; x < width; x++ {
            pixels[y*width+x] = exr.RGBA{
                R: exr.HalfFromFloat32(float32(x) / float32(width)),
                G: exr.HalfFromFloat32(float32(y) / float32(height)),
                B: exr.HalfFromFloat32(0.5),
                A: exr.HalfFromFloat32(1.0),
            }
        }
    }

    // Write the file
    err := exr.WriteRGBA("output.exr", width, height, pixels,
        exr.WithCompression(exr.CompressionPIZ),
    )
    if err != nil {
        log.Fatal(err)
    }
}
```

### Using the Low-Level API

```go
package main

import (
    "log"

    "github.com/mrjoshuak/go-openexr/exr"
    "github.com/mrjoshuak/go-openexr/half"
)

func main() {
    width, height := 1920, 1080

    // Create header
    header := exr.NewHeader(width, height)
    header.SetCompression(exr.CompressionZIP)

    // Add channels (Name is required, XSampling/YSampling default to 1)
    header.Channels().Add(exr.Channel{Name: "R", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
    header.Channels().Add(exr.Channel{Name: "G", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
    header.Channels().Add(exr.Channel{Name: "B", Type: exr.PixelTypeHalf, XSampling: 1, YSampling: 1})
    header.Channels().Add(exr.Channel{Name: "Z", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})

    // Create pixel data
    rPixels := make([]half.Half, width*height)
    gPixels := make([]half.Half, width*height)
    bPixels := make([]half.Half, width*height)
    zPixels := make([]float32, width*height)

    // Fill pixel data...

    // Create frame buffer with slices
    fb := exr.NewFrameBuffer()
    fb.Insert("R", exr.NewSliceFromHalf(rPixels, width, height))
    fb.Insert("G", exr.NewSliceFromHalf(gPixels, width, height))
    fb.Insert("B", exr.NewSliceFromHalf(bPixels, width, height))
    fb.Insert("Z", exr.NewSliceFromFloat32(zPixels, width, height))

    // Write file
    writer, err := exr.NewWriter("output.exr", header)
    if err != nil {
        log.Fatal(err)
    }
    defer writer.Close()

    writer.SetFrameBuffer(fb)
    if err := writer.WritePixels(height); err != nil {
        log.Fatal(err)
    }
}
```

## Package Structure

```
github.com/mrjoshuak/go-openexr/
├── exr/           # Core I/O - file reading/writing, headers, frame buffers
├── half/          # IEEE 754 half-precision float (float16)
├── compression/   # All compression codecs + HTJ2K progressive decode APIs
├── exrmeta/       # Standard attribute accessors & frame rate utilities
├── exrutil/       # EXR utilities - validation, comparison, channel extraction
└── exrid/         # ID Manifest / Cryptomatte support
```

### exr Package

The main package provides:

- `File` - Read-only access to EXR files
- `Writer` - Write EXR files
- `Header` - File metadata and attributes
- `ChannelList` - Channel definitions
- `FrameBuffer` - Pixel data containers
- `RGBA` - Convenience type for RGBA images

### half Package

IEEE 754 half-precision (binary16) floating point:

```go
import "github.com/mrjoshuak/go-openexr/half"

h := half.FromFloat32(3.14159)
f := h.Float32()
```

### exrmeta Package

Typed accessors for standard OpenEXR attributes:

```go
import "github.com/mrjoshuak/go-openexr/exrmeta"

// Set production metadata
exrmeta.SetOwner(header, "Studio XYZ")
exrmeta.SetCapDate(header, "2026-01-05T10:30:00Z")

// Frame rate with standard constants
exrmeta.SetFramesPerSecond(header, exrmeta.FPS24)      // 24 fps cinema
exrmeta.SetFramesPerSecond(header, exrmeta.FPS23976)   // 23.976 fps NTSC film
exrmeta.SetFramesPerSecond(header, exrmeta.FPS2997)    // 29.97 fps NTSC

// Frame rate utilities
fps := exrmeta.FramesPerSecond(header)
if exrmeta.IsDropFrame(*fps) {
    fmt.Println("Using drop-frame timecode")
}
fmt.Println(exrmeta.FrameRateName(*fps))  // "24 fps (Cinema)"
fmt.Printf("%.3f fps\n", exrmeta.RationalToFloat(*fps))

// Camera information
exrmeta.SetCameraInfo(header, exrmeta.CameraInfo{
    Make:  "ARRI",
    Model: "ALEXA 35",
})
exrmeta.SetAperture(header, 2.8)
exrmeta.SetISOSpeed(header, 800)

// Environment maps
exrmeta.SetEnvMap(header, exrmeta.EnvMapLatLong)

// Color management
exrmeta.SetChromaticities(header, exr.Chromaticities{
    RedX: 0.64, RedY: 0.33,
    GreenX: 0.30, GreenY: 0.60,
    BlueX: 0.15, BlueY: 0.06,
    WhiteX: 0.3127, WhiteY: 0.329,
})
```

### exrutil Package

EXR-specific utility functions:

```go
import "github.com/mrjoshuak/go-openexr/exrutil"

// Get file info without full parsing
info, _ := exrutil.GetFileInfo("render.exr")
fmt.Printf("Size: %dx%d, Channels: %v\n", info.Width, info.Height, info.Channels)

// Extract specific channels
depth, _ := exrutil.ExtractChannel(file, "Z")
rgb, _ := exrutil.ExtractChannels(file, "R", "G", "B")

// List layers in multi-layer EXR
layers := exrutil.ListLayers(header) // ["diffuse", "specular", "ao"]

// Validate file integrity
result, _ := exrutil.ValidateFile("render.exr")
if !result.Valid {
    fmt.Println("Errors:", result.Errors)
}

// Compare files
match, diffs, _ := exrutil.CompareFiles("a.exr", "b.exr", exrutil.CompareOptions{
    Tolerance: 0.001,
})
```

### exrid Package

ID Manifest support for Cryptomatte and object ID workflows:

```go
import "github.com/mrjoshuak/go-openexr/exrid"

// Create a Cryptomatte manifest
manifest := exrid.NewCryptomatteManifest("CryptoObject", []string{
    "Hero", "Villain", "Background",
})
exrid.SetManifest(header, manifest)

// Read manifest from file
manifest, _ := exrid.GetManifest(file.Header(0))
group := manifest.LookupChannel("CryptoObject00.R")

// Look up object name by ID
if names, ok := group.Lookup(pixelID); ok {
    fmt.Println("Object:", names[0])
}

// Compute Cryptomatte hash
hash := exrid.CryptomatteHash("Hero")
hashFloat := exrid.CryptomatteHashFloat("Hero") // As float32 for pixel comparison
```

### Compression

Supported compression methods (11 codecs):

| Method             | ID  | Description             |
| ------------------ | --- | ----------------------- |
| `CompressionNone`  | 0   | No compression          |
| `CompressionRLE`   | 1   | Run-length encoding     |
| `CompressionZIPS`  | 2   | ZIP, single scanline    |
| `CompressionZIP`   | 3   | ZIP, 16 scanlines       |
| `CompressionPIZ`   | 4   | Wavelet + Huffman       |
| `CompressionPXR24` | 5   | Lossy 24-bit float      |
| `CompressionB44`   | 6   | 4x4 block, fixed rate   |
| `CompressionB44A`  | 7   | B44 with flat detection |
| `CompressionDWAA`  | 8   | DCT, 32 scanlines       |
| `CompressionDWAB`  | 9   | DCT, 256 scanlines      |
| `CompressionHTJ2K` | 10  | HTJ2K wavelet (lossy), progressive decode support |

DWA compression quality can be configured via the header:

```go
header.SetDWACompressionLevel(45.0) // Default is 45.0 (visually lossless)
// Lower values = higher compression, more artifacts
// Higher values = less compression, better quality
```

## API Documentation

Full API documentation will be available at [pkg.go.dev](https://pkg.go.dev/github.com/mrjoshuak/go-openexr) once published.

### Core Types

#### Header

```go
type Header struct {
    // Metadata and attributes
}

func NewHeader(width, height int) *Header
func (h *Header) DataWindow() Box2i
func (h *Header) DisplayWindow() Box2i
func (h *Header) Channels() *ChannelList
func (h *Header) Compression() Compression
func (h *Header) SetCompression(c Compression)
```

#### Channel

```go
type PixelType int

const (
    PixelTypeUint  PixelType = 0
    PixelTypeHalf  PixelType = 1
    PixelTypeFloat PixelType = 2
)

type Channel struct {
    Name      string
    Type      PixelType
    XSampling int
    YSampling int
    PLinear   bool
}
```

#### FrameBuffer

```go
type Slice struct {
    Type      PixelType
    Base      unsafe.Pointer  // Pointer to pixel at (0, 0)
    XStride   int             // Bytes between adjacent pixels in a row
    YStride   int             // Bytes between adjacent rows
    XSampling int             // Horizontal subsampling (1 = full resolution)
    YSampling int             // Vertical subsampling (1 = full resolution)
}

// Convenience constructors
func NewSliceFromHalf(data []half.Half, width, height int) Slice
func NewSliceFromFloat32(data []float32, width, height int) Slice
func NewSliceFromUint32(data []uint32, width, height int) Slice

type FrameBuffer struct {
    // Slice storage
}

func NewFrameBuffer() *FrameBuffer
func (fb *FrameBuffer) Insert(name string, slice Slice) error
```

### Options

Configure readers and writers with functional options:

```go
// Writing options
exr.WithCompression(exr.CompressionPIZ)
exr.WithLineOrder(exr.IncreasingY)
exr.WithThreads(4)

// Reading options
exr.WithThreads(4)
```

## Performance

### Parallelism

The library uses Go's concurrency for parallel encoding/decoding:

```go
// Configure thread count for parallel decompression
file, err := exr.OpenFile("large.exr")
// Thread count is configured at the reader level
```

### Memory Usage

For large files, use streaming APIs:

```go
file, err := exr.OpenFile("huge.exr")
defer file.Close()

sr, err := exr.NewScanlineReader(file)
// Set up frame buffer, then read scanlines incrementally
for y := dataWindow.Min.Y; y <= dataWindow.Max.Y; y++ {
    err := sr.ReadPixels(int(y), int(y))
    // Process scanline...
}
```

## Compatibility

This implementation is compatible with files created by:

- OpenEXR C++ library (all versions)
- Nuke, Maya, Houdini, and other VFX software
- Blender, Unity, Unreal Engine

## Test Coverage

Current test coverage by package:

| Package              | Coverage | Notes                                      |
| -------------------- | -------- | ------------------------------------------ |
| `half`               | 96.7%    | Core float16 operations                    |
| `compression`        | 90.6%    | All codecs including HTJ2K                 |
| `exr`                | 90.0%    | Core I/O, scanline, tiled, deep, multipart |
| `exrmeta`            | 97.3%    | Attribute accessors                        |
| `exrutil`            | 91.0%    | Utility functions                          |
| `exrid`              | 91.2%    | ID manifest and Cryptomatte support        |
| `internal/xdr`       | 93.0%    | XDR encoding/decoding                      |
| `internal/interleave`| 90.5%    | Byte interleaving                          |
| `internal/predictor` | 89.8%    | Predictor operations                       |

## Documentation

- [Progress](PROGRESS.md) - Development progress tracking
- [API Documentation](https://pkg.go.dev/github.com/mrjoshuak/go-openexr) - Full API reference on pkg.go.dev

## Contributing

Contributions are welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before starting.

### Development

```bash
# Clone the repository
git clone https://github.com/mrjoshuak/go-openexr.git
cd go-openexr

# Run tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Check coverage
go test -cover ./...
```

### Test Files

Test images are available from the [openexr-images](https://github.com/AcademySoftwareFoundation/openexr-images) repository.

## License

This project is licensed under the Apache License, Version 2.0 - see the [LICENSE](LICENSE) file for details.

Copyright 2025-2026 Joshua Kolden. This is an independent implementation written entirely from scratch in Go—it contains no code from the C++ OpenEXR library or any other implementation.

## Acknowledgments

This project exists thanks to the excellent work of those who created and maintain the OpenEXR format:

- [Industrial Light & Magic](https://www.ilm.com/) — Original creators of the OpenEXR format
- [Academy Software Foundation](https://www.aswf.io/) — Current stewards of the OpenEXR specification
- [OpenEXR Project](https://openexr.com/) — For the comprehensive format documentation and test images

The OpenEXR team's detailed specification and publicly available test files made this independent Go implementation possible. We validate our output against their tools to ensure format compatibility.

## See Also

- [OpenEXR File Format](https://openexr.readthedocs.io/) - Technical documentation
- [OpenEXR Images](https://github.com/AcademySoftwareFoundation/openexr-images) - Test images
