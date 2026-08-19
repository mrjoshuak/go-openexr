# go-openexr

A pure Go implementation of the OpenEXR image file format.

[![CI](https://github.com/mrjoshuak/go-openexr/actions/workflows/ci.yml/badge.svg)](https://github.com/mrjoshuak/go-openexr/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/mrjoshuak/go-openexr.svg)](https://pkg.go.dev/github.com/mrjoshuak/go-openexr)
[![Go Report Card](https://goreportcard.com/badge/github.com/mrjoshuak/go-openexr)](https://goreportcard.com/report/github.com/mrjoshuak/go-openexr)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

## Overview

Read and write OpenEXR in pure Go. Every storage type, every compression codec,
every pixel type — no cgo, no C++ toolchain, no shared libraries to ship.

**Files go-openexr writes are read back correctly by the OpenEXR reference
implementation — all 36 pixel-type and compression combinations, checked on
every release.**

### About the format

OpenEXR is the high-dynamic-range image format the film industry runs on,
created at Industrial Light & Magic. Two things make it worth reaching for
outside VFX as well.

**Real floating point.** Channels hold 16- or 32-bit floats rather than bytes,
or 32-bit integers where you want exact IDs. Nothing clips at white, nothing
bands in a gradient, and a value meaning 40,000 nits or 0.0001 metres survives
the round trip intact.

**Arbitrary named channels.** An EXR is not limited to R, G, B and A. It holds
as many named channels as you like, each with its own type — depth, surface
normals, motion vectors, object IDs, per-object masks — so one file carries a
render *and* everything you need to relight or re-composite it. That makes it a
genuinely good container for scientific and sensor data, not just pictures.

On top of that: tiled storage for random access into huge images, mipmaps,
multi-part files, deep images with several samples per pixel, and eleven
compression methods from lossless to aggressive.


### Why go-openexr?

**Zero CGO Dependencies** — This is a 100% pure Go implementation with no C/C++ bindings. This matters because:

- **Simple cross-compilation**: Build for any platform with a single `go build` command. No need to set up cross-compilers, install platform-specific libraries, or manage toolchains.
- **Easy deployment**: Ship a single static binary. No shared libraries to install, no dependency conflicts, no "works on my machine" issues.
- **Container-friendly**: Perfect for Docker, Kubernetes, and serverless environments where native dependencies complicate builds and bloat images.
- **Reproducible builds**: Go's toolchain ensures consistent builds across environments without native build system variability.

**Full Read/Write Support** — Unlike read-only alternatives, go-openexr provides complete write capabilities for generating EXR files in your pipelines.

**Production-Ready Feature Set** — Implements all major OpenEXR capabilities including deep data, multi-part files, tiled storage with mipmap/ripmap support, and every OpenEXR compression type — eleven methods across twelve compression IDs, HTJ2K appearing as both HTJ2K256 and HTJ2K32 — including progressive HTJ2K decode.

### Features

- **100% Pure Go**: No CGO dependencies, fully portable across platforms
- **HDR Support**: Full high-dynamic-range imaging with half-float (float16) precision
- **Compression Codecs**: None, RLE, ZIPS, ZIP, PIZ, PXR24, B44, B44A, DWAA, DWAB, HTJ2K — see the [support matrix](#compression-support) for what is verified in each direction
- **Tiled Images**: Efficient random access with mipmap and ripmap support
- **Multi-Part Files**: Multiple images in a single file
- **Deep Data**: Variable samples per pixel for compositing workflows
- **Multi-Channel**: Arbitrary channel layouts with layer support
- **Progressive HTJ2K Decoding**: Extract wavelet packets and decode progressively for fast preview workflows
- **Parallel Processing**: Configurable worker pools for efficient encoding/decoding

### OpenEXR Format Compatibility

go-openexr implements the complete [OpenEXR specification](https://openexr.com/):

| Category                                   | Status                                     |
| ------------------------------------------ | ------------------------------------------ |
| Storage types (scanline, tiled, deep)      | ✅ Complete                                |
| All compression types (12 IDs, 11 methods) | See [support matrix](#compression-support) |
| All pixel types (UINT, HALF, FLOAT)        | ✅ Complete                                |
| Mipmap/Ripmap levels                       | ✅ Complete                                |
| Multi-part files                           | ✅ Complete                                |
| Deep scanline/tiled images                 | ✅ Complete                                |
| Standard attributes                        | ✅ Complete                                |
| Preview images                             | ✅ Complete                                |
| Luminance/Chroma (YC)                      | ✅ Complete                                |
| Multi-view/Stereo                          | ✅ Complete                                |

Files produced by go-openexr are checked against the OpenEXR reference implementation itself: the conformance suite compares pixels sample for sample with what OpenImageIO's `oiiotool` reads from the same file, in both directions. The [compression support matrix](#compression-support) records the per-codec detail; every row is now covered in both directions.

## Highlights

### Progressive HTJ2K decoding

Show something on screen before the file finishes arriving. Pull wavelet packets
out of an HTJ2K EXR and feed them to a decoder that produces a better image with
every one.

A packet is one quality layer, at one resolution, for one component. The
low-resolution ones give you a coarse image immediately; the rest sharpen it.
Deliver them in whatever order suits you — by resolution for a fast preview, by
component to get luminance first, by quality layer to trade detail for speed.

The APIs, in the `compression` package:

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

### Float straight out of the decoder

`HTJ2KDecompressFloat()` hands you float32 component images, not raw byte
buffers. HALF channels come back as the exact float32 values they represent, so
there is nothing to unpack before the maths starts.

### Random access without the copy

`HTJ2KBuildPacketIndex()` indexes wavelet packets by byte range in the original
codestream. Address any packet in a large image without holding a second copy of
it in memory.

### Full-precision float compression

HTJ2K carries FLOAT (32-bit) channels bit-for-bit, using the same NLT Type 3
markers as OpenEXR 3.4 with OpenJPH. Depth maps, world-position passes and
anything else where a rounded value is a wrong value.

### PIZ at every channel width

Haar wavelet plus Huffman across half, uint16, float32 and uint32, with
canonical code assignment matching the C++ reference.

### Built on go-jpeg2000

The HTJ2K support comes from [go-jpeg2000](https://github.com/mrjoshuak/go-jpeg2000),
a pure Go JPEG 2000 codec whose output OpenJPH and OpenJPEG both decode exactly.

## Compression support

Every codec is verified in the write direction — the reference reads what we
write — and every codec but HTJ2K is verified in the read direction too. The one
exception is stated in the table rather than omitted from it: OpenImageIO cannot
write an HTJ2K EXR, so no reference-written file exists to read.

`scripts/validate.sh` re-runs the whole matrix on each release. Comparisons are
against the OpenEXR reference implementation via OpenImageIO's `oiiotool`,
sample for sample: bit-identical for a lossless codec, and within a bound
derived from the format for a lossy one.

| Codec         | Reads reference files                | Output read by reference                                  |
| ------------- | ------------------------------------ | --------------------------------------------------------- |
| None          | verified, every sample               | verified, every sample                                    |
| RLE           | verified, every sample               | verified, every sample                                    |
| ZIPS          | verified, every sample               | verified, every sample                                    |
| ZIP           | verified, every sample               | verified, every sample                                    |
| PIZ           | verified, every sample               | verified, every sample                                    |
| PXR24         | verified, every sample               | verified (half/uint exact, float within the 24-bit bound) |
| B44 / B44A    | verified, every sample               | verified via oiiotool                                     |
| DWAA / DWAB   | verified, every sample               | verified (96 cases)                                       |
| HTJ2K (UINT)  | no reference file exists (see below) | verified, bit-identical                                   |
| HTJ2K (FLOAT) | no reference file exists (see below) | verified, bit-identical                                   |
| HTJ2K (HALF)  | no reference file exists (see below) | verified, bit-identical                                   |

None, RLE, ZIPS, ZIP and PIZ are covered in **both** directions by
`exr/conformance_test.go` over `exr/testdata/conformance/`. The read side is
additionally covered by `TestReferenceImagesDecodeExactly` against the official
[openexr-images](https://github.com/AcademySoftwareFoundation/openexr-images)
corpus, which contains 7 PXR24, 4 ZIP and 2 PIZ real-world images — every sample
of all thirteen matches the reference implementation exactly.

B44/B44A and DWAA/DWAB are lossy, so their fixtures carry the reference's own
readback rather than sharing a golden with an uncompressed twin — otherwise the
test would be measuring the codec's loss instead of this library's agreement
with the reference. B44's FLOAT and UINT passthrough channels are compared
exactly, since the format stores them uncompressed. PXR24 write-side coverage compares half
and uint bit-identically and float against the bound implied by keeping 24 of
32 float bits.

**HTJ2K is verified in the write direction only, and the asymmetry is real.**
OpenImageIO 3.1.16 cannot *write* an HTJ2K EXR, so no reference-written file
exists for us to read — which is why those cells say so rather than claiming a
check that cannot be performed. What is verified: we write all six combinations
(half, float and uint at both block sizes) and the reference reads every one
bit-identically against its uncompressed twin, held to the same standard as ZIP
or PIZ with no tolerance.

The read direction is covered one level down instead. go-jpeg2000's own gate
decodes codestreams written by OpenJPH — the library OpenEXR 3.4+ uses for
HTJ2K — bit-exactly. That establishes the codestream reader against a reference;
it does not establish the EXR-container read path, because nothing can produce
that fixture.

This required go-jpeg2000 v1.5.0. Earlier versions could not emit a codestream
OpenJPH would accept: v1.3.0 ignored `HighThroughput` in `EncodeHalf`/
`EncodeFloat`, so Rsiz bit 14 was never set, and wrote the NLT segment short.
`scripts/validate.sh` waives these rows for a build resolved at exactly v1.3.0
and gates them for every other version. If a waived row passes anyway, the
script says so, so the waiver cannot outlive the defect it was granted for.

**Deep images.** Deep data may use None, RLE or ZIPS only — a deep chunk holds
one scanline of variable-length sample data, so the codecs that compress a fixed
block of several scanlines have nothing to work on, and the reference rejects
such a file when it opens it. `IsDeepCompressionSupported` reports this and both
deep writers refuse anything else. All three permitted codecs are gated for deep
scanline and deep tiled in both directions, sample by sample: `scripts/deepgen`
writes fixtures with 0 to 4 samples per pixel — including an entirely empty
scanline and an entirely empty tile, since a fixture with a constant sample
count is read correctly by a writer that assumes one — and `oiiotool --dumpdata`
reports every pixel's sample count and every sample's value for comparison.
Multi-part deep files the reference wrote are read back the same way, part by
part. Deep mipmap levels are not covered: `oiiotool` will not write that fixture.

**Subsampled channels.** Channels with `ySampling > 1` are refused by the B44
and DWA paths rather than silently misread. The chunk layout code assumes one
row per channel per scanline, so the other codecs do not handle them correctly
either; OpenImageIO refuses such files outright.

See [docs/CONFORMANCE.md](docs/CONFORMANCE.md) for how conformance is tested and
why round-trip tests alone are not sufficient.

## Status

**Production Ready** — This project implements the OpenEXR specification. Every
compression type is verified against the reference implementation in both
directions; [ROADMAP.md](ROADMAP.md) lists what is implemented but not yet
verified that way.

- 11 compression methods across 12 IDs (None, RLE, ZIPS, ZIP, PIZ, PXR24, B44, B44A, DWAA, DWAB, HTJ2K256, HTJ2K32)
- Deep scanline and tiled images
- Multi-part files with mixed storage types, verified in the write direction:
  the reference reads back each part's samples from files whose parts differ in
  data window, compression, channel layout and scanline-versus-tiled storage
- Preview images and thumbnails
- Luminance/Chroma (YC) color space
- Multi-view/Stereo support
- All standard metadata attributes
- ID Manifest / Cryptomatte support

Line coverage averages 90%+ across all packages, but that is the weakest of the
three signals here and is listed last deliberately. An audit found 125 candidate
false-assurance tests in this repository — assertions that could not fail because
they compared the library against itself — of which 21 were proven unable to fail
by mutating the code under them. Coverage counted every one of those as covered.

What the guarantees actually rest on:

1. `scripts/validate.sh` — 157 checks against the OpenEXR reference
   implementation, both directions, failing the build on any regression. It
   covers the pixel-type by compression matrix; tiled writing, with plain,
   mipmapped and ripmapped fixtures read back level by level by a program
   linked against libOpenEXR itself; multi-part files read back part by part,
   level by level and channel by channel; and deep images sample by sample, in
   both directions. Each area runs a control and a signal check that must fail,
   so a broken oracle stays distinguishable from a defect.
2. `scripts/mutation/run.py` — deliberately breaks a codec and records whether
   the tests notice. Currently 11 of 20 mutations survive the pre-existing
   tests; all 15 covered by the added spec-anchored tests are killed.
3. Line coverage, which says only that a line ran.

See [PROGRESS.md](PROGRESS.md) for detailed implementation status and
[ROADMAP.md](ROADMAP.md) for what is still unverified.

## Security

Security is a priority for go-openexr. Image parsers are a common attack vector, and we take proactive steps to ensure robustness against malformed or malicious input.

### Continuous Fuzz Testing

We use Go's built-in fuzzing framework to continuously test all parsing code paths:

- **Compression codecs**: Decompressors are fuzz-tested (RLE, ZIP, PIZ, PXR24, B44, DWAA). HTJ2K has no fuzz target yet.
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

    // Read pixels using the high-level RGBA API
    rgbaFile, err := exr.OpenRGBAInputFile("image.exr")
    if err != nil {
        log.Fatal(err)
    }
    defer rgbaFile.Close()

    img, err := rgbaFile.ReadRGBA()
    if err != nil {
        log.Fatal(err)
    }

    r, g, b, a := img.RGBA(0, 0)
    fmt.Printf("Top-left pixel: %v %v %v %v\n", r, g, b, a)
}
```

### Writing an EXR File

```go
package main

import (
    "image"
    "log"

    "github.com/mrjoshuak/go-openexr/exr"
)

func main() {
    width, height := 640, 480

    // Create the output file and configure it through its header
    out, err := exr.NewRGBAOutputFile("output.exr", width, height)
    if err != nil {
        log.Fatal(err)
    }
    out.Header().SetCompression(exr.CompressionPIZ)

    // Fill an RGBA image; Pix is float32, four components per pixel
    img := &exr.RGBAImage{
        Pix:    make([]float32, width*height*4),
        Stride: 4,
        Rect:   image.Rect(0, 0, width, height),
    }
    for y := 0; y < height; y++ {
        for x := 0; x < width; x++ {
            i := (y*width + x) * 4
            img.Pix[i+0] = float32(x) / float32(width)  // R
            img.Pix[i+1] = float32(y) / float32(height) // G
            img.Pix[i+2] = 0.5                          // B
            img.Pix[i+3] = 1.0                          // A
        }
    }

    if err := out.WriteRGBA(img); err != nil {
        log.Fatal(err)
    }
}
```

### Using the Low-Level API

```go
package main

import (
    "log"
    "os"

    "github.com/mrjoshuak/go-openexr/exr"
    "github.com/mrjoshuak/go-openexr/half"
)

func main() {
    width, height := 1920, 1080

    // Create header
    header := exr.NewScanlineHeader(width, height)
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
    f, err := os.Create("output.exr")
    if err != nil {
        log.Fatal(err)
    }
    defer f.Close()

    writer, err := exr.NewScanlineWriter(f, header)
    if err != nil {
        log.Fatal(err)
    }

    writer.SetFrameBuffer(fb)
    // WritePixels takes an inclusive scanline range, not a count.
    if err := writer.WritePixels(0, height-1); err != nil {
        log.Fatal(err)
    }
    if err := writer.Close(); err != nil {
        log.Fatal(err)
    }
}
```

> **Closing a writer is not optional, and its error matters.** The chunk offset
> table records where each chunk landed, so it can only be written once every
> chunk has been. `Close` is what writes it. A writer that is never closed — or
> whose `Close` error is discarded by a bare `defer` — leaves a file whose pixel
> data is intact but whose offset table is all zeroes.
>
> go-openexr rebuilds such a table when reading, so it will recover the file
> (see [docs/CONFORMANCE.md](docs/CONFORMANCE.md)), and so will OpenEXR itself.
> Not every tool is that forgiving. Prefer checking the error:
>
> ```go
> if err := writer.Close(); err != nil {
>     log.Fatal(err)
> }
> ```

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

| Method                | ID  | Description                                            |
| --------------------- | --- | ------------------------------------------------------ |
| `CompressionNone`     | 0   | No compression                                         |
| `CompressionRLE`      | 1   | Run-length encoding                                    |
| `CompressionZIPS`     | 2   | ZIP, single scanline                                   |
| `CompressionZIP`      | 3   | ZIP, 16 scanlines                                      |
| `CompressionPIZ`      | 4   | Wavelet + Huffman                                      |
| `CompressionPXR24`    | 5   | Lossy 24-bit float                                     |
| `CompressionB44`      | 6   | 4x4 block, fixed rate                                  |
| `CompressionB44A`     | 7   | B44 with flat detection                                |
| `CompressionDWAA`     | 8   | DCT, 32 scanlines                                      |
| `CompressionDWAB`     | 9   | DCT, 256 scanlines                                     |
| `CompressionHTJ2K256` | 10  | HTJ2K wavelet, 128x128 code blocks, progressive decode |
| `CompressionHTJ2K32`  | 11  | HTJ2K wavelet, 32x32 code blocks, progressive decode   |

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

### Configuration

Per-file settings live on the header, and are applied before writing:

```go
header := exr.NewScanlineHeader(width, height)
header.SetCompression(exr.CompressionPIZ)
header.SetLineOrder(exr.LineOrderIncreasing)
```

Parallelism is configured process-wide:

```go
exr.SetParallelConfig(exr.ParallelConfig{NumWorkers: 4})
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

go-openexr reads and writes the OpenEXR 2.0 file format, so it interoperates
with the OpenEXR C++ library and with applications built on it — Nuke, Maya,
Houdini, Blender, Unity, Unreal Engine and others.

What is *automatically verified* is narrower than that list, and deliberately
stated as such: see the [compression support matrix](#compression-support) for
which codecs are checked against reference-written files, in which direction,
and where the gaps are. Files using DWAA/DWAB written by other implementations
currently fail to decode.

## Test Coverage

Current test coverage by package:

| Package               | Coverage | Notes                                      |
| --------------------- | -------- | ------------------------------------------ |
| `half`                | 96.7%    | Core float16 operations                    |
| `compression`         | 90.6%    | All codecs including HTJ2K                 |
| `exr`                 | 90.0%    | Core I/O, scanline, tiled, deep, multipart |
| `exrmeta`             | 97.3%    | Attribute accessors                        |
| `exrutil`             | 91.0%    | Utility functions                          |
| `exrid`               | 91.2%    | ID manifest and Cryptomatte support        |
| `internal/xdr`        | 93.0%    | XDR encoding/decoding                      |
| `internal/interleave` | 90.5%    | Byte interleaving                          |
| `internal/predictor`  | 89.8%    | Predictor operations                       |

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
