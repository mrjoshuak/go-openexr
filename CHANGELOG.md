# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **ZIP/ZIPS: byte reordering and the predictor ran in the wrong order.** OpenEXR
  reorders bytes into even/odd halves and *then* applies the predictor; the
  inverse order was used on both sides. (#4)
- **ZIP/ZIPS/RLE: the predictor omitted OpenEXR's +128/-128 bias.** (#4)
- **RLE: the control-byte convention was inverted.** A non-negative count is a
  repeat run and a negative count a literal run; the two were swapped. (#4)
- **RLE: the byte-reorder and predictor pre-passes were missing entirely.**
  `ImfRleCompressor` applies the same two pre-passes as `ImfZipCompressor`. (#4)
- **Chunks that do not shrink are now stored uncompressed**, as
  `ImfOutputFile` requires, and readers now detect them by size. Without this a
  small or incompressible chunk produced a file conforming readers silently
  misread as raw pixel data. Applies to scanline, tiled and multi-part writers.
- **PIZ: the Huffman fast-decode table truncated the run-length pseudo-symbol.**
  `HUF_ENCSIZE` is 65537 and the run-length symbol is 65536, which does not fit
  the `uint16` symbol table; it decoded as a literal 0 and desynchronised the
  bitstream at the first run.
- **PIZ: the wavelet mispositioned the left-over column and row.** The pivot was
  recomputed as `nx-p` instead of the reference's post-loop position, which
  differs whenever the dimension has bits set below the current level.
- **PIZ: `wdec14_4` kept full-width intermediates** where the reference truncates
  to 16 bits between stages.

Each of these was self-inverse, so the library round-tripped its own files
correctly while being unable to read or write files any conforming OpenEXR
implementation produces.

### Added
- `exr/testdata/conformance/`: EXR files written by the OpenEXR reference
  implementation with golden pixel values from that same implementation, plus
  `scripts/gen-conformance-testdata.sh` to regenerate them. The generator fails
  if a codec's fixture did not actually compress, so a fixture cannot silently
  degrade into an untested store-raw case.
- Exact-value tests against the official ASWF `openexr-images` corpus, fetched by
  `testdata/download.sh` with digests in `exr/testdata/openexr_images.golden`
  (`scripts/gen-reference-goldens.py`).
- Spec-anchored tests for the predictor, the byte reorder and the wavelet,
  asserting against independent transcriptions of the OpenEXR reference rather
  than against other implementations in this repository.

### Changed
- Predictor and byte-reorder call sites across scanline, tiled, deep and
  multi-part now route through the shared `predictor.ReconstructBytes` /
  `DeconstructBytes` helpers instead of repeating the pipeline inline.
- `RLEDecompress` delegates to `RLEDecompressTo`; the two copies had drifted.

## [1.2.0] - 2026-02-28

### Added
- HTJ2K compression of FLOAT (32-bit) channels via `jpeg2000.EncodeFloat`
- HTJ2K decompression of FLOAT channels via `jpeg2000.DecodeFloat`
- Mixed FLOAT/non-FLOAT channel validation with clear error message
- Progressive HTJ2K decode API and float image output
- Channel-aware PIZ compress/decompress matching C++ OpenEXR layout
- Strided 2D Haar wavelet for PIZ float/uint channel support
- Float32 PIZ roundtrip and C++ interop tests

### Fixed
- Huffman canonical code assignment to match OpenEXR C++ algorithm
- All go vet findings (unsafe.Pointer arithmetic, WriteByte signature, ARM64 frame size)
- All staticcheck findings (sync.Pool pointer wrapping, unused types, constant types)

### Changed
- `HTJ2KCompress` now routes FLOAT channels through `EncodeFloat` path
- `HTJ2KDecompress` now routes FLOAT channels through `DecodeFloat` path
- Updated go-jpeg2000 dependency to v1.2.1

## [1.0.7] - 2026-02-16

### Fixed
- Scanline decoder crash on non-zero origin data windows (fixes #2)

## [1.0.6] - 2026-02-16

### Fixed
- `ReadChunk` EOF on multipart EXR files by handling 4-byte part number prefix
- Added regression test for multipart ReadChunk roundtrip

## [1.0.5] - 2026-01-13

### Fixed
- LICENSE file format to match official Apache 2.0 for pkg.go.dev detection

## [1.0.4] - 2026-01-13

### Fixed
- Nil pointer dereference in ScanlineReader/TiledReader
- DWA decompressor slice bounds vulnerability
- Zero sampling and data window dimension validation
- Unknown pixel type validation in readers

### Added
- Fuzz corpus and fuzzing scripts for regression testing
- Security section in README

## [1.0.3] - 2026-01-11

### Added

- GitHub Actions CI workflow for automated testing
- CI runs on Go 1.23, 1.24 across Linux, macOS, and Windows
- Format checking with gofmt

### Changed

- Minimum Go version is now 1.23 (required by dependencies)

## [1.0.2] - 2026-01-11

### Changed

- Improved test coverage to 90%+ across all packages
- Added comprehensive tests for HTJ2K compression
- Added tests for error paths and edge cases

## [1.0.1] - 2026-01-11

### Fixed

- Corrected author name in NOTICE file

## [1.0.0] - 2026-01-11

### Added

- Pure Go implementation of OpenEXR file format (version 2.x)
- Complete format support: scanline, tiled, multipart, deep data
- All compression methods: None, RLE, ZIP, ZIPS, PIZ, PXR24, B44, B44A, DWAA, DWAB
- HTJ2K compression support via go-jpeg2000
- IEEE-754 half-precision float type with SIMD batch operations
- Multi-view stereo and environment map support
- Mipmap and ripmap tiled images
- Cryptomatte ID manifest parsing (exrid package)
- ACES color workflow utilities
- Zero-copy memory-mapped file reading
- SIMD-optimized paths for ARM64 NEON and AMD64 SSE2
- Parallel chunk processing with configurable grain size
- Command-line tools: exrinfo, exrheader, exrcheck, exrmaketiled, and more
- CODE_OF_CONDUCT.md, CONTRIBUTING.md, SECURITY.md documentation

### Performance

- ARM64 NEON SIMD for B44 compression primitives
- AMD64 SSE2 vectorized shift operations
- Parallel scanline/tile compression and decompression
- Object pooling for reduced allocations
- Optimized predictor and interleave operations
