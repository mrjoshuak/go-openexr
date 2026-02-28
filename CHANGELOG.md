# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
