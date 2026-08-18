# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] - 2026-08-17

**Every compression codec now interoperates with the OpenEXR reference
implementation.** B44/B44A and DWAA/DWAB join the codecs fixed in 1.3.0, and
HTJ2K's data race is resolved by the dependency bump.

### Changed
- `go-jpeg2000` raised to v1.3.0, which fixes a data race in its parallel tile
  encoder, brings HTJ2K code-block dimensions into ISO/IEC 15444-1 conformance,
  and hardens its decoder against malformed input. CI now runs `go test -race`
  with no exclusions.

### Deprecated
- `CompressDWAA`, `CompressDWAB`, `DecompressDWAA`, `DecompressDWAB`,
  `DwaCompressor`, `DwaDecompressor` and `DwaChannelData` still exist and still
  compile, but now delegate to `DWACompress` / `DWADecompress`. Their signatures
  carry no channel list, and DWA classifies each channel by name into a
  colour-space-converted triple, an RLE-coded alpha or a lossless passthrough —
  so the old API could only ever describe a single HALF channel, and did not
  produce a codestream any conforming reader could decode. They now do, for that
  one case. Use the new entry points for anything else.

### Fixed
- **DWAA/DWAB now read files written by the OpenEXR reference implementation,
  and write files it reads.** The previous decoder was not a DWA decoder: its
  Huffman stage was a zlib stub, the channel-classification rule length was
  parsed as `uint32` where the format uses `uint16`, and the AC run-length
  coding and block/CSC layouts did not match the specification. All four were
  self-inverse, so round-trip tests passed while nothing OpenEXR produced could
  be decoded. Both directions are now transcriptions of OpenEXR 3.4's
  `internal_dwa_*.h`, verified across a 48-case read sweep and 96 write cases;
  decoding a reference-written file reproduces the reference's own readback to
  within one half-ULP.
- The static Huffman block decoder shared by PIZ and DWA is now factored into
  one implementation (`hufDecompressInto`) rather than duplicated.
- DWA channels with `ySampling != 1` are rejected with a size-mismatch error
  instead of decoding into a differently-shaped image.
- `TestBufferPoolReuse` no longer asserts that `sync.Pool` preserves a buffer's
  contents across `Put`/`Get`, which it is explicitly permitted not to do. The
  test failed roughly one run in five under `-race`, and would have made the new
  race job flaky; it now asserts reuse through the pool's own hit counter.
- **B44/B44A now pass FLOAT and UINT channels through, as OpenEXR does.** These
  channels are stored uncompressed alongside the block-compressed HALF ones.
  Previously the compressor emitted zero bytes for them and the decompressor
  skipped them, leaving zeros — so a mixed-type image lost every non-HALF
  channel in both directions. v1.3.0 refused such images rather than corrupting
  them; they now work. Verified against reference-written mixed-type files:
  FLOAT and UINT channels decode bit-exact, HALF within B44's lossy budget.
- Failure modes in the B44 path that previously produced zeros now return
  errors: `ErrB44Truncated`, `ErrB44DataSize`, `ErrB44BadGeometry`.
- Channels with `ySampling > 1` are rejected by the B44 path with
  `ErrSubsampledChannels` instead of being silently misread. The wider
  subsampling gap is unchanged and still affects the other codecs.

## [1.3.0] - 2026-08-17

**Full codec interoperability with the OpenEXR reference implementation.**
None, RLE, ZIPS, ZIP and PIZ now match `oiiotool` sample for sample in both
directions, verified automatically against reference-written fixtures and the
official ASWF `openexr-images` corpus. This covers the compressions real-world
EXRs actually use — Nuke writes ZIPS by default, OpenImageIO writes ZIP.

Also in this release: a substantially hardened reader, a conformance test suite
built on external ground truth rather than self-comparison, and README examples
that are now compile-checked.

### Upgrading

Files written by earlier versions using ZIP, ZIPS, RLE or PIZ are not readable
by other OpenEXR implementations, and files from other implementations were not
read correctly. Rewriting any affected assets with v1.3.0 produces conforming
output; no API changes are needed to do so.

One behaviour change: **B44/B44A now return an error for FLOAT and UINT
channels** instead of writing them as zeros. Images whose channels are all HALF
are unaffected. Full non-HALF passthrough is landing shortly.

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

- **Files whose chunk offset table was never written are now read.** The table is
  written by `Close`, so an interrupted render — or a caller who never closed the
  writer — leaves it zeroed on otherwise intact data. Such a file previously
  decoded to silent zeroes; it is now recovered by rescanning the chunks, as the
  reference implementation does. Scanline chunks are indexed by their own `y`
  coordinate, which also tolerates chunks stored out of order. This is the
  failure actually reported in #4.
- **Out-of-bounds pointer arithmetic in the strided `Slice` row accessors.** The
  seven non-contiguous read/write loops advanced a pointer one stride past the
  end of the destination after the final element. Never dereferenced, but
  undefined behaviour, and it made `go test -race ./...` abort with a `checkptr`
  failure — so the race suite had never passed.

### Security

Hardening against malformed and hostile files. Each of these turned a value read
straight from a file header into an unbounded allocation or a panic. Several
were fast paths that had dropped a bound their slower twin still had.

- **`ScanlineReader` allocated an unvalidated chunk size.** `readChunkReuse`
  used the packed size from the chunk header directly, so a four-byte field in a
  900-byte file could demand a 2 GiB allocation. `File.ReadChunk` had always
  bounded this; the faster path did not. All such paths now share
  `File.validatePackedSize`.
- **Deep chunk readers allocated unvalidated sample and pixel sizes**, which
  could panic outright on a negative or absurd value.
- **The chunk offset table is bounded by the file size.** A header could declare
  up to 16M chunks — 128 MB of offset table — regardless of how small the file
  actually was. A file of n bytes cannot describe more than n/8 chunks.
- **`AllocateChannels` no longer panics or allocates without limit.** It
  previously divided by a zero sampling factor, indexed `buf[0]` on a zero-size
  channel, and multiplied width by height by pixel size in `int` with no
  overflow or total check. It now validates everything before allocating and
  caps the total at `DefaultAllocationLimit`.
- **`ScanlineReader`'s chunk buffer hint no longer overflows** on a header
  declaring an enormous width and channel count.

### Added
- `AllocateChannelsLimit`, an explicit-ceiling form of `AllocateChannels` that
  reports why it refused rather than degrading silently.

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

### Documentation
- **The three README code examples did not compile.** All of them referenced
  symbols that do not exist (`exr.NewHeader(w, h)`, `exr.NewWriter(path, ...)`,
  `exr.HalfFromFloat32`, `RGBAInputFile.ReadPixels`), and an entire "Options"
  section documented a functional-options API that was never implemented. They
  are rewritten against the real API and are now compile-checked by
  `readme_example_test.go`, so the next drift breaks the build.
- The compression support matrix now states what the conformance suite actually
  covers, per codec and per direction, rather than implying everything is
  verified. `docs/FEATURE_PARITY.md` no longer claims 100% parity without
  distinguishing "implemented" from "verified interoperable".
- New `docs/CONFORMANCE.md` records the compatibility rules this library follows
  (correctness first, strict on output, lenient on input) and why round-trip
  tests are not sufficient for a codec.

### Known issues

These are being closed in follow-up work; they are recorded here rather than
implied away.

- **HTJ2K encoding has a data race in the `go-jpeg2000` v1.2.1 dependency.** Its
  parallel tile encoder shares one `*T1` across goroutines
  (`entropy.(*T1).EncodeFast5` writes while `entropy.(*T1).TruncationPoints`
  reads), so any HTJ2K encode can trip the race detector. No go-openexr code is
  involved and every other package is race-clean; CI runs `-race` with HTJ2K
  skipped until the dependency is fixed.
- **DWAA/DWAB cannot read files written by other implementations.** The DWA
  static-Huffman decoder is still a zlib fallback. See the support matrix in the
  README.
- **B44/B44A now refuse FLOAT and UINT channels** rather than writing them as
  zeros. OpenEXR stores such channels uncompressed alongside the compressed HALF
  ones; that is not implemented here, and silently zeroing them was worse than
  refusing. Images whose channels are all HALF are unaffected.
- **HTJ2K silently decodes HALF channels to all zeros.** FLOAT channels
  round-trip exactly; HALF produces a black image with no error. Measured on a
  64x40 three-channel gradient: 0 of 2560 samples non-zero. Neither case can be
  verified against the reference implementation — OpenImageIO 3.1.16 can neither
  write an HTJ2K EXR nor read one this library produces — so HTJ2K has no
  interoperability coverage at all, and no fuzz target.

### Changed
- Predictor and byte-reorder call sites across scanline, tiled, deep and
  multi-part now route through the shared `predictor.ReconstructBytes` /
  `DeconstructBytes` helpers instead of repeating the pipeline inline.
- `RLEDecompress` delegates to `RLEDecompressTo`; the two copies had drifted.

## [1.2.1] - 2026-02-28

### Fixed
- Removed a local `replace` directive that made the published module
  unbuildable for consumers, and corrected gofmt in `compression/huffman.go`.

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
