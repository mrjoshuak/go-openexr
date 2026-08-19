# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Tiled and multi-part images are now gated by the reference implementation, in
the write direction, level by level and part by part. Eight defects fell out of
the first runs. Every one of them produced a file the reference refuses to open
or reads wrongly, and every one was invisible to the existing suite, because a
round trip reads a file back with the same assumption that wrote it.

### Fixed
- **A tile's offset was recorded in the slot the write arrived in, not the slot
  its coordinates name.** The format indexes a tiled part's chunk offset table
  by tile coordinate — level major, for a ripmap y level major, then tile row,
  then tile column — and a reader looks a tile up by coordinate, so any write
  order other than the canonical one produced a file the reference rejects
  outright: `(EXR_ERR_BAD_CHUNK_LEADER) Corrupt tile (0, 0), level (0, 0)
  (chunk 0): bad tile x coordinate (2, expect 0)`. The reference's own writer
  documents that tiles may be written in any order. `Writer.WriteTileChunkPart`
  now computes the slot from the coordinates (`tileChunkIndex`), so `TiledWriter`
  and `MultiPartOutputFile` accept any order, and `scripts/validate.sh` writes
  three fixtures — one level, mipmap and ripmap — in reverse order to hold them
  to it.
- **`Header.Validate` accepted a tiled header with subsampled channels.** The
  format forbids them, and the reference refuses to open such a file: `channel
  'BY': x subsampling factor is not 1 (2) for a tiled image`. This library wrote
  one happily. `Validate` now returns `ErrTiledSubsampling`, so both the tiled
  and the deep tiled paths refuse it before a byte is written. This is also what
  makes the nearby `XSampling`/`YSampling` divides that `TiledWriter` and
  `MultiPartOutputFile` omit when building a `PIZChannel` unreachable rather than
  merely untested: the file is illegal before the codec sees it.

- **A multi-part file containing a tiled part was unreadable.** The version
  field set the tiled flag whenever any part was tiled, alongside the
  multi-part flag. The two are mutually exclusive — a multi-part file states
  each part's storage in that part's own `type` attribute — and OpenEXR
  rejects the file before reading a pixel:
  `EXR_ERR_FILE_BAD_HEADER Invalid combination of version flags`. Every
  multi-part file this library wrote with a tiled part was refused as a whole
  by the reference implementation.
- **Parts whose data window did not start at y=0 were written from the wrong
  scanlines.** `MultiPartOutputFile` addressed the caller's frame buffer in
  image coordinates while the rest of the package addresses it relative to the
  data window, so a part with an inset or negative origin was shifted by the
  origin and read past the end of the caller's buffer. Tiled parts had the
  same defect in Y. Measured against the reference: 100% of samples differed.
- **Writing a part a scanline at a time produced an unreadable part or an
  error.** `WritePixels` emitted a chunk per call, so any codec that packs
  several scanlines into a chunk got chunks off the grid the format anchors at
  the data window's first scanline, then failed with "too many chunks
  written". Chunks are now emitted whole, and lines that do not yet complete
  one are held.
- **Parts declaring HTJ2K compression were stored uncompressed.** The
  multi-part compressor had no HTJ2K case and fell through to a default that
  returned the samples unchanged. The result still reads back — a chunk no
  smaller than its unpacked size is raw by definition — so the only symptom
  was a part that advertised a compression it had never had applied. The
  default now returns an error instead of silently storing raw.

### Added
- `scripts/tiledgen` writes 24 tiled fixtures — one level, mipmap and ripmap;
  tile sizes that divide the image, that leave partial tiles on both edges, and
  that are larger than the image; both rounding modes; a data window that does
  not start at the origin; `none`, `rle`, `zip`, `zips`, `piz`, `pxr24`, `b44`,
  `dwaa` and `dwab`; and both `WriteMipmapTiledImage` and
  `WriteRipmapTiledImage` — and beside each one the samples it must hold and the
  geometry it must claim.
- `scripts/exrtiledump` reads those files with the OpenEXR reference
  implementation itself and prints every sample of every level, so nothing in
  this library participates in checking them. `scripts/validate.sh` gained 32
  checks that compare all 456,220 samples, diff the reference's level arithmetic
  against this library's, cross-check level 0 with `oiiotool`, and run three
  controls first: the reference's own `exrmaketiled` output must satisfy the same
  expectation, `oiiotool` must round-trip its own tiled output, and the
  comparator must report a difference it is given deliberately.
- `ErrTiledSubsampling`, and `scripts/testdata/tiled_subsampled_invalid.exr` —
  a file this library wrote before the guard existed, kept so the gate can
  confirm every run that the reference still refuses it.
- Multi-part files are gated against the reference implementation.
  `scripts/multipartgen` writes six of them — an embedded-proxy pair, three
  differing data windows including a negative origin, eight codecs one per
  part with HTJ2K beside ZIP and PIZ, four unrelated channel layouts, a
  scanline part beside two tiled parts, and a scanline master beside a
  mipmapped tiled proxy — with the intended samples beside each part as plain
  PFMs, one per channel per resolution level, and `scripts/validate.sh` asks
  the reference, part by part, level by level and channel by channel, whether
  the file holds them. The gate runs 97 checks, up from 44, and includes a
  control (the reference reading its own two-part file through the same
  procedure) and two signal checks (the same comparison against deliberately
  wrong truth, which must fail).
- `NewMultiPartWriter` refuses parts that disagree about an attribute every
  part must share — display window, pixel aspect ratio, time code,
  chromaticities — with `ErrConflictingAttributes`. OpenEXR rejects such a
  file on both writing and reading, so it could only ever be opened by this
  library.
- Every part of a multi-part file now declares its own `chunkCount`, which the
  format requires and the reference implementation always writes.

## [1.4.2] - 2026-08-19

### Fixed
- **B44 encoding produced non-conforming output on amd64.** The SSE2 pack
  routine computed `(tMax - t[i]) << 1` in 16-bit lanes (`PSUBW`, `PADDW`,
  `PSRLW`), so it wrapped at 65536 whenever that difference exceeded 32767 —
  reachable for any block spanning a wide enough range once the ordered
  transform has flipped the negatives. `ImfB44Compressor.cpp` does the same
  arithmetic in `int`. amd64 builds therefore wrote B44 blocks the reference
  decodes differently, while arm64 builds were correct.

  Every conformance run had been on arm64, where this path has always been
  scalar, so nothing local caught it; CI did. The vectorised routine is removed
  rather than patched — fixing it means widening to 32-bit lanes, and shipping
  assembly that cannot be verified on the machine at hand is what produced the
  defect. amd64 now uses the same scalar path as arm64, at the cost of roughly
  the 23% B44 encode win the assembly bought. See ROADMAP.md.

### Added
- `scripts/validate.sh` runs the test suite under the other `GOARCH` as well,
  so an architecture-specific defect is caught locally rather than by CI.
- `ROADMAP.md`, ordered around what codestream-level access to HTJ2K chunks
  makes possible, with the reasoning recorded.

### Documentation
- The README stated the opposite of what is now true in its most load-bearing
  places: HTJ2K was recorded as having no external oracle in either direction,
  OpenImageIO was said to be unable to read our HTJ2K output, and PXR24 write
  was listed as uncovered. All corrected against measurements.
- The HTJ2K read direction is stated honestly rather than claimed: OpenImageIO
  cannot write an HTJ2K EXR, so no reference-written file exists for us to read.
  Thirty of the thirty-six combinations are verified both ways; the six HTJ2K
  ones are verified writing only, and the table says so.
- The overview leads with what the library does rather than what was fixed, and
  opens with a short description of the format for readers who have not used
  OpenEXR before.
- The codec count was given three different ways. OpenEXR has 11 compression
  methods across 12 IDs, because HTJ2K appears as both HTJ2K256 and HTJ2K32; the
  README now says that once and consistently.
- Line coverage is no longer the headline verification claim. It is listed last,
  behind the reference-implementation gate and the mutation harness, with the
  reason: 90%+ coverage coexisted with 125 candidate false-assurance tests, 21
  of them proven unable to fail.

## [1.4.1] - 2026-08-18

HTJ2K now interoperates. All six HTJ2K rows in the validation gate are
bit-identical to the reference, closing the last gap in the pixel-type by
compression matrix: **all 36 combinations now agree with the OpenEXR reference
under `oiiotool --diff`**, with no excused rows.

### Fixed
- `htj2kCompressFloat` never set `Options.HighThroughput`, so the float path
  emitted a baseline Part 1 codestream under an OpenEXR compression id that
  declares HTJ2K. OpenJPH rejected the result with "Rsiz bit 14 is not set (this
  is not a JPH file)".
- Requires go-jpeg2000 v1.5.0. Earlier versions could not emit a codestream the
  reference accepts: v1.3.0's `EncodeHalf`/`EncodeFloat` ignored
  `HighThroughput` and its NLT segment was written short. Those are fixed
  upstream, and with them the six HTJ2K rows are gated as hard checks rather
  than excused.
- `TestHTJ2KExtractPackets` and `TestHTJ2KBuildPacketIndex` asserted properties
  of go-jpeg2000's former private tile container. Re-anchored to the conforming
  packet model, with the packet count derived from the codestream's own COD
  marker rather than from the container layout.
- The `HTJ2KCompress` block-size parameter is named `blockWidth`, matching what
  it is. Documentation only; the signature's types and arity are unchanged.


### Added
- `ROADMAP.md`, listing what OpenEXR supports that this library does not yet
  verify, with the acceptance standard for each.
- `scripts/mutation/`, a repeatable harness that breaks a codec deliberately and
  records whether the tests notice. Current state: 11 of 20 mutations survive
  the pre-existing tests, and all 15 covered by the added spec-anchored tests
  are killed.
- `scripts/validate.sh` and PXR24 write-side coverage (half and uint
  bit-identical, float within the bound the 24-bit format implies).

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
