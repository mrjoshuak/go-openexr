# go-openexr Progress Tracker

## Codec Interoperability: issue #4 and the defects it uncovered

### Date: August 17, 2026

Branch: `fix/zip-rle-spec-conformance` (open)

Issue #4 reported that float32 scanline reads returned zeros or garbage. The
uncompressed part of the report turned out to be a missing `ScanlineWriter.Close`
in the reporter's code, but chasing it surfaced a much larger problem: **ZIP,
ZIPS, RLE and PIZ were all non-interoperable in both directions**, and the test
suite could not see it because every codec test was symmetric — round-trip, or
one in-repo implementation against another, or a fixture this library generated.

Fixed (details in CHANGELOG):

- [x] ZIP/ZIPS reorder-then-predictor ordering
- [x] Predictor +128/-128 bias (all variants, including the SIMD paths)
- [x] RLE control-byte convention (non-negative = repeat, negative = literal)
- [x] RLE missing reorder + predictor pre-passes
- [x] "Store the chunk raw when it does not shrink" rule, read and write, for
      scanline, tiled and multi-part
- [x] PIZ Huffman run-length pseudo-symbol truncated by a `uint16` table
- [x] PIZ wavelet left-over column/row pivot
- [x] PIZ `wdec14_4` intermediate truncation

Verified: all 15 pixel-type x compression combinations now pass
`oiiotool --diff`, and go-openexr decodes the official ASWF `openexr-images`
corpus bit-exactly.

### Fuzzing

`go test -race ./...` had never passed (a `checkptr` violation in the strided
`Slice` accessors), and the fuzz targets bounded their own input rather than
letting the library's bounds be the thing under test. Both are fixed: the
harness now allocates through `AllocateChannelsLimit` and applies no geometry
guard of its own, so a hostile header reaches the same code a real caller would.

Doing that immediately surfaced a genuine allocation bomb in
`ScanlineReader.readChunkReuse` (2 GB peak from a 900-byte input, now 120 MB).
CI now runs `-race`, which also enables `checkptr`.

Known: `go test -fuzz` at the default 16 workers can still be OOM-killed on a
machine under load, because each worker loads the ~39 MB corpus. That is worker
count times corpus size, not a library allocation; `-parallel=4` runs clean.

### Test integrity work

- [x] Conformance corpus with external ground truth (`exr/testdata/conformance/`)
- [x] Exact-value tests against the ASWF `openexr-images` corpus
- [x] Spec-anchored predictor, byte-reorder and wavelet tests
- [ ] Remaining backlog: an audit found 125 candidate false-assurance tests, of
      which 21 were proven unable to fail by mutation testing. The ZIP, PIZ,
      B44, DWA, HTJ2K, huffman and half test files still rest largely on
      round-trip or self-referential assertions and need spec anchors.
- [ ] `TiledWriter`/`MultiPartOutputFile` `PIZChannel` construction omits the
      XSampling/YSampling divides that the scanline path performs (latent;
      only bites subsampled channels).
- [ ] `ScanlineWriter` produces a truncated file if `Close` is never called,
      silently. This is what made issue #4 look like a read bug.

## OSS Release: v0.1.0 Preparation

### Date: January 10, 2026

### Completed

- [x] Repository restructured (moved source from `go/` to root)
- [x] New git repository initialized with clean history
- [x] Comprehensive `.gitignore` created
- [x] `LICENSE` and `NOTICE` updated for 2024-2025
- [x] `README.md` updated with correct paths
- [x] `CONTRIBUTING.md` created with contribution guidelines
- [x] `SECURITY.md` created with vulnerability reporting policy
- [x] Godoc comments audited and improved
- [x] All tests passing (including race detection)
- [x] Initial commit created

### Code Quality Fixes Applied

**Critical/High Severity:**

- Removed unrestricted file path operations
- Added size limits for headers and chunks to prevent DoS
- Implemented proper parallel worker cleanup
- Added context cancellation support
- Fixed error handling in deferred functions

**Medium Severity:**

- Replaced unchecked type assertions with comma-ok idiom
- Fixed unsafe string conversions to proper byte slices
- Added validation for header fields
- Capped worker counts at reasonable maximums

**Low Severity:**

- Added package-level constants for magic numbers
- Created sentinel errors for better error handling
- Added godoc comments to internal functions

---

## Previous Work: B44 SIMD Optimization

### Status: In Progress

**Goal:** Implement high-performance SIMD optimizations for B44 compression across all major instruction sets.

### Completed Tasks

- [x] Fixed GrainSize parallelization bug (was preventing parallel compression)
  - Changed default from GrainSize=4 to GrainSize=1
  - Resulted in 5-7x speedup for PIZ, B44, DWAA compression

- [x] Created ARM64 NEON assembly for B44 (`compression/b44_arm64.s`)
  - `toOrderedSIMD` - Sign-magnitude to ordered conversion
  - `findMaxSIMD` - Horizontal maximum reduction
  - `fromOrderedSIMD` - Inverse conversion

- [x] Updated `b44_arm64.go` with assembly declarations
- [x] Tested ARM64 NEON implementation (all tests pass)
- [x] Created B44 SIMD correctness tests (`b44_simd_test.go`)

### ARM64 NEON SIMD Benchmark Results (Apple M3 Max)

| Function          | Time/op | Values | Per-Value |
| ----------------- | ------- | ------ | --------- |
| `toOrderedSIMD`   | 2.3 ns  | 16     | 0.14 ns   |
| `findMaxSIMD`     | 0.88 ns | 16     | 0.05 ns   |
| `fromOrderedSIMD` | 1.4 ns  | 16     | 0.09 ns   |

### B44 Performance Comparison (Flowers.exr, Half-Precision)

| Metric     | Go     | C++    | Ratio       |
| ---------- | ------ | ------ | ----------- |
| B44 Write  | 1.35ms | 0.34ms | 3.9x slower |
| B44A Write | 1.40ms | 0.35ms | 4.0x slower |

_Improvement from ~6x to ~4x slower with SIMD functions. Further gains require vectorizing the inner pack loop._

### Windows AMD64 Benchmark Results (AMD Ryzen 9 3950X)

| Benchmark     | Time/op | Throughput |
| ------------- | ------- | ---------- |
| PackB44       | 300 ns  | -          |
| B44Compress   | 158 µs  | 156 MB/s   |
| B44Decompress | 60 µs   | 410 MB/s   |

### Cross-Platform Comparison (64x64 3-channel image)

| Platform        | Compress          | Decompress       |
| --------------- | ----------------- | ---------------- |
| ARM64 (M3 Max)  | 68 µs (358 MB/s)  | 28 µs (877 MB/s) |
| AMD64 (Ryzen 9) | 158 µs (156 MB/s) | 60 µs (410 MB/s) |

_ARM64 is ~2.3x faster than AMD64, likely due to Apple Silicon's unified memory and newer architecture._

### Completed

- [x] AMD64 SSE2 implementation (already existed)
- [x] Windows/AMD64 testing - All B44/SIMD tests pass
- [x] Cross-platform benchmarks

### Inner Loop Vectorization - SSE2 Success

Successfully implemented SSE2 vectorized `shiftRoundSIMD` for AMD64 using `PSRLW xmm, xmm` for runtime uniform shifts.

**Key insight:** While the shift amount varies per block (runtime variable), it's uniform across all 16 values within a block. SSE2's `PSRLW` instruction supports shifting by a value in another XMM register, which Go's AMD64 assembler does support.

**Files created:**

- `compression/b44_pack_amd64.s` - SSE2 assembly implementation

**Results:**

| Platform              | Before            | After             | Improvement    |
| --------------------- | ----------------- | ----------------- | -------------- |
| AMD64 (Ryzen 9 3950X) | 158 µs (156 MB/s) | 122 µs (202 MB/s) | **23% faster** |
| ARM64 (Apple M3 Max)  | 68 µs (358 MB/s)  | 74 µs (330 MB/s)  | ~10% slower\*  |

\*ARM64 uses unrolled pure Go since Go's assembler doesn't expose NEON USHL with register shifts.

**Technical notes:**

1. AMD64 SSE2 `PSRLW xmm, xmm` works with shift count from low 64 bits of second register
2. ARM64 NEON `USHL` requires register shifts but Go's assembler doesn't expose it
3. Unrolled scalar code on ARM64 nearly matches original inlined performance

### Notes

- HTJ2K dependency requires local go-jpeg2000 module (excluded for Windows testing)
- Interleave tests have an unrelated issue on Windows (not B44-related)

---

## Recent Session: Performance Benchmarking & Fixes

### Date: January 2026

### Key Findings

1. **GrainSize Bug:** Parallelization was disabled for complex compressions
   - PIZ (23 chunks) < GrainSize \* NumCPU (32) → ran sequentially
   - Fixed by changing GrainSize from 2/4 to 1

2. **Performance After Fix:**

| Compression | Go Write | C++ Write | Ratio                 |
| ----------- | -------- | --------- | --------------------- |
| none        | 0.53ms   | 1.93ms    | **0.27x** (Go faster) |
| rle         | 2.11ms   | 2.23ms    | **0.95x** (Go faster) |
| zips        | 6.93ms   | 3.47ms    | 2.00x                 |
| zip         | 5.32ms   | 2.38ms    | 2.24x                 |
| piz         | 7.45ms   | 2.26ms    | 3.30x                 |
| pxr24       | 3.44ms   | 2.35ms    | 1.46x                 |
| b44         | 1.92ms   | 0.33ms    | 5.84x                 |
| b44a        | 1.27ms   | 0.34ms    | 3.71x                 |
| dwaa        | 4.57ms   | 3.41ms    | **1.34x**             |
| dwab        | 31.47ms  | 7.56ms    | 4.16x                 |

### Files Modified

- `exr/parallel.go` - Changed default GrainSize
- `cmd/exrmetrics/main.go` - Updated GrainSize setting

---

## Prior Work: Determinism Support

### Branch: `feature/determinism-support`

### Features Implemented

1. **Header Attribute Ordering**
   - Attributes serialized in alphabetical order
   - `Header.Attributes()` returns sorted slice

2. **Compression Level Detection**
   - `DetectZlibFLevel()` extracts FLEVEL from zlib headers
   - `ZIPCompressLevel()` allows configurable compression
   - Headers preserve detected FLEVEL for round-trip

3. **ID Manifest Determinism**
   - Manifest entries sorted by ID before encoding
   - Cryptomatte JSON keys in alphabetical order

### Documentation

- `docs/DETERMINISTIC_ROUNDTRIP.md` - API reference
- `upstream/ASWF/proposal/DETERMINISM_IMPROVEMENTS.md` - C++ proposal

---

## Optimization Reference

See `docs/B44_SIMD_OPTIMIZATION.md` for detailed B44 optimization plan.
