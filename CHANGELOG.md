# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.18] - 2026-08-20

Fifth fix from the parity audit: line order.

### Fixed
- **`lineOrder` was stored in the header and ignored.** Every scanline file was
  written front to back whatever it declared, so a file saying DECREASING_Y —
  "first scan line has highest y coordinate", in the reference's own words —
  was laid out ascending. A reader streaming the file rather than seeking
  through the chunk offset table gets the rows in the opposite order to the one
  the header promises.

  Nothing could catch this. libOpenEXR seeks through the offset table and does
  not care about the physical order, so it reads such a file without complaint;
  a round trip through this library is equally blind. Only inspecting the
  layout shows it, and the check added here does that by sorting the offsets
  rather than walking the table — walking the table shows ascending y for every
  file and measures nothing, which is part of why this survived.

  The offset table stays ordered by increasing y in both orders, because that
  is the index a reader seeks with. `Writer.WriteChunkPartAt` is new, so a
  writer emitting chunks out of order can say which slot it means rather than
  having it inferred from arrival.

### Added
- **RANDOM_Y is refused on a scanline part**, quoting the reference's own
  definition: `ImfLineOrder.h` says it is "only for tiled files; tiles are
  written in random order". A scanline part is chunks of consecutive rows and
  has no way to express it. It used to be accepted and written as increasing,
  producing a file whose header claimed something the format does not allow.

### Gate
- 262 checks, 0 failures, 0 skipped. Two new checks: the physical layout of a
  file in each order, and that the reference reads the same samples from both —
  so reordering the chunks is proved not to have disturbed the offset table.
- Mutations 47 and 48: `lineorder-ignored` and
  `random-y-accepted-on-scanline`. Both survive the pre-existing tests.

### A correction to the audit
The parity audit reported that the reference *refuses* a scanline file
declaring RANDOM_Y. It does not — `exrheader`, `exrpartdump` and `oiiotool` all
read one without complaint, because they seek through the offset table. The
defect is real and worth fixing, but it is a silent divergence from the
format's own definition rather than a loud failure, which is a different and
somewhat worse thing.

## [1.4.17] - 2026-08-20

Fourth fix from the parity audit: luminance/chroma. Three defects, and the
first is the one that mattered most.

### Fixed
- **The chroma channels carried the wrong quantity.** The format stores
  `(R-Y)/Y` and `(B-Y)/Y`; this library stored the plain differences `R-Y` and
  `B-Y`. That is perfectly self-consistent — its own reader undid its own
  writer exactly, so every round-trip test passed — and it means something
  different from what the file declares. The chroma of every YC file written
  here was wrong for every other reader, and every YC file written elsewhere
  was read wrongly here.

- **The chroma writer wrote each value at a quarter of its position.** It passed
  chroma *plane* coordinates to `SetHalf`, which takes window-absolute ones and
  divides by the channel's sampling — so the index was divided a second time and
  three quarters of the chroma plane was never written at all. Measured: 48 of
  64 cells left at zero on a 16x16 image.

- **The chroma upsampler made the same mistake on the way back**, reading a
  quarter of the plane over and over. This is the third and fourth instance of
  one bug — the row functions in 1.4.16 were the first two — so both call sites
  now scale by the slice's own sampling factor rather than by a literal.

  Together with 1.4.16, a file this library writes is now read by libOpenEXR
  with Y, RY and BY matching the format's definition to 2.4e-4, which is inside
  half precision.

### Changed
- `TestYCRoundTrip` now states its tolerance per fixture and keeps a
  deliberately pathological one. Storing the ratios is correct and worse
  conditioned than storing the differences: as Y approaches zero the ratios grow
  without bound, so averaging the chroma of a 2x2 block whose luminance varies
  reconstructs its darkest pixel poorly. Measured, worst channel error against
  the minimum luminance in the image: 0.0217 → 0.066, 0.275 → 0.0082,
  0.575 → 0.0047. That is the format's conditioning, and the dark fixture is
  kept to show it rather than removed to make a number look better.

### Gate
- 260 checks, 0 failures, 0 skipped. The check is deliberately not a round trip:
  `scripts/ycgen` writes a file, libOpenEXR reads its Y, RY and BY planes, and
  `scripts/yccheck.py` compares them against the definition computed
  independently from the source. With a signal check that rescaled chroma is
  rejected — the exact shape of the defect being fixed.
- Mutations 45 and 46: `yc-plain-differences` (symmetric) and
  `yc-chroma-plane-index`. Both survive the pre-existing tests.

## [1.4.16] - 2026-08-20

Third fix from the parity audit: subsampled channels. This was the largest of
them — broken on both axes, in both directions, on almost every codec, and
invisible to every test because each defect was applied identically to the
reader and the writer.

### Fixed
- **Horizontal subsampling put every sample in the wrong column.** The row
  functions are indexed by *stored* column — a channel with xSampling 2 has half
  as many columns as the window is wide — and their per-pixel fallbacks passed
  that index to an accessor that divides by xSampling again. Since the fast
  paths require xSampling of 1, the fallback is the only path a subsampled
  channel ever takes. Measured against libOpenEXR on a 16x16 file: **316 of 512
  samples wrong, on ten of the twelve codecs**, with the reference reading the
  files without complaint.

- **Vertical subsampling made every chunk the wrong size.** A channel with
  ySampling n stores a row only where the image row is a multiple of n, so a
  chunk carries fewer of its rows than of a full-rate channel's — and two chunks
  of equal height can differ in size. Every channel was contributing a row to
  every scanline. Measured: `none`, `rle`, `zips`, `zip` and `piz` produced files
  libOpenEXR **could not decompress at all**, and `pxr24` produced one it read
  with **359 of 384 samples wrong**.

- **PIZ and PXR24 then panicked rather than producing a wrong file.** Once the
  chunk sizes were right, both codecs still consumed a row per channel per
  scanline: `index out of range [768] with length 768`. Both now take the
  chunk's first row and derive each channel's row count from it.

- **Multi-part PIZ with a subsampled channel panicked.** That path handed PIZ
  the window's width for every channel, where the scanline path had always
  computed it per channel: `index out of range [128] with length 128`.

  All six codecs that can carry a subsampled channel now agree with libOpenEXR
  sample for sample, at 4:2:2 and 4:2:0, in both directions.

### Gate
- 257 checks, 0 failures, 0 skipped. A new section covers twelve codec and
  sampling combinations: `scripts/subsampgen` writes a file and reads it back,
  `scripts/exrpartdump` reads the same file with libOpenEXR, and the two are
  compared by key so a dropped channel is reported apart from a wrong sample.
  With a signal check, and a check that a codec which cannot carry ySampling
  above one refuses rather than panicking.
- The multi-part subsampled fixture gained a third part, compressed with PIZ,
  and both subsampled parts are now compared — checking only the first would
  have left the panic unseen.
- Mutations 42, 43 and 44: `row-fallback-double-divides`,
  `ysampling-rows-ignored` and `multipart-piz-full-width`. The first two are
  marked symmetric. All three survive the pre-existing tests and are killed only
  by the new ones, which is the measurement that says the old suite could not
  have caught them.

## [1.4.15] - 2026-08-20

Second fix from the parity audit: header attribute parsing.

### Fixed
- **`floatvector` carried a count prefix the format does not have.** The
  attribute is the float values alone; the count is the declared size divided
  by four, which is how the reference derives it. This library wrote a leading
  int32 and expected one, and each direction failed differently.

  Writing: the reference read one value too many, the count surfacing as a
  denormal. Measured with `oiiotool` on a vector of (1.5, 2.5, 3.5), the
  reference reported `4.2039e-45, 1.5, 2.5, 3.5`.

  Reading: a reference-written vector took the bits of its first float as a
  count, failed the size check, and since an attribute error fails the open, a
  single such attribute made the whole file unreadable.

  The old tests asserted the prefix, so the round trip they checked was
  self-consistent and wrong — the same shape as the ACES defect in 1.4.14.

- **An attribute's declared size was never reconciled against what its type
  read.** When the two disagreed the reader carried on from the wrong offset:
  every attribute after it was parsed from the middle of something else, and
  because the header ends at an empty name it could stop early with a partial
  header. The observable result was `Channels()` returning nil for a file
  libOpenEXR refuses outright — a parse failure surfacing as missing data
  instead of an error, which is the worst available outcome. `ReadAttribute`
  now checks it and names the offending attribute. An unknown type is still
  preserved as raw bytes and still consumes exactly its declared size.

### Gate
- 254 checks, 0 failures, 0 skipped. Two new checks: the reference reading a
  `floatvector` this library wrote, and nine Go-side assertions covering the
  wire format, the lying-size refusal and unknown-type preservation. The size
  reconciliation has no external counterpart — the reference has no API for
  "parse this header and tell me where you stopped".
- Mutations 40 and 41: `floatvector-count-prefix` and
  `attribute-size-unchecked`. The first is marked symmetric, since it was
  applied identically to reader and writer, which is exactly why no round trip
  could see it.
- The ACES fixture added in 1.4.14 used an unseeded noise pattern. A gate check
  whose tolerance is one ULP cannot be built on content that changes every run:
  it can pass repeatedly and then fail on an image nobody can regenerate. The
  pattern is seeded now and confined to [0, 1], where one half-float ULP is at
  most 2^-11, and the threshold is twice that — derived from the format rather
  than from what happened to pass. Three consecutive runs at 254 checks, 0
  failures.

## [1.4.14] - 2026-08-19

First fix from the libOpenEXR parity audit.

### Fixed
- **ACES colour conversion was wrong by up to 58% per channel.** It produced a
  well-formed file with the wrong colours: no error, no failed round trip.
  Measured against the reference `exr2aces` on a Rec.709 constant of
  (0.8, 0.2, 0.1):

  ```
  reference : 0.446045  0.244141  0.123413
  this repo : 0.317871  0.101868  0.079590
  ```

  Three places mixed two matrix conventions. Imath composes row vectors as
  `v*M`, so it stores its matrices transposed and its products read left to
  right; this file applies `M*v`. The Bradford constants had been copied in
  Imath's form, the adaptation product was composed in Imath's order, and so was
  the final RGB to XYZ to ACES chain — while `RGBtoXYZ` and the per-pixel
  application were `M*v`. The mixture is self-consistent enough to look right.

  All three are now `M*v`. The conversion matches the reference within one
  half-float ULP, which is the format's own precision rather than a tolerance
  chosen to pass.

  **Why nothing caught it.** Every check the area had went out through this
  library and came back through it, and a round trip cannot see a colour
  transform that is wrong in both directions. The pre-existing
  `TestChromaticAdaptation` looked like coverage — it asserts that equal white
  points give the identity — but a *consistently* transposed pair satisfies that
  exactly. It verified self-consistency, not correctness. The mutation added
  here restores the original code precisely, and that pre-existing test survives
  it; only the new checks kill it.

### Gate
- 252 checks, 0 failures, 0 skipped. Two new checks: two fixtures converted by
  both this library and the reference `exr2aces` and compared within one half
  ULP, and a signal check that the comparison rejects an unconverted file.
- A 39th mutation, `aces-bradford-transposed`.

### Recorded
- ROADMAP now lists, under Later, the features libOpenEXR has that this library
  does not — `isComplete()`, the tiled-RGBA writer, `exrstdattr`/`exrmakepreview`/
  `exrmanifest`, the OpenEXRUtil image layer, the C core API, the RGBA reader's
  refusal of non-zero data window origins, ID manifest interop, and header
  validation looser than the reference's. Those are absent features rather than
  wrong answers, so they are a separate target rather than part of this work.

## [1.4.13] - 2026-08-19

### Added
- **`File.ReadRegionLevel`** — a viewport of a chosen resolution level.
  `ReadRegion` is it at level (0, 0) and is unchanged.

  This is the call an HD view of a 5K plate needs. Everything under it already
  took a level; the one-call API was hardcoded to zero. Gated against libOpenEXR
  at three mipmap levels, with a mutation that resolves every request at level 0
  — the fixture's content differs per level by a constant, so a substituted
  level is unmistakable rather than merely blurry.

  Worth stating alongside it: the pyramid is the mechanism for lower-resolution
  display, and the codestream's own resolution levels are not. A mipmap level is
  computed by a real downsample filter when the file is written; a
  reduced-resolution decode of a float chunk averages reinterpreted bit patterns
  and is unusable for display wherever the image spans exponents or touches
  zero.

- `HTJ2KEncodeOptions.QualityLayers`, which **refuses**, and is present to say
  so with the measurement attached.

  Layers would be bitrate-scalable playback from the original frames. The
  mechanism works — decoding a rate-allocated three-layer codestream of
  half-float content, the first layer alone is 23.5% of the code-block data at
  0.8% worst-case error, no pixel more than 10% off. Truncation perturbs each
  coefficient by a bounded amount, and a float's bit pattern is roughly
  logarithmic in its value, so a bounded error there is a bounded *relative*
  error in the sample. That is the right behaviour for HDR and is emphatically
  not the reduced-resolution case.

  What stops it is the reference. libOpenEXR's HTJ2K support is OpenJPH, and
  handing OpenJPH a four-layer chunk produces *"The current implementation
  supports 1 quality layer only. This codestream has 4 quality layers"* — the
  file is unreadable by anything else. Unlike a precinct partition, which the
  reference reads exactly, that is not a trade worth offering, so it is refused
  rather than written silently. See the roadmap for what would unblock it.

### Gate
- 250 checks, 0 failures, 0 skipped. Two new checks: an 8x8 rectangle of three
  mipmap levels against libOpenEXR, and the multi-layer refusal beside a control
  that a precinct request and a default request still work.
- A 38th mutation, `region-level-ignored`.
- The mutation harness rejected its own manifest partway through this work: the
  precinct anchor had drifted when `apply` was edited, and it said so —
  *"anchor text occurs 0 times, manifest says 1"* — instead of silently testing
  nothing. That is the behaviour the anchor counts exist for, and it caught a
  case where the CHANGELOG's own anchored edits had not.

## [1.4.12] - 2026-08-19

### Documentation
- Supplies the CHANGELOG entries for **1.4.8 through 1.4.11**, and the ROADMAP
  items for the scanline viewport and the precinct opt-in. Those four tags
  shipped without them.

  Worth recording because the failure was silent and the shape is general. A
  pre-commit hook rejected one release command, which killed the *whole* command
  including the CHANGELOG edit in it; the commit was then re-issued on its own
  and the edit was not. Every later entry anchored its insertion on the previous
  version's heading, so once 1.4.8's entry was missing, 1.4.9's replacement
  found no anchor and did nothing, and so did 1.4.10's and 1.4.11's. Four
  releases of documentation disappeared without a single error.

  An anchored text replacement that silently succeeds when it matches nothing is
  the same defect as a test that passes when it checks nothing, and it was fixed
  the same way: the script that supplied these asserts that each anchor exists
  and is unique.

  The code, gate and tests of 1.4.8 through 1.4.11 were unaffected — this is
  documentation catching up with what those releases already did.

## [1.4.11] - 2026-08-19

### Added
- **A precinct partition, as an opt-in.** `compression.HTJ2KEncodeOptions` with
  `PrecinctSizeLog2`, reachable from either writer through
  `SetHTJ2KEncodeOptions`, plus `HTJ2KCompressOptions`. The default is
  untouched: a chunk written without asking is still what libOpenEXR would have
  written, which is the bargain this package makes with every other reader.

  Asking for a partition is the one thing this library will write that the
  reference encoder would not, so three things are gated separately.

  *The default is unchanged.* Pinned against Scod bit 0 of the COD marker — the
  codestream's own statement about whether it carries a partition — and not only
  by comparing the optioned and un-optioned paths against each other. That
  comparison alone is not enough, and a mutation proved it: forcing precincts on
  by default moves every entry point together and they still agree. The first
  version of that test survived the mutation.

  *The reference still reads it.* libOpenEXR 3.4.14 reads a precinct-partitioned
  file to the same 262144 samples as the plain one, exactly. That was the open
  question rather than an assumption.

  *And it buys something.* On a 512x512 chunk a 128x128 region decodes 66794
  code-block bytes instead of 151960, for 2.51% more file. What it is really for
  is addressability: without a partition a resolution is a single packet
  covering the whole chunk, so the packet index returns all of it however small
  the region — 18 of 18 packets, 100% of the bytes. That is the ceiling this
  lifts, and it is why the trade is offered rather than taken.

### Gate
- 248 checks, 0 failures, 0 skipped.
- A 37th mutation, `htj2k-precinct-default-on`, writes a partition when none was
  asked for. No external oracle can catch it — the reference reads both files
  identically — so only the codestream's own signalling can.

## [1.4.10] - 2026-08-19

### Added
- **`File.ReadRegion` serves a scanline part.** It refused them outright before,
  which mattered more than it sounds: scanline is the format's default storage,
  so the viewport path applied to a minority of the EXRs in the world.

  The geometry differs from a tiled part in both directions. A scanline chunk is
  the full width of the data window by 32 or 256 rows, so a viewport pulls whole
  rows and the chunk-level saving is weaker — a 128x128 rectangle of a
  reference-written 512x512 file reads 9 of 32 chunks and 299886 of 1066785
  bytes. For HTJ2K, though, the viewport is a small part of a very wide chunk,
  and that is where the codestream saving is largest: a 256x256 viewport of a
  2048x512 scanline part decodes 31254 of 57096 code-block bytes, against a
  256x256 tile where the chunk is already viewport-sized and nothing can be
  skipped at all.

  Subsampled channels are refused rather than guessed at, as on the tiled path.

### Gate
- 246 checks, 0 failures, 0 skipped. A new check reads a rectangle of a scanline
  file oiiotool wrote and compares it against `scripts/exrpartdump`'s reading of
  the same file, so nothing this library produced is in the comparison, and
  asserts the chunk and byte counts fell.
- A 36th mutation, `scanline-region-origin`, takes the region's columns as
  absolute rather than relative to the data window — invisible at a window of
  (0, 0), which is what every scanline fixture here used before.

## [1.4.9] - 2026-08-19

Reduced-resolution decode of an HTJ2K chunk works, and the refusal it replaces
turned out to be a measurement error rather than a limitation.

### Fixed
- **`HTJ2KDecodeOptions.ReduceResolution` is honoured.** A chunk decodes at
  half, quarter or eighth resolution and costs proportionally less — on a
  256x256 float chunk, reduce 1 puts 66% of the code-block bytes through the
  block coder, reduce 2 34%, reduce 3 15%. Bit-identical to
  `ojph_expand -skip_res` on the chunk's own codestream at four levels.

  The refusal rested on comparing a reduced decode against a downsample of the
  full decode. That is not what a reduced decode produces — the LL band at
  resolution r is the image the wavelet reconstructs at that scale, not an
  arithmetic average of the finer one — so the two disagreed by construction and
  the disagreement was read as a defect. Against the reference's own reduced
  decode, this library was already exact.

### Read this before using it
A reduced decode is **not a proxy image**, and that is the format's doing rather
than this implementation's. An EXR HTJ2K chunk carries float samples as
reinterpreted bit patterns under an NLT point transform, so the wavelet runs
over bit patterns and the reduced LL is a log-domain average of them. Measured
on a ramp over [0, 2): one level of reduction produces values from 2.2e-23 to
17.75, and `ojph_expand` produces the same values bit for bit. Anything wanting
a viewable half-resolution frame must downsample the samples, not the
codestream. What a reduced decode buys is cost.

### Changed
- `go-jpeg2000` is now v1.5.6, which is where the reduced decode lives, and
  which also stopped silently wrapping a coefficient that has no sample to map
  back to — 9 of 256 samples wrong against OpenJPH on extreme content, reported
  now rather than returned.

### Gate
- 245 checks, 0 failures, 0 skipped. A new check decodes one tile through the
  EXR API at four reduction levels and compares each against
  `ojph_expand -skip_res` on the codestream it extracted from that same chunk,
  so the two decoders are compared on identical bytes.
- A 35th mutation, `htj2k-reduce-dropped`, has the option silently ignored. It
  is the shape of defect this whole area was built around, and the dimensions
  are what notice.
- `scripts/exrreduce` (with a self-contained PFM comparator, so this
  repository's gate needs nothing from go-jpeg2000's scripts) and a
  single-channel fixture from `scripts/viewportgen`, since PFM carries one or
  three components and two have nowhere to go.

## [1.4.8] - 2026-08-19

Opening a file no longer reads the file. Until now it did, which made the
byte-range path worth nothing in the case it was built for.

### Fixed
- **`Open` fetched the entire file, and refused any file over 64 MiB.** The
  header's length was computed as `size - 8` — the whole rest of the file — so
  `Open` pulled all of it into memory, and the 64 MiB DoS bound meant for the
  header was applied to that figure and rejected every larger file. A
  4096x4096 float frame is 55 MiB; a 5632x5632 one is 100 MiB and could not be
  opened at all.

  Measured on v1.4.7, counting bytes at the `io.ReaderAt`:

  ```
  1024x1024  (3.7 MiB): Open read 3868638 bytes (100.0% of file)
  2048x2048 (14.3 MiB): Open read 14950734 bytes (100.0% of file)
  4096x4096 (54.6 MiB): Open read 57213503 bytes (100.0% of file)
  ```

  So `File.ReadRegion` fetching 2% of a frame was pure addition on top of 100%
  already fetched. The chunk offset table, `ChunkRange`, `ChunksForRegion` and
  the viewport read were all real and all pointless from a cold open.

  `Open` now fetches a 64 KiB prefix and parses the headers and offset tables
  out of it. When one prefix is not enough it grows — and grows *exactly*,
  because once the headers have parsed the chunk counts are known and so is the
  table's size, so a large file costs at most one further fetch rather than a
  doubling search. The 64 MiB bound now applies to the header, which is what it
  was for.

  The same measurement after:

  ```
  1024x1024  (3.7 MiB): Open read 65544 bytes (1.7% of file)
  4096x4096 (54.6 MiB): Open read 65544 bytes (0.1% of file)
  5632x5632 (100.5 MiB): Open read 65544 bytes (0.1% of file)   <- previously refused
  ```

  End to end, a 256x256 viewport of a 5632x5632 HTJ2K frame now costs **1.0% of
  the file**, open included. On v1.4.7 the same read cost 101.7% of a 4096x4096
  frame, and the 5632 one could not be opened.

### Gate
- 244 checks, 0 failures, 0 skipped. A new check pins all three properties: a
  prefix rather than the file, exact growth when the offset table is larger than
  one prefix (16384 chunks, a 128 KiB table, opened in 196931 bytes across 4
  calls), and a file past the header cap opening at all.
- A 34th mutation, `open-reads-whole-file`, restores the whole-file prefix.
  Nothing about correctness changes when it is applied — every sample still
  reads back — so only a test that counts bytes at the `ReaderAt` can see it.
  That is why the added test counts.

## [1.4.7] - 2026-08-19

Fixes [#7](https://github.com/mrjoshuak/go-openexr/issues/7). A frame buffer
that does not cover the pixels being read or written is now an error naming the
mismatch, instead of a write past the end of every plane.

### Fixed
- **A mismatched frame buffer corrupted memory silently.** This library
  addresses a frame buffer in the data window's own coordinates, and wrote
  through unchecked pointer arithmetic. A buffer allocated for a window at the
  origin, against a file whose data window is somewhere else, therefore did not
  cover the rectangle being read — and the reader wrote outside it and returned
  nil. Measured on v1.4.6 with a 10x8 buffer at (0, 0) against a data window at
  (5, 3): **30 float32 words overwritten past the end of each of four planes**,
  no error. The report described it as wrong pixels; wrong pixels were the
  visible part.

  `Slice` now declares how much storage it has, every constructor in this
  package sets it, and `ScanlineReader.ReadPixels`, `ScanlineWriter.WritePixels`
  and the tiled decode path check coverage before touching anything. The row
  functions clip as a backstop for any path that does not check.

  **What this means for callers.** A read or write that was quietly landing
  outside its buffer now returns `ErrFrameBufferTooSmall`, wrapped with the
  channel, the storage it declares and the rectangle asked for. That is a
  behaviour change for code that was already wrong, and it is the point: the
  error says what to fix. Nothing that was correct changes.

  Correct usage was, and remains, `AllocateChannels(channels, dataWindow)` — or
  any buffer whose window is stated with `Slice.WithOrigin`. Band-shaped buffers
  work and are gated: allocate for the absolute rows you intend to read, and
  read those rows.

  A `Slice` built as a struct literal declares no extent and is checked as
  before, which is to say not at all. Nothing existing breaks.

### Added
- `ErrFrameBufferTooSmall`, `FrameBuffer.CheckCoverage`, `Slice.Width`,
  `Slice.Height`, `Slice.HasExtent`, `Slice.Covers` and `Slice.CoversBox`.
- `NewRGBAFrameBufferForWindow`, and `OriginX`/`OriginY` on `RGBAFrameBuffer`.
  `NewRGBAFrameBuffer` leaves the origin at zero, which is right only when the
  data window starts there; the existing test for an offset window used it and
  was one of the paths writing out of bounds.

### Gate
- 243 checks, 0 failures, 0 skipped. A new block asserts both halves: a covering
  buffer reads exactly — the whole window and a band of it — and a short one is
  refused with a guard band proving nothing past the planes was touched.
- A 33rd mutation, `framebuffer-coverage-check`, removes the coverage check. The
  guard-band test is what kills it, which is deliberate: an assertion on the
  error message alone would pass against a version that still corrupted memory.

## [1.4.6] - 2026-08-19

A rectangle of a tiled file can now be read without reading or decompressing the
rest of it, and what that saves is measured rather than asserted.

### Added
- **`File.ReadRegion`.** It resolves a rectangle to the tiles that hold it by
  reading chunk headers alone, fetches only those chunks, and for HTJ2K decodes
  only the code-blocks the rectangle can reach. `RegionSamples` carries one
  float32 plane per channel in the region's own coordinates, together with the
  chunks read, the file bytes read, and the code-block bytes decoded and
  skipped.

  Measured by the gate against libOpenEXR on a 512x512 HTJ2K file in 256x256
  tiles with its data window at (13, -7), for a 128x128 rectangle straddling two
  tiles: 2 of 4 chunks, 13856 of 25905 file bytes, 11114 code-block bytes
  decoded against 2017 skipped, and every sample identical to what the reference
  reads for the same rectangle.

  Two limits are stated rather than left to be inferred. The codestream saving
  is modest because the format fixes it: an EXR HTJ2K chunk must be the chunk
  the reference would have written — 128x32 code-blocks, five decompositions, no
  precinct partition — so addressing is per code-block, and a code-block's
  influence spans about 64 samples at the lowest resolution. Below roughly a
  256x256 tile nothing can be skipped at all and the whole saving is at the
  chunk level. And HTJ2K is the only compression with an interior to address; a
  ZIP chunk decompresses whole or not at all, so for every other codec this
  saves the chunk reads and reports a skipped count of zero. The gate asserts
  that zero on a ZIP file `exrmaketiled` wrote, because claiming a saving that
  does not exist is the easiest way for this to overstate itself.

- `HTJ2KPartialResult.DecodedBytes` and `.SkippedBytes`, so a region decode of a
  chunk reports what it spent. A region decode that decompressed everything and
  cropped would return identical samples; this is what tells the two apart.

### Changed
- `go-jpeg2000` is now v1.5.4, which is where the region decode itself lives.

### Internal
- The tiled reader's decompression switch is a function, `decompressTileData`,
  rather than a block inside `ReadTileLevel`. `ReadRegion` decompresses tiles it
  fetched by byte range, with no frame buffer and no level walk, and a second
  copy of that switch would be a second place for a codec to go missing — which
  is exactly how HTJ2K came to be absent from the tiled path in the first place.

### Gate
- A new section, "viewport reads: a rectangle costs a rectangle": eight checks
  covering the whole-window control, the rectangle against the reference's own
  samples, the chunk-level and code-block-level savings, the signal check, and
  the same rectangle read out of a ZIP file written entirely by OpenEXR's tools.
  242 checks, 0 failures, 0 skipped.
- A 32nd mutation, `region-tile-origin`: `ReadRegion` places a tile at its tile
  index times the tile size, dropping the data window's origin. It survives the
  origin-window test and is killed by the offset-window one, which is why the
  test fixtures now run at both (0, 0) and (13, -7). Every coordinate system a
  viewport read touches coincides at the origin.

## [1.4.5] - 2026-08-19

Frame buffer coordinates are window-absolute again, and every storage type in
the format is now gated in both directions with no measured gaps left.

### Fixed
- **Frame buffer coordinates are the data window's own again** (issue #5).
  v1.4.4 stopped the garbage collector throwing `found bad pointer in Go heap`
  by removing the origin bias from `Slice.Base`, which made coordinates
  window-relative and moved the problem onto callers holding image coordinates.
  `Slice` now carries the window's minimum as `OriginX`/`OriginY` and
  `PixelAddr` subtracts it, so `Base` stays inside its allocation *and*
  `GetFloat32(x, 4096)` means row 4096 of the image. Code written against
  v1.4.2 needs no change; code adapted to v1.4.4's relative indexing does.

  Converting the internal paths found four defects, each invisible while every
  window started at (0, 0): the scanline reader and writer, the tiled reader and
  writer, and the multi-part scanline and tile packers all indexed relative to
  the window; `ReadRowHalf` and its siblings held two conventions in one
  function, the fast path treating `xStart` as window-relative and the
  per-pixel fallback as absolute, so which was right depended on the pixel
  type; and `RowAddr` named absolute column zero, seven pixels before the row's
  own data for a window at x=7.
- **Subsampled channels in multi-part parts were packed at the full width.** A
  channel with `XSampling` 2 stores every second column and contributes half as
  many samples per line; packing the full width made the chunk longer than the
  format says and put every channel after it at the wrong offset.
  `NewMultiPartWriter` now also refuses `YSampling` above 1, which removes whole
  rows and which the chunk layout cannot express, exactly as `ScanlineWriter`
  refuses it.
- **`DeepTiledWriter` could only write one resolution level.** Its offset table
  was sized for one level and indexed by `tileY*tilesX + tileX`, so every level
  after the first overwrote the first one's slots — while the reader had always
  derived the index per level. Both now share one derivation. It also captured
  the tile description at construction, so a caller asking for a mipmap through
  `SetTileDescription` got a single-level file and no indication.
- **HTJ2K was not implemented for tiles at all.** Both identifiers were missing
  from the tiled compression switch, so a tiled header declaring `htj2k256` or
  `htj2k32` failed at write time.
- **The chunk offset table was written only by `Close`.** A caller who never
  called it left a file whose table was all zeros: this library's reader
  recovered by scanning and the reference, which locates every chunk through
  the table, did not. The table is now written as soon as the last chunk the
  header promised has been written, so a complete image is complete either way
  and the two files are byte-identical.

### Added
- Deep parts in multi-part files: `MultiPartOutputFile.SetDeepFrameBuffer` and
  `WriteDeepPixels`, sharing the single-part chunk packing rather than
  repeating it, with the version field's deep flag set when any part is deep.
- `DeepImageState`, `Header.SetDeepImageState`/`DeepImageState` and
  `VerifyDeepImageState`. The attribute is a claim about the samples that
  nothing in the format checks, so a file declaring "tidy" over unsorted or
  overlapping samples is accepted everywhere and shows up much later as a
  subtly wrong composite; the verifier is what makes the claim falsifiable.
- `File.ChunkRange`, `ChunkRanges`, `NumChunks`, `ChunksForScanlines` and
  `ChunksForRegion`: a chunk's byte range without decompressing it, and the
  chunks a row range or a viewport touches. Composed with go-jpeg2000's packet
  index, a 32x32 viewport of a 128x128 HTJ2K image resolves to 1 of 16 chunks,
  674 of 6611 bytes, and then to individual packet ranges — before any pixel
  data is read.
- `HTJ2KDecompressPartial` and `HTJ2KDecodeOptions`, which expose the
  codestream's own resolution, region and quality-layer capabilities. Two of
  the three are refused by the codec today rather than honoured; the tests
  assert the refusal so this package does not appear to offer more than it
  delivers.
- `Slice.WithOrigin`, for a slice built over a bare buffer whose window does not
  start at the origin.

### Validation
The gate runs 234 checks with no failures, no skips and no measured gaps, from
209 at v1.4.4. New oracles were needed twice because oiiotool cannot do the job:
`scripts/exrpartdump` (oiiotool refuses subsampled channels outright) and
`scripts/exrdeeptiledump` (`--selectmip` does not compose with `--dumpdata` for
deep images, so every level but the first comes back empty). Three gap rows
turned out to name the wrong tool rather than an unmeasurable property —
`exrmaketiled` tiles a uint file, `exrmultipart -combine` assembles multi-level
multi-part fixtures, and generated mipmap levels can be checked against the
reference's own filter by properties that hold whatever filter each side chose.

The mutation manifest holds 31 entries across every codec, each killed by
something.

## [1.4.4] - 2026-08-19

Closing the read directions for tiled and multi-part files found a memory-safety
defect that had been present the whole time and could not be reached from any
test this repository had, because every one of them read back through the same
wrong arithmetic that wrote.

### Fixed
- **Reading or writing any image whose data window did not start at (0, 0)
  wrote outside the frame buffer.** `AllocateChannels` biased `Slice.Base` by
  the data window origin, so `PixelAddr` expected absolute image coordinates,
  while every reader and writer in the package addresses the frame buffer
  relative to the data window — `ScanlineWriter` computes `bufY := y - minY`,
  and the tiled path indexes each level from zero. For a window at (17, -9) the
  two disagreed by 1,118 bytes on a 64x48 half image, and because `Slice` writes
  through an unchecked `unsafe.Pointer` the disagreement did not raise an error:
  it corrupted the heap. Reading a reference-written ripmapped file with an
  offset window crashed the Go runtime outright with `marked free object in
  span`. A caller reading back through the same addressing saw correct values,
  which is why no round trip ever caught it, and why the smaller cases appeared
  to pass while silently writing out of bounds. The bias is gone; the pixel at
  the data window's minimum corner is buffer position (0, 0), as the rest of the
  package always assumed.

### Added
- The read direction is gated for tiled and multi-part files, so every storage
  type in the format is now checked in both directions. `scripts/exrtileread`
  and `scripts/exrmpread` read files the reference wrote and are compared
  against the reference's own reading of the same file — 36 tiled fixtures
  across three level modes, four codecs, exact and partial tile fits and an
  offset data window; and multi-part fixtures with scanline and tiled parts,
  compared channel by channel with every difference threshold pinned to zero.
  Nothing this library wrote takes part in either. The gate runs 209 checks, up
  from 157.
- `exr/framebuffer_origin_test.go` asserts that every address a frame buffer's
  slices can produce lies inside the buffer allocated for them, for windows with
  positive, negative and mixed origins — the invariant whose absence hid the
  defect above — and `scripts/mutation/mutations.json` gained an entry that
  reintroduces the bias and confirms the new tests catch it while the existing
  round trips stay green.

## [1.4.3] - 2026-08-19

Tiled, multi-part and deep images are now gated by the reference implementation
in the write direction, and deep images in both. Thirteen defects fell out of
the first runs. Every one of them produced a file the reference refuses to open or
reads wrongly, and every one was invisible to the existing suite, because a round
trip reads a file back with the same assumption that wrote it. The gate grew
from 44 checks to 157, and prints ten measured gaps on every run. Five new
entries in `scripts/mutation/mutations.json` reproduce these defects and
confirm the new checks die on them — for four of the five, the pre-existing
round-trip tests stay green, which is the false-assurance shape itself.

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

- **Every deep image this library wrote was unreadable outside it.** Deep
  scanline and deep tiled writing had never been read by anything but this
  library's own reader, which shared each of the defects below, so the round
  trip was green and the files were not OpenEXR:

  - the deep scanline chunk header was 20 bytes rather than 28 and the deep
    tile chunk header 32 rather than 40, both omitting the trailing
    `unpackedSizeOfSampleData`. Nothing else in a deep chunk says how many
    samples it holds, so OpenEXR could not size its buffer and answered "Some
    scanline chunks were missing or corrupted";
  - a single-part deep tiled file set the tiled bit *and* the deep bit in the
    version field. Those two are mutually exclusive — what makes such a file
    tiled is its `deeptile` type attribute — so the reference refused every
    deep tiled file at the header with "Invalid combination of version flags";
  - the sample data was stored pixel-major (all channels of pixel 0, then all
    channels of pixel 1). The format stores it one scanline at a time and,
    within a scanline, one channel at a time, every sample of every pixel for
    that channel before the next channel begins. Files read as scrambled;
  - the pixel offset table accumulated across the whole chunk or tile. The
    counts are cumulative along each scanline and restart on the next one;
  - a deep tile that hangs off the right or bottom edge of the data window is
    stored clipped, and the reader assumed a full tile;
  - a block that compression does not make smaller is stored raw, which is how
    the format distinguishes the two — there is no flag. The readers always
    tried to decompress, so every deep file the reference wrote came back as
    "corrupted ZIP data", and the writers always compressed.

- **Deep files could be written with ZIP and PIZ.** Only NONE, RLE and ZIPS are
  permitted for deep data; the reference rejects anything else on open with
  EXR_ERR_INVALID_ATTR "Invalid compression for deep data" (measured for ZIP,
  PIZ, B44 and both HTJ2K variants). `IsDeepCompressionSupported` said ZIP and
  PIZ were fine, and the PIZ path silently ZIP-compressed the data while the
  header claimed PIZ. Both deep writers now return `ErrDeepNotSupported`
  instead of producing a file no other reader will open.

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
- `scripts/validate.sh` gates deep images against the reference implementation,
  in both directions: `scripts/deepgen` writes deep scanline and deep tiled
  fixtures with 0 to 4 samples per pixel (including an entirely empty scanline
  and an entirely empty tile) for each permitted codec, and `oiiotool
  --dumpdata` is asked to read back every sample of every pixel, compared by
  `scripts/deepdiff.awk`; and this library is asked to read deep images the
  reference itself wrote, compared against the reference's own reading of them.
  A control confirms `oiiotool` round-trips its own deep output first, so a
  broken oracle stays distinguishable from a defect. 28 checks, taking the gate
  from 44 to 72.
- `exr/deep_wireformat_test.go` asserts the deep chunk layout byte for byte —
  header sizes and fields, the per-scanline sample count table, channel-major
  sample order, edge-tile clipping and the version flags — so the format is
  held even where `oiiotool` is not installed.

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
