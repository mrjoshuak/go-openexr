# Roadmap

The goal is complete OpenEXR support in pure Go, with every codec demonstrated
against the reference implementation rather than against ourselves.

## How anything gets marked done here

Issue #4 was reported as "float32 scanline reads return zeros". Chasing it found
that ZIP, ZIPS, RLE and PIZ were all non-interoperable in both directions, and
that the test suite could not see it because every codec test was symmetric: a
round trip, or one in-repo implementation against another, or a fixture this
library generated. A defect applied identically to the encoder and the decoder
is invisible to all three.

An audit then found 125 candidate false-assurance tests, 21 of them *proven*
unable to fail by mutation testing.

So an item below is done when:

1. The reference implementation reads what we write — bit-identical for a
   lossless codec, within a bound derived from the format for a lossy one.
2. We read reference-written files to the same standard.
3. Both run in `scripts/validate.sh`.
4. Any new test is shown to be able to fail, by mutating its subject and
   watching it die. `scripts/mutation/run.py` automates this.

Assertions anchor to the specification or to a reference-written fixture, never
to our own output.

## Why this order

Most of what follows is conformance hygiene. One item is not, and it sits first
because it is the only thing here that a wrapping format cannot do for you.

OpenEXR handles multi-resolution entirely at the file level: mipmap and ripmap
levels in tiled files, with the compressor treated as an opaque per-chunk unit
that decompresses whole or not at all. The compressor interface takes no
resolution or quality parameter — `dwaCompressionLevel` is the sole exception in
the format.

But an HTJ2K chunk is a JPEG 2000 codestream, and that codestream keeps every
capability the standard gives it: resolution levels, precinct-addressable
packets, quality layers. `HTJ2KBuildPacketIndex()` already locates packets as
byte ranges without copying. So a reader that knows the trick can pull a
viewport, at a chosen resolution, from an ordinary conforming HTJ2K EXR — no
mipmaps required, no proxy pyramid, and for a file in object storage, an HTTP
range request rather than a decode service.

This is an extension, not a fork. The files stay byte-identical to what any
other implementation writes and reads; other readers simply decompress the whole
chunk, exactly as the format specifies. Mipmapped input stays supported for
files written by tools that do not know the trick — but that path gives levels
only, not regions and not progressive quality, so it is a genuine fallback
rather than an equivalent.

Both halves of the capability live in go-jpeg2000 and are unfinished there:
precinct partitions are mis-read, and the HT encoder emits the cleanup pass only
so there is no quality progression to serve. Its roadmap now carries both at the
top. This one carries the API that would expose them.

## Now

### Codestream-level decode through the EXR API — surfaced, and blocked below

`HTJ2KDecompressPartial` and `HTJ2KDecodeOptions` expose the codestream's own
capabilities — resolution, region, quality layers — and document that they are
an extension beyond the reference compressor interface rather than a format
feature. Nothing written this way changes a file.

Two of the three cannot be honoured yet, and building this is what found out
why. In go-jpeg2000, `Config.DecodeArea` was declared, documented as "specifies
a region to decode", and read by nothing — a region request returned the whole
image. `Config.ReduceResolution` was correct for ordinary integer samples and
returned wavelet-domain values as floats for any codestream carrying an NLT
point transform, which an EXR HTJ2K chunk always does: dimensions right, samples
off by 175 on a ramp spanning 0 to 2. Both refuse rather than mislead as of
go-jpeg2000 v1.5.2, and this package's tests assert the refusal.

So a viewport resolves to byte ranges today — that is `File.ChunkRange` and the
packet index, which need none of this — but turning those bytes into fewer
pixels than the chunk holds does not work yet.

Done when a region or reduced-resolution decode of an HTJ2K chunk reads a
demonstrable subset of the chunk, and its samples match the same region, or a
downsample, of a full decode. It depends on the region-decode item in
go-jpeg2000's roadmap.

### ~~Expose the chunk offset table for indexed reads~~ — done, and it composes

`File.ChunkRange` gives a chunk's offset and length by reading its header
alone, `File.ChunkRanges` gives the whole table, and `ChunksForScanlines` and
`ChunksForRegion` turn a row range or a viewport at a chosen level into the
chunks that hold it. Nothing is decompressed.

The two indexes compose, which is the point: for a 128x128 image in 32x32 tiles
under HTJ2K, a 32x32 viewport resolves to 1 of 16 chunks and 674 of 6611 bytes,
and that chunk's codestream then yields 6 packets each with its own byte range,
through go-jpeg2000's packet index. A viewport becomes a set of byte ranges
before any pixel data is read.

Closing this also closed a hole it ran straight into: **HTJ2K was not
implemented for tiles at all**. Both identifiers were missing from the tiled
compression switch, so a tiled header declaring `htj2k256` or `htj2k32`
produced "compression not yet implemented" from the writer — the one
compression a tiled cloud workflow most wants. It is implemented on both sides
now and gated, half, float and mipmapped, with libOpenEXR reading every level
back.

### ~~Correct the tests that still deny HTJ2K works~~ — done

`TestHTJ2K_NotSupported` logged "Not supported (intentional limitation)" and
asserted nothing, and `TestCompliance_Summary` listed HTJ2K as "[ ] Not
Supported (requires CGO)". Both HTJ2K compressions are implemented, verified
bit-identical against the reference for half, float and uint at both block
sizes, and now over tiles as well — so those tests were not merely vacuous,
they told a reader the opposite of the truth.

`TestHTJ2KIsSupported` replaces them and dies under a mutation that swaps the
two chunk sizes.

### ~~`ScanlineWriter` truncates silently~~ — done

The chunk offset table can only be filled in once the offsets are known, so it
was written by `Close` alone. A caller who never called `Close` left a file
whose table was all zeros — and this library's own reader recovered by scanning
while the reference, which locates every chunk through the table, did not. The
file looked fine here and was unreadable everywhere else, which is why the
report read as "float32 scanline reads return zeros".

The table is now written as soon as the last chunk the header promised has been
written, so a complete image is complete whether or not `Close` follows; the two
files are byte-identical. `Close` still finalises a deliberately short one.

Worth recording: the first test written for this checked the table through
`File.Offsets`, which returns what the *reader* worked out — and the reader
reconstructs a table by scanning when the stored one is unusable. A mutation
restoring the old behaviour showed the test passing on a file no other
implementation could open. It reads the file's own bytes now.

### ~~`PIZChannel` subsampling in tiled and multi-part writers~~ — moot, measured

`TiledWriter` and `MultiPartOutputFile` omit the XSampling/YSampling divides the
scanline path performs when building a `PIZChannel`. The divides turn out to be
unreachable: the format forbids subsampled channels in a tiled image, and the
reference refuses to open one — `channel 'BY': x subsampling factor is not 1 (2)
for a tiled image`. This library used to write such a file happily; since
`Header.Validate` returns `ErrTiledSubsampling` it cannot, and the file is
illegal before the codec ever sees it.

`scripts/validate.sh` keeps both halves measured every run: that the reference
still rejects `scripts/testdata/tiled_subsampled_invalid.exr`, and that this
library still refuses to write another. The multi-part tiled path inherits the
same guard through `Validate`; multi-part *scanline* parts are unaffected, since
subsampling is legal there and the scanline path does divide.

## Next

### ~~Tiled and multi-resolution coverage~~ — done, both directions, levels included

The write direction is now gated. `scripts/tiledgen` writes 24 fixtures — one
level, mipmap and ripmap, both rounding modes, tile sizes that divide the image
and tile sizes that leave partial tiles, a tile larger than the image, a
non-origin data window, and nine codecs — and `scripts/exrtiledump`, linked
against libOpenEXR, reads every level of every one back. It found two defects on
its first run, both of which produced files the reference refuses to open (see
CHANGELOG). Controls run first: the reference's own `exrmaketiled` output must
satisfy the same expectation, and the comparator must fail when it is given a
mismatched pair.

The read direction is now gated too, and it found the worst defect in this
repository's history. exrmaketiled writes 36 fixtures — one level, mipmap and
ripmap, four codecs, tile sizes that divide the image and tile sizes that do
not, and a data window at (17, -9) — `scripts/exrtiledump` reads each with
libOpenEXR, `scripts/exrtileread` reads the same file with this library, and the
two dumps are compared sample by sample. Nothing this library wrote takes part.

Generated level *contents* are gated too, which this file previously called
unmeasurable — "nothing external can say what level 3 should contain". That is
true of the values and false of everything else. `exrmaketiled` generates levels
from the same source, and four things must hold whatever filter either side
chose: the two agree on which samples exist, level 0 is exact because it is the
source, the 1x1 level is exact because it is the image's mean and every
2x2-supported filter preserves it, and the per-level difference never grows with
depth. Measured across seven levels: 0, 0.120, 0.070, 0.026, 0.008, 0.002, 0. A
filter difference averages out like that; a wrong axis or a wrong scale does
not.

Deep tiled files are covered by the deep section below.

This is also the compatibility half of the strategy above: mipmapped output is
what readers that do not know the codestream trick will use, and mipmapped input
is how this library serves proxies from files other tools wrote.

### Window-absolute frame buffer coordinates

`Slice` addresses a frame buffer in window-relative coordinates: for a window
whose minimum is (17, -9), the pixel there is `(0, 0)`. The C++ convention, and
what this package documented before v1.4.4, is window-absolute — that pixel is
`(17, -9)` — achieved by biasing the base pointer so the minimum lands at
`buffer[0]`.

That bias is not expressible in Go. The biased pointer is outside its
allocation, and the collector rejects it with `found bad pointer in Go heap`,
intermittently, depending on where the offset happens to land (issue #5). v1.4.4
fixed it by removing the bias, which made the coordinates relative.

The better fix, and the one a downstream consumer asked for, is to carry the
origin as data: `OriginX`/`OriginY` on `Slice`, subtracted in `PixelAddr` and
`RowAddr`, `Base` left pointing at the allocation. That keeps the pointer valid
*and* the coordinates absolute, so no caller has to change.

It is not a seven-call-site change. Every internal path addresses the buffer
relative to the window — `ScanlineWriter` computes `bufY := y - minY`, the tiled
path indexes each level from zero, and the deep, multi-part, mipmap and
colour-transform paths do the same — so making the accessors absolute makes all
of them wrong for a non-zero-origin window. Attempted once: the reference-image
tests hung and `TestMultiPartTilesWrittenOutOfOrder` failed, and it was reverted
rather than shipped half-converted.

Done when every internal call site passes window-absolute coordinates, `Base` is
inside its allocation for every window, `go test -race ./...` is clean with
`checkptr` for non-zero-origin windows, and the gate's off-origin tiled fixtures
still read exactly.

### ~~Finish the false-assurance backlog~~ — every mutation is killed

`scripts/mutation/mutations.json` holds 31 mutations across every codec the
package implements — B44, DWA, HTJ2K, PIZ, PXR24, RLE, ZIP, huffman and half —
and the deep, tiled, multi-part, scanline and frame-buffer paths beside them.
Every one is killed by something: 23 by spec-anchored tests added for them, and
8 by tests that were already there. None survives everything.

The 16 that survive the *pre-existing* tests are the measurement, not a
complaint: each is a deviation a round trip cannot see, and each now dies
against an assertion anchored to the specification or to a reference-written
fixture.

Two things the harness caught about itself while this was finished, both worth
keeping. It reports an anchor that no longer matches the source rather than
silently testing nothing — two anchors had rotted under this session's own
edits. And a PXR24 mutation that edited the NaN branch survived a test using
only finite values, which is a mutation aimed at code the test never reaches;
retargeting it at the finite path is what made it meaningful.

### ~~Deep coverage~~ — done in both directions, levels and semantics included

Deep is now gated both ways: `scripts/deepgen` writes deep scanline and deep
tiled fixtures with 0 to 4 samples per pixel, including an entirely empty
scanline and an entirely empty tile, and `oiiotool --dumpdata` reads back every
sample of every pixel; and this library reads deep files the reference itself
wrote. It found seven defects, which together meant the reference rejected every
deep file this library produced and this library rejected every deep file the
reference produced — while `go test ./...` was green, because writer and reader
shared every one of them.

Deep mipmap and ripmap levels are written and gated. `DeepTiledWriter` sized its
offset table for one level and indexed it by `tileY*tilesX + tileX`, so every
level after the first overwrote the first one's slots — while the reader had
always derived the index per level, so the two disagreed the moment a second
level existed. Both now use one derivation. A second defect sat behind it: the
writer captured the tile description at construction, so a caller who asked for
a mipmap through `SetTileDescription` got a single-level file and no indication.
`scripts/exrdeeptiledump` reads all 6 mipmap levels and all 36 ripmap levels
back through libOpenEXR; oiiotool cannot, because `--selectmip` does not compose
with `--dumpdata` for deep images and every level but the first comes back
empty.

Deep parts in multi-part files are written and gated, which the section below
covers from the other side.

Deep sample semantics are asserted as far as they can be. `deepImageState` is a
typed attribute now — the reference reads back what this library writes — and
`VerifyDeepImageState` checks that the samples actually satisfy the claim, since
nothing in the format does: a file declaring "tidy" over unsorted or overlapping
samples is accepted by every reader and shows up much later as a composite that
is subtly wrong. Alpha premultiplication remains a convention this library
neither imposes nor checks, which is what the reference does too.

### ~~The multi-part cases the gate does not reach~~ — none remain

Multi-part files are now gated: `scripts/multipartgen` writes parts that differ
in data window, compression, channel layout and storage type, and the reference
reads each one back sample for sample. Five defects came out of it, all in
CHANGELOG.

The read direction is gated too: oiiotool writes multi-part fixtures — a
scanline pair differing in pixel type and channel layout, and a pair with tiled
parts — `scripts/exrmpread` reads them with this library into PFMs, and the
reference is asked for the same channel of the same part with every difference
threshold pinned to zero. The part count is checked as well, so a reader that
finds one part in a two-part file fails rather than passing on the part it did
find. Writing the fixture measured something on its own: the reference refuses
parts that disagree about the display window, which is the rule
`NewMultiPartWriter` enforces as `ErrConflictingAttributes`.

Ripmapped tiled parts are gated too: `scripts/mpripgen` writes a scanline master
beside a ripmapped tiled part, and `scripts/exrtiledump -part 1` — libOpenEXR
addressing one part of a multi-part file — reads all 49 of its independent x and
y levels, 48387 samples, exactly. oiiotool cannot serve as the oracle there,
because `--selectmip` addresses levels by a single index and a ripmap has none.

Multi-level parts are gated in the read direction too, which this file called
impossible: "oiiotool writes a one-level file for `-o:mipmap=1` and drops levels
on `--siappend`, so no reference-written multi-level multi-part fixture could be
produced at all". It named the wrong tool. `exrmaketiled` generates the levels
and `exrmultipart -combine` assembles the file, both from OpenEXR itself, so the
fixture is reference-written end to end — and this library reads all 7 mipmap
levels and all 49 ripmap levels of it exactly.

Subsampled channels are handled and gated. `buildScanlineData` packed the full
width for every channel, so a channel with `XSampling` 2 — which stores every
second column and contributes half as many samples per line — made the chunk
longer than the format says and put every channel after it at the wrong offset.
It now packs each channel at its own width, and `NewMultiPartWriter` refuses
`YSampling` above 1, which removes whole rows and which this library's chunk
layout cannot express, exactly as `ScanlineWriter` refuses it.

The oracle there is `scripts/exrpartdump`, linked against libOpenEXR, because
oiiotool cannot read subsampled channels at all: it refuses the file with
"Subsampled channels are not supported" and exposes only the unsubsampled parts
of a multi-part file containing one — which would have measured nothing while
appearing to pass. Measured: 6912 samples across 1x, 2x and 4x channels in one
part, all exact.

Deep parts are handled and gated. `MultiPartOutputFile` exposed only
`WritePixels` and `WriteTile`, so this library could not write one at all — a
gap in the writer rather than in the fixtures. It has `SetDeepFrameBuffer` and
`WriteDeepPixels` now, sharing the single-part writer's chunk packing rather
than repeating it, and the multi-part version field sets the deep flag when any
part is deep, which it previously hardcoded false.

The first attempt produced a file the reference refused for *every* part, with
"Some scanline chunks were missing or corrupted" naming neither the part nor the
cause. The chunks were well formed and entirely empty: `getSortedChannels` reads
a field the packer had not been given, returned nil, and the sample loop wrote
nothing — packed and unpacked sizes both zero. Comparing the bytes against a
file `exrmultipart` assembled from the same data is what found it. The packing
now refuses a chunk with no channels rather than producing one.


## Later

### Lossy codec tolerances derived rather than measured

B44, B44A, DWAA and DWAB are gated with bounds stated in the script header. The
PXR24 bound is derived from the format (24 of 32 float bits); the B44 and DWA
bounds are stated but should be derived from their quantisation rather than from
what we happen to observe, so that a regression that stays inside the observed
error is still caught.

### Attribute and metadata conformance

Header attributes round-trip, but the reference has never been asked whether it
agrees with our serialisation of the less common ones — chromaticities,
environment maps, deep image state, ID manifests.

### Restore a vectorised B44 pack

The SSE2 inner loop was removed in v1.4.2 because it computed in 16-bit lanes
what the reference computes in `int`, wrapping for wide-range blocks and
producing non-conforming output on amd64 only. amd64 now uses the same scalar
path as arm64, which costs roughly the 23% B44 encode win the assembly bought.

Done when a vector implementation widens to 32-bit lanes, is byte-identical to
`refPackB44` over the spec-vector corpus, and is verified by running the suite
under that GOARCH rather than by inspection.

### Performance

B44 and DWA were profiled before the correctness work; those numbers are stale.
Worth re-measuring against the C++ implementation once the feature set is
complete.

## Dependency note

HTJ2K support requires go-jpeg2000 v1.5.0, which this module pins. Earlier versions cannot emit
a codestream the reference accepts, for two reasons that were measured:
`EncodeHalf`/`EncodeFloat` ignored `Options.HighThroughput` so Rsiz bit 14 was
never set, and the NLT segment was written short. `scripts/validate.sh` excuses
the six HTJ2K rows only for a build resolved at exactly v1.3.0, and reports a
row that passes while excused as a closed gap — the excuse cannot outlive the
defect.
