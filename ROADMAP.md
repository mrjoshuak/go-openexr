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

### Expose codestream-level decode through the EXR API

`HTJ2KDecompress(src, expectedSize, channels)` takes no options, which matches
the reference compressor interface exactly. go-jpeg2000's `Config` underneath it
carries `ReduceResolution`, `DecodeArea` and `QualityLayers`, and none of the
three is reachable from this package.

Adding a variant that accepts them is an extension beyond what the reference
offers, and worth marking as such in its documentation so nobody mistakes it for
a format feature.

Done when a caller can decode an HTJ2K chunk at a chosen resolution and region
without decompressing the whole chunk, when the bytes read are demonstrably a
subset of the chunk rather than the whole of it, and when the result matches a
full decode downsampled to the same resolution.

### Correct the tests that still deny HTJ2K works

`exr.TestHTJ2K_NotSupported` and `TestCompliance_Summary` state that HTJ2K is
unsupported and assert nothing — they log. Both HTJ2K compressions are now
verified bit-identical against the reference for half, float and uint at both
block sizes, so these tests are not merely vacuous, they are wrong.

Done when they assert the current behaviour and die under mutation.

### `ScanlineWriter` truncates silently

If `Close` is never called the chunk offset table is never written and the file
is quietly truncated. This is what made issue #4 look like a read bug and cost
the reporter real time.

Done when a missing `Close` is either impossible or produces an error, and a
test proves the file is not silently short.

### `PIZChannel` subsampling in tiled and multi-part writers

`TiledWriter` and `MultiPartOutputFile` omit the XSampling/YSampling divides the
scanline path performs. Latent — it only bites subsampled channels — but it is
the same class of geometry defect that made go-jpeg2000's odd-width images
non-conformant, and it is unexercised.

Done when subsampled channels round-trip through the reference in tiled and
multi-part files.

## Next

### Tiled and multi-resolution write coverage

`validate.sh` writes scanline images only. `exrmaketiled`-style mipmap and
ripmap levels, and tiled files generally, are untested against the reference.

This is also the compatibility half of the strategy above: mipmapped output is
what readers that do not know the codestream trick will use, and mipmapped input
is how this library serves proxies from files other tools wrote. Both directions
want covering.

### Finish the false-assurance backlog

The audit's 125 candidates are partly addressed: 15 mutations now die against
spec-anchored tests that previously survived. The remainder still rest on round
trips or self-referential comparisons. The ZIP, PIZ, B44, DWA, HTJ2K, huffman
and half test files are where they concentrate.

Done when every mutation in `scripts/mutation/mutations.json` is killed, and the
manifest has grown to cover each codec's core invariants.

### Deep and multi-part coverage against the reference

Deep scanline and deep tiled images, and multi-part files, are implemented but
have never been read by the reference implementation. The write side is
unverified in exactly the way the scanline codecs were before v1.4.0.

Done when the gate covers deep and multi-part writes the way it covers the 36
pixel-type by compression combinations.


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
