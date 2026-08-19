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

## Now

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

### Tiled and multi-resolution write coverage

`validate.sh` writes scanline images only. `exrmaketiled`-style mipmap and
ripmap levels, and tiled files generally, are untested against the reference.

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
