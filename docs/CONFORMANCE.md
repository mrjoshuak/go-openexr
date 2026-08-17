# Conformance and Compatibility

This document records the rules this library follows when the specification,
the OpenEXR reference implementation, and real-world files disagree — and the
testing discipline that keeps us honest about it.

## The rules, in priority order

**1. Correctness wins.** We implement the format as specified. We do not
replicate a bug in another implementation to stay byte-identical with it. If
the reference implementation is lossy where the format is not — for example,
OpenImageIO quiets signalling NaNs on read while the half bit patterns in the
file are exact — we preserve what the file actually contains.

**2. Everything we write must be readable by conforming implementations.** Our
output is held to the specification strictly. There is no "our dialect".

**3. We read whatever we can make sense of.** Input is treated leniently. A
file that a conforming reader can recover, we recover too, even when it is
malformed. Being strict on output and lenient on input costs nothing and is
what makes a library usable against the files that actually exist.

**4. Leniency never invents data.** We recover what is present; we do not
guess at what is missing. When a file is unrecoverable we return an error —
never zeroes, never partial data presented as complete. Silence is the worst
failure mode, because a caller comparing against a zero-initialised buffer
cannot tell the difference between "read correctly" and "read nothing".

## What leniency means in practice

- **Chunk offset tables are rebuilt when unusable.** The table is written last,
  so an interrupted render or a caller who never closed the writer leaves it
  zeroed on otherwise intact data. We rescan the chunks and reconstruct it, as
  `reconstructLineOffsets` does in the reference. See `exr/reconstruct.go`.
  Scanline chunks are indexed by their own `y` coordinate rather than by file
  order, which additionally tolerates chunks stored out of order.
- **Truncated files yield their readable prefix.** Reconstruction keeps every
  chunk it could parse before the damage instead of failing the whole file.
- **Chunks that did not shrink are stored raw**, per `ImfOutputFile`, and
  detected on read by size alone. This is a specification rule, not a
  concession; a writer that ignores it produces files conforming readers
  silently misinterpret.

## What strictness means in practice

Output is verified against the reference implementation, not against ourselves.
`scripts/gen-conformance-testdata.sh` builds a corpus written by `oiiotool`,
with golden pixel values from that same implementation, and the generator
*fails* if a codec's fixture did not actually compress — otherwise the chunk
would be stored raw and the codec under test would never run.

## Why the tests are shaped the way they are

Every codec defect found in August 2026 — a missing predictor bias, an inverted
byte-reorder order, a flipped RLE control-byte convention, a mispositioned
wavelet pivot, a truncated Huffman symbol — shared one property: **it was
self-inverse.** Encode and decode deviated from the specification in exactly
the same way, so the library round-tripped its own files perfectly while being
unable to read or write anything produced by a conforming implementation.

A test of the form `assert decode(encode(x)) == x` cannot detect that class of
bug. Neither can a test comparing two implementations from this repository
against each other (SIMD against pure Go, batch against scalar), nor one
reading a fixture this library generated. All three validate self-consistency,
not correctness.

So, when adding or changing a codec:

- **Anchor to something outside this repository.** Compare against a file
  written by the reference implementation, against golden values it produced,
  or against a literal transcription of the reference algorithm included in the
  test for that purpose (see `compression/wavelet_spec_test.go` and
  `internal/predictor/spec_test.go`).
- **Best of all, anchor to the artifact's definition.** `AllHalfValues.exr` is
  defined to contain every 16-bit pattern at pixel `(x, y) = y*256 + x`. That
  needs no oracle at all and catches things a float32 comparison cannot, such
  as a signalling NaN being quieted.
- **Round-trip tests are still worth having** — they catch asymmetric mistakes
  cheaply — but they must never be the only assertion for a codec.
- **A test that reads a file and asserts nothing about the pixels is not a
  correctness test.** Do not downgrade a decode failure to `t.Logf`, and do not
  `t.Skip` a codec because it "may have compatibility issues". That comment was
  in this repository, it was correct, and it is why PIZ stayed broken.

## Regenerating the corpora

```bash
# Reference-written fixtures with golden pixel values (requires oiiotool)
./scripts/gen-conformance-testdata.sh

# The official ASWF openexr-images corpus (downloaded, not committed)
cd testdata && ./download.sh

# Digests for that corpus (requires the OpenImageIO Python module)
python3 scripts/gen-reference-goldens.py
```

The `openexr-images` tests skip when the corpus is absent, so a clean checkout
still tests fully against the committed conformance fixtures.
