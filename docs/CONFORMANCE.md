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

Codec bugs in a format library share a characteristic shape: **they are
self-inverse.** When an encoder and its decoder deviate from the specification
in the same way, the pair round-trips perfectly while agreeing with nobody else.
A missing predictor bias, an inverted byte-reorder order, a flipped RLE
control-byte convention, a mispositioned wavelet pivot, a truncated Huffman
symbol — every one of these is invisible to a test that only compares the
library against itself.

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
  `t.Skip` a codec because it "may have compatibility issues" — a skip like that
  is a bug report worth acting on, not a workaround.

## Structure, not only samples

The same argument applies above the pixel level. A multi-part file that this
library writes and reads back can be perfectly self-consistent and still be one
no other implementation will open: the version field can claim something the
part headers contradict, the parts can disagree about an attribute the format
requires them to share, and a chunk offset table can be in the order the caller
wrote its chunks rather than the order a reader looks them up in. None of that
changes a single sample, and none of it is visible to a round trip.

`scripts/multipartgen` therefore writes multi-part files whose parts disagree
on purpose — in data window, compression, channel layout and storage type —
and `scripts/validate.sh` asks the reference implementation for each part's
name, storage type, compression, data window, channel list and chunk count
before it compares any pixels. The samples are carried beside the fixture as
plain PFMs, a format with no EXR code near it, so what the comparison anchors
to was never produced by the code under test.

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
