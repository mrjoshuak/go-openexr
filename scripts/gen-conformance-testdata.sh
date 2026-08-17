#!/usr/bin/env bash
#
# Regenerates the external-ground-truth conformance corpus in
# exr/testdata/conformance/.
#
# Every .exr in that directory is written by the OpenEXR reference
# implementation (via OpenImageIO's oiiotool), and every .golden file holds the
# pixel values that same implementation reads back from it. Nothing here is
# produced by go-openexr. That is the entire point: a fixture this library
# generated would encode any bug this library has as the expected answer, which
# is how an inverted RLE convention and a missing predictor bias once survived a
# fully green test suite.
#
# One golden per (pattern, pixel type) is shared by every compression of that
# combination, since they must all decode to identical pixels. The script
# verifies that before writing the golden.
#
# Requires: oiiotool (brew install openimageio).
# Run from the repository root.

set -euo pipefail

OUT=exr/testdata/conformance
rm -rf "$OUT"
mkdir -p "$OUT"

if ! command -v oiiotool >/dev/null 2>&1; then
	echo "oiiotool not found; install OpenImageIO to regenerate the corpus" >&2
	exit 1
fi

# A smooth 4-corner gradient. Every pixel differs from its neighbours, so a
# stride or offset error cannot hide the way it can in constant-colour data,
# and the data still compresses, exercising the real compressed path.
GRADIENT="fill:topleft=0,0,0,1:topright=1,0,0.25,1:bottomleft=0,1,0.5,1:bottomright=1,1,0.75,1"

# Uniform noise is incompressible, so every codec's output grows rather than
# shrinks. That forces OpenEXR's "store the chunk raw instead" rule, which is a
# distinct code path in both the reader and the writer.
NOISE="noise:type=uniform:min=0:max=1:seed=7"

COMPRESSIONS="none rle zip zips piz"

# emit_group writes one .exr per compression plus a single shared .golden.
#
# $5 says whether the payload is expected to compress. For the gradient groups
# it is "shrink", and the script fails if a codec's file is not meaningfully
# smaller than the uncompressed one — otherwise the reference implementation
# would have stored the chunk raw, the codec would never run during the test,
# and the fixture would silently assert nothing. That trap is what let PIZ stay
# broken: at small sizes its fixtures were all stored raw.
emit_group() {
	local group=$1 pattern=$2 depth=$3 dims=$4 expect=$5
	local first="" name none_size this_size

	for c in $COMPRESSIONS; do
		name="${group}_${c}"
		oiiotool --pattern "$pattern" "$dims" 4 \
			-d "$depth" --compression "$c" \
			-o "$OUT/$name.exr"

		# Ground truth: the reference implementation reading its own file.
		# The "channel list:" line records the order the values are printed in,
		# so the test never has to assume it.
		{
			oiiotool --info -v "$OUT/$name.exr" | grep "channel list:"
			oiiotool --dumpdata "$OUT/$name.exr" | tail -n +2
		} >"$OUT/$name.tmp"

		if [ -z "$first" ]; then
			first="$OUT/$name.tmp"
		elif ! cmp -s "$first" "$OUT/$name.tmp"; then
			echo "FATAL: $name decodes differently from ${group}_none" >&2
			exit 1
		fi
	done

	if [ "$expect" = shrink ]; then
		none_size=$(wc -c <"$OUT/${group}_none.exr")
		for c in $COMPRESSIONS; do
			[ "$c" = none ] && continue
			this_size=$(wc -c <"$OUT/${group}_${c}.exr")
			if [ "$this_size" -ge "$none_size" ]; then
				echo "FATAL: ${group}_${c}.exr ($this_size b) did not shrink below" \
					"${group}_none.exr ($none_size b); the chunk was stored raw and" \
					"the $c codec would never run. Use a larger image." >&2
				exit 1
			fi
		done
	fi

	mv "$first" "$OUT/$group.golden"
	rm -f "$OUT"/${group}_*.tmp
	echo "  $group ($dims $depth, $expect)"
}

# 71x40 is deliberately awkward: taller than ZIP's 16-scanline chunk and PIZ's
# 32-scanline chunk so files span several chunks, and a multiple of neither, so
# the final chunk is partial. It is also large enough that a 32-bit gradient
# genuinely compresses.
echo "generating gradient fixtures (codecs must actually compress)"
emit_group grad_half "$GRADIENT" half 71x40 shrink
emit_group grad_float "$GRADIENT" float 71x40 shrink
emit_group grad_uint "$GRADIENT" uint32 71x40 shrink

# Small on purpose: the store-raw path triggers regardless of size, so there is
# no reason to carry large incompressible fixtures in the repository.
echo "generating incompressible fixtures (exercise the store-raw path)"
emit_group noise_half "$NOISE" half 21x19 grow
emit_group noise_float "$NOISE" float 21x19 grow

# emit_lossy writes one DWA fixture and its own golden.
#
# DWA cannot share a group's golden the way the lossless codecs do: the values
# the reference implementation reads back out of a DWA file are not the values
# it read out of the uncompressed one. So each fixture carries the reference's
# own view of itself, and the test asserts against that rather than against the
# original pattern. Anything else would be measuring DWA's loss instead of this
# library's agreement with the reference.
emit_lossy() {
	local name=$1 pattern=$2 depth=$3 dims=$4 comp=$5 chnames=$6
	local none_size this_size

	oiiotool --pattern "$pattern" "$dims" 4 \
		-d "$depth" --chnames "$chnames" --compression "$comp" \
		-o "$OUT/$name.exr"

	{
		oiiotool --info -v "$OUT/$name.exr" | grep "channel list:"
		oiiotool --dumpdata "$OUT/$name.exr" | tail -n +2
	} >"$OUT/$name.golden"

	# Same trap as above: a fixture that did not shrink was stored raw and the
	# codec under test would never run.
	oiiotool --pattern "$pattern" "$dims" 4 \
		-d "$depth" --chnames "$chnames" --compression none \
		-o "$OUT/$name.none.tmp.exr"
	none_size=$(wc -c <"$OUT/$name.none.tmp.exr")
	this_size=$(wc -c <"$OUT/$name.exr")
	rm -f "$OUT/$name.none.tmp.exr"
	if [ "$this_size" -ge "$none_size" ]; then
		echo "FATAL: $name.exr ($this_size b) did not shrink below uncompressed" \
			"($none_size b); the chunk was stored raw and DWA would never run." >&2
		exit 1
	fi
	echo "  $name ($dims $depth $comp $chnames)"
}

# 35x40 keeps the goldens small while still being awkward for DWA: 35 columns
# leave a partial 8x8 block on the right, 40 rows are five full block rows, and
# 40 scanlines span two DWAA chunks (32 + 8) but only one DWAB chunk.
#
# The channel sets cover all three of DWA's schemes. R,G,B,A puts R, G and B
# through the colour space conversion and A through the lossless RLE path;
# R,G,B,Z leaves Z to the lossless deflate path, since no rule names it.
echo "generating DWA fixtures (lossy: each carries its own golden)"
emit_lossy grad_half_dwaa "$GRADIENT" half 35x40 dwaa R,G,B,A
emit_lossy grad_half_dwab "$GRADIENT" half 35x40 dwab R,G,B,A
emit_lossy grad_float_dwaa "$GRADIENT" float 35x40 dwaa R,G,B,A
emit_lossy grad_float_dwab "$GRADIENT" float 35x40 dwab R,G,B,A
emit_lossy gradz_half_dwaa "$GRADIENT" half 35x40 dwaa R,G,B,Z
emit_lossy noise_half_dwaa "$NOISE" half 35x40 dwaa R,G,B,A

echo "done: $OUT"
du -sh "$OUT"
