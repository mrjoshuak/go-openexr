#!/usr/bin/env bash
#
# validate.sh — the gate this repository has to pass.
#
# It runs three kinds of check, and the last two are the ones that matter:
#
#   1. The project's own standards: it builds, it is formatted, it vets clean,
#      staticcheck is quiet, the whole suite passes, and it passes again under
#      the race detector.
#
#   2. An external oracle. scripts/interopgen writes every (pixel type x
#      compression) combination this library supports, and the OpenEXR
#      reference implementation — via oiiotool, which is not this code — is
#      asked whether each compressed file holds the same image as its
#      uncompressed twin. A defect applied identically to the encoder and the
#      decoder is invisible to a round-trip test and fails here.
#
#   3. The same oracle on multi-part files, whose parts may each have their own
#      data window, compression, channel list and storage type.
#      scripts/multipartgen writes them together with the samples it intended,
#      as plain PFMs, and the reference is asked part by part and channel by
#      channel whether the file holds them. Section 5 below says what that
#      covers and what it does not.
#   4. The same oracle for deep images, sample by sample: scripts/deepgen
#      writes deep scanline and deep tiled fixtures with a varying number of
#      samples per pixel, the reference is asked to read every sample of every
#      pixel back, and this library is asked to read deep images the reference
#      itself wrote. See the section's own header, below, for what that does
#      and does not assert. Both deep writers were non-interoperable in every
#      one of their parts while the suite was green.
#
# Every combination interopgen writes is declared "exact" or "lossy" in its
# manifest, from the format specification and never from a measurement:
#
#   exact  the reference must read the compressed file as bit-identical to the
#          uncompressed twin. No tolerance, no percentage of allowed outliers.
#   lossy  the reference must read the file at all, and every sample must be
#          within the tolerance derived below for that codec.
#
# TOLERANCES. These are upper bounds derived from each codec's specified
# precision. They are deliberately not fitted to what this library currently
# produces — the measured maximum is printed next to the bound on every run, so
# a regression that stays under the bound is still visible, and a combination
# that exceeds the bound is reported as a failure with both numbers.
#
# interopgen's fixture is a smooth gradient whose samples lie in [0,1], so a
# relative bound and an absolute bound are the same number here.
#
#   pxr24, FLOAT      2^-15 = 3.0517578125e-05
#       PXR24 keeps 24 of a float's 32 bits: sign, all 8 exponent bits and the
#       top 15 mantissa bits, dropping the low 8. A 15-bit explicit significand
#       spaces neighbouring values 2^-15 apart relative to the value, which is
#       the guarantee the format makes. (This library's encoder rounds to
#       nearest rather than truncating, so it is really held to 2^-16 — that
#       tighter bound is asserted per sample in
#       exr/pxr24_conformance_test.go. The gate uses the format's bound so it
#       does not depend on an encoder's choice of rounding.) Denormals and
#       values within one ulp of FLT_MAX are outside the bound and outside the
#       fixture.
#
#   b44 / b44a, HALF  2^-5 = 3.125e-02
#       B44 codes each 4x4 block of halves as one base value plus 6-bit
#       differences at a shared shift, choosing the smallest shift that fits.
#       Reconstruction is therefore off by at most half a step, i.e. the
#       block's code range divided by 62, measured in half codes; one half code
#       is one half ulp, at most 2^-10 of the block's largest value. For a
#       block spanning at most one binade that is 2^-5 of the block maximum,
#       and no block maximum here exceeds full scale. Blocks that span k
#       binades trade precision at the dark end proportionally, so this is a
#       bound for smooth content such as the fixture, not a universal one:
#       B44's guarantee is relative to each block's own maximum.
#
#   dwaa / dwab, HALF and FLOAT   1.0e-01
#       At the default dwaCompressionLevel of 45 the quantiser's base error is
#       45/100000 = 4.5e-04, scaled per DCT coefficient by the JPEG luminance
#       table normalised by its smallest entry (see dwaJpegQuantY in
#       compression/dwa_encode.go). Those tolerances sum to 4.5e-04 * 3688/10 =
#       0.166 over an 8x8 block, and the orthonormal inverse DCT's basis peak
#       is 1/4, so a pixel can move by at most 0.0415 in the domain DWA codes
#       in. That domain is non-linear, with a slope of at most about 2.2
#       against linear at full scale, giving 0.091. Rounded up: 0.1.
#
# HTJ2K is lossless for every pixel type, so its six combinations are declared
# exact and held to the same bit-identical standard as zip or piz. They are run
# through exactly the same diff as every other row.
#
# HISTORICAL, retained as the reason the excuse exists. Until go-jpeg2000
# v1.4.0 these six rows could not pass: this repository wrote them correctly but
# v1.3.0 could not emit a codestream the reference would accept, for two reasons
# that were both measured:
#
#   * v1.3.0's EncodeHalf and EncodeFloat ignore Options.HighThroughput, so
#     Rsiz bit 14 is never set and OpenJPH stops at
#     "Rsiz bit 14 is not set (this is not a JPH file)" (ojph_params.cpp:867).
#   * v1.3.0 writes the NLT segment with a one-byte Cnlt and Lnlt = 5. ISO/IEC
#     15444-2 A.3.10 makes Cnlt sixteen bits and Lnlt 6, and OpenJPH rejects
#     anything else with "Unsupported NLT type" (ojph_params.cpp:2256).
#
# Both are fixed in go-jpeg2000 v1.4.0, which this module now requires, so all
# six rows are gated exactly like zip or piz. The excuse survives only for a
# build resolved at exactly v1.3.0 with no replacement, and a row that passes
# while excused is reported as a closed gap — the excuse cannot outlive the
# defect.
#
# Usage:  bash scripts/validate.sh
#         STRICT=1 bash scripts/validate.sh   # treat skips as failures
#
# Exits non-zero if any check fails. Requires Go; oiiotool (OpenImageIO) and
# staticcheck are used when present and reported as skipped when not.

set -uo pipefail

STRICT=${STRICT:-0}
REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$REPO" || exit 1

WORK=$(mktemp -d "${TMPDIR:-/tmp}/go-openexr-validate.XXXXXX")
trap 'rm -rf "$WORK"' EXIT

failures=0
skips=0
checked=0

pass() {
	printf '  ok   - %s\n' "$*"
	checked=$((checked + 1))
}
fail() {
	printf '  FAIL - %s\n' "$*"
	failures=$((failures + 1))
	checked=$((checked + 1))
}
skip() {
	printf '  SKIP - %s\n' "$*"
	skips=$((skips + 1))
	if [ "$STRICT" = "1" ]; then
		failures=$((failures + 1))
	fi
}
note() { printf '  ..   - %s\n' "$*"; }
section() { printf '\n=== %s ===\n' "$*"; }

echo "go-openexr validation gate"
echo "repo: $REPO"
echo "go:   $(go version 2>/dev/null || echo 'not installed')"
echo "date: $(date)"

# ---------------------------------------------------------------------------
# 1. Build and static analysis.
# ---------------------------------------------------------------------------
section "build and static analysis"

build_ok=1
if out=$(go build ./... 2>&1) && [ -z "$out" ]; then
	pass "go build ./..."
else
	fail "go build ./...
$out"
	build_ok=0
fi

if unformatted=$(gofmt -l . 2>/dev/null) && [ -z "$unformatted" ]; then
	pass "gofmt -l . (no files need formatting)"
else
	fail "gofmt: $(echo "$unformatted" | tr '\n' ' ')"
fi

if out=$(go vet ./... 2>&1) && [ -z "$out" ]; then
	pass "go vet ./..."
else
	fail "go vet ./...
$out"
fi

if command -v staticcheck >/dev/null 2>&1; then
	if out=$(staticcheck ./... 2>&1) && [ -z "$out" ]; then
		pass "staticcheck ./..."
	else
		fail "staticcheck ./...
$(echo "$out" | head -20)"
	fi
else
	skip "staticcheck not installed (go install honnef.co/go/tools/cmd/staticcheck@latest)"
fi

# ---------------------------------------------------------------------------
# 2. The test suite, twice: plain, and under the race detector.
# ---------------------------------------------------------------------------
section "test suite"

if [ "$build_ok" = "0" ]; then
	skip "go test ./... (build failed)"
	skip "go test -race ./... (build failed)"
else
	if go test ./... >"$WORK/test.log" 2>&1; then
		pass "go test ./... ($(grep -c '^ok' "$WORK/test.log") packages)"
	else
		fail "go test ./..."
		grep -E '^(---|FAIL|\s+---)' "$WORK/test.log" | head -20
		cp "$WORK/test.log" /tmp/validate_test.log 2>/dev/null &&
			note "full log: /tmp/validate_test.log"
	fi

	if go test -race ./... >"$WORK/race.log" 2>&1; then
		pass "go test -race ./... ($(grep -c '^ok' "$WORK/race.log") packages)"
	else
		fail "go test -race ./..."
		grep -E '^(---|FAIL|WARNING|\s+---)' "$WORK/race.log" | head -20
		cp "$WORK/race.log" /tmp/validate_race.log 2>/dev/null &&
			note "full log: /tmp/validate_race.log"
	fi

	# The other architecture. This package carries per-architecture assembly for
	# B44, so a defect can live in one build and not the other: an SSE2 pack
	# routine computed in 16-bit lanes what the reference computes in int, and
	# produced non-conforming output on amd64 only. Every gate run had been on
	# arm64, so CI found it rather than this script. Running the suite for the
	# other GOARCH here costs a few seconds and closes that hole.
	OTHER_ARCH=amd64
	[ "$(go env GOARCH)" = "amd64" ] && OTHER_ARCH=arm64
	if GOARCH=$OTHER_ARCH go test ./... >"$WORK/cross.log" 2>&1; then
		pass "go test ./... under GOARCH=$OTHER_ARCH ($(grep -c '^ok' "$WORK/cross.log") packages)"
	elif grep -qiE "exec format error|cannot execute|bad CPU type" "$WORK/cross.log"; then
		skip "GOARCH=$OTHER_ARCH: this host cannot run those binaries; CI covers it"
	else
		fail "go test ./... under GOARCH=$OTHER_ARCH"
		grep -E '^(---|FAIL|\s+---)' "$WORK/cross.log" | head -20
	fi
fi

# ---------------------------------------------------------------------------
# 3. The conformance tests, counted. These compare against files the reference
#    implementation wrote; a corpus that has gone missing makes them all skip,
#    which a green suite would not otherwise show.
# ---------------------------------------------------------------------------
section "conformance assertions (read direction)"

CONFORMANCE_FLOOR=30

if [ "$build_ok" = "0" ]; then
	skip "conformance count (build failed)"
else
	conf=$(go test ./exr/ -run 'TestConformance|TestReferenceImages|TestAllHalfValues' -v 2>&1)
	ran=$(printf '%s\n' "$conf" | grep -cE '^\s*--- PASS')
	skipped=$(printf '%s\n' "$conf" | grep -cE '^\s*--- SKIP')
	if printf '%s\n' "$conf" | grep -qE '^\s*--- FAIL'; then
		fail "conformance tests"
		printf '%s\n' "$conf" | grep -E '^\s*--- FAIL' | head -10
	elif [ "$ran" -lt "$CONFORMANCE_FLOOR" ]; then
		fail "only $ran conformance assertions ran (floor is $CONFORMANCE_FLOOR); is testdata/conformance missing?"
	else
		pass "conformance: $ran assertions passed, $skipped skipped (floor $CONFORMANCE_FLOOR)"
	fi
	if [ -d exr/testdata/conformance ]; then
		n=$(find exr/testdata/conformance -name '*.exr' | wc -l | tr -d ' ')
		note "corpus: $n files written by the reference implementation"
	fi

	# ---- what Open fetches ------------------------------------------------
	#
	# The byte-range path is worth nothing if opening the file reads all of it,
	# and until v1.4.8 it did: the header size was computed as size-8, so Open
	# pulled the whole file into memory and then refused anything over 64 MiB
	# outright — an ordinary 4096x4096 float frame. Measured on v1.4.7:
	# 57,213,503 of 57,213,503 bytes read at open, after which ReadRegion's 2%
	# was pure addition.
	#
	# The checks below pin all three: a prefix rather than the file, growth
	# when one prefix is not enough, and a file past the header cap opening at
	# all.
	op=$(go test ./exr/ -run 'TestOpenFetchesOnlyTheFrontOfTheFile|TestOpenGrowsThePrefixWhenTheOffsetTableIsLarge|TestOpenAcceptsAFileLargerThanTheHeaderCap' -v 2>&1)
	opran=$(printf '%s\n' "$op" | grep -cE '^\s*--- PASS')
	if printf '%s\n' "$op" | grep -qE '^\s*--- FAIL'; then
		fail "open fetches a prefix: $(printf '%s\n' "$op" | grep -E '^\s*--- FAIL' | head -1)"
	elif [ "$opran" -lt 3 ]; then
		fail "open fetches a prefix: only $opran assertions ran"
	else
		measured=$(printf '%s\n' "$op" | sed -n 's/.*\(Open read [0-9]* of [0-9]* bytes.*\)$/\1/p' | head -1)
		pass "open fetches a prefix, not the file (${measured:-measured}), grows it when the offset table is larger, and opens a file past the 64 MiB header cap"
	fi

	# ---- frame buffers that do not cover the window (issue #7) -------------
	#
	# This library addresses a frame buffer in the data window's own
	# coordinates. A buffer allocated for a window at the origin against a file
	# whose window is elsewhere therefore does not cover the pixels being read,
	# and until v1.4.7 that wrote past the end of every plane and returned nil
	# — measured at 30 float32 words past each of four planes. The tests below
	# check both halves: a covering buffer reads exactly, including a
	# band-shaped one, and a buffer that cannot hold the read is refused with
	# nothing written past it.
	#
	# There is no external oracle for this one and it is not pretending
	# otherwise: libOpenEXR biases the base pointer instead, so it has no
	# equivalent API to disagree with. What the reference does gate is the
	# offset window itself, in the tiled and multi-part read sections below.
	wr=$(go test ./exr/ -run 'TestReadThroughAnOffsetWindow|TestFrameBufferThatCannotHoldTheReadIsRefused|TestWriteThroughAFrameBufferThatIsTooSmallIsRefused|TestTiledReadRefusesAFrameBufferThatCannotHoldATile' -v 2>&1)
	wrran=$(printf '%s\n' "$wr" | grep -cE '^\s*--- PASS')
	if printf '%s\n' "$wr" | grep -qE '^\s*--- FAIL'; then
		fail "offset-window frame buffers: $(printf '%s\n' "$wr" | grep -E '^\s*--- FAIL' | head -1)"
	elif [ "$wrran" -lt 6 ]; then
		fail "offset-window frame buffers: only $wrran assertions ran; the guard is not being exercised"
	else
		pass "offset-window frame buffers: $wrran assertions — a covering buffer reads exactly, a short one is refused and writes nothing past its planes"
	fi
fi

# ---------------------------------------------------------------------------
# 4. The external oracle: every combination, checked by the reference
#    implementation rather than by this library.
# ---------------------------------------------------------------------------
section "external oracle (write direction, all pixel type x compression)"

# tolerance <type> <codec> — the documented bound for a lossy combination,
# empty when the combination has no derived bound (which is itself a failure:
# nothing lossy gates on a number nobody wrote down).
tolerance() {
	case "$2:$1" in
	pxr24:float) echo "3.0517578125e-05" ;;
	b44:half | b44a:half) echo "3.125e-02" ;;
	dwaa:half | dwab:half | dwaa:float | dwab:float) echo "1.0e-01" ;;
	*) echo "" ;;
	esac
}

# le <a> <b> — numeric a <= b, tolerating exponent notation.
le() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a + 0 <= b + 0) }'; }

if ! command -v oiiotool >/dev/null 2>&1; then
	skip "oiiotool not installed; the reference implementation cannot be consulted (brew install openimageio)"
elif [ "$build_ok" = "0" ]; then
	skip "external oracle (build failed)"
else
	FIX="$WORK/fixtures"
	mkdir -p "$FIX"
	if ! go run ./scripts/interopgen "$FIX" >"$WORK/interopgen.log" 2>&1; then
		fail "scripts/interopgen could not write the fixtures"
		cat "$WORK/interopgen.log"
	else
		note "$(tail -1 "$WORK/interopgen.log") into $FIX"
		note "oiiotool: $(oiiotool --version 2>/dev/null)"
		note "exact = bit-identical to the uncompressed twin, no tolerance"
		note "lossy = decodes, and max error <= the bound derived in this script's header"

		# The HTJ2K rows are excused only while go-jpeg2000 is pinned at exactly
		# v1.3.0 with no replacement; see this script's header for the two
		# v1.3.0 defects that make the reference refuse those codestreams.
		j2kmod=$(go list -m github.com/mrjoshuak/go-jpeg2000 2>/dev/null)
		case "$j2kmod" in
		*"=>"*) htj2k_excused=0 ;;
		"github.com/mrjoshuak/go-jpeg2000 v1.3.0") htj2k_excused=1 ;;
		*) htj2k_excused=0 ;;
		esac
		note "go-jpeg2000: ${j2kmod:-unresolved}"
		if [ "$htj2k_excused" = "1" ]; then
			note "htj2k rows are excused at v1.3.0 (see header); any other version gates them"
		else
			note "htj2k rows are gated as hard checks (go-jpeg2000 is past the v1.3.0 defects)"
		fi

		gaps=0
		while IFS=$'\t' read -r file type codec expect; do
			case "$file" in \#* | "") continue ;; esac
			base="$FIX/wr_${type}_none.exr"
			path="$FIX/$file"

			# The uncompressed row is the baseline every other row is compared
			# against; diffing it with itself would assert nothing, so it is
			# only required to be readable by the reference.
			if [ "$codec" = "none" ]; then
				if info=$(oiiotool --info -v "$path" 2>&1) &&
					printf '%s\n' "$info" | grep -q 'channel list:'; then
					pass "$(printf '%-5s %-9s baseline: the reference reads it (%s)' \
						"$type" "$codec" "$(printf '%s\n' "$info" | grep -m1 -o '[0-9]* x *[0-9]*, [0-9]* channel')")"
				else
					fail "$type $codec: the reference cannot read the uncompressed baseline"
				fi
				continue
			fi

			# One diff with every threshold pinned to zero: PASS then means
			# bit-identical, and the reported maximum error is the measurement
			# the lossy rows are held to.
			out=$(oiiotool --fail 0 --failpercent 0 --hardfail 0 \
				--warn 0 --warnpercent 0 --hardwarn 0 \
				--diff "$base" "$path" 2>&1)
			verdict=$(printf '%s\n' "$out" | grep -oE '^(PASS|FAILURE)' | head -1)
			maxerr=$(printf '%s\n' "$out" | sed -n 's/.*Max error *= *\([^ ]*\).*/\1/p' | head -1)
			[ -n "$maxerr" ] || maxerr=0

			# An excused HTJ2K row still runs the full diff above, so the
			# excuse is re-measured every run rather than assumed. A row that
			# passes while excused says so and should be gated.
			if [ "$htj2k_excused" = "1" ]; then
				case "$codec" in
				htj2k*)
					if [ "$verdict" = "PASS" ]; then
						note "KNOWN GAP CLOSED: $type/$codec is bit-identical at go-jpeg2000 v1.3.0 — delete the excuse in this script"
					else
						reason=$(printf '%s\n' "$out" | grep -m1 -E 'ojph|ERROR|error' | cut -c1-110)
						printf '  gap  - %-5s %-9s go-jpeg2000 v1.3.0 cannot emit a codestream the reference accepts: %s\n' \
							"$type" "$codec" "${reason:-unknown refusal}"
						gaps=$((gaps + 1))
						continue
					fi
					;;
				esac
			fi

			if [ -z "$verdict" ]; then
				fail "$(printf '%-5s %-9s the reference could not read the file: %s' \
					"$type" "$codec" "$(printf '%s\n' "$out" | grep -m1 -i 'error' | cut -c1-120)")"
				continue
			fi

			case "$expect" in
			exact)
				if [ "$verdict" = "PASS" ]; then
					pass "$(printf '%-5s %-9s exact: bit-identical to the uncompressed twin' "$type" "$codec")"
				else
					fail "$(printf '%-5s %-9s exact by specification, but the reference reads a difference (max error %s)' \
						"$type" "$codec" "$maxerr")"
				fi
				;;
			lossy)
				tol=$(tolerance "$type" "$codec")
				if [ -z "$tol" ]; then
					fail "$(printf '%-5s %-9s lossy with no documented tolerance; derive one in scripts/validate.sh' "$type" "$codec")"
				elif le "$maxerr" "$tol"; then
					pass "$(printf '%-5s %-9s lossy: decodes, max error %s <= %s' "$type" "$codec" "$maxerr" "$tol")"
				else
					fail "$(printf '%-5s %-9s lossy: max error %s exceeds the derived bound %s' \
						"$type" "$codec" "$maxerr" "$tol")"
				fi
				;;
			*)
				fail "$(printf '%-5s %-9s manifest says %q; expected exact or lossy' "$type" "$codec" "$expect")"
				;;
			esac
		done <"$FIX/manifest.tsv"

		[ "$gaps" -eq 0 ] || note "$gaps combinations excused because go-jpeg2000 is pinned at v1.3.0; bumping that dependency gates them"
	fi
fi

# ---------------------------------------------------------------------------
# 5. Tiled images, every resolution level, read back by the reference
#    implementation itself.
#
#    scripts/tiledgen writes the fixtures — plain tiled, mipmapped and
#    ripmapped, with tile sizes that divide the image and tile sizes that leave
#    partial tiles on both edges — and beside each file it writes the samples
#    that file is meant to hold and the geometry it is meant to claim.
#    scripts/exrtiledump links against libOpenEXR and prints what the reference
#    actually finds, level by level. Nothing in this library reads the files, so
#    a defect applied identically to the tiled writer and the tiled reader — the
#    shape of every codec defect found in v1.4.0 — fails here.
#
#    Two things are compared per fixture:
#
#      geometry  the dumper prints the mode, tile size, rounding, level count
#                and every level's size and tile count, all computed by the
#                reference from the file. tiledgen prints the same lines from
#                this library's own level arithmetic. They are diffed, so the
#                two implementations of the format's level math must agree
#                exactly rather than merely both produce a readable file.
#      samples   every sample of every level, by (level, x, y, channel) key.
#                An "exact" row must be bit-identical; a "lossy" row must decode
#                with a maximum error inside the bound derived for that codec in
#                this script's header. Missing or invented samples fail the row
#                whatever the error is, so a dump that stops early cannot pass.
#
#    THE LOSSY FIXTURE IS NOT THE SCANLINE ONE. B44's error bound is relative to
#    each 4x4 block's own maximum and holds for blocks spanning at most one
#    binade; scripts/interopgen's gradient runs down to zero, and at the small
#    mipmap levels one block then covers a quarter of the image and several
#    binades. Measured there, B44 costs 0.107 — and the OpenEXR reference
#    encoder costs exactly the same 0.107 on the same samples, so it is the
#    codec and the content, not the encoder. The tiled fixtures keep the lossy
#    channels inside [0.5, 1], one binade, which makes the derived bound valid
#    at every level. See scripts/tiledgen/main.go.
#
#    Three controls run before any of it, because this project has been fooled
#    by a broken oracle and by a fixture with no signal, and both look exactly
#    like a real defect:
#
#      a. exrmaketiled, the reference's own tiling tool, writes a tiled file
#         from the same pixels; it must satisfy the same expectation. If it does
#         not, the expectation or the dumper is wrong, not this library.
#      b. oiiotool tiles its own output and reads it back, so a failure that is
#         really OpenImageIO's stays distinguishable from one that is ours.
#      c. The comparator is handed two mismatched pairs and must report both a
#         difference and a shortfall. A check that cannot fail is not coverage,
#         and an audit of this repository found 21 tests that could not.
# ---------------------------------------------------------------------------
section "external oracle (tiled: one level, mipmap, ripmap, every level)"

# tiletol <codec> — the bound a lossy tiled row is held to, from this script's
# header. Same numbers as the scanline rows: the codec does not change because
# the chunk is a tile.
tiletol() {
	case "$1" in
	b44 | b44a) echo "3.125e-02" ;;
	dwaa | dwab) echo "1.0e-01" ;;
	pxr24) echo "3.0517578125e-05" ;;
	*) echo "" ;;
	esac
}

# tilecmp <expect> <dump> — compare and publish the five measurements.
CMP_SAMPLES=0
CMP_MISSING=0
CMP_EXTRA=0
CMP_MAXERR=0
CMP_AT=-
tilecmp() {
	line=$(awk -f "$REPO/scripts/tilecmp.awk" "$1" "$2") || return 1
	# shellcheck disable=SC2086
	set -- $line
	CMP_SAMPLES=${1#samples=}
	CMP_MISSING=${2#missing=}
	CMP_EXTRA=${3#extra=}
	CMP_MAXERR=${4#maxerr=}
	CMP_AT=${5#at=}
}

TILEDUMP="$WORK/exrtiledump"
TDIR="$WORK/tiled"

if [ "$build_ok" = "0" ]; then
	skip "tiled oracle (build failed)"
elif ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists OpenEXR 2>/dev/null; then
	skip "OpenEXR development files not found by pkg-config; the tiled oracle cannot be built (brew install openexr)"
elif ! command -v c++ >/dev/null 2>&1; then
	skip "no c++ compiler; the tiled oracle cannot be built"
elif ! out=$(c++ -std=c++17 -O1 -o "$TILEDUMP" scripts/exrtiledump/exrtiledump.cpp \
	$(pkg-config --cflags --libs OpenEXR) 2>&1); then
	fail "could not build scripts/exrtiledump against the reference implementation
$(echo "$out" | head -10)"
else
	mkdir -p "$TDIR"
	note "oracle: scripts/exrtiledump linked against OpenEXR $(pkg-config --modversion OpenEXR)"

	if ! go run ./scripts/tiledgen "$TDIR" >"$WORK/tiledgen.log" 2>&1; then
		fail "scripts/tiledgen could not write the tiled fixtures"
		cat "$WORK/tiledgen.log"
	else
		note "$(tail -1 "$WORK/tiledgen.log") into $TDIR"

		# --- control (a): a file this library did not write ------------------
		ctl="$TDIR/control_ref_one.exr"
		if ! command -v exrmaketiled >/dev/null 2>&1; then
			skip "control: exrmaketiled not installed; the reference cannot be asked to write the same tiled file"
		elif ! exrmaketiled -o -t 32 32 -z zip \
			"$TDIR/t_one_partial_half_zip_twin.exr" "$ctl" >"$WORK/mk.log" 2>&1; then
			fail "control: exrmaketiled could not tile the fixture: $(head -2 "$WORK/mk.log")"
		elif ! "$TILEDUMP" "$ctl" >"$WORK/ctl.dump" 2>"$WORK/ctl.err"; then
			fail "control: the dumper cannot read the reference's own tiled file: $(head -1 "$WORK/ctl.err")"
		else
			tilecmp "$TDIR/t_one_partial_half_zip.expect" "$WORK/ctl.dump"
			if [ "$CMP_MISSING" = "0" ] && [ "$CMP_EXTRA" = "0" ] && le "$CMP_MAXERR" 0; then
				pass "control: exrmaketiled's own tiled file satisfies the same expectation ($CMP_SAMPLES samples, max error 0)"
			else
				fail "control: the reference's own tiled file does not match the expectation (missing $CMP_MISSING, extra $CMP_EXTRA, max error $CMP_MAXERR at $CMP_AT) — the oracle or the fixture is wrong, not the writer"
			fi

			# The same tool, asked for a mipmap: the dumper must reach levels
			# past 0 on a file the reference wrote, and agree with this
			# library's level arithmetic about how many there are.
			if exrmaketiled -m -t 16 16 -z zip \
				"$TDIR/t_mip_half_zip_twin.exr" "$TDIR/control_ref_mip.exr" >"$WORK/mk2.log" 2>&1 &&
				"$TILEDUMP" -info "$TDIR/control_ref_mip.exr" >"$WORK/ctl2.dump" 2>&1; then
				if diff -u "$TDIR/t_mip_half_zip.structure" "$WORK/ctl2.dump" >"$WORK/ctl2.diff" 2>&1; then
					pass "control: the reference's own mipmap has the geometry this library computes ($(grep -c '^# level' "$WORK/ctl2.dump") levels)"
				else
					fail "control: this library and the reference disagree about the geometry of a mipmap the reference itself wrote
$(head -8 "$WORK/ctl2.diff")"
				fi
			else
				fail "control: exrmaketiled could not write a mipmap, or the dumper could not read it: $(head -2 "$WORK/mk2.log")"
			fi
		fi

		# --- control (b): oiiotool round-trips its own tiled output ----------
		if ! command -v oiiotool >/dev/null 2>&1; then
			skip "control: oiiotool not installed; the second reader cannot be consulted"
		elif oiiotool "$TDIR/t_one_partial_half_zip_twin.exr" --tile 32 32 \
			-o "$TDIR/control_oiio.exr" >"$WORK/oiio.log" 2>&1 &&
			oiiotool --fail 0 --failpercent 0 --hardfail 0 --warn 0 --warnpercent 0 --hardwarn 0 \
				--diff "$TDIR/t_one_partial_half_zip_twin.exr" "$TDIR/control_oiio.exr" 2>&1 |
			grep -q '^PASS'; then
			pass "control: oiiotool tiles its own output and reads it back identically"
		else
			fail "control: oiiotool cannot round-trip its own tiled output; a tiled failure below may be OpenImageIO's, not this library's"
		fi

		# --- control (c): the comparator can fail ---------------------------
		if "$TILEDUMP" "$TDIR/t_one_partial_half_zip.exr" >"$WORK/selftest.dump" 2>&1; then
			tilecmp "$TDIR/t_one_partial_half_b44.expect" "$WORK/selftest.dump"
			differs=$CMP_MAXERR
			tilecmp "$TDIR/t_mip_half_zip.expect" "$WORK/selftest.dump"
			short=$CMP_MISSING
			if le 1e-9 "$differs" && [ "$short" -gt 0 ]; then
				pass "control: the comparator reports a wrong value (max error $differs) and a missing level ($short samples), so the checks below can fail"
			else
				fail "control: the comparator reported no difference (max error $differs) or no shortfall (missing $short) between fixtures known to differ; every tiled check below is vacuous"
			fi
		else
			fail "control: the dumper could not read the plain tiled fixture at all"
		fi

		# --- the fixtures ---------------------------------------------------
		oiio_bad=""
		oiio_seen=0
		while IFS=$'\t' read -r file type codec mode expect levels chunks tile note; do
			case "$file" in \#* | "") continue ;; esac
			name=${file%.exr}
			label=$(printf '%-7s %-5s %-5s %-9s' "$mode" "$tile" "$type" "$codec")

			if ! "$TILEDUMP" "$TDIR/$file" >"$WORK/t.dump" 2>"$WORK/t.err"; then
				fail "$label the reference cannot read it: $(head -1 "$WORK/t.err" | cut -c1-140)"
				continue
			fi

			# Geometry first: a file whose levels are the wrong size can still
			# hold self-consistent samples.
			grep '^#' "$WORK/t.dump" >"$WORK/t.structure"
			if ! diff -u "$TDIR/$name.structure" "$WORK/t.structure" >"$WORK/t.gdiff" 2>&1; then
				fail "$label geometry: the reference computes different levels than this library
$(grep -E '^[-+][^-+]' "$WORK/t.gdiff" | head -6)"
				continue
			fi

			tilecmp "$TDIR/$name.expect" "$WORK/t.dump"
			if [ "$CMP_MISSING" != "0" ] || [ "$CMP_EXTRA" != "0" ]; then
				fail "$label the reference read $CMP_SAMPLES samples, $CMP_MISSING missing and $CMP_EXTRA unexpected ($note)"
			else
				case "$expect" in
				exact)
					if le "$CMP_MAXERR" 0; then
						pass "$label exact: $CMP_SAMPLES samples over $levels levels, bit-identical ($note)"
					else
						fail "$label exact by specification, but the reference reads $CMP_MAXERR at (lx,ly,x,y,ch)=$CMP_AT ($note)"
					fi
					;;
				lossy)
					tol=$(tiletol "$codec")
					if [ -z "$tol" ]; then
						fail "$label lossy with no documented tolerance; derive one in scripts/validate.sh"
					elif le "$CMP_MAXERR" "$tol"; then
						pass "$label lossy: $CMP_SAMPLES samples over $levels levels, max error $CMP_MAXERR <= $tol ($note)"
					else
						fail "$label lossy: max error $CMP_MAXERR exceeds the derived bound $tol, at (lx,ly,x,y,ch)=$CMP_AT"
					fi
					;;
				*)
					fail "$label manifest says $expect; expected exact or lossy"
					;;
				esac
			fi

			# A second reader, over a different code path: OpenImageIO is asked
			# whether the tiled file holds the same level-0 image as the
			# scanline twin the gate above already vouches for.
			if command -v oiiotool >/dev/null 2>&1; then
				oiio_seen=$((oiio_seen + 1))
				out=$(oiiotool --fail 0 --failpercent 0 --hardfail 0 \
					--warn 0 --warnpercent 0 --hardwarn 0 \
					--diff "$TDIR/${name}_twin.exr" "$TDIR/$file" 2>&1)
				verdict=$(printf '%s\n' "$out" | grep -oE '^(PASS|FAILURE)' | head -1)
				maxerr=$(printf '%s\n' "$out" | sed -n 's/.*Max error *= *\([^ ]*\).*/\1/p' | head -1)
				[ -n "$maxerr" ] || maxerr=0
				tol=$(tiletol "$codec")
				[ -n "$tol" ] || tol=0
				if [ -z "$verdict" ]; then
					oiio_bad="$oiio_bad $name(unreadable)"
				elif ! le "$maxerr" "$tol"; then
					oiio_bad="$oiio_bad $name($maxerr>$tol)"
				fi
			fi
		done <"$TDIR/manifest.tsv"

		if [ "$oiio_seen" -gt 0 ]; then
			if [ -z "$oiio_bad" ]; then
				pass "oiiotool agrees with the scanline twin on level 0 of all $oiio_seen tiled fixtures"
			else
				fail "oiiotool disagrees with the scanline twin on:$oiio_bad"
			fi
		fi

		# --- the format forbids subsampled channels in a tiled image ---------
		#
		# OpenEXR's sanity check refuses them outright:
		#   "channel 'BY': x subsampling factor is not 1 (2) for a tiled image"
		# so a header this library is willing to tile with one produces a file
		# no reader can open. scripts/testdata/tiled_subsampled_invalid.exr is
		# such a file, written by this library at 5e793a5 before the guard
		# existed; the reference must still refuse it, which is what keeps the
		# guard below honest rather than a comment about a rule nobody checks.
		INVALID=scripts/testdata/tiled_subsampled_invalid.exr
		if "$TILEDUMP" -info "$INVALID" >"$WORK/inv.log" 2>&1; then
			fail "the reference now opens a tiled file with subsampled channels; re-derive the guard below, its premise has changed"
		else
			# exrinfo reports the reason rather than just the refusal, so quote
			# it when it is installed: a refusal for some unrelated reason (a
			# truncated fixture, say) would otherwise look like agreement.
			why=$(head -1 "$WORK/inv.log")
			if command -v exrinfo >/dev/null 2>&1; then
				exrinfo -v "$INVALID" >"$WORK/inv2.log" 2>&1
				detail=$(grep -m1 -i 'subsampling' "$WORK/inv2.log")
				[ -z "$detail" ] || why=$detail
			fi
			pass "the reference refuses a tiled file with subsampled channels: $(printf '%s' "$why" | cut -c1-120)"
		fi

		while IFS=$'\t' read -r guard result detail; do
			case "$guard" in \#* | "") continue ;; esac
			if [ "$result" = "rejected" ]; then
				pass "guard $guard: this library refuses it ($detail)"
			else
				fail "guard $guard: this library accepts a header the reference cannot read; it will write a file nothing can open"
			fi
		done <"$TDIR/guards.tsv"

		# ---- generated mipmap levels, against the reference tool's ---------
		#
		# The format specifies no downsampling filter, so no implementation's
		# generated levels are "correct" the way a codec's output is. That does
		# not make them unmeasurable. exrmaketiled generates levels from the
		# same source, and four things must hold whatever filter each side
		# chose: the two agree on which samples exist, level 0 is exact because
		# it is the source, the 1x1 level is exact because it is the image's
		# mean and every 2x2-supported filter preserves it, and the per-level
		# difference never grows with depth — a filter difference averages out,
		# a wrong axis or a wrong scale does not.
		#
		# Measured: 0, 0.120, 0.070, 0.026, 0.008, 0.002, 0 across seven levels
		# of uniform noise in [0,1].
		if ! command -v exrmaketiled >/dev/null 2>&1; then
			gap "generated mipmap levels: exrmaketiled is not installed"
		else
			MDIR2="$WORK/mipgen"
			mkdir -p "$MDIR2"
			if ! go build -o "$MDIR2/mipcmp" ./scripts/mipcmp/ 2>"$MDIR2/build.err"; then
				fail "generated mipmap levels: could not build scripts/mipcmp: $(head -1 "$MDIR2/build.err")"
			elif ! oiiotool --pattern noise:type=uniform 64x64 1 --chnames Y -d half \
				-o "$MDIR2/src.exr" >/dev/null 2>&1; then
				gap "generated mipmap levels: oiiotool could not write the source image"
			elif ! exrmaketiled -m -t 16 16 "$MDIR2/src.exr" "$MDIR2/ref.exr" >/dev/null 2>&1; then
				gap "generated mipmap levels: exrmaketiled could not generate a reference mipmap"
			elif ! "$MDIR2/mipcmp" "$MDIR2/src.exr" "$MDIR2/ours.exr" 16 2>"$MDIR2/gen.err"; then
				fail "generated mipmap levels: this library could not generate them: $(head -1 "$MDIR2/gen.err" | cut -c1-90)"
			else
				"$TILEDUMP" "$MDIR2/ref.exr" >"$MDIR2/ref.dump" 2>/dev/null
				"$TILEDUMP" "$MDIR2/ours.exr" >"$MDIR2/ours.dump" 2>/dev/null
				if out=$(python3 scripts/mipdiff.py "$MDIR2/ref.dump" "$MDIR2/ours.dump" 2>&1); then
					pass "generated mipmap levels agree with exrmaketiled's: $(printf '%s' "$out" | head -1)"
				else
					fail "generated mipmap levels: $(printf '%s' "$out" | tail -1 | cut -c1-110)"
				fi

				# Signal: the comparison must reject values that are wrong.
				python3 - "$MDIR2/ours.dump" "$MDIR2/shifted.dump" <<'MIPEOF'
import sys
out = open(sys.argv[2], 'w')
for line in open(sys.argv[1]):
    if line.startswith('#'):
        out.write(line)
        continue
    f = line.split()
    if len(f) >= 6:
        f[5] = str(float(f[5]) + 0.5)
        out.write(' '.join(f) + '\n')
MIPEOF
				if python3 scripts/mipdiff.py "$MDIR2/ref.dump" "$MDIR2/shifted.dump" >/dev/null 2>&1; then
					fail "generated mipmap levels signal check: the comparison accepted every level shifted by 0.5"
				else
					pass "generated mipmap levels signal check: the comparison rejects shifted values"
				fi
			fi
		fi
		# ---- measured gaps ------------------------------------------------
	fi
fi

# 6. The external oracle on multi-part files.
#
#    A multi-part file gives each part its own header, so the parts may
#    disagree about the data window, the compression, the channel list and the
#    storage type while sharing one display window. Nothing outside this
#    library had ever read one of these files. scripts/multipartgen writes
#    six of them — an embedded-proxy pair, three data windows including one
#    with a negative origin, eight codecs one per part with HTJ2K beside ZIP
#    and PIZ, four unrelated channel layouts, a scanline part beside two tiled
#    parts, and a scanline master beside a mipmapped tiled proxy — and beside
#    every part it writes the samples it intended, as one plain PFM per channel
#    per resolution level (a header and raw little-endian floats, a format with
#    no EXR code anywhere near it) and a text table for integer channels.
#
#    The reference implementation is then asked, part by part, level by level
#    and channel by channel, whether the file holds those samples. Every codec
#    here is lossless for the pixel type it is paired with, so every comparison
#    is exact: no tolerance, no percentage of allowed outliers.
#
#    Three things keep the measurement honest:
#
#      control      the reference writes a two-part file with the same two
#                   data windows from the same PFMs, and the identical
#                   extract-and-compare procedure runs on it. A broken oracle
#                   and a broken writer look the same until this passes.
#      signal       the same procedure is run against deliberately wrong truth
#                   — one part against another part's samples, one channel
#                   against another channel's. It has to report a difference.
#                   A fixture that compares equal to the wrong answer gates
#                   nothing, and this repository has 21 proven examples of a
#                   check that could not fail.
#      structure    the reference is asked for each part's name, storage type,
#                   compression, data window and channel list, so a file whose
#                   pixels happen to land in the right place but whose headers
#                   are wrong still fails.
# ---------------------------------------------------------------------------
section "external oracle (multi-part: parts differing in window, codec, channels and storage)"

# mp_extract <file> <part> <level> <channel> <out> — ask the reference for one
# channel of one resolution level of one part, moved to the origin so it can be
# compared with a PFM.
mp_extract() {
	oiiotool -i "$1" --subimage "$2" --selectmip "$3" --ch "$4" \
		--origin +0+0 --fullpixels -o "$5" 2>&1
}

# mp_diff <a> <b> — one diff with every threshold pinned to zero, so PASS
# means bit-identical.
mp_diff() {
	oiiotool --fail 0 --failpercent 0 --hardfail 0 \
		--warn 0 --warnpercent 0 --hardwarn 0 \
		--diff "$1" "$2" 2>&1
}

# mp_verdict <output> — PASS, FAILURE, or empty when the reference errored.
mp_verdict() { printf '%s\n' "$1" | grep -oE '^(PASS|FAILURE)' | head -1; }

# mp_maxerr <output> — the reported maximum difference, 0 when none.
mp_maxerr() {
	m=$(printf '%s\n' "$1" | sed -n 's/.*Max error *= *\([^ ]*\).*/\1/p' | head -1)
	printf '%s' "${m:-0}"
}

# mp_field <info> <label> — a quoted scalar from oiiotool's --printinfo.
mp_field() { printf '%s\n' "$1" | sed -n "s/^ *$2: \"\(.*\)\"\$/\1/p" | head -1; }

# mp_reason <output> — the reference's first complaint, with the scratch
# directory removed so the message itself survives the line length.
mp_reason() {
	printf '%s\n' "$1" | grep -m1 -iE 'error|exception' |
		sed -e "s|$WORK/multipart/||g" -e 's|(/[^)]*)|()|g' | cut -c1-150
}

if ! command -v oiiotool >/dev/null 2>&1; then
	skip "oiiotool not installed; multi-part files cannot be put to the reference (brew install openimageio)"
elif [ "$build_ok" = "0" ]; then
	skip "multi-part external oracle (build failed)"
else
	MP="$WORK/multipart"
	mkdir -p "$MP"
	if ! go run ./scripts/multipartgen "$MP" >"$WORK/multipartgen.log" 2>&1; then
		fail "scripts/multipartgen could not write every multi-part fixture"
		grep -E '^FAIL' "$WORK/multipartgen.log" | head -10
	fi
	if [ ! -f "$MP/parts.tsv" ]; then
		fail "scripts/multipartgen wrote no manifest; nothing could be measured"
	else
		note "$(tail -1 "$WORK/multipartgen.log") into $MP"
		note "exact = the reference reads back the samples the writer was given, bit for bit"

		# ---- control ------------------------------------------------------
		# The reference writes the same two data windows itself, and the same
		# procedure is run on the result.
		ctrl_spec=$(awk -F'\t' '$1 == "mp_windows.exr" && ($2 == 0 || $2 == 1) {
			printf "%s %s %s %s %s\n", $2, $6, $7, $8, $9 }' "$MP/parts.tsv")
		set -- $ctrl_spec
		if [ $# -eq 10 ] && [ -f "$MP/mp_windows.p0.R.pfm" ] && [ -f "$MP/mp_windows.p1.R.pfm" ]; then
			c0w=$4 c0h=$5 c1x=$7 c1y=$8
			if oiiotool -i "$MP/mp_windows.p0.R.pfm" --chnames R \
				--fullsize "${c0w}x${c0h}+0+0" --attrib oiio:subimagename c0 \
				-i "$MP/mp_windows.p1.R.pfm" --chnames R \
				--origin "+${c1x}+${c1y}" --fullsize "${c0w}x${c0h}+0+0" \
				--attrib oiio:subimagename c1 \
				--siappendall -d float -o "$WORK/mpcontrol.exr" >"$WORK/mpcontrol.log" 2>&1; then
				ctrl_ok=1
				for cp in 0 1; do
					err=$(mp_extract "$WORK/mpcontrol.exr" "$cp" 0 R "$WORK/mpctrl_$cp.exr")
					if [ ! -f "$WORK/mpctrl_$cp.exr" ]; then
						ctrl_ok=0
						ctrl_why="the reference could not re-read its own part $cp: $(printf '%s' "$err" | head -1)"
						break
					fi
					out=$(mp_diff "$WORK/mpctrl_$cp.exr" "$MP/mp_windows.p$cp.R.pfm")
					if [ "$(mp_verdict "$out")" != "PASS" ]; then
						ctrl_ok=0
						ctrl_why="the reference's own part $cp does not match the samples it was given (max error $(mp_maxerr "$out"))"
						break
					fi
				done
				if [ "$ctrl_ok" = "1" ]; then
					pass "control: the reference round-trips its own two-part file with two different data windows through this exact procedure"
				else
					fail "control: $ctrl_why — the oracle or the procedure is broken, so every multi-part measurement below is uninterpretable"
				fi
			else
				fail "control: the reference could not write a two-part file from the fixture's own samples: $(head -1 "$WORK/mpcontrol.log")"
			fi
		else
			fail "control: the fixture for the control is missing; nothing distinguishes a broken oracle from a broken writer"
		fi

		# ---- files: does the reference accept the file at all -------------
		while IFS=$'\t' read -r file nparts note_text; do
			case "$file" in \#* | "") continue ;; esac
			info=$(oiiotool -i "$MP/$file" --printinfo:verbose=1 2>&1)
			got=$(printf '%s\n' "$info" | sed -n 's/^ *oiio:subimages: \([0-9]*\).*/\1/p' | head -1)
			if [ -z "$got" ]; then
				fail "$(printf '%-16s the reference refuses the file: %s' "$file" \
					"$(mp_reason "$info")")"
			elif [ "$got" != "$nparts" ]; then
				fail "$(printf '%-16s the reference finds %s parts, the writer wrote %s' "$file" "$got" "$nparts")"
			else
				pass "$(printf '%-16s %s parts, read by the reference (%s)' "$file" "$got" "$note_text")"
			fi
		done <"$MP/files.tsv"

		# ---- parts: headers, then samples ---------------------------------
		while IFS=$'\t' read -r file part name typ comp minx miny width height chans tile levels; do
			case "$file" in \#* | "") continue ;; esac
			path="$MP/$file"
			label=$(printf '%-16s part %s %-9s' "$file" "$part" "$name")

			# oiiotool cannot read subsampled channels — it refuses the file
			# outright and exposes only the unsubsampled parts of a multi-part
			# file containing one. Those parts are checked against libOpenEXR
			# directly by the exrpartdump block further down, which is a
			# stronger oracle, not a weaker one. Routing them there rather than
			# failing here is the difference between using the right tool and
			# dropping the check.
			case "$file" in
			mp_subsampled.exr)
				note "$label handled by the exrpartdump check: oiiotool cannot read subsampled channels"
				continue
				;;
			esac

			info=$(oiiotool -i "$path" --subimage "$part" --printinfo:verbose=1 2>&1)
			if ! printf '%s\n' "$info" | grep -q 'channel list:'; then
				fail "$label the reference cannot read this part: $(mp_reason "$info")"
				continue
			fi

			gw=$(printf '%s\n' "$info" | sed -n '1s/^ *\([0-9][0-9]*\) *x *\([0-9][0-9]*\).*/\1/p')
			gh=$(printf '%s\n' "$info" | sed -n '1s/^ *\([0-9][0-9]*\) *x *\([0-9][0-9]*\).*/\2/p')
			gx=$(printf '%s\n' "$info" | sed -n 's/^ *pixel data origin: x=\(-*[0-9]*\), y=\(-*[0-9]*\).*/\1/p' | head -1)
			gy=$(printf '%s\n' "$info" | sed -n 's/^ *pixel data origin: x=\(-*[0-9]*\), y=\(-*[0-9]*\).*/\2/p' | head -1)
			[ -n "$gx" ] || gx=0
			[ -n "$gy" ] || gy=0
			gname=$(mp_field "$info" name)
			gcomp=$(mp_field "$info" compression)
			gchans=$(printf '%s\n' "$info" | sed -n 's/^ *channel list: //p' | head -1 |
				tr -d ' ' | tr ',' '\n' | sort | paste -sd, -)
			want_chans=$(printf '%s' "$chans" | tr ',' '\n' | cut -d: -f1 | sort | paste -sd, -)

			gtile=$(printf '%s\n' "$info" | sed -n 's/^ *tile size: \([0-9]*\) x \([0-9]*\).*/\1x\2/p' | head -1)
			glevels=$(printf '%s\n' "$info" | sed -n 's/^ *MIP-map levels: //p' | head -1 | wc -w | tr -d ' ')
			[ "$glevels" != "0" ] || glevels=1
			want_tile=""
			[ "$tile" = "-" ] || want_tile="${tile}x${tile}"

			# The storage type, the per-channel pixel types and the name of
			# any compression OpenImageIO does not have in its own table are
			# only in the header, and exrheader — the reference's own header
			# dumper — is what reports them verbatim.
			blk=""
			if command -v exrheader >/dev/null 2>&1; then
				blk=$(exrheader "$path" 2>/dev/null | awk -v p=" part $part:" '
					/^ part [0-9]+:/ { inblk = ($0 == p) } inblk { print }')
			fi
			gtyp=$(printf '%s\n' "$blk" | sed -n 's/^type (type string): "\(.*\)"$/\1/p' | head -1)
			hcomp=$(printf '%s\n' "$blk" |
				sed -n 's/^compression (type compression): \([^:]*\):.*/\1/p' | head -1)
			# oiiotool reports no compression name for codecs it does not know
			# (htj2k256 and htj2k32 among them), so the header is the
			# authority and oiiotool is the cross-check.
			if [ -z "$gcomp" ]; then
				gcomp="$hcomp"
				[ -n "$gcomp" ] || note "$label the reference's oiiotool reports no compression name for \"$comp\"; exrheader is not installed, so that field is unmeasured"
			elif [ -n "$hcomp" ] && [ "$hcomp" != "$gcomp" ]; then
				gcomp="$hcomp"
			fi

			why=""
			[ -n "$gw" ] && [ -n "$gh" ] || why="the reference reported no size for this part"
			[ -n "$why" ] || [ "$gw/$gh" = "$width/$height" ] || why="data window size ${gw}x${gh}, wrote ${width}x${height}"
			[ -n "$why" ] || [ "$gtile" = "$want_tile" ] || why="tile size \"${gtile:-none}\", wrote \"${want_tile:-none}\""
			[ -n "$why" ] || [ "$glevels" = "$levels" ] || why="$glevels resolution levels, wrote $levels"
			[ -n "$why" ] || [ "$gx/$gy" = "$minx/$miny" ] || why="data window origin ($gx,$gy), wrote ($minx,$miny)"
			[ -n "$why" ] || [ "$gname" = "$name" ] || why="part named \"$gname\", wrote \"$name\""
			[ -n "$why" ] || [ -z "$gcomp" ] || [ "$gcomp" = "$comp" ] || why="compression \"$gcomp\", wrote \"$comp\""
			[ -n "$why" ] || [ "$gchans" = "$want_chans" ] || why="channels $gchans, wrote $want_chans"
			[ -n "$why" ] || [ -z "$gtyp" ] || [ "$gtyp" = "$typ" ] || why="type \"$gtyp\", wrote \"$typ\""
			# chunkCount is required in every part of a multi-part file: it is
			# all a reader has to go on for a part type it does not recognise.
			[ -n "$why" ] || [ -z "$blk" ] || printf '%s\n' "$blk" | grep -q '^chunkCount ' ||
				why="no chunkCount attribute, which the format requires in every part of a multi-part file"

			if [ -z "$why" ] && [ -n "$blk" ]; then
				for pair in $(printf '%s' "$chans" | tr ',' ' '); do
					cn=${pair%%:*}
					ct=${pair##*:}
					case "$ct" in
					half) want="16-bit floating-point" ;;
					float) want="32-bit floating-point" ;;
					uint) want="32-bit unsigned integer" ;;
					*) want="" ;;
					esac
					line=$(printf '%s\n' "$blk" | sed -n "s/^    $cn, \(.*\), sampling.*/\1/p" | head -1)
					if [ -z "$line" ]; then
						why="channel $cn is missing from the part's channel list"
						break
					fi
					if [ -n "$want" ] && [ "$line" != "$want" ]; then
						why="channel $cn is $line, wrote $want"
						break
					fi
				done
			fi

			if [ -n "$why" ]; then
				fail "$label header: $why"
			else
				pass "$(printf '%s header: %sx%s at (%s,%s), %s, %s, %s' "$label" \
					"$width" "$height" "$minx" "$miny" "$typ" "$comp" "$want_chans")"
			fi

			# Samples, channel by channel, against the truth written beside
			# the fixture.
			nch=0
			bad=""
			while IFS=$'\t' read -r cfile cpart cname kind truth level; do
				case "$cfile" in \#* | "") continue ;; esac
				[ "$cfile" = "$file" ] && [ "$cpart" = "$part" ] || continue
				nch=$((nch + 1))
				[ -n "$bad" ] && continue
				clabel="$cname"
				[ "$level" = "0" ] || clabel="$cname at level $level"
				if [ "$kind" = "uint" ]; then
					# Integer channels do not survive the reference's float
					# conversion, so they are read back in their native type
					# and compared value by value.
					rm -f "$WORK/mpu.exr"
					err=$(oiiotool --native -i "$path" --subimage "$cpart" --selectmip "$level" --ch "$cname" \
						-d uint32 -o "$WORK/mpu.exr" 2>&1)
					if [ ! -f "$WORK/mpu.exr" ]; then
						bad="$clabel: the reference could not read it: $(printf '%s' "$err" | head -1)"
						continue
					fi
					oiiotool --native --dumpdata -i "$WORK/mpu.exr" 2>/dev/null |
						sed -n 's/^ *Pixel ([0-9]*, [0-9]*): \([0-9]*\).*/\1/p' >"$WORK/mpu.txt"
					if ! cmp -s "$WORK/mpu.txt" "$MP/$truth"; then
						first=$(diff "$MP/$truth" "$WORK/mpu.txt" 2>/dev/null | head -3 | tr '\n' ' ')
						bad="$clabel: the reference reads different integers ($first)"
					fi
					continue
				fi
				rm -f "$WORK/mpc.exr"
				err=$(mp_extract "$path" "$cpart" "$level" "$cname" "$WORK/mpc.exr")
				if [ ! -f "$WORK/mpc.exr" ]; then
					bad="$clabel: the reference could not read it: $(printf '%s' "$err" | head -1)"
					continue
				fi
				out=$(mp_diff "$WORK/mpc.exr" "$MP/$truth")
				case "$(mp_verdict "$out")" in
				PASS) ;;
				FAILURE) bad="$clabel: max error $(mp_maxerr "$out"), $(printf '%s\n' "$out" | sed -n 's/.*Max error.*@ \((.*)\).*/at \1/p' | head -1)" ;;
				*) bad="$clabel: the reference could not compare it: $(printf '%s\n' "$out" | grep -m1 -i error | cut -c1-120)" ;;
				esac
			done <"$MP/chans.tsv"

			if [ "$nch" -eq 0 ]; then
				fail "$label samples: no channel was compared; the check would pass whatever the writer emitted"
			elif [ -n "$bad" ]; then
				fail "$label samples: $bad"
			else
				pass "$(printf '%s samples: %s channel-and-level comparisons exact' "$label" "$nch")"
			fi
		done <"$MP/parts.tsv"

		# ---- signal -------------------------------------------------------
		# The same comparison against deliberately wrong truth. Both of these
		# must report a difference; if either passes, the fixture cannot tell
		# a swapped part or a swapped channel from a correct one.
		sig_check() {
			# $1 file, $2 part, $3 channel, $4 truth of something else, $5 what
			[ -f "$MP/$1" ] && [ -f "$MP/$4" ] || {
				fail "signal: $5 could not be measured; the fixture is missing"
				return
			}
			rm -f "$WORK/mps.exr"
			mp_extract "$MP/$1" "$2" 0 "$3" "$WORK/mps.exr" >/dev/null 2>&1
			if [ ! -f "$WORK/mps.exr" ]; then
				fail "signal: $5 could not be measured; the reference could not read $1 part $2"
				return
			fi
			out=$(mp_diff "$WORK/mps.exr" "$MP/$4")
			if [ "$(mp_verdict "$out")" = "FAILURE" ]; then
				pass "signal: $5 is detected (max error $(mp_maxerr "$out"))"
			else
				fail "signal: $5 compares equal — the fixture has no signal and every exact row above asserts nothing"
			fi
		}
		sig_check mp_codecs.exr 0 R mp_codecs.p1.R.pfm "one part's samples put where another part's belong"
		sig_check mp_channels.exr 0 R mp_channels.p0.G.pfm "one channel's samples put where another channel's belong"

		# ---- ripmapped tiled part inside a multi-part file -----------------
		#
		# A ripmap's x and y levels are independent, so its chunk offset table
		# has a different layout from a mipmap's; the mipmapped part above
		# walks one level per step and never exercises it.
		#
		# The oracle is scripts/exrtiledump with -part, which opens the file as
		# multi-part and addresses one tiled part through libOpenEXR. oiiotool
		# cannot serve here: --selectmip addresses levels by a single index,
		# which a ripmap does not have.
		if [ ! -x "$TILEDUMP" ]; then
			gap "ripmapped multi-part part: exrtiledump could not be built against the reference"
		else
			RPDIR="$WORK/mprip"
			if ! err=$(go run ./scripts/mpripgen "$RPDIR" 2>&1); then
				fail "ripmapped multi-part part: this library could not write one: $(printf '%s' "$err" | head -1 | cut -c1-90)"
			elif ! "$TILEDUMP" -part 1 "$RPDIR/mp_ripmap.exr" >"$RPDIR/rip.dump" 2>"$RPDIR/rip.err"; then
				fail "ripmapped multi-part part: the reference refused it: $(head -1 "$RPDIR/rip.err" | cut -c1-100)"
			else
				line=$(awk -f "$REPO/scripts/tilecmp.awk" "$RPDIR/mp_ripmap.expect" "$RPDIR/rip.dump")
				lvls=$(grep -c '^# level' "$RPDIR/rip.dump")
				case "$line" in
				*"missing=0 extra=0 maxerr=0 "*)
					pass "ripmapped multi-part part: the reference reads all $lvls levels exactly ($line)" ;;
				*)
					fail "ripmapped multi-part part: $line" ;;
				esac

				# The scanline part beside it must still read, or the check
				# above is satisfied by a file whose other part is broken.
				# oiiotool pads the dimensions, so match them loosely; what is
				# being asserted is that the file opens and reports two
				# subimages at the master's size, not the exact spacing.
				if info=$(oiiotool --info "$RPDIR/mp_ripmap.exr" 2>&1) &&
					printf '%s' "$info" | grep -qE "96 +x +64" &&
					printf '%s' "$info" | grep -q "2 subimages"; then
					pass "ripmapped multi-part control: the scanline master beside it still opens"
				else
					fail "ripmapped multi-part control: the scanline master is not readable: $(printf '%s' "$info" | head -1 | cut -c1-90)"
				fi

				# Signal: the comparison must reject a dump that is wrong.
				sed 's/ \([0-9.eE+-]*\)$/ 12345/' "$RPDIR/rip.dump" >"$RPDIR/rip.wrong"
				sig=$(awk -f "$REPO/scripts/tilecmp.awk" "$RPDIR/mp_ripmap.expect" "$RPDIR/rip.wrong")
				case "$sig" in
				*maxerr=0*) fail "ripmapped multi-part signal check: deliberately wrong values compared clean ($sig)" ;;
				*) pass "ripmapped multi-part signal check: the comparison reports wrong values" ;;
				esac
			fi
		fi
		# ---- subsampled channels in a multi-part part ----------------------
		#
		# A channel with XSampling above 1 stores every n-th column, so it
		# contributes ceil(width/n) samples per line rather than width of them.
		# buildScanlineData packed the full width for every channel, which made
		# the chunk longer than the format says and put every channel after a
		# subsampled one at the wrong offset. YSampling above 1 removes whole
		# rows, which this library's chunk layout cannot express;
		# NewMultiPartWriter refuses it, as ScanlineWriter does, and the guard
		# is checked below.
		#
		# The oracle is scripts/exrpartdump rather than oiiotool: oiiotool
		# cannot read subsampled channels at all — it refuses the file with
		# "Subsampled channels are not supported (channel \"BY\" has sampling
		# 2,1)" — and silently exposes only the unsubsampled parts of a
		# multi-part file containing one, which would have measured nothing
		# while appearing to pass.
		PARTDUMP="$WORK/exrpartdump"
		if ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists OpenEXR; then
			gap "subsampled multi-part channels: OpenEXR development files are not installed"
		elif ! out=$(c++ -std=c++17 -O1 -o "$PARTDUMP" scripts/exrpartdump/exrpartdump.cpp \
			$(pkg-config --cflags --libs OpenEXR) 2>&1); then
			fail "subsampled multi-part channels: could not build scripts/exrpartdump: $(printf '%s' "$out" | head -1 | cut -c1-90)"
		elif [ ! -f "$MP/mp_subsampled.exr" ]; then
			gap "subsampled multi-part channels: the fixture was not written"
		else
			if ! "$PARTDUMP" -part 1 "$MP/mp_subsampled.exr" >"$WORK/sub.dump" 2>"$WORK/sub.err"; then
				fail "subsampled multi-part channels: the reference refused the file: $(head -1 "$WORK/sub.err" | cut -c1-100)"
			elif out=$(python3 scripts/subpartcmp.py "$WORK/sub.dump" "$MP" mp_subsampled 1 2>&1); then
				pass "subsampled multi-part channels: the reference reads every channel exactly ($out)"
			else
				fail "subsampled multi-part channels: $out"
			fi

			# YSampling above 1 must be refused rather than written, since the
			# chunk layout cannot express a scanline missing rows. The guard
			# lives in NewMultiPartWriter and is asserted by a Go test; what is
			# checked here is that the test exists and passes, so the refusal
			# cannot quietly disappear.
			if go test ./exr/ -run TestMultiPartRefusesYSubsampling >/dev/null 2>&1; then
				pass "subsampled multi-part channels: ySampling above 1 is refused rather than written"
			else
				fail "subsampled multi-part channels: NewMultiPartWriter does not refuse ySampling above 1"
			fi

			# Signal: the comparison must reject values that are wrong.
			sed 's/ \([0-9.eE+-]*\)$/ 9.5/' "$WORK/sub.dump" >"$WORK/sub.bad"
			if python3 scripts/subpartcmp.py "$WORK/sub.bad" "$MP" mp_subsampled 1 >/dev/null 2>&1; then
				fail "subsampled multi-part channels signal check: deliberately wrong values compared clean"
			else
				pass "subsampled multi-part channels signal check: the comparison rejects wrong values"
			fi
		fi
		# ---- measured gaps ------------------------------------------------
		# Recorded rather than omitted: a row nobody writes down is a row
		# nobody notices is missing.
	fi
fi

# 7. Deep images: the same oracle, sample for sample.
#
#    A deep pixel holds a variable number of samples, so two things have to
#    survive the trip through an outside reader: the sample count table, which
#    says how many samples each pixel has, and the samples themselves. A writer
#    that assumes a constant sample count reads back correctly from its own
#    reader and produces a file no one else can open — which is exactly what
#    both deep writers did before this section existed.
#
#    WHAT THE REFERENCE CAN ASSERT. oiiotool --dumpdata prints, for every pixel
#    of a deep image, the sample count and then every sample's value for every
#    channel, by name. That is the whole of the deep payload, so this section
#    compares it position by position against scripts/deepgen's expectation:
#
#      * sample count per pixel, including pixels with zero samples
#      * the set of channels present on each sample, by name
#      * every sample value of every channel, in sample order
#      * the header structure: deep, dimensions, channel names, tile size
#
#    WHAT IT DOES NOT ASSERT. --dumpdata prints decimal text, so a value is
#    compared to the precision the reference prints (about eight significant
#    digits) rather than bit for bit. The fixture's neighbouring values are at
#    least 1/128 apart and its tolerance is 1e-6, so the gap between "equal as
#    printed" and "equal as bits" is four orders of magnitude smaller than the
#    smallest defect that could hide in it. Deep files also carry no alpha
#    premultiplication or sample-ordering convention that this section checks:
#    it asserts the samples come back in the order they were written, nothing
#    about what they mean.
#
#    CODECS. The OpenEXR specification allows deep data to be compressed with
#    NONE, RLE, ZIPS and ZIP only; the block codecs (PIZ, PXR24, B44, B44A,
#    DWAA, DWAB) operate on fixed-size scanline blocks and have no defined
#    behaviour on variable-length deep sample data. All four permitted codecs
#    are gated here, for deep scanline and for deep tiled.
#
#    CONTROL. Before measuring this library, the reference is asked to
#    round-trip a deep image of its own making, as scanline and as tiled, with
#    the same comparison. A broken oracle and a real defect look identical, and
#    this repository has been fooled by both; if the control fails, this whole
#    section reports a skip rather than blaming the library.
# ---------------------------------------------------------------------------
section "external oracle (deep images: sample counts and per-sample values)"

DEEP_TOL=1e-6

# dump_deep <file> — the reference's per-pixel dump with the punctuation turned
# into separators, which is what scripts/deepdiff.awk parses.
dump_deep() { oiiotool --dumpdata "$1" 2>&1 | sed -e 's/[():,]/ /g'; }

if ! command -v oiiotool >/dev/null 2>&1; then
	skip "oiiotool not installed; deep images cannot be checked against the reference"
elif [ "$build_ok" = "0" ]; then
	skip "deep external oracle (build failed)"
else
	DCTL="$WORK/deepctl"
	DEEP="$WORK/deep"
	mkdir -p "$DCTL" "$DEEP"

	# --- the control -----------------------------------------------------
	# Two deep images at different depths, merged, so the reference's own
	# fixture has 0, 1 and 2 samples per pixel: a constant fixture would
	# compare equal against a reader that ignores the sample count table.
	# --deepen gives a pixel no samples where it is black, so the black corner
	# of the first and the black top edge of the second overlap in exactly one
	# pixel, which ends up with no samples at all.
	ctl_ok=1
	{
		oiiotool --pattern fill:topleft=0,0,0:topright=1,0,0:bottomleft=0,1,0:bottomright=0,0,1 \
			8x6 3 --chnames R,G,B --deepen -d half -o "$DCTL/a.exr" &&
			oiiotool --pattern fill:topleft=0,0,0:topright=0,0,0:bottomleft=1,1,1:bottomright=1,1,1 \
				8x6 3 --chnames R,G,B --deepen:z=2.5 -d half -o "$DCTL/b.exr" &&
			oiiotool "$DCTL/a.exr" "$DCTL/b.exr" --deepmerge -d half -o "$DCTL/ctl.exr" &&
			oiiotool "$DCTL/ctl.exr" -d half -o "$DCTL/ctl_scan.exr" &&
			oiiotool "$DCTL/ctl.exr" --tile 4 4 -d half -o "$DCTL/ctl_tile.exr" &&
			oiiotool "$DCTL/ctl.exr" "$DCTL/b.exr" --siappend -d half -o "$DCTL/ctl_mpscan.exr" &&
			oiiotool "$DCTL/ctl_tile.exr" "$DCTL/b.exr" --tile 4 4 --siappend -d half -o "$DCTL/ctl_mptile.exr"
	} >"$DCTL/gen.log" 2>&1 || ctl_ok=0

	if [ "$ctl_ok" = "0" ]; then
		skip "the reference could not build a deep control image: $(head -1 "$DCTL/gen.log")"
	else
		dump_deep "$DCTL/ctl.exr" >"$DCTL/ctl.dump"
		zeros=$(grep -c ' 0 samples' "$DCTL/ctl.dump")
		ones=$(grep -c ' 1 samples' "$DCTL/ctl.dump")
		twos=$(grep -c ' 2 samples' "$DCTL/ctl.dump")
		if [ "$zeros" -lt 1 ] || [ "$ones" -lt 1 ] || [ "$twos" -lt 1 ]; then
			skip "the deep control image has no varying sample counts ($zeros empty, $ones one-sample, $twos two-sample pixels); it would compare equal against anything"
			ctl_ok=0
		else
			note "control fixture: $zeros pixels with no samples, $ones with one, $twos with two"
		fi
	fi

	if [ "$ctl_ok" = "1" ]; then
		for form in scan tile; do
			dump_deep "$DCTL/ctl_$form.exr" >"$DCTL/ctl_$form.dump"
			if out=$(awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
				"$DCTL/ctl.dump" "$DCTL/ctl_$form.dump" 2>&1); then
				pass "control: the reference round-trips its own deep $form image sample for sample"
			else
				fail "control: the reference does not round-trip its own deep $form image; the oracle is broken, not this library
$out"
				ctl_ok=0
			fi
		done
	fi

	# --- the read direction ----------------------------------------------
	# The control images were written by the reference, not by this library, so
	# they are the one deep fixture here whose bytes this code had no hand in.
	# Reading them and comparing against the reference's own reading of the
	# same file gates the deep readers the way the fixtures below gate the
	# writers. This is how the "packed size equals unpacked size means the
	# block is stored raw" rule was found: without it every one of these files
	# was reported as corrupt ZIP data.
	if [ "$ctl_ok" = "1" ]; then
		for form in scan tile; do
			if ! go run ./scripts/deepgen -dump "$DCTL/ctl_$form.exr" >"$DCTL/ours_$form.dump" 2>"$DCTL/ours_$form.err"; then
				fail "read direction: this library cannot read the deep $form image the reference wrote: $(head -1 "$DCTL/ours_$form.err")"
				continue
			fi
			sed -e 's/[():,]/ /g' "$DCTL/ours_$form.dump" >"$DCTL/ours_$form.norm"
			if out=$(awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
				"$DCTL/ctl_$form.dump" "$DCTL/ours_$form.norm" 2>&1); then
				pass "read direction: this library reads the reference's own deep $form image sample for sample"
			else
				fail "read direction: this library reads the reference's deep $form image differently than the reference does:
$out"
			fi
		done

		# Multi-part deep, both parts. The two parts hold different sample
		# counts and different values, so a reader that quietly returns part 0
		# for every part is caught rather than passed.
		for form in mpscan mptile; do
			for part in 0 1; do
				oiiotool -a --dumpdata "$DCTL/ctl_$form.exr" 2>&1 |
					awk -v n="$part" '$1 == "subimage" && $2 + 0 == n { f = 1; next } $1 == "subimage" { f = 0 } f' |
					sed -e 's/[():,]/ /g' >"$DCTL/${form}_$part.ref"
				if ! [ -s "$DCTL/${form}_$part.ref" ]; then
					skip "the reference could not dump part $part of its own multi-part deep $form image"
					continue
				fi
				if ! go run ./scripts/deepgen -dump "$DCTL/ctl_$form.exr" "$part" \
					>"$DCTL/${form}_$part.ours" 2>"$DCTL/${form}_$part.err"; then
					fail "read direction: this library cannot read part $part of the reference's multi-part deep $form image: $(head -1 "$DCTL/${form}_$part.err")"
					continue
				fi
				sed -e 's/[():,]/ /g' "$DCTL/${form}_$part.ours" >"$DCTL/${form}_$part.norm"
				if out=$(awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
					"$DCTL/${form}_$part.ref" "$DCTL/${form}_$part.norm" 2>&1); then
					pass "read direction: multi-part deep $form, part $part, sample for sample"
				else
					fail "read direction: multi-part deep $form part $part reads differently than the reference reads it:
$out"
				fi
			done
		done
	fi

	if [ "$ctl_ok" = "0" ]; then
		skip "deep fixtures not measured: the control above did not hold"
	elif ! go run ./scripts/deepgen "$DEEP" >"$WORK/deepgen.log" 2>&1; then
		fail "scripts/deepgen could not write the deep fixtures"
		cat "$WORK/deepgen.log"
	else
		note "$(tail -1 "$WORK/deepgen.log") into $DEEP"
		note "counts and every sample value are compared against the reference's own reading"

		while IFS=$'\t' read -r file kind codec chans tile status; do
			case "$file" in \#* | "") continue ;; esac
			path="$DEEP/$file"
			label=$(printf '%-12s %-4s %-5s' "$kind" "$codec" "$(printf '%s' "$file" | sed -e 's/^d[st]_//' -e 's/_.*//')")

			# A codec the reference refuses for deep data must be refused by
			# this library too, rather than written into a file nothing else
			# can open. deepgen reports which happened; a row it managed to
			# write arrives with status "ok" and is measured below like any
			# other, against a reference that will not read it.
			if [ "$status" = "refused" ]; then
				pass "$label refused, as the reference refuses this codec for deep data (EXR_ERR_INVALID_ATTR)"
				continue
			fi

			# Structure first: the reference has to agree it is a deep image
			# of the right shape with the right channels, before its reading
			# of the samples means anything.
			info=$(oiiotool --info -v "$path" 2>&1)
			if ! printf '%s\n' "$info" | grep -q 'channel list:'; then
				fail "$label the reference cannot read the file: $(printf '%s\n' "$info" | grep -m1 -iE 'error|ERROR' | cut -c1-140)"
				fail "$label samples not compared (the reference could not open the file)"
				continue
			fi

			structure_ok=1
			printf '%s\n' "$info" | grep -q '13 x   20, 5 channel, deep' || structure_ok=0
			for c in $(printf '%s' "$chans" | tr ',' ' '); do
				printf '%s\n' "$info" | grep -q "channel list:.*\b$c\b" || structure_ok=0
			done
			if [ "$tile" != "-" ]; then
				# "4x4" in the manifest, "tile size: 4 x 4" in the reference's
				# report of the header it read.
				printf '%s\n' "$info" | grep -q "tile size: $(printf '%s' "$tile" | sed 's/x/ x /')" || structure_ok=0
			else
				printf '%s\n' "$info" | grep -q 'tile size:' && structure_ok=0
			fi
			if [ "$structure_ok" = "1" ]; then
				pass "$label structure: $(printf '%s\n' "$info" | grep -m1 -o '13 x   20, 5 channel, deep [a-z]*'), channels $chans, tile $tile"
			else
				fail "$label structure: the reference reads $(printf '%s\n' "$info" | sed -n '2,4p' | tr '\n' ' ' | cut -c1-140), expected 13x20 deep, channels $chans, tile $tile"
			fi

			dump_deep "$path" >"$DEEP/$file.dump"
			exp=$(sed -e 's/[():,]/ /g' "$path.expect")
			printf '%s\n' "$exp" >"$DEEP/$file.expnorm"
			if out=$(awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
				"$DEEP/$file.expnorm" "$DEEP/$file.dump" 2>&1); then
				pass "$label 260 pixels, 470 samples: every count and every sample value read back as written"
			else
				fail "$label the reference reads different deep data than was written:
$out"
			fi
		done <"$DEEP/manifest.tsv"
	fi
fi

# ---------------------------------------------------------------------------
		# ---- multi-level parts, read direction -----------------------------
		#
		# Recorded as ungatable because oiiotool writes a one-level file for
		# -o:mipmap=1 and drops levels on --siappend. It named the wrong tool.
		# exrmaketiled generates the levels and exrmultipart combines the
		# result into a multi-part file, both from OpenEXR itself, so the
		# fixture is reference-written end to end and this library only reads.
		#
		# Note for anyone reproducing: exrmultipart's "::partname" suffix
		# silently produces a file with headers and no pixel data. Without it
		# the same command succeeds. The check below would pass on such a file
		# — comparing nothing to nothing — so it asserts a sample count first.
		MLDIR=""
		if ! command -v exrmultipart >/dev/null 2>&1; then
			gap "multi-level multi-part read: exrmultipart is not installed"
		elif ! command -v exrmaketiled >/dev/null 2>&1; then
			gap "multi-level multi-part read: exrmaketiled is not installed"
		elif [ ! -x "$TILEDUMP" ]; then
			gap "multi-level multi-part read: exrtiledump could not be built against the reference"
		else
			MLDIR="$WORK/mplevels"
			mkdir -p "$MLDIR"
			# scripts/exrtileread is built by the tiled read section further
			# down, which has not run yet; build it here so the two sections
			# stay independent of each other's order.
			TILEREAD="$WORK/exrtileread"
			if [ ! -x "$TILEREAD" ] && ! go build -o "$TILEREAD" ./scripts/exrtileread/ 2>"$MLDIR/build.err"; then
				fail "multi-level multi-part read: could not build scripts/exrtileread: $(head -1 "$MLDIR/build.err")"
				MLDIR=""
			fi
		fi
		if [ -n "${MLDIR:-}" ] && [ -d "${MLDIR:-/nonexistent}" ]; then
			oiiotool --pattern noise:type=uniform 64x64 3 -d half -o "$MLDIR/flat.exr" >/dev/null 2>&1
			for mode in m r; do
				case $mode in
				m) label="mipmapped" ;;
				r) label="ripmapped" ;;
				esac
				if ! exrmaketiled "-$mode" -t 16 16 "$MLDIR/flat.exr" "$MLDIR/lv_$mode.exr" >/dev/null 2>&1; then
					gap "multi-level multi-part read: exrmaketiled would not write a $label file"
					continue
				fi
				if ! exrmultipart -combine -i "$MLDIR/flat.exr" -i "$MLDIR/lv_$mode.exr" \
					-o "$MLDIR/mp_$mode.exr" >/dev/null 2>&1; then
					gap "multi-level multi-part read: exrmultipart would not combine the $label file"
					continue
				fi
				"$TILEDUMP" -part 1 "$MLDIR/mp_$mode.exr" >"$MLDIR/$mode.ref" 2>/dev/null
				n=$(grep -vc '^#' "$MLDIR/$mode.ref")
				if [ "${n:-0}" -lt 1000 ]; then
					fail "multi-level multi-part read ($label): the fixture holds $n samples; it is empty, so nothing would be measured"
					continue
				fi
				if ! "$TILEREAD" -part 1 "$MLDIR/mp_$mode.exr" >"$MLDIR/$mode.got" 2>"$MLDIR/$mode.err"; then
					fail "multi-level multi-part read ($label): this library could not read it: $(head -1 "$MLDIR/$mode.err" | cut -c1-100)"
					continue
				fi
				line=$(awk -f "$REPO/scripts/tilecmp.awk" "$MLDIR/$mode.ref" "$MLDIR/$mode.got")
				lv=$(grep -c '^# level' "$MLDIR/$mode.ref")
				case "$line" in
				*"missing=0 extra=0 maxerr=0 "*)
					pass "multi-level multi-part read ($label): all $lv levels match the reference's own reading ($line)" ;;
				*)
					fail "multi-level multi-part read ($label): $line" ;;
				esac
			done
		fi

	# --- deep parts inside a multi-part file -------------------------------
	#
	# This library could not write one at all: MultiPartOutputFile exposed only
	# WritePixels and WriteTile, so the gap was in the writer rather than in the
	# fixtures. It now has SetDeepFrameBuffer and WriteDeepPixels, and the
	# multi-part version field sets the deep flag when any part is deep, which
	# it previously hardcoded false.
	#
	# The deep part is extracted with exrmultipart -separate before comparison,
	# because oiiotool's --dumpdata reads subimage 0 whatever --subimage is
	# given. The extraction is the reference copying its own chunks, so nothing
	# this library wrote does the reading.
	if ! command -v exrmultipart >/dev/null 2>&1; then
		gap "deep multi-part: exrmultipart is not installed"
	else
		MPD="$WORK/mpdeep"
		if ! err=$(go run ./scripts/mpdeepgen "$MPD" 2>&1); then
			fail "deep multi-part: this library could not write one: $(printf '%s' "$err" | head -1 | cut -c1-95)"
		elif ! exrmultipart -separate -i "$MPD/mp_deep.exr" -o "$MPD/sep" >/dev/null 2>&1; then
			fail "deep multi-part: the reference could not read the file back"
		elif [ ! -f "$MPD/sep.2.exr" ]; then
			fail "deep multi-part: the reference did not produce the deep part"
		else
			# Every part must survive, not only the deep one: a file whose flat
			# part is broken fails the same way and would otherwise look like a
			# deep problem.
			flat_ok=1
			for si in 0 1; do
				if oiiotool -i "$MPD/mp_deep.exr" --subimage "$si" --printinfo:verbose=1 2>&1 |
					grep -qiE 'corrupt|error'; then
					flat_ok=0
				fi
			done
			if [ "$flat_ok" = "1" ]; then
				pass "deep multi-part: the reference reads both the flat and the deep part"
			else
				fail "deep multi-part: the reference refuses a part of the file"
			fi

			# deepImageState: a claim about the samples that nothing in the
			# format checks. The fixture declares "tidy" and
			# VerifyDeepImageState confirms the samples satisfy it before the
			# claim is written, so what is asserted here is that the reference
			# reads the attribute back as a typed attribute rather than
			# dropping or mangling it.
			if exrheader "$MPD/mp_deep.exr" 2>&1 | grep -q "deepImageState (type deepImageState)"; then
				pass "deep multi-part: the reference reads the deepImageState attribute this library wrote"
			else
				fail "deep multi-part: the reference does not see a deepImageState attribute"
			fi
			if go test ./exr/ -run 'TestVerifyDeepImageState|TestDeepImageStateRoundTrips' >/dev/null 2>&1; then
				pass "deep sample semantics: a false deepImageState claim is caught rather than written"
			else
				fail "deep sample semantics: VerifyDeepImageState does not catch a false claim"
			fi

			dump_deep "$MPD/sep.2.exr" >"$MPD/got.dump"
			pix=$(grep -c 'Pixel' "$MPD/got.dump")
			if [ "${pix:-0}" -lt 100 ]; then
				fail "deep multi-part: the reference read $pix pixels from the deep part; it holds 117"
			elif sed -e 's/[():,]/ /g' "$MPD/mp_deep.txt" >"$MPD/want.dump" &&
				out=$(awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
					"$MPD/want.dump" "$MPD/got.dump" 2>&1); then
				pass "deep multi-part: every sample of the deep part reads back as written ($pix pixels)"
			else
				fail "deep multi-part: $(printf '%s' "$out" | head -2 | tr '\n' ' ' | cut -c1-110)"
			fi

			# Signal: the comparison must reject a dump that is wrong. A deep
			# chunk with no channels set produced exactly this shape — a well
			# formed chunk holding no samples, packed and unpacked sizes both
			# zero — and the reference reported only "Some scanline chunks were
			# missing or corrupted", naming neither the part nor the cause.
			sed 's/A=[0-9.]*/A=9.5/g' "$MPD/got.dump" >"$MPD/bad.dump"
			if awk -v TOL="$DEEP_TOL" -f scripts/deepdiff.awk \
				"$MPD/want.dump" "$MPD/bad.dump" >/dev/null 2>&1; then
				fail "deep multi-part signal check: deliberately wrong samples compared clean"
			else
				pass "deep multi-part signal check: the comparison reports wrong samples"
			fi
		fi
	fi

	# --- deep mipmap and ripmap levels -------------------------------------
	#
	# DeepTiledWriter wrote LevelModeOne only. Its offset table was sized for
	# one level and indexed by tileY*tilesX+tileX, so every level after the
	# first overwrote the first one's slots — while the reader had always
	# derived the index per level, so the two disagreed the moment a second
	# level existed. Both now use one derivation, deepTileChunkIndex.
	#
	# A second defect sat behind it: the writer captured the tile description
	# at construction, so a caller who asked for a mipmap through
	# SetTileDescription got a single-level file and no indication.
	#
	# The oracle is scripts/exrdeeptiledump rather than oiiotool, because
	# --selectmip does not compose with --dumpdata for deep images: every level
	# but the first comes back with no pixels, so a generator that wrote level
	# 3 into level 1's slots would pass anything built on it.
	DEEPTILE="$WORK/exrdeeptiledump"
	if ! command -v pkg-config >/dev/null 2>&1 || ! pkg-config --exists OpenEXR; then
		gap "deep tiled levels: OpenEXR development files are not installed"
	elif ! out=$(c++ -std=c++17 -O1 -o "$DEEPTILE" scripts/exrdeeptiledump/exrdeeptiledump.cpp \
		$(pkg-config --cflags --libs OpenEXR) 2>&1); then
		fail "deep tiled levels: could not build scripts/exrdeeptiledump: $(printf '%s' "$out" | head -1 | cut -c1-90)"
	else
		for mode in mipmap ripmap; do
			DLV="$WORK/deeplevels_$mode"
			case $mode in
			mipmap) arg="" ; stem="deep_mip.exr" ;;
			ripmap) arg="ripmap"; stem="deep_rip.exr" ;;
			esac
			if ! err=$(go run ./scripts/deepmipgen "$DLV" $arg 2>&1); then
				fail "deep tiled levels ($mode): this library could not write one: $(printf '%s' "$err" | head -1 | cut -c1-90)"
				continue
			fi
			if ! "$DEEPTILE" "$DLV/$stem" >"$DLV/got.dump" 2>"$DLV/err"; then
				fail "deep tiled levels ($mode): the reference refused it: $(head -1 "$DLV/err" | cut -c1-100)"
				continue
			fi
			if out=$(python3 scripts/deepmipcmp.py "$DLV/got.dump" "$DLV" "$stem" 2>&1); then
				pass "deep tiled levels ($mode): the reference reads every level exactly ($out)"
			else
				fail "deep tiled levels ($mode): $out"
			fi
		done

		# Signal: the comparison must reject samples that are wrong. Without
		# this the checks above are satisfied by a comparator that never fails.
		DLV="$WORK/deeplevels_mipmap"
		if [ -f "$DLV/got.dump" ]; then
			sed 's/A=[0-9.]*/A=9.5/g' "$DLV/got.dump" >"$DLV/bad.dump"
			if python3 scripts/deepmipcmp.py "$DLV/bad.dump" "$DLV" deep_mip.exr >/dev/null 2>&1; then
				fail "deep tiled levels signal check: deliberately wrong samples compared clean"
			else
				pass "deep tiled levels signal check: the comparison reports wrong samples"
			fi
		fi
	fi
		# ---- measured gaps ------------------------------------------------
# ---------------------------------------------------------------------------
# 8. The read direction for tiled files.
#
#    Sections 5 to 7 all run the same way round: this library writes, the
#    reference reads. That leaves the readers resting on round trips, and a
#    round trip cannot see a convention the reader and the writer share — which
#    is the exact shape of every defect this gate has found so far.
#
#    So here nothing this library wrote is involved. exrmaketiled writes the
#    fixture, scripts/exrtiledump reads it with libOpenEXR and prints every
#    sample of every level, scripts/exrtileread reads the same file with this
#    library and prints the same format, and scripts/tilecmp.awk compares them
#    by key. A missing sample and an invented one are reported separately from
#    a wrong one, so a reader that silently drops a level cannot pass on the
#    levels it did produce.
#
#    A control runs first — the two dumps of a file both tools agree on — and a
#    signal check confirms the comparator reports a difference when handed a
#    deliberately mismatched pair.
# ---------------------------------------------------------------------------
section "external oracle (tiled read direction: the reference writes, this library reads)"

if ! command -v exrmaketiled >/dev/null 2>&1; then
	skip "exrmaketiled not installed; the reference cannot write tiled fixtures (brew install openexr)"
elif ! command -v oiiotool >/dev/null 2>&1; then
	skip "oiiotool not installed; the base image for exrmaketiled cannot be generated"
elif [ "$build_ok" = "0" ]; then
	skip "tiled read direction (build failed)"
elif [ ! -x "$TILEDUMP" ]; then
	skip "tiled read direction (exrtiledump could not be built against the reference)"
else
	RDIR="$WORK/tiledread"
	mkdir -p "$RDIR"
	TILEREAD="$WORK/exrtileread"

	if ! go build -o "$TILEREAD" ./scripts/exrtileread/ 2>"$RDIR/build.err"; then
		fail "could not build scripts/exrtileread: $(head -1 "$RDIR/build.err")"
	else
		# Two base images: one whose dimensions are a whole number of tiles, and
		# one whose are not, so partial tiles on both edges are exercised.
		oiiotool --pattern noise:type=uniform 64x48 3 -d half -o "$RDIR/base_even.exr" >/dev/null 2>&1
		oiiotool --pattern noise:type=uniform 37x23 3 -d half -o "$RDIR/base_odd.exr" >/dev/null 2>&1
		oiiotool "$RDIR/base_even.exr" --origin +17-9 --fullpixels -o "$RDIR/base_off.exr" >/dev/null 2>&1

		if [ ! -s "$RDIR/base_even.exr" ] || [ ! -s "$RDIR/base_odd.exr" ]; then
			fail "oiiotool could not write the base images for the tiled read direction"
		else
			# ---- control: the oracle and this library must agree on a file
			# neither of them wrote, before any of it is believed.
			exrmaketiled -t 16 16 "$RDIR/base_even.exr" "$RDIR/ctl.exr" >/dev/null 2>&1
			if [ ! -s "$RDIR/ctl.exr" ]; then
				fail "control: exrmaketiled could not write a tiled file"
			else
				"$TILEDUMP" "$RDIR/ctl.exr" >"$RDIR/ctl.ref" 2>"$RDIR/ctl.referr"
				if [ ! -s "$RDIR/ctl.ref" ]; then
					fail "control: the reference could not read its own tiled file: $(head -1 "$RDIR/ctl.referr" | cut -c1-110)"
				else
					pass "control: the reference reads its own exrmaketiled output ($(grep -cv '^#' "$RDIR/ctl.ref") samples)"

					# ---- signal: the comparator must fail when it should.
					"$TILEDUMP" "$RDIR/ctl.exr" | sed 's/ \([0-9.eE+-]*\)$/ 99999/' >"$RDIR/ctl.wrong"
					sig=$(awk -f "$REPO/scripts/tilecmp.awk" "$RDIR/ctl.ref" "$RDIR/ctl.wrong")
					case "$sig" in
					*maxerr=0*) fail "signal: tilecmp reported no difference against deliberately wrong values ($sig)" ;;
					*) pass "signal: tilecmp reports the difference it is given ($sig)" ;;
					esac
				fi
			fi

			# ---- the matrix: level mode x compression x tile fit.
			for base in even odd off; do
				case $base in
				even) tile="16 16" ;;
				odd) tile="8 8" ;;
				off) tile="16 16" ;;
				esac
				[ -s "$RDIR/base_$base.exr" ] || continue
				for mode in one m r; do
					case $mode in
					one) flag="" ;;
					m) flag="-m" ;;
					r) flag="-r" ;;
					esac
					for codec in none zip piz b44; do
						name="${base}_${mode}_${codec}"
						# shellcheck disable=SC2086
						exrmaketiled $flag -t $tile -z $codec \
							"$RDIR/base_$base.exr" "$RDIR/$name.exr" >/dev/null 2>&1
						if [ ! -s "$RDIR/$name.exr" ]; then
							note "GAP: exrmaketiled would not write $name; no fixture, so nothing measured"
							continue
						fi
						"$TILEDUMP" "$RDIR/$name.exr" >"$RDIR/$name.ref" 2>/dev/null
						if [ ! -s "$RDIR/$name.ref" ]; then
							fail "read $name: the reference could not read the file it just wrote"
							continue
						fi
						if ! "$TILEREAD" "$RDIR/$name.exr" >"$RDIR/$name.got" 2>"$RDIR/$name.err"; then
							fail "read $name: this library could not read a file the reference wrote: $(head -1 "$RDIR/$name.err" | cut -c1-110)"
							continue
						fi
						line=$(awk -f "$REPO/scripts/tilecmp.awk" "$RDIR/$name.ref" "$RDIR/$name.got")
						missing=$(printf '%s' "$line" | sed -n 's/.*missing=\([0-9]*\).*/\1/p')
						extra=$(printf '%s' "$line" | sed -n 's/.*extra=\([0-9]*\).*/\1/p')
						maxerr=$(printf '%s' "$line" | sed -n 's/.*maxerr=\([^ ]*\).*/\1/p')
						if [ "$missing" != "0" ] || [ "$extra" != "0" ]; then
							fail "read $name: this library and the reference disagree about which samples exist ($line)"
						elif [ "$maxerr" != "0" ]; then
							fail "read $name: this library read a file the reference wrote and got different values ($line)"
						else
							pass "read $name: every sample of every level matches the reference's own reading ($line)"
						fi
					done
				done
			done

			# uint32, read direction.
			#
			# This was recorded as ungatable because oiiotool will not write a
			# uint EXR. It named the wrong tool: exrmaketiled tiles one
			# happily, and the base only has to exist — interopgen has already
			# written one for the pixel-type matrix. The tiled container and
			# the truth both still come from the reference; this library
			# supplies only the source samples, which both sides then read
			# independently.
			ubase="$FIX/wr_uint_none.exr"
			if [ ! -f "$ubase" ]; then
				gap "read uint tiled: no uint fixture to tile"
			elif ! exrmaketiled -t 16 16 "$ubase" "$RDIR/uint_one.exr" >/dev/null 2>&1; then
				gap "read uint tiled: exrmaketiled would not tile the uint fixture"
			else
				"$TILEDUMP" "$RDIR/uint_one.exr" >"$RDIR/uint_one.ref" 2>/dev/null
				if ! "$TILEREAD" "$RDIR/uint_one.exr" >"$RDIR/uint_one.got" 2>"$RDIR/uint_one.err"; then
					fail "read uint tiled: this library could not read it: $(head -1 "$RDIR/uint_one.err" | cut -c1-100)"
				else
					line=$(awk -f "$REPO/scripts/tilecmp.awk" "$RDIR/uint_one.ref" "$RDIR/uint_one.got")
					case "$line" in
					*"missing=0 extra=0 maxerr=0 "*)
						pass "read uint tiled: every sample matches the reference's own reading ($line)" ;;
					*)
						fail "read uint tiled: $line" ;;
					esac
				fi
			fi
		fi
	fi
fi

# ---------------------------------------------------------------------------
# 9. The read direction for multi-part files.
#
#    Same argument as section 8, and the same shape. oiiotool writes a
#    multi-part file, scripts/exrmpread reads it with this library and writes
#    one PFM per part per channel, and the reference is asked for the same
#    channel of the same part. The two are compared with every oiiotool
#    threshold pinned to zero, so PASS means bit-identical rather than close.
#
#    Note what the fixture itself measured: the reference refuses to write
#    parts that disagree about the display window, which is the rule
#    NewMultiPartWriter enforces as ErrConflictingAttributes. The fixtures
#    below therefore share a display window and differ in everything else.
# ---------------------------------------------------------------------------
section "external oracle (multi-part read direction: the reference writes, this library reads)"

if ! command -v oiiotool >/dev/null 2>&1; then
	skip "oiiotool not installed; the reference cannot write multi-part fixtures (brew install openimageio)"
elif [ "$build_ok" = "0" ]; then
	skip "multi-part read direction (build failed)"
else
	MDIR="$WORK/mpread"
	mkdir -p "$MDIR"
	MPREAD="$WORK/exrmpread"

	if ! go build -o "$MPREAD" ./scripts/exrmpread/ 2>"$MDIR/build.err"; then
		fail "could not build scripts/exrmpread: $(head -1 "$MDIR/build.err")"
	else
		# Two parts that differ in pixel type, channel layout and name, sharing
		# the display window the format requires them to share.
		oiiotool --pattern noise:type=uniform 48x32 3 -d half \
			--attrib oiio:subimagename beauty -o "$MDIR/a.exr" >/dev/null 2>&1
		oiiotool --pattern noise:type=uniform 48x32 1 -d float --chnames Z \
			--attrib oiio:subimagename depth -o "$MDIR/b.exr" >/dev/null 2>&1
		oiiotool --pattern noise:type=uniform 48x32 3 -d half --tile 16 16 \
			--compression zip --attrib oiio:subimagename tiled -o "$MDIR/c.exr" >/dev/null 2>&1

		oiiotool "$MDIR/a.exr" "$MDIR/b.exr" --siappend -o "$MDIR/mp_scan.exr" >/dev/null 2>&1
		oiiotool "$MDIR/a.exr" "$MDIR/c.exr" --siappend -o "$MDIR/mp_tiled.exr" >/dev/null 2>&1

		if [ ! -s "$MDIR/mp_scan.exr" ]; then
			fail "oiiotool could not write a multi-part fixture; the read direction cannot be measured"
		else
			# ---- control: the reference must be able to read back the part and
			# channel it just wrote, or a later FAILURE means nothing.
			mp_extract "$MDIR/mp_scan.exr" 0 0 R "$MDIR/ctl.pfm" >/dev/null 2>&1
			if [ ! -s "$MDIR/ctl.pfm" ]; then
				fail "control: the reference could not extract a channel from its own multi-part file"
			else
				pass "control: the reference reads a part and channel out of its own multi-part file"
			fi

			for f in mp_scan mp_tiled; do
				[ -s "$MDIR/$f.exr" ] || { note "GAP: oiiotool would not write $f.exr; nothing measured for it"; continue; }
				rm -rf "$MDIR/${f}_out"
				if ! "$MPREAD" "$MDIR/$f.exr" "$MDIR/${f}_out" >"$MDIR/$f.parts" 2>"$MDIR/$f.err"; then
					fail "read $f: this library could not read a multi-part file the reference wrote: $(head -1 "$MDIR/$f.err" | cut -c1-110)"
					continue
				fi

				# Structure: the part count this library reports must be the
				# count the reference wrote, so a reader that finds one part in
				# a two-part file fails here rather than passing on the part it
				# did find.
				ours=$(grep -c '^part ' "$MDIR/$f.parts")
				theirs=$(oiiotool --info -v "$MDIR/$f.exr" 2>/dev/null | grep -c '^ *subimage ')
				if [ "$theirs" = "0" ]; then
					theirs=$(exrheader "$MDIR/$f.exr" 2>/dev/null | grep -c '^ *name (type string)')
				fi
				if [ "$ours" = "$theirs" ] && [ "$ours" != "0" ]; then
					pass "read $f: this library finds the same $ours parts the reference wrote"
				else
					fail "read $f: this library found $ours parts, the reference wrote $theirs"
				fi

				# Pixels: every channel of every part, bit for bit.
				while read -r _ pnum _ _ _ _ _ _ chans; do
					for ch in $(printf '%s' "$chans" | tr ',' ' '); do
						ref="$MDIR/${f}_ref_p${pnum}_${ch}.pfm"
						got="$MDIR/${f}_out/p${pnum}_l0_${ch}.pfm"
						if [ ! -s "$got" ]; then
							fail "read $f part $pnum channel $ch: this library produced no samples"
							continue
						fi
						out=$(mp_extract "$MDIR/$f.exr" "$pnum" 0 "$ch" "$ref")
						if [ ! -s "$ref" ]; then
							fail "read $f part $pnum channel $ch: the reference could not extract it ($(mp_reason "$out"))"
							continue
						fi
						d=$(mp_diff "$ref" "$got")
						case "$(mp_verdict "$d")" in
						PASS) pass "read $f part $pnum channel $ch: bit-identical to the reference's own reading" ;;
						FAILURE) fail "read $f part $pnum channel $ch: this library read a file the reference wrote and got different values (max error $(mp_maxerr "$d"))" ;;
						*) fail "read $f part $pnum channel $ch: the reference could not compare ($(mp_reason "$d"))" ;;
						esac
					done
				done <"$MDIR/$f.parts"
			done

			# ---- signal: comparing two channels that genuinely differ must
			# report FAILURE, or every PASS above is worthless.
			if [ -s "$MDIR/mp_scan_out/p0_l0_R.pfm" ]; then
				mp_extract "$MDIR/mp_scan.exr" 0 0 G "$MDIR/sig.pfm" >/dev/null 2>&1
				sd=$(mp_diff "$MDIR/sig.pfm" "$MDIR/mp_scan_out/p0_l0_R.pfm")
				case "$(mp_verdict "$sd")" in
				FAILURE) pass "signal: the reference reports a difference between two channels that differ" ;;
				*) fail "signal: the reference reported no difference between two channels that differ; the comparison above proves nothing" ;;
				esac
			fi

		fi
	fi
fi



# ---------------------------------------------------------------------------
# 10. Viewport reads: a rectangle, resolved to byte ranges and decoded without
#     touching the rest of the file.
#
#     Sections 8 and 9 read whole files. This one reads part of one, which is
#     the capability the chunk offset table and the codestream packet index
#     were built for: File.ReadRegion turns a rectangle into the chunks that
#     hold it by reading chunk headers, fetches only those chunks, and for
#     HTJ2K decodes only the code-blocks the rectangle can reach.
#
#     Two things have to hold at once and each is checked separately, because
#     either alone is easy and worthless. The samples must be the ones
#     libOpenEXR reads for the same rectangle — scripts/exrtiledump prints
#     every sample of the file and the viewport dump is compared against the
#     part of it inside the rectangle, by key, so a missing sample and an
#     invented one are reported apart from a wrong one. And the read must
#     genuinely cost less: a viewport that decompressed everything and cropped
#     would satisfy the first check completely.
#
#     The HTJ2K fixture is 512x512 in 256x256 tiles with its data window at
#     (13, -7). Those numbers are load-bearing and scripts/viewportgen records
#     why: the reference codec's HTJ2K parameters are fixed at 128x32
#     code-blocks and five decompositions, so a code-block's influence spans
#     about 64 samples at the lowest resolution and inside a smaller tile
#     nothing can ever be skipped. The offset window is there because image,
#     tile and codestream coordinates all coincide when a window starts at
#     (0, 0).
# ---------------------------------------------------------------------------
section "external oracle (viewport reads: a rectangle costs a rectangle)"

VPDIR="$WORK/viewport"
VIEWPORT="$WORK/exrviewport"
VPGEN="$WORK/viewportgen"
mkdir -p "$VPDIR"

if [ ! -x "$TILEDUMP" ]; then
	skip "exrtiledump could not be built against the reference; a viewport read has no oracle"
elif ! go build -o "$VIEWPORT" ./scripts/exrviewport/ 2>"$VPDIR/build.err"; then
	fail "viewport: could not build scripts/exrviewport: $(head -1 "$VPDIR/build.err")"
elif ! go build -o "$VPGEN" ./scripts/viewportgen/ 2>"$VPDIR/build.err"; then
	fail "viewport: could not build scripts/viewportgen: $(head -1 "$VPDIR/build.err")"
else
	# The viewport, in image coordinates. The data window starts at (13, -7),
	# so window-relative this is 200..327 by 32..159 — it straddles the
	# boundary between the two tiles of the top row and reaches neither edge
	# of either.
	VX0=213; VY0=25; VX1=340; VY1=152
	RX0=200; RY0=32; RX1=327; RY1=159

	if ! "$VPGEN" "$VPDIR" >"$VPDIR/gen.log" 2>&1; then
		fail "viewport: viewportgen would not write the fixture: $(head -1 "$VPDIR/gen.log")"
	elif ! "$TILEDUMP" "$VPDIR/vp_htj2k.exr" >"$VPDIR/truth.dump" 2>"$VPDIR/dump.err"; then
		fail "viewport: the reference would not read the HTJ2K fixture: $(head -1 "$VPDIR/dump.err")"
	else
		pass "viewport: the reference reads the 512x512 HTJ2K fixture written for this section"

		# ---- control: the whole image, through the same path ---------------
		#
		# Without this, every check below is satisfied by a reader that is
		# wrong everywhere in the same way, since the expectation is filtered
		# from a dump of the same file.
		if "$VIEWPORT" "$VPDIR/vp_htj2k.exr" 13 -7 524 504 >"$VPDIR/whole.dump" 2>"$VPDIR/whole.err"; then
			awk '!/^#/' "$VPDIR/truth.dump" >"$VPDIR/truth.level0"
			awk '!/^#/' "$VPDIR/whole.dump" >"$VPDIR/whole.samples"
			tilecmp "$VPDIR/truth.level0" "$VPDIR/whole.samples"
			if [ "$CMP_MISSING" = 0 ] && [ "$CMP_EXTRA" = 0 ] && [ "$CMP_MAXERR" = "0" ]; then
				pass "viewport control: the whole data window read as one region matches the reference exactly ($CMP_SAMPLES samples)"
			else
				fail "viewport control: whole-window read differs: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
			fi
		else
			fail "viewport control: exrviewport would not read the whole window: $(head -1 "$VPDIR/whole.err")"
		fi

		# ---- the viewport itself -------------------------------------------
		if "$VIEWPORT" "$VPDIR/vp_htj2k.exr" $VX0 $VY0 $VX1 $VY1 >"$VPDIR/vp.dump" 2>"$VPDIR/vp.err"; then
			awk -v x0=$RX0 -v y0=$RY0 -v x1=$RX1 -v y1=$RY1 \
				'!/^#/ && $1==0 && $2==0 && $3>=x0 && $3<=x1 && $4>=y0 && $4<=y1' \
				"$VPDIR/truth.dump" >"$VPDIR/vp.expect"
			awk '!/^#/' "$VPDIR/vp.dump" >"$VPDIR/vp.samples"
			nexp=$(wc -l <"$VPDIR/vp.expect" | tr -d ' ')
			if [ "$nexp" -eq 0 ]; then
				fail "viewport: the expectation is empty; nothing was compared"
			else
				tilecmp "$VPDIR/vp.expect" "$VPDIR/vp.samples"
				if [ "$CMP_MISSING" = 0 ] && [ "$CMP_EXTRA" = 0 ] && [ "$CMP_MAXERR" = "0" ]; then
					pass "viewport: a 128x128 rectangle of a 512x512 HTJ2K file matches the reference exactly ($CMP_SAMPLES samples)"
				else
					fail "viewport: rectangle differs: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
				fi
			fi

			# ---- and it must cost less --------------------------------------
			read_chunks=$(awk '/^# chunks/ {print $3}' "$VPDIR/vp.dump")
			all_chunks=$(awk '/^# chunks/ {print $4}' "$VPDIR/vp.dump")
			read_bytes=$(awk '/^# filebytes/ {print $3}' "$VPDIR/vp.dump")
			all_bytes=$(awk '/^# filebytes/ {print $4}' "$VPDIR/vp.dump")
			cb_decoded=$(awk '/^# codeblocks/ {print $3}' "$VPDIR/vp.dump")
			cb_skipped=$(awk '/^# codeblocks/ {print $4}' "$VPDIR/vp.dump")
			if [ "${read_chunks:-0}" -lt "${all_chunks:-0}" ] && [ "${read_bytes:-0}" -lt "${all_bytes:-0}" ]; then
				pass "viewport: it read $read_chunks of $all_chunks chunks and $read_bytes of $all_bytes file bytes"
			else
				fail "viewport: it read $read_chunks of $all_chunks chunks and $read_bytes of $all_bytes file bytes; a rectangle must read less than the file"
			fi
			if [ "${cb_skipped:-0}" -gt 0 ]; then
				pass "viewport: the block coder ran on $cb_decoded code-block bytes and skipped $cb_skipped inside the chunks it did read"
			else
				fail "viewport: no code-block data was skipped; the chunks were decoded whole and cropped"
			fi
		else
			fail "viewport: exrviewport would not read the rectangle: $(head -1 "$VPDIR/vp.err")"
		fi

		# ---- signal: the comparison must reject wrong samples ---------------
		if [ -s "$VPDIR/vp.samples" ]; then
			awk '{ $6 = $6 + 0.5; print }' "$VPDIR/vp.samples" >"$VPDIR/vp.bad"
			tilecmp "$VPDIR/vp.expect" "$VPDIR/vp.bad"
			if [ "$CMP_MAXERR" = "0" ]; then
				fail "viewport signal check: deliberately wrong samples compared clean"
			else
				pass "viewport signal check: the comparison reports wrong samples (maxerr=$CMP_MAXERR)"
			fi
		fi
	fi

	# ---- the precinct opt-in --------------------------------------------
	#
	# A precinct partition is the one thing this library will write that
	# libOpenEXR's compressor would not have produced, so it is opt-in and the
	# default is untouched. Three things have to hold and each is checked
	# separately.
	#
	# The default must be unchanged: the file written without the option must
	# still be byte-for-byte what it was, which the whole HTJ2K section above
	# already asserts against the reference. Here that is pinned by the two
	# files differing at all — if they did not, the option is being ignored and
	# every other check is vacuous.
	#
	# The reference must still read it. This is the question, not an
	# assumption: a conforming HTJ2K codestream with a precinct partition is
	# something the reference decoder has never been asked for by its own
	# encoder. It reads it exactly.
	#
	# And it must buy something, or it is a gratuitous deviation.
	if [ ! -x "$TILEDUMP" ]; then
		skip "precinct opt-in: exrtiledump could not be built against the reference"
	elif [ ! -f "$VPDIR/vp_htj2k_prec.exr" ] || [ ! -f "$VPDIR/vp_htj2k_1ch.exr" ]; then
		fail "precinct opt-in: the fixtures were not written"
	elif cmp -s "$VPDIR/vp_htj2k_prec.exr" "$VPDIR/vp_htj2k_1ch.exr"; then
		fail "precinct opt-in: the two files are byte-identical; the option was ignored"
	elif ! "$TILEDUMP" "$VPDIR/vp_htj2k_1ch.exr" >"$VPDIR/prec_plain.dump" 2>"$VPDIR/prec.err"; then
		fail "precinct opt-in: the reference would not read the default file: $(head -1 "$VPDIR/prec.err")"
	elif ! "$TILEDUMP" "$VPDIR/vp_htj2k_prec.exr" >"$VPDIR/prec_prec.dump" 2>"$VPDIR/prec.err"; then
		fail "precinct opt-in: the reference would not read the precinct file: $(head -1 "$VPDIR/prec.err")"
	else
		tilecmp "$VPDIR/prec_plain.dump" "$VPDIR/prec_prec.dump"
		if [ "$CMP_MISSING" = 0 ] && [ "$CMP_EXTRA" = 0 ] && [ "$CMP_MAXERR" = "0" ]; then
			psz=$(wc -c <"$VPDIR/vp_htj2k_prec.exr" | tr -d " ")
			dsz=$(wc -c <"$VPDIR/vp_htj2k_1ch.exr" | tr -d " ")
			pass "precinct opt-in: libOpenEXR reads a precinct-partitioned file exactly ($CMP_SAMPLES samples identical to the default file), for $dsz -> $psz bytes"
		else
			fail "precinct opt-in: the reference read the precinct file differently: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
		fi

		# And the saving it was written for.
		if out=$(go test ./compression/ -run "TestHTJ2KPrecinctsBuyAddressability|TestHTJ2KDefaultOutputIsUnchangedByTheOptionsType" -v 2>&1); then
			line=$(printf '%s\n' "$out" | sed -n "s/.*\(512x512 chunk: .*\)\$/\1/p" | head -1)
			pass "precinct opt-in: it buys a smaller region decode and the default output is unchanged (${line:-measured})"
		else
			fail "precinct opt-in: $(printf '%s\n' "$out" | grep -E "htj2k_precinct_test" | head -1 | cut -c1-110)"
		fi
	fi

	# ---- a rectangle of a mipmap level -----------------------------------
	#
	# This is the answer for HD playback of a 5K plate, and it is worth being
	# explicit about why it rather than the codestream's own resolution levels.
	# A mipmap level is computed by a real downsample filter when the file is
	# written, so it is a proper image; a reduced-resolution decode of a float
	# chunk averages reinterpreted bit patterns and is not. The pyramid is the
	# mechanism, and this checks that a rectangle of one level comes back
	# exactly.
	#
	# The fixture is the mipmapped HTJ2K file the tiled section already writes
	# and the reference already reads level by level, so nothing new is being
	# trusted here beyond the level argument itself.
	MIPFIX="$TDIR/t_mip_half_htj2k256.exr"
	if [ ! -f "$MIPFIX" ]; then
		skip "mipmap level viewport: the mipmapped HTJ2K fixture was not written"
	elif ! "$TILEDUMP" "$MIPFIX" >"$VPDIR/mip_truth.dump" 2>"$VPDIR/mip.err"; then
		fail "mipmap level viewport: the reference would not read the fixture: $(head -1 "$VPDIR/mip.err")"
	else
		mip_fail=0
		mip_ran=0
		for lv in 0 1 2; do
			if ! "$VIEWPORT" -level "$lv" "$MIPFIX" 0 0 7 7 >"$VPDIR/mip_vp$lv.dump" 2>"$VPDIR/mip_vp.err"; then
				fail "mipmap level viewport, level $lv: $(head -1 "$VPDIR/mip_vp.err")"
				mip_fail=1
				continue
			fi
			awk -v lv="$lv" '''!/^#/ && $1==lv && $2==lv && $3<=7 && $4<=7''' \
				"$VPDIR/mip_truth.dump" >"$VPDIR/mip_exp$lv"
			awk '''!/^#/''' "$VPDIR/mip_vp$lv.dump" >"$VPDIR/mip_got$lv"
			if [ ! -s "$VPDIR/mip_exp$lv" ]; then
				fail "mipmap level viewport, level $lv: the expectation is empty"
				mip_fail=1
				continue
			fi
			mip_ran=$((mip_ran + 1))
			tilecmp "$VPDIR/mip_exp$lv" "$VPDIR/mip_got$lv"
			if [ "$CMP_MISSING" != 0 ] || [ "$CMP_EXTRA" != 0 ] || [ "$CMP_MAXERR" != "0" ]; then
				fail "mipmap level viewport, level $lv: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
				mip_fail=1
			fi
		done
		if [ "$mip_ran" -eq 0 ]; then
			fail "mipmap level viewport: nothing was compared"
		elif [ "$mip_fail" -eq 0 ]; then
			pass "mipmap level viewport: an 8x8 rectangle of $mip_ran mipmap levels matches the reference exactly, each read from that level's own chunks"
		fi
	fi

	# ---- what the encoder will not write --------------------------------
	#
	# Quality layers would be the mechanism for playing a 5K plate at a lower
	# bitrate straight from the original file, and the mechanism works: on
	# half-float content, the first of three rate-allocated layers is 23.5% of
	# the code-block data at 0.8% worst-case error. What stops it is that
	# libOpenEXR's HTJ2K support is OpenJPH, which refuses any codestream with
	# more than one layer — measured, by writing one and handing it to the
	# reference:
	#
	#   ojph error 0x00030053: The current implementation supports 1 quality
	#   layer only. This codestream has 4 quality layers
	#
	# So the option refuses rather than producing a file nothing else opens.
	# The refusal is checked here beside a control, because a validator that
	# rejects everything would satisfy half of it.
	if out=$(go test ./compression/ -run "TestHTJ2KQualityLayersAreRefused" -v 2>&1); then
		pass "encoder deviations: a multi-layer chunk is refused, with the reference's own error as the reason, and precinct and default requests still work"
	else
		fail "encoder deviations: $(printf '%s\n' "$out" | grep -E "htj2k_precinct_test" | head -1 | cut -c1-110)"
	fi

	# ---- a rectangle of a scanline part, on a file the reference wrote ----
	#
	# Scanline is the format's default storage and most EXRs in the world use
	# it; ReadRegion refused those outright until now. The geometry differs
	# from a tiled part in both directions: a chunk is the full width of the
	# data window by 32 or 256 rows, so a viewport pulls whole rows and the
	# chunk-level saving is weaker — while for HTJ2K the viewport is a small
	# part of a very wide chunk, which is where the codestream saving is
	# largest.
	#
	# The fixture here is written by oiiotool, so nothing this library produced
	# is involved, and scripts/exrpartdump reads it with libOpenEXR. Its
	# coordinates are the channel's own, which for unsubsampled channels are
	# the data window's, so the two dumps share a key space after the
	# reordering awk does below.
	# exrpartdump is built by the multi-part section above, which may not have
	# run; build it here so the two sections stay independent of each other's
	# order.
	PARTDUMP="${PARTDUMP:-$WORK/exrpartdump}"
	if [ ! -x "$PARTDUMP" ] && command -v pkg-config >/dev/null 2>&1 && pkg-config --exists OpenEXR; then
		c++ -std=c++17 -O1 -o "$PARTDUMP" scripts/exrpartdump/exrpartdump.cpp \
			$(pkg-config --cflags --libs OpenEXR) >"$VPDIR/pd.err" 2>&1 || true
	fi
	if ! command -v oiiotool >/dev/null 2>&1; then
		skip "scanline viewport: oiiotool is not installed (brew install openimageio)"
	elif [ ! -x "$PARTDUMP" ]; then
		skip "scanline viewport: exrpartdump could not be built against the reference"
	elif ! oiiotool --pattern noise:type=uniform 512x512 3 -d half -o "$VPDIR/scan.exr" >"$VPDIR/scan.log" 2>&1; then
		fail "scanline viewport: oiiotool would not write the fixture: $(head -1 "$VPDIR/scan.log")"
	elif ! "$PARTDUMP" "$VPDIR/scan.exr" >"$VPDIR/scan_truth.dump" 2>"$VPDIR/scan_dump.err"; then
		fail "scanline viewport: the reference would not read it: $(head -1 "$VPDIR/scan_dump.err")"
	elif ! "$VIEWPORT" "$VPDIR/scan.exr" 100 100 227 227 >"$VPDIR/scan_vp.dump" 2>"$VPDIR/scan_vp.err"; then
		fail "scanline viewport: exrviewport would not read the rectangle: $(head -1 "$VPDIR/scan_vp.err")"
	else
		# exrpartdump prints "<channel> <x> <y> <value>"; tilecmp wants
		# "<lx> <ly> <x> <y> <channel> <value>".
		awk '''!/^#/ && NF==4 && $2>=100 && $2<=227 && $3>=100 && $3<=227 {
			print 0, 0, $2, $3, $1, $4 }''' "$VPDIR/scan_truth.dump" >"$VPDIR/scan_vp.expect"
		awk '''!/^#/''' "$VPDIR/scan_vp.dump" >"$VPDIR/scan_vp.samples"
		if [ ! -s "$VPDIR/scan_vp.expect" ]; then
			fail "scanline viewport: the expectation is empty; nothing was compared"
		else
			tilecmp "$VPDIR/scan_vp.expect" "$VPDIR/scan_vp.samples"
			if [ "$CMP_MISSING" = 0 ] && [ "$CMP_EXTRA" = 0 ] && [ "$CMP_MAXERR" = "0" ]; then
				sread=$(awk '''/^# chunks/ {print $3}''' "$VPDIR/scan_vp.dump")
				sall=$(awk '''/^# chunks/ {print $4}''' "$VPDIR/scan_vp.dump")
				sb=$(awk '''/^# filebytes/ {print $3}''' "$VPDIR/scan_vp.dump")
				sbt=$(awk '''/^# filebytes/ {print $4}''' "$VPDIR/scan_vp.dump")
				if [ "${sread:-0}" -lt "${sall:-0}" ] && [ "${sb:-0}" -lt "${sbt:-0}" ]; then
					pass "scanline viewport: a rectangle of a file oiiotool wrote matches the reference exactly ($CMP_SAMPLES samples), reading $sread of $sall chunks and $sb of $sbt bytes"
				else
					fail "scanline viewport: samples match but it read $sread of $sall chunks and $sb of $sbt bytes; a rectangle must read less"
				fi
			else
				fail "scanline viewport: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
			fi
		fi
	fi

	# ---- reduced-resolution decode of a chunk ----------------------------
	#
	# The other dimension the codestream offers. libOpenEXR has no reduced
	# decode to compare against — a chunk decompresses whole there, which is
	# what makes this an extension — so the oracle is one layer down: an HTJ2K
	# chunk is a JPEG 2000 codestream and ojph_expand -skip_res reconstructs
	# exactly the resolution this library is asked for. scripts/exrreduce reads
	# the tile through the EXR API and writes the codestream out beside its
	# result, so the two decoders are compared on the same bytes.
	#
	# What is NOT asserted is that the result looks like a downsample. It does
	# not, and the reference agrees it does not: an EXR chunk carries float
	# samples as reinterpreted bit patterns, so the wavelet runs over bit
	# patterns and the LL band is a log-domain average. On a ramp over [0, 2)
	# one level of reduction produces values from 2.2e-23 to 17.75, from this
	# library and from ojph_expand alike. Comparing against a downsample
	# instead of against the reference is exactly what had this capability
	# refused for a year, so the gate compares against the reference.
	REDUCE="$WORK/exrreduce"
	if ! command -v ojph_expand >/dev/null 2>&1; then
		skip "reduced-resolution chunk decode: ojph_expand is not installed (brew install openjph)"
	elif [ ! -f "$VPDIR/vp_htj2k_1ch.exr" ]; then
		fail "reduced-resolution chunk decode: the single-channel fixture was not written"
	elif ! go build -o "$REDUCE" ./scripts/exrreduce/ 2>"$VPDIR/red.err"; then
		fail "reduced-resolution chunk decode: could not build scripts/exrreduce: $(head -1 "$VPDIR/red.err")"
	else
		rd_fail=0
		rd_ran=0
		rd_line=""
		for r in 0 1 2 3; do
			if ! out=$("$REDUCE" "$VPDIR/vp_htj2k_1ch.exr" 0 0 "$r" \
				"$VPDIR/red_ours$r.pfm" "$VPDIR/red_cs.j2c" 2>&1); then
				fail "reduced-resolution chunk decode r=$r: $(printf '%s\n' "$out" | head -1)"
				rd_fail=1
				continue
			fi
			if [ "$r" = "0" ]; then
				ojph_expand -i "$VPDIR/red_cs.j2c" -o "$VPDIR/red_ref$r.pfm" >/dev/null 2>&1
			else
				ojph_expand -i "$VPDIR/red_cs.j2c" -o "$VPDIR/red_ref$r.pfm" -skip_res "$r" >/dev/null 2>&1
			fi
			if [ ! -s "$VPDIR/red_ref$r.pfm" ]; then
				gap "reduced-resolution chunk decode r=$r: the oracle would not produce it"
				continue
			fi
			rd_ran=$((rd_ran + 1))
			if ! d=$("$REDUCE" cmp "$VPDIR/red_ref$r.pfm" "$VPDIR/red_ours$r.pfm" 2>&1); then
				fail "reduced-resolution chunk decode r=$r: $d"
				rd_fail=1
			elif [ "$r" != "0" ]; then
				rd_line=$(printf '%s\n' "$out" | head -1)
			fi
		done
		if [ "$rd_ran" -eq 0 ]; then
			fail "reduced-resolution chunk decode: nothing was compared"
		elif [ "$rd_fail" -eq 0 ]; then
			pass "reduced-resolution chunk decode: $rd_ran levels bit-identical to ojph_expand -skip_res on the same codestream (${rd_line:-measured})"
		fi
	fi

	# ---- the same read, on a file the reference wrote --------------------
	#
	# Everything above starts from a file this library produced. A viewport
	# read that agreed with the reference only on this library's own output
	# would be the same shape of defect the read-direction sections exist to
	# catch, so the same rectangle is read out of a ZIP file written entirely
	# by OpenEXR's own tools.
	if ! command -v exrmaketiled >/dev/null 2>&1; then
		skip "viewport on a reference-written file: exrmaketiled is not installed (brew install openexr)"
	elif ! command -v oiiotool >/dev/null 2>&1; then
		skip "viewport on a reference-written file: oiiotool is not installed (brew install openimageio)"
	elif ! oiiotool --pattern noise:type=uniform 512x512 3 -d half -o "$VPDIR/flat.exr" >"$VPDIR/oiio.log" 2>&1; then
		fail "viewport on a reference-written file: oiiotool would not write the source: $(head -1 "$VPDIR/oiio.log")"
	elif ! exrmaketiled -t 256 256 "$VPDIR/flat.exr" "$VPDIR/vp_zip.exr" >"$VPDIR/mk.log" 2>&1; then
		fail "viewport on a reference-written file: exrmaketiled would not tile it: $(head -1 "$VPDIR/mk.log")"
	elif ! "$TILEDUMP" "$VPDIR/vp_zip.exr" >"$VPDIR/ztruth.dump" 2>"$VPDIR/zdump.err"; then
		fail "viewport on a reference-written file: the reference would not read it back: $(head -1 "$VPDIR/zdump.err")"
	elif ! "$VIEWPORT" "$VPDIR/vp_zip.exr" 200 32 327 159 >"$VPDIR/zvp.dump" 2>"$VPDIR/zvp.err"; then
		fail "viewport on a reference-written file: exrviewport would not read the rectangle: $(head -1 "$VPDIR/zvp.err")"
	else
		awk '!/^#/ && $1==0 && $2==0 && $3>=200 && $3<=327 && $4>=32 && $4<=159' \
			"$VPDIR/ztruth.dump" >"$VPDIR/zvp.expect"
		awk '!/^#/' "$VPDIR/zvp.dump" >"$VPDIR/zvp.samples"
		if [ ! -s "$VPDIR/zvp.expect" ]; then
			fail "viewport on a reference-written file: the expectation is empty; nothing was compared"
		else
			tilecmp "$VPDIR/zvp.expect" "$VPDIR/zvp.samples"
			if [ "$CMP_MISSING" = 0 ] && [ "$CMP_EXTRA" = 0 ] && [ "$CMP_MAXERR" = "0" ]; then
				pass "viewport: a rectangle of a file exrmaketiled wrote matches the reference exactly ($CMP_SAMPLES samples)"
			else
				fail "viewport on a reference-written file: missing=$CMP_MISSING extra=$CMP_EXTRA maxerr=$CMP_MAXERR at=$CMP_AT"
			fi
		fi
		zread=$(awk '/^# chunks/ {print $3}' "$VPDIR/zvp.dump")
		zall=$(awk '/^# chunks/ {print $4}' "$VPDIR/zvp.dump")
		zskip=$(awk '/^# codeblocks/ {print $4}' "$VPDIR/zvp.dump")
		if [ "${zread:-0}" -lt "${zall:-0}" ] && [ "${zskip:-1}" -eq 0 ]; then
			pass "viewport: a ZIP file saves at the chunk level only — $zread of $zall chunks, and no claimed code-block saving"
		else
			fail "viewport: ZIP read $zread of $zall chunks and claimed $zskip skipped code-block bytes; a ZIP chunk has no addressable interior"
		fi
	fi
fi



section "result"
echo "checks run: $checked, failures: $failures, skipped: $skips"
if [ "$failures" -ne 0 ]; then
	echo "VALIDATION FAILED"
	exit 1
fi
echo "VALIDATION PASSED"
