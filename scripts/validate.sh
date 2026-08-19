#!/usr/bin/env bash
#
# validate.sh — the gate this repository has to pass.
#
# It runs two kinds of check, and the second is the one that matters:
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

		# ---- measured gaps ------------------------------------------------
		# Recorded rather than omitted: a row nobody writes down is a row
		# nobody notices is missing.
		note "GAP: deep parts in a multi-part file are not gated — MultiPartOutputFile exposes only WritePixels and WriteTile, so this library cannot write a deepscanline or deeptiled part into a multi-part file at all"
		note "GAP: ripmapped tiled parts inside a multi-part file are not gated — the mipmapped part above covers one level per step, but a ripmap's independent x and y levels are a different chunk offset table and are unexercised"
		note "GAP: subsampled channels (XSampling or YSampling above 1) are not gated in multi-part parts"
		note "GAP: the multi-part READ direction is not gated — the reference can write multi-part files (the control above is one), but nothing here asks this library to read one back and compare, so MultiPartInputFile rests on round trips"
	fi
fi

# ---------------------------------------------------------------------------
section "result"
echo "checks run: $checked, failures: $failures, skipped: $skips"
if [ "$failures" -ne 0 ]; then
	echo "VALIDATION FAILED"
	exit 1
fi
echo "VALIDATION PASSED"
