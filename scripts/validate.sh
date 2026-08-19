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
section "result"
echo "checks run: $checked, failures: $failures, skipped: $skips"
if [ "$failures" -ne 0 ]; then
	echo "VALIDATION FAILED"
	exit 1
fi
echo "VALIDATION PASSED"
