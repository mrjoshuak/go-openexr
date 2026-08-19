# deepdiff.awk — compare a deep image sample for sample against what this
# library meant to write.
#
#   awk -v TOL=1e-6 -f scripts/deepdiff.awk expected.txt reference.txt
#
# Both arguments are streams of lines in the shape oiiotool --dumpdata prints
# for a deep image, with the punctuation already turned into spaces by the
# caller (sed 's/[():,]/ /g'), so a line reads:
#
#   Pixel 3 1 2 samples A=0.5 Z=1.25 / A=0.625 Z=1.375
#
# The first file is the expected reading, the second is the reading being
# checked. In the write direction the expectation is what scripts/deepgen meant
# to write and the second file is what the OpenEXR reference implementation read
# back; in the read direction the expectation is the reference's own reading of
# one of its own files and the second file is this library's reading of it.
# Every pixel must appear in both, with the same sample count, the same channel
# names on every sample, and values agreeing to TOL. Samples are matched by
# position and channels by name, because OpenImageIO reorders RGB channels for
# display while preserving which value belongs to which name.
#
# TOL exists only because the reference prints decimal text: oiiotool rounds a
# float to about eight significant digits, so a bit-identical value can differ
# in the last printed place. The fixture's values are spaced at least 1/128
# apart, so any real defect is millions of times larger than TOL.
#
# It prints one line per disagreement (at most LIMIT of them) and exits 1 if
# there was any, 0 only if every pixel of the expectation was found and matched.

BEGIN {
	tol = TOL + 0
	if (LIMIT == "") LIMIT = 8
	bad = 0
	epix = 0
	dpix = 0
}

function report(msg) {
	bad++
	if (bad <= LIMIT) print "    " msg
}

$1 == "Pixel" {
	key = $2 "," $3
	n = $4
	# Fields 5.. are "samples" then the tokens, with "/" between samples.
	s = 1
	t = 0
	for (i = 6; i <= NF; i++) {
		if ($i == "/") { s++; continue }
		eq = index($i, "=")
		if (eq == 0) continue
		nm = substr($i, 1, eq - 1)
		vl = substr($i, eq + 1)
		t++
		if (NR == FNR) {
			ename[key, t] = nm
			evalue[key, t] = vl
			esample[key, t] = s
		} else {
			dvalue[key, s, nm] = vl
			dseen[key, s, nm] = 1
		}
	}
	if (NR == FNR) {
		if (key in ecount) { report("expectation lists pixel (" key ") twice"); next }
		ecount[key] = n
		etok[key] = t
		epix++
	} else {
		dcount[key] = n
		dtok[key] = t
		dpix++
	}
	next
}

END {
	if (epix == 0) {
		print "    the expectation is empty: nothing was compared"
		exit 1
	}
	if (dpix == 0) {
		print "    the file being checked has no deep pixels at all"
		exit 1
	}
	for (key in ecount) {
		if (!(key in dcount)) {
			report("pixel (" key ") is missing from the reading being checked")
			continue
		}
		if (dcount[key] + 0 != ecount[key] + 0) {
			report("pixel (" key "): sample count read as " dcount[key] ", expected " ecount[key])
			continue
		}
		if (dtok[key] + 0 != etok[key] + 0) {
			report("pixel (" key "): " dtok[key] " channel values were read, expected " etok[key])
			continue
		}
		for (t = 1; t <= etok[key]; t++) {
			nm = ename[key, t]
			s = esample[key, t]
			if (!((key SUBSEP s SUBSEP nm) in dseen)) {
				report("pixel (" key ") sample " s ": channel " nm " is missing")
				continue
			}
			e = evalue[key, t] + 0
			d = dvalue[key, s, nm] + 0
			diff = e - d
			if (diff < 0) diff = -diff
			if (diff > tol)
				report("pixel (" key ") sample " s " channel " nm ": read " dvalue[key, s, nm] ", expected " evalue[key, t])
		}
	}
	for (key in dcount)
		if (!(key in ecount))
			report("pixel (" key ") was read but is not in the expectation")

	if (bad > LIMIT) print "    ... and " (bad - LIMIT) " more"
	if (bad > 0) exit 1
	exit 0
}
