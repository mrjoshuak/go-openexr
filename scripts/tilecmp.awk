# tilecmp.awk — compare a sample dump against the samples a fixture was
# written from, and print the count and the largest disagreement.
#
#   awk -f tilecmp.awk expected.txt actual.txt
#
# Both files hold lines "lx ly x y CHANNEL value"; lines beginning with '#' are
# structure and are ignored. The comparison is by key, not by position, so a
# dump that reorders its output still compares, and a dump that is missing
# samples or invents them is reported rather than passing on the samples it did
# produce.
#
# Output is one line, five whitespace-free fields so a shell can split it:
#
#   samples=<n> missing=<n> extra=<n> maxerr=<x> at=<lx,ly,x,y,channel>
#
# A caller treats anything but missing=0 extra=0 as a failure, and holds maxerr
# to the tolerance for that row.

/^#/ { next }

NR == FNR {
	key = $1 " " $2 " " $3 " " $4 " " $5
	want[key] = $6
	nwant++
	next
}

{
	key = $1 " " $2 " " $3 " " $4 " " $5
	if (!(key in want)) {
		extra++
		next
	}
	d = $6 - want[key]
	if (d < 0) d = -d
	if (d > maxerr) {
		maxerr = d
		at = $1 "," $2 "," $3 "," $4 "," $5
	}
	seen[key] = 1
	n++
}

END {
	for (k in want)
		if (!(k in seen)) missing++
	printf "samples=%d missing=%d extra=%d maxerr=%.9g at=%s\n",
		n + 0, missing + 0, extra + 0, maxerr + 0, (at == "" ? "-" : at)
}
