#!/usr/bin/env python3
"""Compare two dumps of a subsampled EXR by key.

    subsampcmp.py <reference.dump> <ours.dump>

Both hold lines "<channel> <x> <y> <value>" in each channel's own coordinates,
which for a channel with xSampling 2 means column x of the channel is column 2x
of the image. Lines beginning with '#' are ignored.

The comparison is by key rather than by position, so a dump that reorders its
output still compares, and a missing sample and an invented one are reported
apart from a wrong one — a reader that silently drops a channel cannot pass on
the channels it did produce.

Exits non-zero and prints what differs; prints nothing and exits zero when the
two agree.
"""
import sys


def load(path):
    out = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith("#"):
                continue
            f = line.split()
            if len(f) != 4:
                continue
            out[(f[0], int(f[1]), int(f[2]))] = float(f[3])
    return out


def main():
    if len(sys.argv) != 3:
        print("usage: subsampcmp.py <reference.dump> <ours.dump>", file=sys.stderr)
        return 2
    ref = load(sys.argv[1])
    got = load(sys.argv[2])

    if not ref:
        print("the reference dump holds no samples: nothing was compared")
        return 1

    missing = [k for k in ref if k not in got]
    extra = [k for k in got if k not in ref]
    wrong = [k for k in ref if k in got and abs(ref[k] - got[k]) > 1e-3]

    if missing or extra or wrong:
        parts = []
        if missing:
            k = sorted(missing)[0]
            parts.append("%d missing (first %s)" % (len(missing), k))
        if extra:
            k = sorted(extra)[0]
            parts.append("%d invented (first %s)" % (len(extra), k))
        if wrong:
            k = sorted(wrong)[0]
            parts.append("%d wrong (first %s: %g vs %g)" % (len(wrong), k, ref[k], got[k]))
        print("%d samples compared; %s" % (len(ref), ", ".join(parts)))
        return 1
    return 0


sys.exit(main())
