#!/usr/bin/env python3
"""Compare an exrpartdump of a subsampled part against the fixture's own planes.

scripts/multipartgen writes one PFM per channel, at the channel's own size — a
channel with xSampling 2 has ceil(width/2) columns. exrpartdump prints what
libOpenEXR reads back for the same channel, in the same coordinates. Any
disagreement is this library packing the chunk at the wrong width, which is what
made every channel after a subsampled one land at the wrong offset.

    subpartcmp.py <dump> <truthdir> <fileStem> <part>

Prints one line per channel and a summary, and exits non-zero if anything
differs or if a channel has no truth file to compare against.
"""

import glob
import os
import struct
import sys


def read_pfm(path):
    with open(path, 'rb') as fh:
        if fh.readline().strip() != b'Pf':
            raise ValueError("%s is not a single-channel PFM" % path)
        w, h = (int(v) for v in fh.readline().split())
        float(fh.readline())
        vals = struct.unpack('<%df' % (w * h), fh.read(w * h * 4))
    # PFM stores rows bottom to top.
    return w, h, [vals[(h - 1 - y) * w + x] for y in range(h) for x in range(w)]


def read_uint_table(path):
    rows = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            rows.append([float(v) for v in line.split()])
    if not rows:
        raise ValueError("%s is empty" % path)
    return len(rows[0]), len(rows), [v for r in rows for v in r]


def main():
    if len(sys.argv) != 5:
        print("usage: subpartcmp.py <dump> <truthdir> <fileStem> <part>", file=sys.stderr)
        return 2
    dump, truthdir, stem, part = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

    got = {}
    for line in open(dump):
        if line.startswith('#'):
            continue
        f = line.split()
        if len(f) != 4:
            continue
        got.setdefault(f[0], {})[(int(f[1]), int(f[2]))] = float(f[3])

    if not got:
        print("the dump holds no samples")
        return 1

    total = bad = 0
    parts = []
    for ch in sorted(got):
        matches = glob.glob(os.path.join(truthdir, "%s.p%s.%s.*" % (stem, part, ch)))
        matches = [m for m in matches if m.endswith('.pfm') or m.endswith('.txt')]
        if not matches:
            print("channel %s has no truth file" % ch)
            return 1
        path = matches[0]
        w, h, ref = read_pfm(path) if path.endswith('.pfm') else read_uint_table(path)

        n = b = 0
        for y in range(h):
            for x in range(w):
                v = got[ch].get((x, y))
                if v is None:
                    b += 1
                elif abs(v - ref[y * w + x]) > 1e-6:
                    b += 1
                n += 1
        parts.append("%s %dx%d=%d" % (ch, w, h, b))
        total += n
        bad += b

    print("%d of %d samples differ (%s)" % (bad, total, " ".join(parts)))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
