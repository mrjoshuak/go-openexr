#!/usr/bin/env python3
"""Check that the Y, RY and BY planes libOpenEXR reads out of a file this
library wrote are the ones the format defines.

    check.py <exrpartdump-output> <width> <height>

The source image is the smooth ramp ycgen writes, so the expectation can be
computed here independently:

    Y  = 0.2126 R + 0.7152 G + 0.0722 B        (Rec.709 luminance)
    RY = (R - Y) / Y                            averaged over each 2x2 block
    BY = (B - Y) / Y

This is the check a round trip cannot make. Storing the plain differences
instead of the ratios is self-consistent — the library's own reader undoes its
own writer exactly — and means something different from what the file claims.
"""
import sys

KR, KG, KB = 0.2126, 0.7152, 0.0722


def source(x, y, w, h):
    r = 0.2 + 0.6 * x / (w - 1)
    g = 0.3 + 0.5 * y / (h - 1)
    b = 0.25 + 0.4 * (x + y) / (w + h - 2)
    return r, g, b


def main():
    dump, w, h = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
    got = {}
    for line in open(dump):
        if line.startswith("#"):
            continue
        f = line.split()
        if len(f) == 4:
            got[(f[0], int(f[1]), int(f[2]))] = float(f[3])
    if not got:
        print("the dump holds no samples")
        return 1

    worst = {"Y": (0.0, ""), "RY": (0.0, ""), "BY": (0.0, "")}

    for y in range(h):
        for x in range(w):
            r, g, b = source(x, y, w, h)
            want = KR * r + KG * g + KB * b
            v = got.get(("Y", x, y))
            if v is None:
                print("Y (%d,%d) missing from the reference's reading" % (x, y))
                return 1
            d = abs(v - want)
            if d > worst["Y"][0]:
                worst["Y"] = (d, "(%d,%d) %g vs %g" % (x, y, v, want))

    for cy in range((h + 1) // 2):
        for cx in range((w + 1) // 2):
            sry = sby = n = 0.0
            for dy in range(2):
                for dx in range(2):
                    px, py = cx * 2 + dx, cy * 2 + dy
                    if px >= w or py >= h:
                        continue
                    r, g, b = source(px, py, w, h)
                    yy = KR * r + KG * g + KB * b
                    sry += (r - yy) / yy
                    sby += (b - yy) / yy
                    n += 1
            for name, want in (("RY", sry / n), ("BY", sby / n)):
                v = got.get((name, cx, cy))
                if v is None:
                    print("%s (%d,%d) missing from the reference's reading" % (name, cx, cy))
                    return 1
                d = abs(v - want)
                if d > worst[name][0]:
                    worst[name] = (d, "(%d,%d) %g vs %g" % (cx, cy, v, want))

    # Half precision on values around 1 is about 2^-11; the chroma ratios are
    # small, so 0.002 is a couple of ULPs of headroom and comes from the format
    # rather than from what happened to pass.
    tol = 0.002
    bad = [k for k in worst if worst[k][0] > tol]
    if bad:
        for k in bad:
            print("%s worst %.5g at %s" % (k, worst[k][0], worst[k][1]))
        return 1
    print("Y %.2g  RY %.2g  BY %.2g worst deviation" %
          (worst["Y"][0], worst["RY"][0], worst["BY"][0]))
    return 0


sys.exit(main())
