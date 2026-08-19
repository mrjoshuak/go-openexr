#!/usr/bin/env python3
"""Compare two exrtiledump outputs of the same image, level by level.

Written for generated mipmap levels, where the format specifies no downsampling
filter and so no implementation's values are "correct" the way a codec's output
is. What can still be checked, and is:

  * the two agree on which samples exist at all, so a generator that placed a
    level wrongly or produced the wrong number of them fails;
  * level 0 is exact, since it is the source and no filter has touched it;
  * the deepest level is exact, because a single sample is the mean of the
    image and every 2x2-supported filter preserves it;
  * the per-level maximum difference never grows with depth, which a filter
    difference does not do and a wrong axis, a wrong scale or an accumulating
    offset does.

Prints one line per level and a verdict line, and exits non-zero on failure.
"""

import sys


def load(path):
    out = {}
    with open(path) as fh:
        for line in fh:
            if line.startswith('#'):
                continue
            f = line.split()
            if len(f) < 6:
                continue
            out[(f[0], f[1], f[2], f[3], f[4])] = float(f[5])
    return out


def main():
    if len(sys.argv) != 3:
        print("usage: mipdiff.py <reference.dump> <ours.dump>", file=sys.stderr)
        return 2

    ref, ours = load(sys.argv[1]), load(sys.argv[2])
    if not ref or not ours:
        print("one of the dumps is empty")
        return 1

    missing = sum(1 for k in ref if k not in ours)
    extra = sum(1 for k in ours if k not in ref)
    if missing or extra:
        print("geometry differs: %d samples missing, %d invented" % (missing, extra))
        return 1

    per, counts = {}, {}
    for k, v in ref.items():
        lvl = (int(k[0]), int(k[1]))
        d = abs(ours[k] - v)
        # Assign unconditionally: a level whose samples all match would never
        # enter the map under "if d > per.get(...)", and then level 0 — the one
        # level that must be exact — would be missing from the check entirely.
        per[lvl] = max(per.get(lvl, 0.0), d)
        counts[lvl] = counts.get(lvl, 0) + 1

    levels = sorted(per)
    parts = []
    for lvl in levels:
        parts.append("l%d=%.6g" % (lvl[0], per[lvl]))

    problems = []
    if per[levels[0]] != 0.0:
        problems.append("level 0 differs by %g; it is the source and no filter has touched it"
                        % per[levels[0]])
    deepest = levels[-1]
    if counts[deepest] == 1 and per[deepest] != 0.0:
        problems.append("the 1x1 level differs by %g; it is the image's mean, which every "
                        "2x2-supported filter preserves" % per[deepest])
    # From level 1 onward: level 0 is the source and exact by construction, so
    # including it would compare "no filter applied" against "one filter
    # applied" and always report growth.
    filtered = levels[1:]
    for a, b in zip(filtered, filtered[1:]):
        if per[b] > per[a] + 1e-12:
            problems.append("level %d differs more than level %d (%.6g > %.6g); a filter "
                            "difference shrinks with depth, a wrong axis or scale does not"
                            % (b[0], a[0], per[b], per[a]))

    print("%d levels, %d samples, per-level max difference %s"
          % (len(levels), len(ref), " ".join(parts)))
    for p in problems:
        print("  " + p)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
