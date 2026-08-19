#!/usr/bin/env python3
"""Compare an exrdeeptiledump against the per-level expectations that produced it.

The dump emits one section per resolution level, headed by "# level lx ly w h",
and mipmap and ripmap files repeat the same (x, y) coordinates at every level —
so the sections have to be paired with their own expectation file rather than
concatenated. Pairing them by position rather than by level would compare a
ripmap's levels in the wrong order and report differences that are the harness's
rather than the file's, which is what the first version of this did.

    deepmipcmp.py <dump> <dir> <stem>

expects <dir>/<stem>.l<lx>_<ly>.txt beside the dump. Prints one summary line and
exits non-zero if any level differs or has no expectation.
"""

import os
import re
import subprocess
import sys
import tempfile

PUNCT = re.compile(r'[():,]')


def main():
    if len(sys.argv) != 4:
        print("usage: deepmipcmp.py <dump> <dir> <stem>", file=sys.stderr)
        return 2
    dump_path, d, stem = sys.argv[1], sys.argv[2], sys.argv[3]

    awk = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'deepdiff.awk')
    if not os.path.exists(awk):
        print("deepdiff.awk not found beside this script")
        return 1

    sections = []
    cur = None
    for line in open(dump_path):
        m = re.match(r'^# level (\d+) (\d+) ', line)
        if m:
            cur = (int(m.group(1)), int(m.group(2)))
            sections.append((cur, []))
            continue
        if cur is not None and line.startswith('Pixel'):
            sections[-1][1].append(line.rstrip('\n'))

    if not sections:
        print("the dump holds no levels")
        return 1

    bad = []
    pixels = 0
    with tempfile.TemporaryDirectory() as tmp:
        w_path = os.path.join(tmp, 'want')
        g_path = os.path.join(tmp, 'got')
        for (lx, ly), lines in sections:
            want = os.path.join(d, "%s.l%d_%d.txt" % (stem, lx, ly))
            if not os.path.exists(want):
                bad.append("level %d,%d has no expectation file" % (lx, ly))
                continue
            with open(w_path, 'w') as fh:
                fh.write(PUNCT.sub(' ', open(want).read()))
            with open(g_path, 'w') as fh:
                fh.write(PUNCT.sub(' ', '\n'.join(lines) + '\n'))
            r = subprocess.run(['awk', '-f', awk, w_path, g_path],
                               capture_output=True, text=True)
            if r.returncode:
                first = (r.stdout + r.stderr).strip().splitlines()
                bad.append("level %d,%d: %s" % (lx, ly, first[0] if first else "differs"))
            pixels += len(lines)

    print("%d levels, %d pixels, %d mismatched%s"
          % (len(sections), pixels, len(bad), (": " + "; ".join(bad[:2])) if bad else ""))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
