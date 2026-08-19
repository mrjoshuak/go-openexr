# Fixtures the gate needs and cannot generate

## tiled_subsampled_invalid.exr

A tiled EXR whose `RY` and `BY` channels carry `xSampling = ySampling = 2`.

The OpenEXR format forbids subsampled channels in a tiled image, and the
reference implementation refuses to open this file:

```
$ exrinfo -v tiled_subsampled_invalid.exr
ERROR 'tiled_subsampled_invalid.exr' (EXR_ERR_INVALID_ATTR): channel 'BY':
  x subsampling factor is not 1 (2) for a tiled image
```

This library wrote it, at commit 5e793a5, because `Header.Validate` did not
check the rule — `TiledWriter` produced a file no reader can open, and the
symptom nobody had noticed was that `TiledWriter` and `MultiPartOutputFile` also
skip the `xSampling`/`ySampling` divides the scanline path performs when it
builds a `PIZChannel`. The divides are not the defect: the file is illegal
before the codec ever sees it.

`Header.Validate` now rejects such a header, so `scripts/tiledgen` can no longer
produce this file, which is why it is committed here. `scripts/validate.sh`
checks both halves every run: that the reference still refuses this file (if it
ever stops, the guard's premise is gone and the guard must be re-derived), and
that this library refuses to write another one.
