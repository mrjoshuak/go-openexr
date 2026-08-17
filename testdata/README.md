# External Test Data

This directory contains external test files used for integration testing. These files are **not committed to the repository** - they must be downloaded separately.

## Quick Setup

Run the download script to fetch all external test data:

```bash
cd testdata
./download.sh
```

Or use `go generate`:

```bash
go generate ./testdata/...
```

## Manual Download

If you prefer to download files manually or the script fails:

### Cryptomatte Test Files

Source: [Psyop/Cryptomatte](https://github.com/Psyop/Cryptomatte) (Official Cryptomatte repository)

Download these files to `testdata/cryptomatte/`:

| File                        | URL                                                                                     |
| --------------------------- | --------------------------------------------------------------------------------------- |
| `bunny_CryptoObject.exr`    | https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoObject.exr    |
| `bunny_CryptoMaterial.exr`  | https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoMaterial.exr  |
| `bunny_CryptoAsset.exr`     | https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoAsset.exr     |
| `testGrid_CryptoObject.exr` | https://github.com/Psyop/Cryptomatte/raw/master/sample_images/testGrid_CryptoObject.exr |

Example using curl:

```bash
mkdir -p cryptomatte
cd cryptomatte
curl -LO https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoObject.exr
curl -LO https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoMaterial.exr
curl -LO https://github.com/Psyop/Cryptomatte/raw/master/sample_images/bunny_CryptoAsset.exr
curl -LO https://github.com/Psyop/Cryptomatte/raw/master/sample_images/testGrid_CryptoObject.exr
```

## Running Tests with External Data

Tests that require external data will skip automatically if the files are not present:

```bash
# Run all tests (external data tests will skip if files missing)
go test ./...

# Run only if you have downloaded test data
go test ./exrid/... -v
```

To see which tests are skipped:

```bash
go test ./... -v 2>&1 | grep -i skip
```

## Directory Structure

```
testdata/
├── README.md           # This file
├── download.sh         # Download script
├── .gitignore          # Prevents committing test data
├── cryptomatte/        # Cryptomatte sample files
│   ├── bunny_CryptoObject.exr
│   ├── bunny_CryptoMaterial.exr
│   └── ...
└── openexr-images/     # Official ASWF conformance images
    ├── AllHalfValues.exr
    ├── stripes.exr
    └── ...
```

## openexr-images (ASWF conformance corpus)

Source: [AcademySoftwareFoundation/openexr-images](https://github.com/AcademySoftwareFoundation/openexr-images)

These are the images the OpenEXR project itself uses to exercise decoders: every
half value, the full float range, ramps, NaN/Inf handling, and a tiled image.
`download.sh` fetches the `TestImages/` set plus one scanline and one tiled
image.

They are the ground truth for `exr.TestReferenceImagesDecodeExactly`, which
compares every decoded sample against a digest of what the OpenEXR reference
implementation reads from the same file. The digests live in
`exr/testdata/openexr_images.golden` and are committed; the images themselves
are not. Regenerate the digests with:

```bash
python3 scripts/gen-reference-goldens.py   # needs the OpenImageIO Python module
```

The tests skip when the corpus is absent, so a clean checkout still runs the
committed conformance fixtures in `exr/testdata/conformance/`.

## License

The Cryptomatte sample files are from the [Psyop/Cryptomatte](https://github.com/Psyop/Cryptomatte) repository and are subject to their license terms.

The openexr-images files are from the Academy Software Foundation and are
distributed under the terms in that repository's `LICENSE`.

## Adding New Test Data Sources

When adding new external test data:

1. Add download instructions to this README
2. Update `download.sh` with the new URLs
3. Update `.gitignore` if needed to exclude the new files
4. Write tests that gracefully skip when files are missing
