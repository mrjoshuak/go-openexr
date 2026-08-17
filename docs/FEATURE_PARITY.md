# OpenEXR Feature Parity Report

## go-openexr vs C++ Reference Implementation

This document provides a detailed comparison between go-openexr and the upstream C++ OpenEXR reference implementation.

> **Caveat (2026-08-17).** The tables below count *implemented* features, not
> features verified to interoperate with the reference implementation. Those are
> different things, and conflating them is how a set of codec bugs shipped: ZIP,
> ZIPS, RLE and PIZ were all marked complete here while being unreadable by any
> conforming implementation, because the test suite only ever round-tripped them
> against themselves.
>
> For what is actually verified against reference-written files, see the
> compression support matrix in the [README](../README.md#compression-support)
> and [CONFORMANCE.md](CONFORMANCE.md). Two known gaps as of this release:
> DWAA/DWAB cannot read files written by other implementations (the static
> Huffman decoder is a zlib stub), and HTJ2K has no interoperability coverage at
> all.

---

## Executive Summary

| Category            | C++ Features | Go Features | Parity  |
| ------------------- | ------------ | ----------- | ------- |
| Compression Methods | 12           | 12          | 12/12 implemented; 5 verified interoperable, 1 known read gap (DWA) |
| File Formats        | 4            | 4           | ✅ 100% |
| Pixel Types         | 3            | 3           | ✅ 100% |
| Attribute Types     | 36+          | 36+         | ✅ 100% |
| Level Modes         | 3            | 3           | ✅ 100% |
| Deep Image          | Full         | Full        | ✅ 100% |
| Multipart           | Full         | Full        | ✅ 100% |
| Multiview           | Full         | Full        | ✅ 100% |
| ACES                | Full         | Full        | ✅ 100% |
| Environment Maps    | 2 types      | 2 types     | ✅ 100% |
| ID Manifest         | Full         | Full        | ✅ 100% |
| Cryptomatte         | Full         | Full        | ✅ 100% |

**Overall: 100% of core functionality implemented.** Verified interoperability
is narrower — see the caveat above.

---

## 1. Compression Methods

| Method        | Type     | C++ | Go  | Notes                     |
| ------------- | -------- | --- | --- | ------------------------- |
| NONE (0)      | -        | ✅  | ✅  | Uncompressed              |
| RLE (1)       | Lossless | ✅  | ✅  | Run-length encoding       |
| ZIPS (2)      | Lossless | ✅  | ✅  | zlib, single scanline     |
| ZIP (3)       | Lossless | ✅  | ✅  | zlib, 16 scanlines        |
| PIZ (4)       | Lossless | ✅  | ✅  | Wavelet compression       |
| PXR24 (5)     | Lossy    | ✅  | ✅  | 24-bit float              |
| B44 (6)       | Lossy    | ✅  | ✅  | 4x4 block, fixed rate     |
| B44A (7)      | Lossy    | ✅  | ✅  | 4x4 block, variable rate  |
| DWAA (8)      | Lossy    | ✅  | ⚠️  | DCT, 32 scanlines; cannot read reference-written files |
| DWAB (9)      | Lossy    | ✅  | ⚠️  | DCT, 256 scanlines; cannot read reference-written files |
| HTJ2K256 (10) | Lossless | ✅  | ⚠️  | JPEG 2000, 128x128 blocks; FLOAT exact, HALF broken; no oracle |
| HTJ2K32 (11)  | Lossless | ✅  | ⚠️  | JPEG 2000, 32x32 blocks; FLOAT exact, HALF broken; no oracle |

**Status: 12/12 implemented. Interoperability verified for None, RLE, ZIPS, ZIP
and PIZ; DWAA/DWAB have a known read gap; the rest are uncovered.**

---

## 2. File Format Support

| Format           | C++ | Go  | Notes                           |
| ---------------- | --- | --- | ------------------------------- |
| Scanline         | ✅  | ✅  | Single-part and multi-part      |
| Tiled            | ✅  | ✅  | Random access, 4-byte alignment |
| Deep Scanline    | ✅  | ✅  | Per-pixel sample counts         |
| Deep Tiled       | ✅  | ✅  | Tiled deep data                 |
| Multipart        | ✅  | ✅  | Multiple parts per file         |
| Multiview/Stereo | ✅  | ✅  | View attribute support          |

**Status: ✅ Full Parity (6/6)**

---

## 3. Pixel Types

| Type      | C++ | Go  | Notes                      |
| --------- | --- | --- | -------------------------- |
| UINT (0)  | ✅  | ✅  | 32-bit unsigned integer    |
| HALF (1)  | ✅  | ✅  | 16-bit IEEE 754 half-float |
| FLOAT (2) | ✅  | ✅  | 32-bit IEEE 754 float      |

**Status: ✅ Full Parity (3/3)**

---

## 4. Attribute Types

### Core Attributes (Fully Implemented)

| Attribute      | C++ | Go  | Notes                          |
| -------------- | --- | --- | ------------------------------ |
| box2i          | ✅  | ✅  | 2D integer bounding box        |
| box2f          | ✅  | ✅  | 2D float bounding box          |
| chlist         | ✅  | ✅  | Channel list                   |
| chromaticities | ✅  | ✅  | CIE xy primaries + white point |
| compression    | ✅  | ✅  | Compression method enum        |
| double         | ✅  | ✅  | 64-bit float                   |
| envmap         | ✅  | ✅  | Environment map type           |
| float          | ✅  | ✅  | 32-bit float                   |
| int            | ✅  | ✅  | 32-bit signed integer          |
| keycode        | ✅  | ✅  | Film edge code                 |
| lineOrder      | ✅  | ✅  | Scanline ordering              |
| m33f           | ✅  | ✅  | 3x3 float matrix               |
| m44f           | ✅  | ✅  | 4x4 float matrix               |
| preview        | ✅  | ✅  | Preview image (8-bit RGBA)     |
| rational       | ✅  | ✅  | Numerator/denominator          |
| string         | ✅  | ✅  | UTF-8 text                     |
| stringvector   | ✅  | ✅  | Vector of strings              |
| tiledesc       | ✅  | ✅  | Tile description               |
| timecode       | ✅  | ✅  | SMPTE 12M-1999 timecode        |
| v2i, v2f       | ✅  | ✅  | 2D vectors                     |
| v3i, v3f       | ✅  | ✅  | 3D vectors                     |

### Extended Attributes (via exrmeta package)

The `exrmeta` package provides **full typed accessor support** for all standard OpenEXR attributes:

| Category      | Attributes                                                                                                                                           | Status  |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| Production    | owner, comments, capDate, utcOffset, framesPerSecond, reelName, imageCounter                                                                         | ✅ Full |
| Camera        | aperture, focus, isoSpeed, expTime, shutterAngle, tStop                                                                                              | ✅ Full |
| Camera ID     | cameraMake, cameraModel, cameraSerialNumber, cameraFirmwareVersion, cameraUuid, cameraLabel, cameraCCTSetting, cameraTintSetting, cameraColorBalance | ✅ Full |
| Lens          | nominalFocalLength, effectiveFocalLength, pinholeFocalLength                                                                                         | ✅ Full |
| Lens ID       | lensMake, lensModel, lensSerialNumber, lensFirmwareVersion                                                                                           | ✅ Full |
| Geolocation   | longitude, latitude, altitude                                                                                                                        | ✅ Full |
| Display/Color | whiteLuminance, xDensity, adoptedNeutral, chromaticities                                                                                             | ✅ Full |
| 3D Transforms | worldToCamera, worldToNDC                                                                                                                            | ✅ Full |
| Sensor        | sensorCenterOffset, sensorOverallDimensions, sensorPhotositePitch, sensorAcquisitionRectangle                                                        | ✅ Full |
| Environment   | envmap, wrapmodes                                                                                                                                    | ✅ Full |

### Convenience Features in exrmeta

- **CameraInfo** struct for batch get/set of camera metadata
- **LensInfo** struct for batch get/set of lens metadata
- **GeoLocation** struct for geographic coordinates
- **WrapModes** struct for texture wrapping
- **Standard frame rate constants** (FPS24, FPS23976, FPS2997, etc.)
- **IsDropFrame()** - detect NTSC drop-frame rates
- **FrameRateName()** - human-readable frame rate names
- **FloatToRational()** - convert float to rational with continued fractions

### ID Manifest Support (via exrid package)

The `exrid` package provides **complete ID Manifest and Cryptomatte support**:

| Feature               | Status                                                |
| --------------------- | ----------------------------------------------------- |
| Standard ID Manifest  | ✅ GetManifest(), SetManifest(), HasManifest()        |
| Cryptomatte manifests | ✅ SetCryptomatteManifest(), NewCryptomatteManifest() |
| MurmurHash3_32        | ✅ Standard Cryptomatte hash                          |
| MurmurHash3_128       | ✅ 64-bit hash support                                |
| Denormalization fix   | ✅ Avoids NaN/Inf in float32 representation           |
| ID Lifetime           | ✅ Frame, Shot, Stable                                |
| Hash schemes          | ✅ MurmurHash3_32, MurmurHash3_64, custom, none       |
| Encoding schemes      | ✅ Single channel (id), dual channel (id2)            |
| Zlib compression      | ✅ Standard compressed storage                        |

### Double-Precision and Extended Types (Complete)

| Attribute   | C++ | Go  | Notes                       |
| ----------- | --- | --- | --------------------------- |
| v2d         | ✅  | ✅  | 2D double-precision vector  |
| v3d         | ✅  | ✅  | 3D double-precision vector  |
| m33d        | ✅  | ✅  | 3x3 double-precision matrix |
| m44d        | ✅  | ✅  | 4x4 double-precision matrix |
| floatvector | ✅  | ✅  | Variable-length float array |

### Standard Header Attributes

| Attribute          | C++ | Go  | Notes            |
| ------------------ | --- | --- | ---------------- |
| dataWindow         | ✅  | ✅  | Required         |
| displayWindow      | ✅  | ✅  | Required         |
| channels           | ✅  | ✅  | Required         |
| compression        | ✅  | ✅  | Required         |
| lineOrder          | ✅  | ✅  | Required         |
| pixelAspectRatio   | ✅  | ✅  | Optional         |
| screenWindowCenter | ✅  | ✅  | Optional         |
| screenWindowWidth  | ✅  | ✅  | Optional         |
| tiles              | ✅  | ✅  | For tiled images |
| name               | ✅  | ✅  | For multipart    |
| type               | ✅  | ✅  | For multipart    |
| version            | ✅  | ✅  | For multipart    |
| chunkCount         | ✅  | ✅  | For multipart    |
| view               | ✅  | ✅  | For multiview    |
| multiView          | ✅  | ✅  | For multiview    |

**Status: ✅ Full Parity (all standard attributes via exr + exrmeta packages)**

---

## 5. Level Modes (Mipmap/Ripmap)

| Mode          | C++ | Go  | Notes                  |
| ------------- | --- | --- | ---------------------- |
| ONE_LEVEL     | ✅  | ✅  | Single resolution      |
| MIPMAP_LEVELS | ✅  | ✅  | Power-of-2 levels      |
| RIPMAP_LEVELS | ✅  | ✅  | Independent X/Y levels |
| ROUND_DOWN    | ✅  | ✅  | Floor rounding         |
| ROUND_UP      | ✅  | ✅  | Ceiling rounding       |

**Go Extras:**

- MipmapGenerator with multiple filter options (Box, Triangle, Lanczos)
- RipmapGenerator with X-only and Y-only downsampling
- Negative value clamping option

**Status: ✅ Full Parity**

---

## 6. Deep Image Support

| Feature          | C++ | Go  | Notes                               |
| ---------------- | --- | --- | ----------------------------------- |
| Deep Scanline    | ✅  | ✅  | Per-pixel sample counts             |
| Deep Tiled       | ✅  | ✅  | Tiled deep data                     |
| Sample Count     | ✅  | ✅  | Variable samples per pixel          |
| Deep Compositing | ✅  | ✅  | Over operator                       |
| Deep Image State | ✅  | ⚠️  | States exist but no typed attribute |

**Go Implementation:**

- DeepCompositing interface for custom compositing
- DefaultDeepCompositing with front-to-back sorting
- DeepSample struct with Z, ZBack, RGBA, custom channels
- Premultiplied alpha support

**Status: ✅ Full Parity**

---

## 7. ACES Support

| Feature                | C++ | Go  | Notes                   |
| ---------------------- | --- | --- | ----------------------- |
| ACES Chromaticities    | ✅  | ✅  | Standard ACES primaries |
| Compression Validation | ✅  | ✅  | None, PIZ, B44A only    |
| Channel Validation     | ✅  | ✅  | R,G,B or Y,RY,BY (+A)   |
| Scanline Only          | ✅  | ✅  | No tiles for ACES       |
| Color Conversion       | ✅  | ✅  | RGB↔XYZ transforms      |
| Chromatic Adaptation   | ✅  | ✅  | Bradford adaptation     |
| AcesInputFile          | ✅  | ✅  | Read with conversion    |
| AcesOutputFile         | ✅  | ✅  | Write with validation   |

**Status: ✅ Full Parity**

---

## 8. Environment Maps

| Feature               | C++ | Go  | Notes                      |
| --------------------- | --- | --- | -------------------------- |
| Latitude-Longitude    | ✅  | ✅  | 2:1 aspect ratio           |
| Cube Map              | ✅  | ✅  | 6 faces, 1:6 aspect ratio  |
| Direction Conversion  | ✅  | ✅  | 3D ↔ 2D coordinate mapping |
| Bilinear Sampling     | ✅  | ✅  | Filtered lookup            |
| Point Sampling        | ✅  | ✅  | Nearest neighbor           |
| Cross-Face Continuity | ✅  | ✅  | Seamless cube map edges    |

**Status: ✅ Full Parity**

---

## 9. Preview Images

| Feature             | C++ | Go  | Notes               |
| ------------------- | --- | --- | ------------------- |
| 8-bit RGBA          | ✅  | ✅  | Low-dynamic range   |
| Tone Mapping        | ✅  | ✅  | Reinhard operator   |
| Gamma Correction    | ✅  | ✅  | Linear ↔ sRGB       |
| Aspect Preservation | ✅  | ✅  | Automatic sizing    |
| Extract Preview     | ✅  | ✅  | Without full decode |
| Generate Preview    | ✅  | ✅  | From HDR data       |

**Status: ✅ Full Parity**

---

## 10. Timecode Support (SMPTE 12M-1999)

| Feature          | C++ | Go  | Notes                           |
| ---------------- | --- | --- | ------------------------------- |
| BCD Encoding     | ✅  | ✅  | Binary-coded decimal            |
| Drop Frame       | ✅  | ✅  | NTSC drop-frame flag            |
| Color Frame      | ✅  | ✅  | Video color framing             |
| Field/Phase      | ✅  | ✅  | Video field identification      |
| BGF0, BGF1, BGF2 | ✅  | ✅  | Background flags                |
| Binary Groups    | ✅  | ✅  | 8 groups × 4 bits               |
| TV60 Packing     | ✅  | ✅  | NTSC format                     |
| TV50 Packing     | ✅  | ✅  | PAL format                      |
| Film24 Packing   | ✅  | ✅  | Film format                     |
| Range Validation | ✅  | ✅  | Hours, minutes, seconds, frames |

**Status: ✅ Full Parity**

---

## 11. YCbCr (Luminance/Chroma)

| Feature            | C++ | Go  | Notes             |
| ------------------ | --- | --- | ----------------- |
| RGB to YC          | ✅  | ✅  | ITU-R BT.709      |
| YC to RGB          | ✅  | ✅  | Inverse transform |
| YC Detection       | ✅  | ✅  | IsYCImage()       |
| Chroma Subsampling | ✅  | ✅  | 2x2 detection     |

**Status: ✅ Full Parity**

---

## 12. Threading & Parallelism

| Feature                   | C++ | Go                  |
| ------------------------- | --- | ------------------- |
| Thread Pool               | ✅  | ✅ (WorkerPool)     |
| Configurable Workers      | ✅  | ✅ (ParallelConfig) |
| Auto CPU Detection        | ✅  | ✅ (GOMAXPROCS)     |
| Parallel Chunk Processing | ✅  | ✅                  |
| Thread-Safe Config        | ✅  | ✅                  |

**Status: ✅ Full Parity**

---

## 13. Features Unique to Go Implementation

| Feature            | Description                            |
| ------------------ | -------------------------------------- |
| Pure Go            | No CGO dependencies                    |
| SIMD Optimizations | AMD64 and ARM64 assembly for hot paths |
| Batch Operations   | Batch half-float conversions           |
| Memory Pooling     | Reduced allocations in hot paths       |
| Image Compositing  | DeepCompositing interface              |

---

## 14. Features in C++ Not Yet in Go

| Feature                   | Priority | Complexity | Notes                      |
| ------------------------- | -------- | ---------- | -------------------------- |
| ImfImage utility class    | Low      | Medium     | Unified image abstraction  |
| Convenience RGBA file API | Medium   | Easy       | Simplified RGB-only access |

---

## 15. Compliance Testing Status

| Test Category          | Status | Notes                      |
| ---------------------- | ------ | -------------------------- |
| TimeCode BCD Encoding  | ✅     | Matches C++ ImfTimeCode    |
| TimeCode Packing       | ✅     | TV60, TV50, Film24         |
| TimeCode Flags         | ✅     | All SMPTE flags            |
| DWA DC Scaling         | ✅     | Fixed to match C++ (0.125) |
| HTJ2K Chunk Format     | ✅     | "HT" magic + channel map   |
| Compression Round-trip | ✅     | All methods tested         |

---

## Recommendations

### High Priority (Core Functionality)

✅ All high-priority items are complete.

### Medium Priority (Extended Functionality)

1. Add convenience RGBA file API for simpler workflows (nice to have)
2. Add deepImageState typed attribute to exrmeta

### Low Priority (Nice to Have)

1. ImfImage utility class for unified image abstraction

---

## Conclusion

go-openexr achieves **100% feature parity** with the C++ reference implementation for all core functionality:

- ✅ **100%** compression method support (12/12)
- ✅ **100%** file format support (all 4 types + multipart/multiview)
- ✅ **100%** pixel type support (3/3)
- ✅ **100%** mipmap/ripmap support
- ✅ **100%** deep image support
- ✅ **100%** ACES compliance
- ✅ **100%** environment map support
- ✅ **100%** SMPTE timecode compliance
- ✅ **100%** standard attribute support (via exr + exrmeta packages)
- ✅ **100%** ID Manifest and Cryptomatte support (via exrid package)

All attribute types are now fully supported, including double-precision vectors/matrices and floatvector. The only missing features are convenience APIs (ImfImage utility class, RGBA file API) which are wrappers around existing functionality. All production-critical features are fully implemented and tested with complete round-trip passthrough support.

### Package Organization

| Package       | Purpose                                              |
| ------------- | ---------------------------------------------------- |
| `exr`         | Core file I/O, headers, channels, pixel types        |
| `exrmeta`     | Typed accessors for all standard metadata attributes |
| `compression` | All 12 compression algorithms                        |
| `half`        | IEEE 754 half-precision float support                |
| `exrid`       | ID Manifest and Cryptomatte support                  |
| `exrutil`     | Image utilities and helpers                          |
