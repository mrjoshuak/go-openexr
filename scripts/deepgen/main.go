// Command deepgen writes deep EXR fixtures whose every sample is known, so an
// external tool can independently confirm the OpenEXR reference implementation
// reads back exactly what this library wrote.
//
// This exists for the same reason scripts/interopgen does: a defect applied
// identically to this library's deep writer and its deep reader is invisible to
// a round trip. DeepScanlineWriter and DeepTiledWriter had never been read by
// anything but this library, and both wrote a deep chunk header the OpenEXR
// specification does not define.
//
// THE FIXTURE. Deep pixels carry a variable number of samples, so a fixture
// with a constant sample count is compared equal by a writer that assumes a
// constant sample count. Every fixture here therefore varies:
//
//	counts     (3*x + 7*y) mod 5, so 0, 1, 2, 3 and 4 samples all occur and
//	           zero-sample pixels are scattered through every chunk and tile
//	row 4      entirely zero samples: an empty scanline inside a chunk
//	tile 1,1   entirely zero samples: an empty tile, whose sample data is a
//	           zero-length block
//	values     v(channel, x, y, sample) = (5*(W*y + x) + sample + 1) / 2^(3+c)
//
// The value is distinct for every (x, y, sample) within a channel and scaled
// differently per channel, so a transposed x/y, a misordered sample, a
// misplaced tile, a swapped channel and an off-by-one in the sample count table
// each produce a different number. The numerator stays below 2048 and the
// scale is a power of two, so every value is exactly representable in HALF as
// well as FLOAT: the lossless codecs are held to equality, not to a tolerance.
//
// Alongside each file it writes a .expect listing every pixel in the same shape
// oiiotool --dumpdata prints, so the comparison in scripts/validate.sh is
// between what this library meant to write and what the reference read.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/mrjoshuak/go-openexr/exr"
	"github.com/mrjoshuak/go-openexr/half"
)

// Fixture geometry. The height is deliberately more than one ZIP chunk (16
// scanlines) and not a multiple of it, so the sample count table is exercised
// across scanlines inside a chunk, across chunk boundaries, and in a short
// final chunk. The width is not a multiple of the tile size, so the right-hand
// tile column is partial.
const (
	fixW    = 13
	fixH    = 20
	tileW   = 4
	tileH   = 4
	emptyY  = 4 // this scanline has no samples at all
	emptyTX = 1 // this tile has no samples at all
	emptyTY = 1
)

// sampleCount returns the number of samples at a pixel.
func sampleCount(x, y int) uint32 {
	if y == emptyY {
		return 0
	}
	if x/tileW == emptyTX && y/tileH == emptyTY {
		return 0
	}
	return uint32((3*x + 7*y) % 5)
}

// value returns the sample value for channel index c at pixel (x, y), sample s.
// The numerator is below 2048 and the divisor is a power of two, so the result
// is exact in HALF and in FLOAT.
func value(c, x, y, s int) float32 {
	num := 5*(fixW*y+x) + s + 1
	return float32(num) / float32(int(1)<<uint(3+c))
}

// uintValue returns the UINT sample value at the same position. UINT channels
// carry integers so a reinterpreted bit pattern is obvious.
func uintValue(c, x, y, s int) uint32 {
	return uint32(1000*c + 5*(fixW*y+x) + s + 1)
}

type channel struct {
	name string
	typ  exr.PixelType
}

// The two channel sets. The float set is the ordinary deep layout; the mixed
// set puts all three pixel types in one file, since the sample stride depends
// on the per-channel type and a writer that assumes one type is only visible
// here.
var (
	floatChans = []channel{
		{"A", exr.PixelTypeFloat},
		{"B", exr.PixelTypeFloat},
		{"G", exr.PixelTypeFloat},
		{"R", exr.PixelTypeFloat},
		{"Z", exr.PixelTypeFloat},
	}
	mixedChans = []channel{
		{"B", exr.PixelTypeHalf},
		{"G", exr.PixelTypeHalf},
		{"R", exr.PixelTypeHalf},
		{"Z", exr.PixelTypeFloat},
		{"id", exr.PixelTypeUint},
	}
)

// fill builds a deep frame buffer with the fixture's counts and values.
func fill(chans []channel) *exr.DeepFrameBuffer {
	fb := exr.NewDeepFrameBuffer(fixW, fixH)
	for _, ch := range chans {
		fb.Insert(ch.name, ch.typ)
	}
	for y := 0; y < fixH; y++ {
		for x := 0; x < fixW; x++ {
			n := sampleCount(x, y)
			fb.SetSampleCount(x, y, n)
			fb.AllocateSamples(x, y)
			for s := 0; s < int(n); s++ {
				for c, ch := range chans {
					switch ch.typ {
					case exr.PixelTypeHalf:
						fb.Slices[ch.name].SetSampleHalf(x, y, s, uint16(half.FromFloat32(value(c, x, y, s))))
					case exr.PixelTypeFloat:
						fb.Slices[ch.name].SetSampleFloat32(x, y, s, value(c, x, y, s))
					case exr.PixelTypeUint:
						fb.Slices[ch.name].SetSampleUint(x, y, s, uintValue(c, x, y, s))
					}
				}
			}
		}
	}
	return fb
}

// expect writes the expected dump: one line per pixel, in the shape
// oiiotool --dumpdata prints, so scripts/validate.sh can compare the two
// with one parser.
func expect(path string, chans []channel) error {
	var b strings.Builder
	for y := 0; y < fixH; y++ {
		for x := 0; x < fixW; x++ {
			n := int(sampleCount(x, y))
			fmt.Fprintf(&b, "Pixel (%d, %d): %d samples ", x, y, n)
			for s := 0; s < n; s++ {
				if s > 0 {
					b.WriteString(" / ")
				} else {
					b.WriteString(": ")
				}
				for c, ch := range chans {
					if c > 0 {
						b.WriteString(" ")
					}
					switch ch.typ {
					case exr.PixelTypeUint:
						fmt.Fprintf(&b, "%s=%d", ch.name, uintValue(c, x, y, s))
					default:
						v := value(c, x, y, s)
						if ch.typ == exr.PixelTypeHalf {
							v = half.FromFloat32(v).Float32()
						}
						fmt.Fprintf(&b, "%s=%s", ch.name, strconv.FormatFloat(float64(v), 'g', -1, 32))
					}
				}
			}
			b.WriteString("\n")
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// dump prints one part of a deep file as this library reads it, in the same
// shape oiiotool --dumpdata prints. It exists so the read direction can be
// gated too: scripts/validate.sh points it at a deep file the reference wrote
// and compares this output against the reference's own reading of that file,
// which is a fixture this library had no hand in producing.
func dump(path string, part int) error {
	f, err := exr.OpenFile(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := f.Header(part)
	if h == nil {
		return fmt.Errorf("%s: no header for part %d", path, part)
	}
	dw := h.DataWindow()
	w, hgt := int(dw.Width()), int(dw.Height())

	partType := ""
	if attr := h.Get(exr.AttrNameType); attr != nil {
		if s, ok := attr.Value.(string); ok {
			partType = s
		}
	}

	cl := h.Channels()
	var chans []channel
	for i := 0; i < cl.Len(); i++ {
		c := cl.At(i)
		chans = append(chans, channel{c.Name, c.Type})
	}

	fb := exr.NewDeepFrameBuffer(w, hgt)
	for _, ch := range chans {
		fb.Insert(ch.name, ch.typ)
	}

	switch partType {
	case exr.PartTypeDeepScanline:
		r, err := exr.NewDeepScanlineReaderPart(f, part)
		if err != nil {
			return err
		}
		r.SetFrameBuffer(fb)
		if err := r.ReadPixelSampleCounts(0, hgt-1); err != nil {
			return err
		}
		for y := 0; y < hgt; y++ {
			for x := 0; x < w; x++ {
				fb.AllocateSamples(x, y)
			}
		}
		if err := r.ReadPixels(0, hgt-1); err != nil {
			return err
		}
	case exr.PartTypeDeepTiled:
		r, err := exr.NewDeepTiledReaderPart(f, part)
		if err != nil {
			return err
		}
		r.SetFrameBuffer(fb)
		if err := r.ReadTiles(0, 0, r.NumTilesX()-1, r.NumTilesY()-1); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%s: part type %q is not deep", path, partType)
	}

	var b strings.Builder
	for y := 0; y < hgt; y++ {
		for x := 0; x < w; x++ {
			n := int(fb.GetSampleCount(x, y))
			fmt.Fprintf(&b, "Pixel (%d, %d): %d samples ", x, y, n)
			for s := 0; s < n; s++ {
				if s > 0 {
					b.WriteString(" / ")
				} else {
					b.WriteString(": ")
				}
				for c, ch := range chans {
					if c > 0 {
						b.WriteString(" ")
					}
					slice := fb.Slices[ch.name]
					switch ch.typ {
					case exr.PixelTypeUint:
						fmt.Fprintf(&b, "%s=%d", ch.name, slice.GetSampleUint(x, y, s))
					case exr.PixelTypeHalf:
						v := half.Half(slice.GetSampleHalf(x, y, s)).Float32()
						fmt.Fprintf(&b, "%s=%s", ch.name, strconv.FormatFloat(float64(v), 'g', -1, 32))
					default:
						v := slice.GetSampleFloat32(x, y, s)
						fmt.Fprintf(&b, "%s=%s", ch.name, strconv.FormatFloat(float64(v), 'g', -1, 32))
					}
				}
			}
			b.WriteString("\n")
		}
	}
	fmt.Print(b.String())
	return nil
}

func writeScanline(path string, chans []channel, comp exr.Compression) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	wr, err := exr.NewDeepScanlineWriter(f, fixW, fixH)
	if err != nil {
		return err
	}
	wr.Header().SetCompression(comp)
	wr.SetFrameBuffer(fill(chans))
	if err := wr.WritePixels(fixH); err != nil {
		return err
	}
	// Finalize is what writes the chunk offset table; without it the file is
	// a header followed by zeros where the offsets should be.
	return wr.Finalize()
}

func writeTiled(path string, chans []channel, comp exr.Compression) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	wr, err := exr.NewDeepTiledWriter(f, fixW, fixH, tileW, tileH)
	if err != nil {
		return err
	}
	wr.Header().SetCompression(comp)
	wr.SetFrameBuffer(fill(chans))
	tx := (fixW + tileW - 1) / tileW
	ty := (fixH + tileH - 1) / tileH
	if err := wr.WriteTiles(0, 0, tx-1, ty-1); err != nil {
		return err
	}
	return wr.Finalize()
}

type row struct {
	name  string
	kind  string // deepscanline or deeptiled
	codec string
	comp  exr.Compression
	chans []channel
	// permitted is true when the OpenEXR reference implementation accepts
	// this codec for deep data. A row that is not permitted must be refused
	// by this library rather than written.
	permitted bool
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: deepgen <outdir> | deepgen -dump <file.exr> [part]")
		os.Exit(2)
	}
	if os.Args[1] == "-dump" {
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: deepgen -dump <file.exr> [part]")
			os.Exit(2)
		}
		part := 0
		if len(os.Args) > 3 {
			var err error
			if part, err = strconv.Atoi(os.Args[3]); err != nil {
				fmt.Fprintf(os.Stderr, "part: %v\n", err)
				os.Exit(2)
			}
		}
		if err := dump(os.Args[2], part); err != nil {
			fmt.Fprintf(os.Stderr, "dump: %v\n", err)
			os.Exit(1)
		}
		return
	}
	outDir := os.Args[1]

	// The codecs the reference implementation accepts for deep data, and the
	// ones it does not. A deep chunk is one scanline of variable-length sample
	// data, so a codec that compresses a fixed block of several scanlines has
	// nothing to operate on; OpenEXR rejects the file when it is opened, with
	// EXR_ERR_INVALID_ATTR "Invalid compression for deep data". That was
	// measured on this machine for ZIP, PIZ, B44 and both HTJ2K variants by
	// patching the compression byte of a deep file the reference does accept.
	//
	// The permitted codecs are written and compared sample for sample. The
	// forbidden ones are expected to be refused by this library rather than
	// written: a row that gets written anyway lands in the manifest as an
	// ordinary row and is measured against the reference, which fails it.
	codecs := []struct {
		name      string
		comp      exr.Compression
		permitted bool
	}{
		{"none", exr.CompressionNone, true},
		{"rle", exr.CompressionRLE, true},
		{"zips", exr.CompressionZIPS, true},
		{"zip", exr.CompressionZIP, false},
		{"piz", exr.CompressionPIZ, false},
	}

	var rows []row
	for _, c := range codecs {
		rows = append(rows,
			row{"ds_float_" + c.name, "deepscanline", c.name, c.comp, floatChans, c.permitted},
			row{"dt_float_" + c.name, "deeptiled", c.name, c.comp, floatChans, c.permitted},
		)
	}
	rows = append(rows,
		row{"ds_mixed_zips", "deepscanline", "zips", exr.CompressionZIPS, mixedChans, true},
		row{"dt_mixed_zips", "deeptiled", "zips", exr.CompressionZIPS, mixedChans, true},
	)

	var manifest strings.Builder
	manifest.WriteString("# file\tkind\tcodec\tchannels\ttile\tstatus\n")

	written, refused, failed := 0, 0, 0
	for _, r := range rows {
		path := filepath.Join(outDir, r.name+".exr")
		var err error
		tile := "-"
		if r.kind == "deeptiled" {
			tile = fmt.Sprintf("%dx%d", tileW, tileH)
			err = writeTiled(path, r.chans, r.comp)
		} else {
			err = writeScanline(path, r.chans, r.comp)
		}
		names := make([]string, len(r.chans))
		for i, ch := range r.chans {
			names[i] = ch.name
		}
		if err != nil {
			if !r.permitted {
				// The library declined to write a codec the reference would
				// refuse. That is the required behaviour, and the manifest
				// carries it as a row so the gate asserts it.
				os.Remove(path)
				fmt.Fprintf(&manifest, "%s.exr\t%s\t%s\t%s\t%s\trefused\n",
					r.name, r.kind, r.codec, strings.Join(names, ","), tile)
				refused++
				continue
			}
			fmt.Printf("FAIL write %-16s: %v\n", r.name, err)
			failed++
			continue
		}
		if err := expect(path+".expect", r.chans); err != nil {
			fmt.Printf("FAIL expect %-16s: %v\n", r.name, err)
			failed++
			continue
		}
		fmt.Fprintf(&manifest, "%s.exr\t%s\t%s\t%s\t%s\tok\n",
			r.name, r.kind, r.codec, strings.Join(names, ","), tile)
		written++
	}

	if err := os.WriteFile(filepath.Join(outDir, "manifest.tsv"), []byte(manifest.String()), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "manifest: %v\n", err)
		os.Exit(1)
	}
	total := 0
	for y := 0; y < fixH; y++ {
		for x := 0; x < fixW; x++ {
			total += int(sampleCount(x, y))
		}
	}
	fmt.Printf("wrote %d deep files (%dx%d, %d samples, counts 0..4), %d codecs refused, %d write failures\n",
		written, fixW, fixH, total, refused, failed)
	if failed > 0 {
		os.Exit(1)
	}
}
