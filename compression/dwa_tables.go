package compression

import (
	_ "embed"
	"encoding/binary"
)

// DWA codes pixels in a perceptually flat space rather than in linear light.
// The mapping is a pair of half -> half lookup tables:
//
//	toNonlinear(x) = x^(1/2.2)                for |x| <= 1
//	                 ln(x)/2.2 + 1            otherwise
//	toLinear(x)    = x^2.2                    for |x| <= 1
//	                 e^(2.2*(x-1))            otherwise
//
// with infinities and NaNs mapped to zero.
//
// The tables below are byte-for-byte copies of dwaCompressorToLinear[] and
// dwaCompressorToNonlinear[] from OpenEXR's src/lib/OpenEXRCore/dwaLookups.h
// (v3.4.1), stored as little-endian uint16.
//
// They are embedded rather than evaluated from the formulae above because
// recomputing them does not reproduce them exactly: pow() and log() differ by
// one ulp between C libraries for a handful of inputs, and rounding those
// results to half moves the table entry. Recomputing in Go leaves 2 of 65536
// entries of toLinear and 8 of toNonlinear disagreeing with the reference. A
// decoder that disagrees with the reference implementation on any input value
// is a decoder that does not read the file the reference wrote, so the
// reference table is what ships. dwa_tables_test.go pins both to the SHA-256
// of the corresponding array in dwaLookups.h.
//
//go:embed dwa_tolinear.bin
var dwaToLinearBytes []byte

//go:embed dwa_tononlinear.bin
var dwaToNonlinearBytes []byte

// dwaToLinear and dwaToNonlinear are the decoded forms of the tables above.
var (
	dwaToLinear    [65536]uint16
	dwaToNonlinear [65536]uint16
)

func init() {
	if len(dwaToLinearBytes) != 2*len(dwaToLinear) || len(dwaToNonlinearBytes) != 2*len(dwaToNonlinear) {
		panic("compression: embedded DWA lookup table has the wrong size")
	}
	for i := range dwaToLinear {
		dwaToLinear[i] = binary.LittleEndian.Uint16(dwaToLinearBytes[i*2:])
		dwaToNonlinear[i] = binary.LittleEndian.Uint16(dwaToNonlinearBytes[i*2:])
	}
}
