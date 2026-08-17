package compression

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"testing"

	"github.com/mrjoshuak/go-openexr/half"
)

// The digests below are of dwaCompressorToLinear[] and
// dwaCompressorToNonlinear[] as they appear in OpenEXR v3.4.1's
// src/lib/OpenEXRCore/dwaLookups.h, each written out as 65536 little-endian
// uint16. Reproduce them with:
//
//	python3 - <<'EOF'
//	import re, struct, hashlib
//	src = open('dwaLookups.h').read()
//	for name in ('dwaCompressorToLinear', 'dwaCompressorToNonlinear'):
//	    i = src.index('unsigned short %s[] = {' % name)
//	    body = src[i:src.index('};', i)]
//	    vals = [int(v, 16) for v in re.findall(r'0x([0-9a-fA-F]{4})', body)]
//	    print(name, hashlib.sha256(struct.pack('<65536H', *vals)).hexdigest())
//	EOF
//
// Pinning them here is what makes the embedded blobs reviewable: the bytes
// this package ships are exactly the reference implementation's table, and a
// well-meaning "regenerate the table from the formula" change fails.
const (
	dwaToLinearDigest    = "72e80880a49b9428073cf87e2b181a6da74cfac9f8f7f4c6e459f0ac1f0225a5"
	dwaToNonlinearDigest = "9439b1fe1d69d77b3ea98dbe0af73e5bb7f4224b61eec3335a40b0c31bbccab8"
)

func TestDwaLookupTablesMatchReference(t *testing.T) {
	for _, tt := range []struct {
		name   string
		data   []byte
		digest string
	}{
		{"dwaCompressorToLinear", dwaToLinearBytes, dwaToLinearDigest},
		{"dwaCompressorToNonlinear", dwaToNonlinearBytes, dwaToNonlinearDigest},
	} {
		if len(tt.data) != 65536*2 {
			t.Errorf("%s is %d bytes, want %d", tt.name, len(tt.data), 65536*2)
			continue
		}
		sum := sha256.Sum256(tt.data)
		if got := hex.EncodeToString(sum[:]); got != tt.digest {
			t.Errorf("%s digest is %s, the reference table's is %s", tt.name, got, tt.digest)
		}
	}
}

// TestDwaLookupTablesFollowTheirFormulae is a sanity check on the embedded
// blobs: they must agree with the curves dwaLookups.cpp derives them from
// everywhere except where the two libraries' pow() disagree by an ulp.
func TestDwaLookupTablesFollowTheirFormulae(t *testing.T) {
	logBase := math.Pow(2.7182818, 2.2)

	check := func(name string, table *[65536]uint16, want func(float64) float64) {
		mismatch := 0
		for i := 1; i < 65536; i++ {
			if i&0x7c00 == 0x7c00 {
				// Infinities and NaNs are defined to map to zero.
				if table[i] != 0 {
					t.Fatalf("%s[%#04x] = %#04x, want 0 for a non-finite input", name, i, table[i])
				}
				continue
			}
			in := half.FromBits(uint16(i)).Float32()
			sign := 1.0
			a := float64(in)
			if a < 0 {
				sign, a = -1, -a
			}
			expect := half.FromFloat64(sign * want(a))
			if table[i] != expect.Bits() {
				mismatch++
			}
		}
		// Ten out of 65536 is the observed cost of libm differences; anything
		// beyond that is a table that is not the function it claims to be.
		if mismatch > 16 {
			t.Errorf("%s disagrees with its formula at %d of 65535 inputs", name, mismatch)
		}
		if table[0] != 0 {
			t.Errorf("%s[0] = %#04x, want 0", name, table[0])
		}
	}

	check("toLinear", &dwaToLinear, func(a float64) float64 {
		if a <= 1.0 {
			return math.Pow(a, 2.2)
		}
		return math.Pow(logBase, a-1.0)
	})
	check("toNonlinear", &dwaToNonlinear, func(a float64) float64 {
		if a <= 1.0 {
			return math.Pow(a, 1.0/2.2)
		}
		return math.Log(a)/math.Log(logBase) + 1.0
	})
}
