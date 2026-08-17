package predictor

import (
	"bytes"
	"math/rand"
	"testing"
)

// refEncode is a direct, deliberately naive transcription of the predictor loop
// in OpenEXR's ImfZipCompressor.cpp:
//
//	int p = t[-1];
//	while (t < stop) { int d = int(t[0]) - p + (128 + 256); p = t[0]; t[0] = d; ++t; }
//
// It exists to pin the optimized implementations to the specification rather
// than to each other.
func refEncode(src []byte) []byte {
	out := append([]byte(nil), src...)
	if len(out) < 2 {
		return out
	}
	p := int(out[0])
	for i := 1; i < len(out); i++ {
		d := int(out[i]) - p + (128 + 256)
		p = int(out[i])
		out[i] = byte(d)
	}
	return out
}

// refDecode transcribes the matching uncompress loop:
//
//	int d = int(t[-1]) + int(t[0]) - 128; t[0] = d;
func refDecode(src []byte) []byte {
	out := append([]byte(nil), src...)
	for i := 1; i < len(out); i++ {
		out[i] = byte(int(out[i-1]) + int(out[i]) - 128)
	}
	return out
}

// TestEncodeMatchesSpecVector checks Encode against a hand-computed vector.
// Deriving the expectation by hand (not by calling Decode) is the whole point:
// a round-trip assertion passes even when both directions are wrong.
func TestEncodeMatchesSpecVector(t *testing.T) {
	// in[0] passes through untouched; every later byte is (cur-prev+128) mod 256.
	in := []byte{10, 15, 20, 25, 24, 0, 255, 128}
	want := []byte{
		10,
		(15 - 10 + 128) & 0xff,
		(20 - 15 + 128) & 0xff,
		(25 - 20 + 128) & 0xff,
		(24 - 25 + 128 + 256) & 0xff,
		(0 - 24 + 128 + 256) & 0xff,
		(255 - 0 + 128) & 0xff,
		(128 - 255 + 128 + 256) & 0xff,
	}

	got := append([]byte(nil), in...)
	Encode(got)
	if !bytes.Equal(got, want) {
		t.Errorf("Encode = %v, want %v", got, want)
	}
	if ref := refEncode(in); !bytes.Equal(ref, want) {
		t.Fatalf("test vector disagrees with reference transcription: %v vs %v", ref, want)
	}
}

// TestDecodeMatchesSpecVector checks Decode against the reference transcription
// on the encoded form of a known input.
func TestDecodeMatchesSpecVector(t *testing.T) {
	in := []byte{10, 15, 20, 25, 24, 0, 255, 128}
	encoded := refEncode(in)

	got := append([]byte(nil), encoded...)
	Decode(got)
	if !bytes.Equal(got, in) {
		t.Errorf("Decode(refEncode(%v)) = %v, want %v", in, got, in)
	}
}

// TestPredictorAgainstReference fuzzes every implementation against the
// reference transcription across sizes that straddle the unrolled and
// remainder paths of each variant.
func TestPredictorAgainstReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	encoders := map[string]func([]byte){
		"Encode":      Encode,
		"EncodeSIMD":  EncodeSIMD,
		"EncodeBatch": EncodeBatch,
	}
	decoders := map[string]func([]byte){
		"Decode":      Decode,
		"DecodeSIMD":  DecodeSIMD,
		"DecodeBatch": DecodeBatch,
	}

	for _, n := range []int{0, 1, 2, 3, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 1000} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(rng.Intn(256))
		}
		wantEnc := refEncode(src)
		wantDec := refDecode(src)

		for name, fn := range encoders {
			got := append([]byte(nil), src...)
			fn(got)
			if !bytes.Equal(got, wantEnc) {
				t.Errorf("%s(n=%d) = %v, want %v", name, n, got, wantEnc)
			}
		}
		for name, fn := range decoders {
			got := append([]byte(nil), src...)
			fn(got)
			if !bytes.Equal(got, wantDec) {
				t.Errorf("%s(n=%d) = %v, want %v", name, n, got, wantDec)
			}
		}
	}
}

// TestEncodeIsNotPlainDifferencing guards the specific regression that shipped:
// an unbiased predictor round-trips perfectly but is not the OpenEXR predictor.
func TestEncodeIsNotPlainDifferencing(t *testing.T) {
	in := []byte{10, 20, 30, 40}
	got := append([]byte(nil), in...)
	Encode(got)

	plain := append([]byte(nil), in...)
	for i := len(plain) - 1; i >= 1; i-- {
		plain[i] = plain[i] - plain[i-1]
	}
	if bytes.Equal(got, plain) {
		t.Error("Encode produced unbiased differences; the OpenEXR +128 bias is missing")
	}
}
