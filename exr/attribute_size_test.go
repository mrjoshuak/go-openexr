package exr

import (
	"bytes"
	"strings"
	"testing"

	"github.com/mrjoshuak/go-openexr/internal/xdr"
)

// buildHeaderBytes assembles a header from raw attribute triples, so a test can
// write a size field that disagrees with the value that follows it. Nothing in
// the ordinary writer can produce that, which is why it has to be done by hand.
func buildHeaderBytes(attrs ...[]byte) []byte {
	var b bytes.Buffer
	for _, a := range attrs {
		b.Write(a)
	}
	b.WriteByte(0) // the empty name that ends the header
	return b.Bytes()
}

func rawAttr(name, typ string, size int32, payload []byte) []byte {
	w := xdr.NewBufferWriter(64)
	w.WriteString(name)
	w.WriteString(typ)
	w.WriteInt32(size)
	w.WriteBytes(payload)
	return w.Bytes()
}

// TestAttributeSizeIsReconciled is the check that turns a silent wrong answer
// back into a refusal.
//
// An attribute's declared size and the bytes its type actually reads must
// agree. When they did not, the reader carried on from the wrong offset: every
// attribute after it was parsed from the middle of something else, and because
// the loop ends at an empty name it could stop early with a half-built header.
// The observable result was Channels() returning nil on a file libOpenEXR
// refuses outright — the parse failure surfacing as missing data rather than as
// an error.
func TestAttributeSizeIsReconciled(t *testing.T) {
	// An int attribute is four bytes. Declaring eight leaves the reader four
	// bytes behind whatever comes next.
	short := rawAttr("liar", "int", 8, []byte{1, 0, 0, 0})
	r := xdr.NewReader(buildHeaderBytes(short))
	_, err := ReadAttribute(r)
	if err == nil {
		t.Fatal("an int attribute declaring eight bytes was accepted; the reader is now " +
			"four bytes out of step and everything after it parses from the wrong offset")
	}
	if !strings.Contains(err.Error(), "liar") {
		t.Errorf("the error does not name the attribute: %v", err)
	}

	// The other direction: a size smaller than the type reads.
	long := rawAttr("liar2", "int", 2, []byte{1, 0, 0, 0})
	r2 := xdr.NewReader(buildHeaderBytes(long))
	if _, err := ReadAttribute(r2); err == nil {
		t.Error("an int attribute declaring two bytes was accepted")
	}

	// A negative size is nonsense and must not be reached by the switch at all.
	neg := rawAttr("liar3", "int", -4, []byte{1, 0, 0, 0})
	r3 := xdr.NewReader(buildHeaderBytes(neg))
	if _, err := ReadAttribute(r3); err == nil {
		t.Error("an attribute declaring a negative size was accepted")
	}

	// The control: an honest attribute must still read, or this check is
	// satisfied by a reader that rejects everything.
	good := rawAttr("honest", "int", 4, []byte{7, 0, 0, 0})
	r4 := xdr.NewReader(buildHeaderBytes(good))
	attr, err := ReadAttribute(r4)
	if err != nil {
		t.Fatalf("an honest int attribute was refused: %v", err)
	}
	if attr == nil || attr.Value.(int32) != 7 {
		t.Errorf("the honest attribute read back as %v, want 7", attr)
	}
}

// TestUnknownAttributeStillRoundTrips guards the case the size check must not
// break: a type this library does not model is kept as raw bytes and consumes
// exactly its declared size, so an unfamiliar attribute is preserved rather
// than making the file unreadable.
func TestUnknownAttributeStillRoundTrips(t *testing.T) {
	payload := []byte{9, 8, 7, 6, 5}
	raw := rawAttr("somethingNew", "notATypeWeKnow", int32(len(payload)), payload)

	r := xdr.NewReader(buildHeaderBytes(raw))
	attr, err := ReadAttribute(r)
	if err != nil {
		t.Fatalf("an unknown attribute type was refused: %v", err)
	}
	got, ok := attr.Value.([]byte)
	if !ok {
		t.Fatalf("an unknown attribute came back as %T, want raw bytes", attr.Value)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("raw bytes = %v, want %v", got, payload)
	}

	// And the reader must be positioned at the end of the header, which is the
	// whole point of reconciling the size.
	next, err := ReadAttribute(r)
	if err != nil {
		t.Fatalf("reading past the unknown attribute: %v", err)
	}
	if next != nil {
		t.Errorf("expected the end of the header after the unknown attribute, got %q", next.Name)
	}
}
