package exr

import (
	"bytes"
	"errors"
	"testing"
)

func subsampledHeader(name string, ys int32) *Header {
	h := NewHeader()
	h.SetDataWindow(Box2i{V2i{0, 0}, V2i{31, 15}})
	h.SetDisplayWindow(Box2i{V2i{0, 0}, V2i{31, 15}})
	h.SetCompression(CompressionZIP)
	h.SetLineOrder(LineOrderIncreasing)
	h.SetPixelAspectRatio(1)
	h.SetScreenWindowCenter(V2f{0, 0})
	h.SetScreenWindowWidth(1)
	cl := NewChannelList()
	cl.Add(Channel{Name: "Y", Type: PixelTypeHalf, XSampling: 1, YSampling: 1})
	cl.Add(Channel{Name: "BY", Type: PixelTypeHalf, XSampling: 2, YSampling: ys})
	h.SetChannels(cl)
	h.Set(&Attribute{Name: AttrNameName, Type: AttrTypeString, Value: name})
	h.Set(&Attribute{Name: AttrNameType, Type: AttrTypeString, Value: PartTypeScanline})
	return h
}

// TestMultiPartRefusesYSubsampling pins the same contract ScanlineWriter keeps.
//
// XSampling above 1 narrows each row, which the chunk layout can express.
// YSampling above 1 removes whole rows from a scanline, which it cannot — so
// the writer must refuse rather than produce a file whose chunks are the wrong
// length. Multi-part had neither the narrowing nor the refusal: it packed the
// full width for every channel, which made the chunk longer than the format
// says and put every channel after a subsampled one at the wrong offset.
func TestMultiPartRefusesYSubsampling(t *testing.T) {
	var buf writeSeekBuffer
	_, err := NewMultiPartWriter(&buf, []*Header{
		subsampledHeader("a", 1),
		subsampledHeader("b", 2),
	})
	if err == nil {
		t.Fatal("a multi-part file with a ySampling of 2 was accepted; the chunk layout cannot express it")
	}
	if !errors.Is(err, ErrSubsampledChannels) {
		t.Errorf("refused with %v, want an error wrapping ErrSubsampledChannels", err)
	}
}

// TestMultiPartAcceptsXSubsampling is the control: the refusal above must be
// specific to the direction that cannot be represented, or it would be
// satisfied by a writer that rejects all subsampling.
func TestMultiPartAcceptsXSubsampling(t *testing.T) {
	var buf writeSeekBuffer
	if _, err := NewMultiPartWriter(&buf, []*Header{
		subsampledHeader("a", 1),
		subsampledHeader("b", 1),
	}); err != nil {
		t.Fatalf("xSampling of 2 was refused: %v", err)
	}
}

// writeSeekBuffer is the smallest io.WriteSeeker over memory that the writer
// needs: it seeks back to fill in the chunk offset table.
type writeSeekBuffer struct {
	buf bytes.Buffer
	pos int64
}

func (w *writeSeekBuffer) Write(p []byte) (int, error) {
	if w.pos < int64(w.buf.Len()) {
		b := w.buf.Bytes()
		n := copy(b[w.pos:], p)
		w.pos += int64(n)
		if n == len(p) {
			return n, nil
		}
		p = p[n:]
	}
	n, err := w.buf.Write(p)
	w.pos += int64(n)
	return n, err
}

func (w *writeSeekBuffer) Seek(off int64, whence int) (int64, error) {
	switch whence {
	case 0:
		w.pos = off
	case 1:
		w.pos += off
	case 2:
		w.pos = int64(w.buf.Len()) + off
	}
	return w.pos, nil
}
