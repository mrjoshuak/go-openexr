// Command fvwrite writes an EXR carrying a floatvector attribute, so the gate
// can ask the reference what it makes of the encoding.
//
// It exists because a round trip cannot see a wire format that is wrong in both
// directions, and this one was: the attribute carried a leading int32 count
// that the format does not have. oiiotool prints the values, so it can say
// whether the count leaked in as an extra element.
//
//	fvwrite <out.exr>
//
// The values are 1.5, 2.5 and 3.5, and there are three of them.
package main

import (
	"os"

	"github.com/mrjoshuak/go-openexr/exr"
)

func main() {
	h := exr.NewScanlineHeader(4, 4)
	h.SetCompression(exr.CompressionNone)
	cl := exr.NewChannelList()
	cl.Add(exr.Channel{Name: "Y", Type: exr.PixelTypeFloat, XSampling: 1, YSampling: 1})
	h.SetChannels(cl)
	h.Set(&exr.Attribute{
		Name:  "myFloats",
		Type:  exr.AttrTypeFloatVector,
		Value: exr.FloatVector{1.5, 2.5, 3.5},
	})

	f, err := os.Create(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w, err := exr.NewScanlineWriter(f, h)
	if err != nil {
		panic(err)
	}
	fb, _ := exr.AllocateChannels(h.Channels(), h.DataWindow())
	s := fb.Get("Y")
	for y := 0; y < 4; y++ {
		for x := 0; x < 4; x++ {
			s.SetFloat32(x, y, float32(y))
		}
	}
	w.SetFrameBuffer(fb)
	if err := w.WritePixels(0, 3); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
}
