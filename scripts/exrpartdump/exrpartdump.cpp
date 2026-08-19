// exrpartdump — print every sample of one scanline part of an EXR, using the
// reference implementation, honouring per-channel subsampling.
//
// It exists because oiiotool cannot read subsampled channels at all — it
// refuses the file outright with "Subsampled channels are not supported
// (channel \"BY\" has sampling 2,1)" — and silently exposes only the
// unsubsampled parts of a multi-part file that contains one. That makes it
// unusable as the oracle for a subsampled part, while libOpenEXR itself reads
// them without complaint.
//
//   exrpartdump [-part N] file.exr
//
// Output is one line per sample:
//
//   <channel> <x> <y> <value>
//
// where x and y are the channel's own coordinates: a channel with xSampling 2
// has ceil(width/2) columns, and column x of it is column 2x of the image.
// Lines beginning with '#' describe the part.

#include <ImfChannelList.h>
#include <ImfFrameBuffer.h>
#include <ImfHeader.h>
#include <ImfInputFile.h>
#include <ImfInputPart.h>
#include <ImfMultiPartInputFile.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace
{

struct ChannelPlane
{
    std::string        name;
    int                xs, ys;
    int                w, h;
    std::vector<float> data;
};

template <class Reader>
int dumpPart (Reader& in, const Imf::Header& h)
{
    Imath::Box2i dw = h.dataWindow ();
    int          w  = dw.max.x - dw.min.x + 1;
    int          hh = dw.max.y - dw.min.y + 1;

    printf ("# window %d %d %d %d\n", dw.min.x, dw.min.y, dw.max.x, dw.max.y);

    std::vector<ChannelPlane> planes;
    for (Imf::ChannelList::ConstIterator i = h.channels ().begin ();
         i != h.channels ().end (); ++i)
    {
        ChannelPlane p;
        p.name = i.name ();
        p.xs   = i.channel ().xSampling;
        p.ys   = i.channel ().ySampling;
        // A subsampled channel stores every xs-th column of every ys-th row,
        // so its plane is smaller than the image in exactly that ratio.
        p.w = (w + p.xs - 1) / p.xs;
        p.h = (hh + p.ys - 1) / p.ys;
        p.data.assign ((size_t) p.w * (size_t) p.h, 0.0f);
        planes.push_back (p);
        printf ("# channel %s %d %d %d %d\n", p.name.c_str (), p.xs, p.ys, p.w, p.h);
    }

    Imf::FrameBuffer fb;
    for (size_t c = 0; c < planes.size (); ++c)
    {
        ChannelPlane& p       = planes[c];
        size_t        xStride = sizeof (float);
        size_t        yStride = (size_t) p.w * sizeof (float);
        // The base addresses image pixel (0,0); the sample for image pixel
        // (x,y) lives at (x/xs, y/ys) of the plane, so the origin shift is
        // divided by the sampling as well.
        char* base = (char*) p.data.data ();
        base -= (size_t) (dw.min.x / p.xs) * xStride +
                (size_t) (dw.min.y / p.ys) * yStride;
        fb.insert (p.name,
                   Imf::Slice (Imf::FLOAT, base, xStride, yStride, p.xs, p.ys));
    }

    in.setFrameBuffer (fb);
    in.readPixels (dw.min.y, dw.max.y);

    for (size_t c = 0; c < planes.size (); ++c)
    {
        const ChannelPlane& p = planes[c];
        for (int y = 0; y < p.h; ++y)
            for (int x = 0; x < p.w; ++x)
                printf ("%s %d %d %.9g\n", p.name.c_str (), x, y,
                        p.data[(size_t) y * p.w + x]);
    }
    return 0;
}

} // namespace

int
main (int argc, char** argv)
{
    int part = -1;
    int argi = 1;
    while (argi < argc && argv[argi][0] == '-')
    {
        if (strcmp (argv[argi], "-part") == 0 && argi + 1 < argc)
        {
            part = atoi (argv[argi + 1]);
            argi += 2;
        }
        else
            break;
    }
    if (argi >= argc)
    {
        fprintf (stderr, "usage: exrpartdump [-part N] file.exr\n");
        return 2;
    }

    try
    {
        if (part >= 0)
        {
            Imf::MultiPartInputFile mp (argv[argi]);
            if (part >= mp.parts ())
            {
                fprintf (stderr, "ERROR %s: part %d of %d\n", argv[argi], part,
                         mp.parts ());
                return 1;
            }
            Imf::InputPart in (mp, part);
            return dumpPart (in, in.header ());
        }
        Imf::InputFile in (argv[argi]);
        return dumpPart (in, in.header ());
    }
    catch (const std::exception& e)
    {
        fprintf (stderr, "ERROR %s: %s\n", argv[argi], e.what ());
        return 1;
    }
}
