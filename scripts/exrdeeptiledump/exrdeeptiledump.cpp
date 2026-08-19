// exrdeeptiledump — print every sample of every resolution level of a deep
// tiled EXR, using the reference implementation.
//
// oiiotool reads a deep tiled file's level 0 and nothing else: --selectmip does
// not compose with --dumpdata for deep images, so every level but the first
// comes back with no pixels at all. A generator that wrote level 3 into level
// 1's chunk slots would pass anything built on that, which is precisely the
// defect this exists to catch — DeepTiledWriter indexed its offset table by
// tileY*tilesX+tileX and so could only ever hold one level.
//
//   exrdeeptiledump file.exr
//
// Output is one line per pixel per level:
//
//   # level <lx> <ly> <w> <h>
//   Pixel (<x>, <y>): <n> samples : A=<v> Z=<v> / ...
//
// which is the shape oiiotool --dumpdata prints, so scripts/deepdiff.awk
// compares the two without a second parser.

#include <ImfChannelList.h>
#include <ImfDeepFrameBuffer.h>
#include <ImfDeepTiledInputFile.h>
#include <ImfHeader.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

namespace
{

const char*
modeName (Imf::LevelMode m)
{
    switch (m)
    {
        case Imf::ONE_LEVEL: return "one";
        case Imf::MIPMAP_LEVELS: return "mipmap";
        case Imf::RIPMAP_LEVELS: return "ripmap";
        default: return "unknown";
    }
}

void
dumpLevel (Imf::DeepTiledInputFile& in, int lx, int ly)
{
    const Imf::Header& h  = in.header ();
    Imath::Box2i       dw = in.dataWindowForLevel (lx, ly);
    int                w  = dw.max.x - dw.min.x + 1;
    int                hh = dw.max.y - dw.min.y + 1;

    printf ("# level %d %d %d %d\n", lx, ly, w, hh);

    std::vector<std::string> names;
    for (Imf::ChannelList::ConstIterator i = h.channels ().begin ();
         i != h.channels ().end (); ++i)
        names.push_back (i.name ());

    std::vector<unsigned int>            counts ((size_t) w * hh, 0);
    std::vector<std::vector<float*>>     ptrs (names.size ());
    std::vector<std::vector<float>>      store (names.size ());

    Imf::DeepFrameBuffer fb;

    // The sample count slice, based so that pixel (dw.min) is element 0.
    char* cbase = (char*) counts.data ();
    cbase -= (size_t) dw.min.x * sizeof (unsigned int) +
             (size_t) dw.min.y * (size_t) w * sizeof (unsigned int);
    fb.insertSampleCountSlice (Imf::Slice (
        Imf::UINT, cbase, sizeof (unsigned int), (size_t) w * sizeof (unsigned int)));

    for (size_t c = 0; c < names.size (); ++c)
    {
        ptrs[c].assign ((size_t) w * hh, nullptr);
        char* pbase = (char*) ptrs[c].data ();
        pbase -= (size_t) dw.min.x * sizeof (float*) +
                 (size_t) dw.min.y * (size_t) w * sizeof (float*);
        fb.insert (names[c],
                   Imf::DeepSlice (Imf::FLOAT, pbase, sizeof (float*),
                                   (size_t) w * sizeof (float*), sizeof (float)));
    }

    in.setFrameBuffer (fb);
    in.readPixelSampleCounts (0, in.numXTiles (lx) - 1, 0, in.numYTiles (ly) - 1, lx, ly);

    // Allocate one contiguous block per channel and point each pixel at its
    // own run, which is what DeepSlice expects.
    size_t total = 0;
    for (size_t i = 0; i < counts.size (); ++i)
        total += counts[i];
    for (size_t c = 0; c < names.size (); ++c)
    {
        store[c].assign (total ? total : 1, 0.0f);
        size_t at = 0;
        for (size_t i = 0; i < counts.size (); ++i)
        {
            ptrs[c][i] = store[c].data () + at;
            at += counts[i];
        }
    }

    in.readTiles (0, in.numXTiles (lx) - 1, 0, in.numYTiles (ly) - 1, lx, ly);

    for (int y = 0; y < hh; ++y)
        for (int x = 0; x < w; ++x)
        {
            size_t i = (size_t) y * w + x;
            printf ("Pixel (%d, %d): %u samples ", x, y, counts[i]);
            for (unsigned int s = 0; s < counts[i]; ++s)
            {
                printf ("%s", s ? " / " : ": ");
                for (size_t c = 0; c < names.size (); ++c)
                    printf ("%s%s=%.9g", c ? " " : "", names[c].c_str (), ptrs[c][i][s]);
            }
            printf ("\n");
        }
}

} // namespace

int
main (int argc, char** argv)
{
    if (argc != 2)
    {
        fprintf (stderr, "usage: exrdeeptiledump file.exr\n");
        return 2;
    }

    try
    {
        Imf::DeepTiledInputFile in (argv[1]);
        printf ("# mode %s\n", modeName (in.levelMode ()));
        printf ("# levels %d %d\n", in.numXLevels (), in.numYLevels ());

        switch (in.levelMode ())
        {
            case Imf::ONE_LEVEL: dumpLevel (in, 0, 0); break;
            case Imf::MIPMAP_LEVELS:
                for (int l = 0; l < in.numLevels (); ++l)
                    dumpLevel (in, l, l);
                break;
            case Imf::RIPMAP_LEVELS:
                for (int ly = 0; ly < in.numYLevels (); ++ly)
                    for (int lx = 0; lx < in.numXLevels (); ++lx)
                        dumpLevel (in, lx, ly);
                break;
            default: fprintf (stderr, "unknown level mode\n"); return 1;
        }
    }
    catch (const std::exception& e)
    {
        fprintf (stderr, "ERROR %s: %s\n", argv[1], e.what ());
        return 1;
    }
    return 0;
}
