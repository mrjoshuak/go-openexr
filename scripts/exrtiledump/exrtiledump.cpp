// exrtiledump — read a tiled OpenEXR file with the OpenEXR reference
// implementation and print every sample of every resolution level.
//
// This is the oracle for the tiled half of scripts/validate.sh. It links
// against libOpenEXR itself, so nothing in go-openexr participates in reading
// the files it is pointed at: if this program and the fixture generator
// disagree about a sample, the file is wrong.
//
//   exrtiledump file.exr          one line per sample: "lx ly x y CHANNEL value"
//   exrtiledump -info file.exr    only the structure lines, each prefixed "#"
//
// Structure lines are printed in both modes and always begin with '#', so the
// comparison in validate.sh can ignore them:
//
//   # mode <one|mipmap|ripmap>
//   # tile <xsize> <ysize> <rounding>
//   # levels <numXLevels> <numYLevels>
//   # level <lx> <ly> <width> <height> <numXTiles> <numYTiles>
//
// Values are printed as %.9g, which round-trips a float exactly and therefore
// also round-trips a half exactly. A sample the reference reads as a different
// number prints as a different string.
//
// Build:  c++ -std=c++17 $(pkg-config --cflags --libs OpenEXR) -o exrtiledump exrtiledump.cpp

#include <ImfChannelList.h>
#include <ImfFrameBuffer.h>
#include <ImfHeader.h>
#include <ImfTileDescription.h>
#include <ImfTiledInputFile.h>

#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

namespace
{

const char* modeName (Imf::LevelMode m)
{
    switch (m)
    {
        case Imf::ONE_LEVEL: return "one";
        case Imf::MIPMAP_LEVELS: return "mipmap";
        case Imf::RIPMAP_LEVELS: return "ripmap";
        default: return "unknown";
    }
}

// dumpLevel reads one resolution level in full and prints it. Every channel is
// requested as FLOAT; half converts to float exactly, so no information the
// file holds is lost on the way to the comparison.
void dumpLevel (Imf::TiledInputFile& in, int lx, int ly, bool samples)
{
    const Imf::Header& h  = in.header ();
    Imath::Box2i       dw = in.dataWindowForLevel (lx, ly);

    int w = dw.max.x - dw.min.x + 1;
    int h_ = dw.max.y - dw.min.y + 1;

    printf ("# level %d %d %d %d %d %d\n", lx, ly, w, h_,
            in.numXTiles (lx), in.numYTiles (ly));
    if (!samples) return;

    std::vector<std::string>          names;
    std::vector<std::vector<float>>   data;

    for (Imf::ChannelList::ConstIterator i = h.channels ().begin ();
         i != h.channels ().end (); ++i)
        names.push_back (i.name ());

    data.resize (names.size ());
    Imf::FrameBuffer fb;
    for (size_t c = 0; c < names.size (); ++c)
    {
        data[c].assign ((size_t) w * (size_t) h_, 0.0f);
        char* base = (char*) data[c].data ();
        // The frame buffer's base pointer addresses pixel (0,0) of the image
        // coordinate system, so shift it by the level's data window origin.
        base -= (size_t) dw.min.x * sizeof (float) +
                (size_t) dw.min.y * (size_t) w * sizeof (float);
        fb.insert (names[c],
                   Imf::Slice (Imf::FLOAT, base, sizeof (float),
                               (size_t) w * sizeof (float)));
    }

    in.setFrameBuffer (fb);
    in.readTiles (0, in.numXTiles (lx) - 1, 0, in.numYTiles (ly) - 1, lx, ly);

    for (size_t c = 0; c < names.size (); ++c)
        for (int y = 0; y < h_; ++y)
            for (int x = 0; x < w; ++x)
                printf ("%d %d %d %d %s %.9g\n", lx, ly, x, y,
                        names[c].c_str (), data[c][(size_t) y * w + x]);
}

} // namespace

int
main (int argc, char** argv)
{
    bool samples = true;
    int  argi    = 1;
    if (argc > 1 && strcmp (argv[1], "-info") == 0)
    {
        samples = false;
        argi    = 2;
    }
    if (argi >= argc)
    {
        fprintf (stderr, "usage: exrtiledump [-info] file.exr\n");
        return 2;
    }

    try
    {
        Imf::TiledInputFile in (argv[argi]);
        const Imf::Header&  h  = in.header ();
        const Imf::TileDescription& td = h.tileDescription ();

        printf ("# mode %s\n", modeName (in.levelMode ()));
        printf ("# tile %u %u %s\n", td.xSize, td.ySize,
                td.roundingMode == Imf::ROUND_DOWN ? "down" : "up");
        printf ("# levels %d %d\n", in.numXLevels (), in.numYLevels ());

        switch (in.levelMode ())
        {
            case Imf::ONE_LEVEL: dumpLevel (in, 0, 0, samples); break;
            case Imf::MIPMAP_LEVELS:
                for (int l = 0; l < in.numLevels (); ++l)
                    dumpLevel (in, l, l, samples);
                break;
            case Imf::RIPMAP_LEVELS:
                for (int ly = 0; ly < in.numYLevels (); ++ly)
                    for (int lx = 0; lx < in.numXLevels (); ++lx)
                        dumpLevel (in, lx, ly, samples);
                break;
            default: fprintf (stderr, "unknown level mode\n"); return 1;
        }
    }
    catch (const std::exception& e)
    {
        fprintf (stderr, "ERROR %s: %s\n", argv[argi], e.what ());
        return 1;
    }

    return 0;
}
