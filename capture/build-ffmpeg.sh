#!/bin/sh
# Builds the minimal ffmpeg that capture/kmsgrab.sh runs: only kmsgrab and
# pulse input, VAAPI (hwmap, scale_vaapi, HEVC/H.264/AV1 encoders), Opus,
# mpegts over UDP. Patches:
#   libavdevice/kmsgrab.c       kmsgrab_formats: ARGB2101010 ("AR30", KWin's
#                               10-bit scanout buffer) as X2RGB10LE
#   libavutil/hwcontext_vaapi.c vaapi_drm_format_map: ARGB2101010 imported
#                               as X2R10G10B10 (alpha ignored)
#   libavdevice/kmsgrab.c       kmsgrab_get_fb2: a framebuffer format change
#                               is taken, not an I/O error: KWin scans out a
#                               full-screen window's own buffer (e.g. 8-bit
#                               XR24) and its 10-bit AR30 composite again as
#                               soon as another window is on top. Each frame
#                               names its format; VAAPI imports it per frame,
#                               scale_vaapi converts any of them to NV12.
# Needs the headers of libdrm, libva, libpulse, opus and nasm. The result is
# capture/build/ffmpeg-9.0.2/ffmpeg; install it as in capture/kmsgrab.sh.
set -eu
VERSION=9.0.2
cd "$(dirname "$0")"
mkdir -p build
cd build
[ -f "ffmpeg-$VERSION.tar.xz" ] || curl -sSfLO "https://ffmpeg.org/releases/ffmpeg-$VERSION.tar.xz"
rm -rf "ffmpeg-$VERSION"
tar xf "ffmpeg-$VERSION.tar.xz"
cd "ffmpeg-$VERSION"

python3 - <<'EOF'
def patch(path, anchor, added):
    replace(path, anchor, anchor + added)

def replace(path, old, new):
    text = open(path).read()
    assert text.count(old) == 1, f"{path}: anchor not found once, the ffmpeg version changed?"
    open(path, "w").write(text.replace(old, new))

patch("libavdevice/kmsgrab.c",
      "    { AV_PIX_FMT_X2RGB10BE, DRM_FORMAT_XRGB2101010 | DRM_FORMAT_BIG_ENDIAN },\n",
      "    // stream patch: KWin scans out ARGB2101010; the alpha bits are ignored.\n"
      "    { AV_PIX_FMT_X2RGB10LE, DRM_FORMAT_ARGB2101010 },\n")
patch("libavutil/hwcontext_vaapi.c",
      "    DRM_MAP(X2R10G10B10, 1, DRM_FORMAT_XRGB2101010),\n",
      "    // stream patch: KWin's ARGB2101010 scanout, imported with alpha ignored.\n"
      "    DRM_MAP(X2R10G10B10, 1, DRM_FORMAT_ARGB2101010),\n")
replace("libavdevice/kmsgrab.c",
        """    if (fb->pixel_format != ctx->drm_format) {
        av_log(avctx, AV_LOG_ERROR, "Plane %"PRIu32" framebuffer "
               "format changed: now %"PRIx32".\\n",
               ctx->plane_id, fb->pixel_format);
        err = AVERROR(EIO);
        goto fail;
    }
""",
        """    // stream patch: KWin switches between direct scanout of a full-screen
    // window and its own composite, which have different formats: go on
    // with the new one, each frame names its own (a format VAAPI cannot
    // import still fails there).
    if (fb->pixel_format != ctx->drm_format) {
        av_log(avctx, AV_LOG_WARNING, "Plane %"PRIu32" framebuffer "
               "format changed: %"PRIx32" -> %"PRIx32".\\n",
               ctx->plane_id, ctx->drm_format, fb->pixel_format);
        ctx->drm_format = fb->pixel_format;
    }
""")
EOF

./configure \
	--disable-everything --disable-doc --disable-debug --disable-ffplay --disable-ffprobe --disable-autodetect \
	--enable-libdrm --enable-vaapi --enable-libpulse --enable-libopus \
	--enable-indev=kmsgrab,pulse \
	--enable-decoder=wrapped_avframe,pcm_s16le,pcm_f32le \
	--enable-encoder=hevc_vaapi,h264_vaapi,av1_vaapi,libopus,wrapped_avframe \
	--enable-filter=hwmap,hwdownload,hwupload,scale_vaapi,format,null,aresample,aformat,anull,fps \
	--enable-muxer=mpegts,null --enable-protocol=udp,file,pipe --enable-parser=hevc,h264,opus \
	>../configure.log
make -j"$(nproc)" >../make.log
ls -l ffmpeg
