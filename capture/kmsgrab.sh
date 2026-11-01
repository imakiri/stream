#!/bin/sh
# Streams the screen to the server without OBS: ffmpeg takes the frames the
# GPU scans out to the monitor (kmsgrab, no copy to memory), scales them and
# encodes HEVC on the GPU (VAAPI), takes the sound of the default output
# (PulseAudio/PipeWire monitor) as 5 ms Opus frames and sends mpegts to the
# server's stream input.
#
# The sound sets the delay of the picture too: the mpegts muxer holds each
# frame until the sound of its time is in, and the player keeps the picture
# in step with the sound. PipeWire runs the whole graph in quanta of 1024
# samples (21 ms) here, so the script forces 256 (5 ms) while it runs and
# puts the previous value back on exit (QUANTUM=0 leaves it alone). Measured
# 2026-11-01, from the frame's capture to its first mpegts packet out, p50:
# picture 26 -> 12 ms, sound 32 -> 16 ms.
#
# The sound input goes first: with kmsgrab as the first input ffmpeg 9.0.2
# put out only ~10 frames a second (64 with the order below, measured
# 2026-11-01 on DP-2).
#
# KWin scans out 10-bit ARGB2101010, which ffmpeg 9.0.2's kmsgrab and VAAPI
# mapping do not know, and switches the screen's buffer format whenever a
# full-screen window gets or loses direct scanout (another window on top);
# stock kmsgrab stops the picture there for good while the sound goes on. A
# minimal ffmpeg with patches for both is built in capture/build (see
# capture/build-ffmpeg.sh). The size can change at that moment too (the
# window's buffer is not the screen's), which kmsgrab does not take: ffmpeg
# fails then (-xerror: any input error ends it, instead of a sound-only
# stream) and the script starts it again after a second. kmsgrab needs CAP_SYS_ADMIN:
#   sudo install -m 755 capture/build/ffmpeg-9.0.2/ffmpeg /usr/local/bin/ffmpeg-kmsgrab
#   sudo setcap cap_sys_admin+ep /usr/local/bin/ffmpeg-kmsgrab
#
# Settings (environment): FFMPEG, CARD (/dev/dri/card1), CRTC (which monitor,
# when there are several; ffmpeg picks the first by default), SIZE (1920x1200),
# FPS (60), BITRATE (8M), PORT (2557, the server's [stream_input] port),
# QUANTUM (256).
set -eu
FFMPEG=${FFMPEG:-/usr/local/bin/ffmpeg-kmsgrab}
CARD=${CARD:-/dev/dri/card1}
SIZE=${SIZE:-1920x1200}
FPS=${FPS:-60}
BITRATE=${BITRATE:-8M}
PORT=${PORT:-2557}
QUANTUM=${QUANTUM:-256}
W=${SIZE%x*}
H=${SIZE#*x}

if [ "$QUANTUM" != 0 ]; then
	previous=$(pw-metadata -n settings 0 clock.force-quantum | sed -n "s/.*value:'\([0-9]*\)'.*/\1/p" | head -1)
	pw-metadata -n settings 0 clock.force-quantum "$QUANTUM" >/dev/null
	trap 'pw-metadata -n settings 0 clock.force-quantum "${previous:-0}" >/dev/null' EXIT
	trap 'exit 130' INT TERM
fi

while :; do
status=0
"$FFMPEG" -hide_banner -loglevel warning -stats -xerror \
	-f pulse -fragment_size 480 -i @DEFAULT_MONITOR@ \
	-device "$CARD" -f kmsgrab -framerate "$FPS" ${CRTC:+-crtc_id "$CRTC"} -i - \
	-filter:v "hwmap=derive_device=vaapi,scale_vaapi=w=$W:h=$H:format=nv12" \
	-c:v hevc_vaapi -rc_mode CBR -b:v "$BITRATE" -maxrate "$BITRATE" -bufsize 500k \
	-g "$FPS" -bf 0 -async_depth 1 \
	-c:a libopus -b:a 160k -frame_duration 5 -application lowdelay \
	-f mpegts -muxdelay 0 -muxpreload 0 -flush_packets 1 \
	"udp://127.0.0.1:$PORT?pkt_size=1316" || status=$?
# 255: ffmpeg was stopped (Ctrl-C, kill): so is the script; other codes
# are errors, an errno as 256 - errno among them (251: EIO)
[ "$status" -ne 255 ] || exit "$status"
echo "kmsgrab.sh: ffmpeg exited with $status, starting it again in 1 s" >&2
sleep 1
done
