#!/bin/sh
# Starts mpv close to live; run ./client next to it in another terminal. End or Ctrl+Right jumps to live.
exec mpv --profile=low-latency --hwdec=auto --audio-buffer=0.05 --script="$(dirname "$0")/mpv-live-edge.lua" "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024"
