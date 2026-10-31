@echo off
rem Starts mpv close to live; run client.exe next to it in another window. End or Ctrl+Right jumps to live.
mpv.exe --profile=low-latency --hwdec=auto --audio-buffer=0.05 --script="%~dp0mpv-live-edge.lua" "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024"
