#!/bin/sh
# Starts ffplay; run ./client next to it in another terminal.
exec ffplay -f mpegts -fflags nobuffer -flags low_delay -framedrop -sync ext "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024&buffer_size=98304"
