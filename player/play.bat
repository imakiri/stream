@echo off
rem Starts the player; run client.exe next to it in another window.
ffplay.exe -f mpegts -fflags nobuffer -flags low_delay -framedrop -sync ext "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024&buffer_size=98304"
