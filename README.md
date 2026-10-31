# stream

Low-latency streaming of an OBS (or ffmpeg) mpegts stream to a few viewers
through a relay on a public host. The stream is protected by forward error
correction instead of retransmissions, so a lost packet costs no round trip,
and it is end-to-end encrypted: the relay sees only sealed packets.

```
OBS ──udp──▶ server ──udp/tcp──▶ relay ──udp/tcp──▶ client ──udp──▶ ffplay
 (local)     (source)            (public host)       (viewer)        (local)
```

- **server**: the stream source. Takes mpegts from OBS on a local UDP port,
  cuts it into chunks, adds Reed-Solomon parity, seals every packet with the
  stream key and sends one copy to the relay. It owns the CA that admits
  clients.
- **relay**: runs on a host everyone can reach. It knows nothing about
  keys; it routes packets by kind and keeps the peers registered. One server
  per relay.
- **client**: a viewer. Gets the stream key from the server in a handshake,
  opens the packets, restores the chunks with FEC and sends the mpegts to a
  local player.

## Architecture

### Packets

Every packet starts with `version` (3) and `kind`
(`src/wire/wire.go`):

| kind | from → to | size | content |
|---|---|---|---|
| 1 relay hello | peer → relay, echoed back | 19 B | role (server/client), peer ID |
| 2 client hello | client → server | 218 B | client certificate, ephemeral X25519 key, signature |
| 3 server hello | server → that client | 294 B | client ID, server certificate, ephemeral key, wrapped stream key, server clock, signature |
| 4 data | server → every client | 1086 B | 14-byte clear header (epoch, packet number) + AES-256-GCM sealed FEC packet |

The relay routes by kind only: client hellos to the server, a server hello
to the client whose ID it names, data to every client. Everything else, and
data from clients, is dropped.

### Relay link and keepalive

Server and client talk to the relay over UDP or TCP (TCP frames packets with
a 2-byte length). The connection is a
`RelayLink` (`src/link.go`): it repeats the relay hello every 5 s, which
registers the peer and keeps NAT mappings open, and the relay echoes it. When
no echo comes for 15 s, or a TCP connection fails, the link dials again with
a backoff of 1 to 10 s; meanwhile writes are dropped, the stream is real
time. The relay follows a UDP peer to a new address on its next hello and
forgets peers that were silent for 30 s.

The link dials again from the previous local address while the machine has
it, so a VPN that comes up for a while (e.g. WARP) pauses the stream rather
than taking it over.

### Proxy

Where the relay cannot be reached directly or its traffic is blocked, the
link can go through a proxy, set in the `[proxy]` section of the config
(`src/proxy.go`), one at most:

- `[proxy.socks5] url = "socks5://127.0.0.1:2080"`: a SOCKS5 proxy such as a
  local sing-box, TCP only. Over TCP a lost packet is resent rather than
  skipped, so losses turn into stalls and delay instead of FEC repairs.
- `[proxy.amneziawg] config = "amneziawg.conf"`: an AmneziaWG tunnel
  (WireGuard with obfuscation against DPI) run inside the program
  (`src/amneziawg`, on `amneziawg-go` and the gVisor network stack), UDP or
  TCP. Only the stream goes into the tunnel: no system VPN, no admin rights.
  The config is the file the Amnezia app exports (`[Interface]` with
  `PrivateKey`, `Address`, `Jc`, `Jmin`, `Jmax`, `S1`, `S2`, `H1`-`H4`...,
  `[Peer]` with `PublicKey`, `Endpoint`, `AllowedIPs`); the path is relative
  to the config file. A plain WireGuard config works too. The relay address
  is resolved by the tunnel's `DNS` if the config has one, else by the
  system.

Packets are marked DSCP EF (46), which home routers and Wi-Fi put into the
voice/video queue.

### Security

A two-level PKI with its own certificate format (`src/wire/certificate.go`,
120 B: ID, expiry, Ed25519 public key, signature by the root). The server
holds the root (CA) key and signs one certificate for itself and one for
every client.

Handshake (`src/wire/handshake.go`): the client sends its certificate and an
ephemeral X25519 key, signed. The server checks the certificate against the
root, answers with its own certificate and ephemeral key, and wraps the
stream key with a key derived (HKDF-SHA256) from the shared X25519 secret and
the transcript. The server signs the server hello together with the client
hello, so an old server hello fits no new client hello. The client accepts
only the server whose certificate ID it got at enrollment.

All clients share one stream key per server run, so the server seals each
packet once. A client re-handshakes when no data opened for 3 s, e.g. after
a server restart (new key).

Enrollment (`src/keystore.go`): the client makes its key pair on the first
start and prints the public key; the server owner signs it with
`server -sign` and sends back an enrollment (base64: client certificate,
root public key, server ID).

### FEC codec

`src/codec`: the encoder collects input into chunks of
`data_parts × 1024 − 22` bytes (8170 B with 8 data parts), flushing a partial
chunk after `dispatcher_timeout` ms without input. Each chunk becomes
`total_parts` packets of 1024 bytes of Reed-Solomon shards; any `data_parts`
of them restore the chunk. The packets of `dispatcher_size` chunks are
interleaved, so a burst loss spreads over several chunks. Each packet carries
the chunk and packet numbers, a random session per encoder run and an 8-byte
SHA-1 prefix.

The encoder hands over the packets of an interleave group at once (40 with
the defaults). Sent back to back they leave at the speed of the network card,
and a shaper on the way (home router, provider) cuts off the tail of the
burst: 15-25 packets in a row, more than FEC restores. So the server paces the
data packets (`src/pacer.go`, `[pacing] rate_mbit` in `server.cfg`, 30 Mbit/s
by default, about three times the wire rate). The relay forwards each packet
as it comes, so this smooths the way to the clients too. The price is the
time a big frame takes at that rate: ~25 ms for a 75 KB keyframe with its
parity, ~5 ms for an ordinary frame.

The decoder collects packets per chunk in a ring of `assembler_size` lines,
restores a chunk as soon as `data_parts` packets are there and puts chunks
back in order in a window of `dispatcher_size` chunks; a chunk that does not
come in time is skipped.

With 8 of 10 parts the stream on the wire is about 1.3 times the video
bitrate: 8 Mbit/s of video take about 10.5 Mbit/s.

### Delay log

The encoder stamps each chunk with the server time its first byte came in;
the server hello carries the server clock, and the client estimates the clock
offset from it and the handshake round trip. Every 5 s the client logs the
end-to-end delay, from the chunk reaching the server to the chunk restored on
the client, and the chunks lost meanwhile:
`e2e delay p50 …, p99 …, max … over N chunks, lost K (M came short of packets)`.
Chunks that came short of packets in groups of neighbours mean burst losses
on the way; whole chunks missing mean longer breaks.

## Running it

Build with Go 1.25 or newer, for the machine each part runs on:

```
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -o relay  ./cmd/relay
GOOS=linux   GOARCH=amd64 CGO_ENABLED=0 go build -o server ./cmd/server
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -o client.exe ./cmd/client
```

Ports used by default: relay `25565` (UDP and TCP), server input from OBS
`2557` (UDP, localhost), client output to the player `25566` (UDP,
localhost).

### Relay (public host)

Copy the binary to `/opt/stream/relay` and install the systemd unit from
`deploy/stream-relay.service`:

```
scp relay root@host:/opt/stream/relay
scp deploy/stream-relay.service root@host:/etc/systemd/system/
ssh root@host 'systemctl daemon-reload && systemctl enable --now stream-relay'
```

Open port 25565 for UDP and TCP in the firewall. For viewers on TCP, use BBR
with fq pacing on the host; with the default cubic a TCP stream of ~12 Mbit/s
at 90 ms backs up on losses:

```
echo tcp_bbr > /etc/modules-load.d/bbr.conf
printf 'net.core.default_qdisc = fq\nnet.ipv4.tcp_congestion_control = bbr\n' > /etc/sysctl.d/90-bbr.conf
modprobe tcp_bbr && sysctl --system && tc qdisc replace dev eth0 root fq
```

Logs: `journalctl -u stream-relay -f` (the speed line is shown only on a
terminal, not in the journal). To update, copy the new binary and
`systemctl restart stream-relay`; peers register again on their own.

### Server (streamer)

1. Put `server` and `server.cfg` (from `cmd/server/server.example.cfg`) in
   one directory and set `[relay] addr` to the relay host.
2. Start it once: `./server`. It creates `ca.key`, `server.key` and
   `server.cert` next to the binary. Keep `ca.key` secret: whoever has it can
   admit clients. Losing it means enrolling every client again.
3. Point OBS at the server: Settings → Output → Output Mode: Advanced →
   Recording → Type: Custom Output (FFmpeg) →
   - FFmpeg Output Type: Output to URL, URL
     `udp://127.0.0.1:2557?pkt_size=1316`, Container Format: mpegts
   - Muxer Settings: `mpegts_flags=+resend_headers` (`muxdelay` and
     `muxpreload` are options of the ffmpeg command, OBS refuses them; its
     muxer starts without that delay anyway)
   - Video: bitrate 8000 Kbps, keyframe interval 60 frames (1 s at 60 fps),
     and by GPU:
     - AMD: encoder `hevc_amf` (on Linux it needs the AMF runtime, e.g. the
       AUR package `amf-amdgpu`), Video Encoder Settings:
       `usage=lowlatency quality=quality rc=cbr enforce_hrd=1 filler_data=1 maxrate=8000k bufsize=500k bf=0 async_depth=1`.
       Without `enforce_hrd=1` AMF ignores `bufsize` and sends frames of up
       to 300 KB.
     - NVIDIA: encoder `hevc_nvenc`, Video Encoder Settings:
       `preset=p4 tune=ll rc=cbr bf=0 zerolatency=1 bufsize=500k`

     `hevc_vaapi` does not work here: this OBS output gives the encoder
     frames in system memory, and VAAPI takes only frames in GPU memory.
   - Audio: encoder `libopus`, 160 Kbps, Audio Encoder Settings
     `frame_duration=10`; and Settings → Audio → Advanced → Low Latency
     Audio Buffering Mode on (OBS restarts). The muxer and the player hold
     each video frame until the audio of the same moment arrives, so the
     audio delay inside OBS is the video delay too: with `aac` and the audio
     buffering OBS grows on its own (42 ms here) the picture came ~90 ms
     later (375 ms from screen to screen instead of 284).

   Then Start Recording. Use constant bitrate with a small buffer (500k is
   1/16 s at 8 Mbit/s): a constant-quality encoder, or a large buffer, sends
   keyframes and scene changes as bursts of 2-4 times the bitrate. No
   B-frames, a keyframe every second so a new or recovering viewer starts
   fast, and `resend_headers` so a player that joins mid-stream finds the
   stream tables.

   A file instead of OBS (`-re` sends it at its own pace):
   ```
   ffmpeg -re -i file.mkv -map 0:v:0 -map 0:a:0 \
     -c:v hevc_amf -usage lowlatency -quality quality -rc cbr -enforce_hrd 1 -filler_data 1 \
     -b:v 8M -maxrate 8M -bufsize 500k -g 60 -bf 0 \
     -c:a libopus -b:a 160k -frame_duration 10 -ac 2 \
     -f mpegts -mpegts_flags +resend_headers -muxdelay 0 -muxpreload 0 \
     "udp://127.0.0.1:2557?pkt_size=1316"
   ```
4. To admit a viewer, sign the public key they send you:
   ```
   ./server -sign <their base64 public key> -days 365
   ```
   It prints the enrollment (and copies it to the clipboard); send it back.

The server flags: `-cfg server.cfg`, `-keys <dir>` (key directory, default:
next to the binary), `-sign <key>` and `-days <n>`.

### Client (viewer)

1. Put `client` (`client.exe`) and `client.cfg` (from
   `cmd/client/client.example.cfg`) in one directory. Set `[relay] addr` to
   the relay host and, if UDP does not get through, `network = "tcp"`, or
   a `[proxy]` (see [Proxy](#proxy)). `[fec]` must match the server.
2. Start it: `./client`. On the first start it makes `client.key`, prints
   the public key and copies it to the clipboard. Send the key to the
   streamer, paste the enrollment they send back, and the client saves it as
   `client.enrollment`. Next starts connect right away.
3. Start the player. mpv with the script from `player/` keeps closest to
   live (measured from screen to screen through the relay: ~285 ms with mpv;
   ffplay keeps ~0.35 s of video queued and showed the picture ~290 ms
   later than mpv; `player/play-mpv.sh` and `player/play-mpv.bat` run this):
   ```
   mpv --profile=low-latency --hwdec=auto --audio-buffer=0.05 --script=player/mpv-live-edge.lua "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024"
   ```
   `--audio-buffer=0.05` is needed: the profile sets it to 0, the audio
   device then runs dry all the time, and mpv, which times the video by the
   audio, stalls and falls behind. The script catches up a small lag (after a
   pause or a stall) by playing 1.1 times faster, shows how far behind live
   you are after seeking back, and End or Ctrl+Right (for keyboards without
   End) jumps to live (it opens the stream again; the picture comes with the
   next keyframe, within a second). So
   does a forward seek (Right, Up, Shift+Right) that would go past the live
   edge, where mpv by itself says "Cannot seek in this stream".

   Or ffplay (`player/play.sh`, `player/play.bat`):
   ```
   ffplay -f mpegts -fflags nobuffer -flags low_delay -framedrop -sync ext "udp://127.0.0.1:25566?overrun_nonfatal=1&fifo_size=1024&buffer_size=98304"
   ```
   `-sync ext` keeps the picture on its own clock: by default ffplay follows
   the audio, and with `-framedrop` it then throws away most video frames of
   a live stream as late, which looks like stutter.

   The client sends the stream to this local UDP port and the player reads
   whatever arrives there; UDP has no listen mode or connection, so the two
   can start in any order and the player can be restarted while the client
   runs. While no player is open, the client drops the stream and logs that
   once.

The client log shows the handshake with the clock offset to the server
(again only when the stream key changes), the end-to-end delay and the lost
chunks every 5 s, and one line when the data from the server stops or comes
again: a server without input from OBS answers handshakes but sends no data. The speed of what comes
from the relay stays on the bottom line of the terminal.

## Measuring the link

`cmd/stats` sends stream-sized packets through the relay and records their
delay:

```
stats -mode reader -addr <relay host> -network udp -out received
stats -mode writer -addr <relay host> -network udp -rate 1250
```

The writer registers as the server, so it replaces a running server while it
runs. Each line of `received` is `<packet number> <sent unix ns> <received
unix ns>`; with both on one machine the delay is a round trip through the
relay.

## Code

| path | what |
|---|---|
| `cmd/relay`, `src/relay` | the relay |
| `cmd/server`, `src/server.go` | the source |
| `cmd/client`, `src/client.go` | the viewer |
| `src/link.go` | `RelayLink`: connection to the relay, keepalive, reconnects |
| `src/proxy.go`, `src/amneziawg` | the way to the relay through a socks5 proxy or an AmneziaWG tunnel |
| `src/wire` | packet kinds, certificates, handshake, data packet sealing |
| `src/codec` | FEC encoder and decoder |
| `src/frame` | packet framing over TCP |
| `src/keystore.go` | key files and enrollment |
| `src/config.go` | TOML configs |
| `cmd/stats` | link delay measurement |
| `player/mpv-live-edge.lua` | mpv script that keeps playback at the live edge |
| `player/play-mpv.sh`, `player/play.sh` (`.bat` on Windows) | start mpv (with the script) or ffplay |
