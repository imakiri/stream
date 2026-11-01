package one.imakiri.stream.app

import android.media.AudioManager
import android.os.Bundle
import android.view.SurfaceHolder
import android.view.SurfaceView
import android.view.WindowManager
import android.widget.Toast
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.interaction.MutableInteractionSource
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.width
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Slider
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.darkColorScheme
import androidx.compose.runtime.collectAsState
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableFloatStateOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.unit.dp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.core.view.WindowCompat
import androidx.core.view.WindowInsetsCompat
import androidx.core.view.WindowInsetsControllerCompat
import dev.jdtech.mpv.MPVLib
import java.io.File
import one.imakiri.stream.streammobile.Streammobile

/**
 * The stream, full screen in landscape: runs the Go client of the profile and
 * plays what it sends to the player port with libmpv. A tap shows the volume
 * and the link state. Leaving the screen stops both.
 */
class PlayerActivity : ComponentActivity() {
    private var mpv: MPVLib? = null
    private var liveEdge: LiveEdge? = null
    private var surfaceView: SurfaceView? = null
    private var markerReader: MarkerReader? = null

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        window.addFlags(WindowManager.LayoutParams.FLAG_KEEP_SCREEN_ON)
        // the volume keys change the media volume, not the ringer
        volumeControlStream = AudioManager.STREAM_MUSIC
        WindowCompat.setDecorFitsSystemWindows(window, false)
        WindowInsetsControllerCompat(window, window.decorView).apply {
            hide(WindowInsetsCompat.Type.systemBars())
            systemBarsBehavior = WindowInsetsControllerCompat.BEHAVIOR_SHOW_TRANSIENT_BARS_BY_SWIPE
        }

        val dir = profileDir(this)
        val profile = savedProfile(dir) ?: run {
            Toast.makeText(this, "no profile: ${savedProfileError(dir) ?: "set one up first"}", Toast.LENGTH_LONG).show()
            finish()
            return
        }
        try {
            ClientSession.start(dir)
        } catch (e: Exception) {
            Toast.makeText(this, "the client did not start: ${e.message}", Toast.LENGTH_LONG).show()
            finish()
            return
        }
        // fifo_size counts 188-byte packets: the desktop's 1024 (190 KB, 150 ms of
        // the stream) overran while the phone's play loop was busy, cutting holes
        // in the audio; 64k packets are 12 MB, buffer_size the socket's receive buffer
        val url = "udp://127.0.0.1:${profile.playerPort}?overrun_nonfatal=1&fifo_size=65536&buffer_size=8388608"
        val mpv = createPlayer()
        this.mpv = mpv
        liveEdge = LiveEdge(mpv, url).also { it.start() }

        setContent {
            MaterialTheme(colorScheme = darkColorScheme()) {
                PlayerScreen(url)
            }
        }
    }

    private fun createPlayer(): MPVLib {
        val mpv = MPVLib.create(this) ?: error("libmpv did not load")
        // the desktop settings of player/play-mpv.sh
        mpv.setOptionString("profile", "low-latency")
        // mpv's own audio buffer is at least the output's: 40 ms here, 20 ms in
        // OpenSL ES (below), instead of 64 + 64 ms with AAudio, see there
        mpv.setOptionString("audio-buffer", "0.04")
        // LiveEdge keeps the speed at 1, so no scaletempo2 (and its ~27 ms)
        // low-latency probes so little that the stream has no picture size yet,
        // and MediaCodec refuses to start without one (width=0, height=0);
        // a longer probe costs only the start
        mpv.setOptionString("demuxer-lavf-probe-info", "yes")
        mpv.setOptionString("demuxer-lavf-analyzeduration", "0.5")
        // vo=mediacodec_embed showed a black screen on a OnePlus 12
        mpv.setOptionString("vo", VIDEO_OUTPUT)
        mpv.setOptionString("gpu-context", "android")
        mpv.setOptionString("opengl-es", "yes")
        mpv.setOptionString("hwdec", "mediacodec,mediacodec-copy")
        // OpenSL ES: with a 10 ms buffer (and audio-buffer 0.02) the demuxer to
        // the speaker took ~42 ms on a OnePlus 12, but the output underran ~10
        // times a minute with a second of stream queued (2026-11-01); 20 ms
        // (and 0.04) is the next try. AAudio's fast path (exclusive MMAP)
        // has a fixed 3072-sample (64 ms) buffer there, and mpv 0.41 keeps a
        // buffer of its own as large as the device's: ~110-145 ms in all.
        mpv.setOptionString("ao", "opensles,aaudio,audiotrack")
        mpv.setOptionString("opensles-buffer-size-in-ms", "20")
        // the AAudio fallback's fast path: the default shared, unhinted stream
        // plays through the mixer's deep buffers, a few hundred ms behind
        for ((name, value) in listOf("aaudio-performance-mode" to "low-latency", "audio-exclusive" to "yes")) {
            val status = mpv.setOptionString(name, value)
            if (status < 0) android.util.Log.w("stream", "mpv option $name=$value: error $status")
        }
        mpv.setOptionString("input-default-bindings", "no")
        mpv.setOptionString("idle", "yes")
        mpv.setOptionString("force-window", "no")
        applyOverrides(mpv)
        mpv.init()
        return mpv
    }

    /**
     * Options from mpv.conf in the profile directory, `name=value` a line, over
     * the ones above: for trying player settings without building the app.
     *   adb shell run-as one.imakiri.stream sh -c 'echo audio-buffer=0.02 > files/profile/mpv.conf'
     */
    private fun applyOverrides(mpv: MPVLib) {
        val file = java.io.File(profileDir(this), "mpv.conf")
        if (!file.exists()) return
        for (line in file.readLines()) {
            val text = line.trim()
            if (text.isEmpty() || text.startsWith("#") || '=' !in text) continue
            val (name, value) = text.split('=', limit = 2).map { it.trim() }
            val status = mpv.setOptionString(name, value)
            android.util.Log.i("stream", "mpv.conf: $name=$value" + if (status < 0) ": error $status" else "")
        }
    }

    @androidx.compose.runtime.Composable
    private fun PlayerScreen(url: String) {
        var controls by remember { mutableStateOf(false) }
        val audio = remember { getSystemService(AudioManager::class.java) }
        val maxVolume = remember { audio.getStreamMaxVolume(AudioManager.STREAM_MUSIC) }
        var volume by remember { mutableFloatStateOf(audio.getStreamVolume(AudioManager.STREAM_MUSIC).toFloat()) }
        var measuring by remember { mutableStateOf(false) }
        var screenToScreen by remember { mutableStateOf("") }
        val speed by ClientSession.speedKbit.collectAsState()
        val log by ClientSession.log.collectAsState()

        Box(
            Modifier
                .fillMaxSize()
                .background(Color.Black)
                .clickable(interactionSource = remember { MutableInteractionSource() }, indication = null) {
                    volume = audio.getStreamVolume(AudioManager.STREAM_MUSIC).toFloat()
                    controls = !controls
                },
        ) {
            AndroidView(
                factory = { context ->
                    SurfaceView(context).apply {
                        holder.addCallback(SurfaceCallback({ this@PlayerActivity.mpv }, url))
                        surfaceView = this
                    }
                },
                modifier = Modifier.fillMaxSize(),
            )
            if (controls) {
                Column(
                    Modifier.align(Alignment.BottomStart).background(Color(0x99000000)).padding(16.dp),
                ) {
                    Text("${speed} kbit/s", color = Color.White)
                    log.lastOrNull { "e2e delay" in it || "data" in it }?.let { Text(it, color = Color.White) }
                    log.lastOrNull()?.let { Text(it, color = Color.LightGray, style = MaterialTheme.typography.bodySmall) }
                    if (measuring) Text(screenToScreen, color = Color.White)
                    TextButton(onClick = {
                        measuring = if (measuring) {
                            stopMeasuring()
                            false
                        } else {
                            startMeasuring { screenToScreen = it }
                        }
                    }) { Text(if (measuring) "Stop measuring" else "Measure latency (latency-marker.html on the PC)") }
                    Text("Volume", color = Color.White, modifier = Modifier.padding(top = 8.dp))
                    Slider(
                        value = volume,
                        onValueChange = {
                            volume = it
                            audio.setStreamVolume(AudioManager.STREAM_MUSIC, it.toInt(), 0)
                        },
                        valueRange = 0f..maxVolume.toFloat(),
                        steps = (maxVolume - 1).coerceAtLeast(0),
                        modifier = Modifier.width(320.dp),
                    )
                }
            }
        }
    }

    /**
     * Starts the screen-to-screen measurement and the client's latency trace,
     * both into the profile directory; false if NTP is not synced, without
     * which the PC's and the phone's times do not compare.
     */
    private fun startMeasuring(show: (String) -> Unit): Boolean {
        val mpv = mpv ?: return false
        val surface = surfaceView ?: return false
        if (ClientSession.trueUnixMicro() == null) {
            Toast.makeText(this, "set an NTP server in the profile (time.google.com) to measure", Toast.LENGTH_LONG).show()
            return false
        }
        val dir = profileDir(this)
        ClientSession.startTrace(File(dir, "trace-client.tsv"))
        val delays = ArrayDeque<Long>()
        markerReader = MarkerReader(surface, mpv, File(dir, "marker.tsv")) { delay ->
            runOnUiThread {
                delays.addLast(delay)
                while (delays.size > 40) delays.removeFirst()
                val sorted = delays.sorted()
                show("screen to screen: $delay ms, p50 ${sorted[sorted.size / 2]} ms over ${sorted.size}")
            }
        }.also { it.start() }
        show("looking for the marker…")
        return true
    }

    private fun stopMeasuring() {
        markerReader?.stop()
        markerReader = null
        ClientSession.stopTrace()
    }

    override fun onStop() {
        super.onStop()
        // in the background nobody watches: let the relay slot go
        finish()
    }

    override fun onDestroy() {
        stopMeasuring()
        liveEdge?.stop()
        mpv?.destroy()
        mpv = null
        if (isFinishing) ClientSession.stop()
        super.onDestroy()
    }
}

private const val VIDEO_OUTPUT = "gpu"

/**
 * Gives mpv the surface to draw on and opens the stream once there is one;
 * player is null once the activity destroyed mpv.
 */
private class SurfaceCallback(private val player: () -> MPVLib?, private val url: String) : SurfaceHolder.Callback {
    private var opened = false

    override fun surfaceCreated(holder: SurfaceHolder) {
        val mpv = player() ?: return
        mpv.attachSurface(holder.surface)
        mpv.setOptionString("force-window", "yes")
        mpv.setOptionString("vo", VIDEO_OUTPUT)
        if (!opened) {
            mpv.command(arrayOf("loadfile", url))
            opened = true
        }
    }

    override fun surfaceChanged(holder: SurfaceHolder, format: Int, width: Int, height: Int) {
        player()?.setPropertyString("android-surface-size", "${width}x$height")
    }

    override fun surfaceDestroyed(holder: SurfaceHolder) {
        val mpv = player() ?: return
        mpv.setOptionString("vo", "null")
        mpv.setOptionString("force-window", "no")
        mpv.detachSurface()
    }
}

