package one.imakiri.stream.app

import android.os.Handler
import android.os.Looper
import android.util.Log
import dev.jdtech.mpv.MPVLib
import java.util.concurrent.atomic.AtomicInteger

/**
 * Keeps playback close to the live edge, the end of what has come in, at
 * normal speed, and tunes how close at run time.
 *
 * The lag behind the edge is sampled 4 times a second; its minimum over the
 * last 2 s is the margin playback really has before it runs dry. The speed is
 * never changed (it is heard, even with the pitch kept), and playback is not
 * cut by seeking either: a seek forward in the live cache stalls mpv longer
 * than it skips (measured: the lag grew to 1 s). So the lag is only raised: a
 * lag 30 ms under the target is made up by pausing as long (see
 * liveEdgeAction). A lag 1 s over the target (after a stall) opens the
 * stream again, which starts at the edge with the next keyframe; up to that
 * the lag stays, as it only grows by underruns. So does a picture stuck more
 * than 1 s behind the sound for 2 s: mpv then plays the sound on and never
 * gets the picture back by itself (seen when the sender's capture lost the
 * picture for a while), and the lag behind the edge looks fine meanwhile.
 *
 * The target margin itself adapts, as TCP's window does the other way round:
 * every audio underrun mpv reports while the queue is short raises it by
 * 50 ms (one with 50 ms over the target queued is the audio output's own and
 * counted apart), and quiet time lowers it by 10 ms steps, so it settles at
 * the least delay this phone and network play without breaks. An underrun is
 * a click, so the probing is careful: the margin an underrun came at is
 * remembered as a floor the margin stays 20 ms above, and the quiet time
 * before each lowering step doubles with every underrun, up to a minute. The
 * floor sinks by 10 ms a minute without underruns, for when the network gets
 * better. With no cut forward, a lower target does not lower the lag that is
 * there: it only sets how short the queue may get before a pause, and from
 * where the 1 s for reopening counts.
 *
 * A new audio output (mpv's "AO:" line: another device, Bluetooth headphones
 * on or off) starts the tuning over: its underruns while it opens say nothing
 * about the network, and the margins learnt for one device do not fit another.
 */
class LiveEdge(private val mpv: MPVLib, private val url: String) {
    private val handler = Handler(Looper.getMainLooper())
    private val underruns = AtomicInteger()
    private val audioOutputsOpened = AtomicInteger()
    private val lags = ArrayDeque<Double>()

    private var margin = INITIAL_MARGIN
    private var floor = 0.0 // the margin of the last underrun
    private var quietBeforeLowering = MIN_QUIET_MS
    private var lastUnderrunAt = 0L
    private var lastLoweredAt = 0L
    private var lastFloorSunkAt = 0L
    private var loggedAt = 0L
    private var ignoreUnderrunsUntil = 0L
    private var underrunsLogged = 0
    private var outputUnderrunsLogged = 0
    private var cuts = 0
    private var frozenChecks = 0 // checks in a row with the picture far behind the sound
    private val parts = LagParts(mpv)

    private val underrunObserver = object : MPVLib.LogObserver {
        override fun logMessage(prefix: String, level: Int, text: String) {
            if ("Audio device underrun" in text) underruns.incrementAndGet()
            if (prefix == "cplayer" && text.startsWith("AO: [")) audioOutputsOpened.incrementAndGet()
        }
    }

    private val check = object : Runnable {
        override fun run() {
            step(System.currentTimeMillis())
            handler.postDelayed(this, CHECK_INTERVAL_MS)
        }
    }

    private val resume = Runnable { mpv.setPropertyBoolean("pause", false) }

    fun start() {
        mpv.addLogObserver(underrunObserver)
        handler.postDelayed(check, CHECK_INTERVAL_MS)
    }

    fun stop() {
        handler.removeCallbacks(check)
        handler.removeCallbacks(resume)
        mpv.removeLogObserver(underrunObserver)
    }

    private fun step(now: Long) {
        adaptMargin(now)
        if (mpv.getPropertyBoolean("pause") == true) return // a pause of ours runs
        val position = mpv.getPropertyDouble("time-pos") ?: return
        val cacheEnd = mpv.getPropertyDouble("demuxer-cache-time") ?: return
        val lag = cacheEnd - position
        lags.addLast(lag)
        while (lags.size > WINDOW_SAMPLES) lags.removeFirst()
        parts.sample(cacheEnd, position)

        val audioPts = mpv.getPropertyDouble("audio-pts")
        frozenChecks = if (audioPts != null && audioPts - position > FROZEN_VIDEO_BEHIND) frozenChecks + 1 else 0
        if (frozenChecks >= WINDOW_SAMPLES) {
            frozenChecks = 0
            mpv.command(arrayOf("loadfile", url, "replace"))
            cut(now, "picture %.1f s behind the sound, opened the stream again".format(audioPts!! - position))
            return
        }

        if (lags.size == WINDOW_SAMPLES) {
            when (val action = liveEdgeAction(lags.min(), margin)) {
                is LiveEdgeAction.Pause -> {
                    mpv.setPropertyBoolean("pause", true)
                    handler.postDelayed(resume, (action.seconds * 1000).toLong())
                    cut(now, "paused %.3f s".format(action.seconds))
                }
                LiveEdgeAction.Reopen -> {
                    mpv.command(arrayOf("loadfile", url, "replace"))
                    cut(now, "%.2f s behind, opened the stream again".format(lags.min()))
                }
                LiveEdgeAction.None -> {}
            }
        }

        if (now - loggedAt >= LOG_INTERVAL_MS) {
            loggedAt = now
            // a cut in this step has emptied the window
            val lagMin = lags.minOrNull() ?: lag
            Log.i(TAG, "live edge: %.3f s behind (min %.3f), target %.3f (floor %.3f), cuts %d, underruns %d (+%d of the output); %s".format(lag, lagMin, margin, floor, cuts, underrunsLogged, outputUnderrunsLogged, parts.report()))
        }
    }

    /** After a cut the lag window starts over: the samples before it are of another lag. */
    private fun cut(now: Long, what: String) {
        cuts++
        lags.clear()
        Log.i(TAG, "live edge: $what (target %.3f)".format(margin))
    }

    /** Raises the target margin on underruns, lowers it while there are none. */
    private fun adaptMargin(now: Long) {
        if (audioOutputsOpened.getAndSet(0) > 0) {
            margin = INITIAL_MARGIN
            floor = 0.0
            quietBeforeLowering = MIN_QUIET_MS
            lastUnderrunAt = now
            lastLoweredAt = now
            lastFloorSunkAt = now
            ignoreUnderrunsUntil = now + AUDIO_OUTPUT_SETTLE_MS
            Log.i(TAG, "live edge: new audio output, tuning from the start")
        }
        var count = underruns.getAndSet(0)
        if (now < ignoreUnderrunsUntil) count = 0
        // an underrun with plenty of stream queued is the audio output's own
        // (its buffer too small for the moment), not the network's: a larger
        // margin would not help it
        val queued = lags.minOrNull()
        if (count > 0 && queued != null && queued > margin + OUTPUT_UNDERRUN_EXCESS) {
            outputUnderrunsLogged += count
            count = 0
        }
        if (count > 0) {
            underrunsLogged += count
            floor = margin
            margin = (margin + MARGIN_STEP_UP * count.coerceAtMost(3)).coerceAtMost(MAX_MARGIN)
            quietBeforeLowering = (quietBeforeLowering * 2).coerceAtMost(MAX_QUIET_MS)
            lastUnderrunAt = now
            lastLoweredAt = now
            lastFloorSunkAt = now
            return
        }
        if (now - lastFloorSunkAt >= FLOOR_SINK_INTERVAL_MS) {
            floor = (floor - MARGIN_STEP_DOWN).coerceAtLeast(0.0)
            lastFloorSunkAt = now
        }
        if (now - lastUnderrunAt >= quietBeforeLowering && now - lastLoweredAt >= quietBeforeLowering) {
            margin = (margin - MARGIN_STEP_DOWN).coerceAtLeast(maxOf(MIN_MARGIN, floor + FLOOR_CLEARANCE))
            lastLoweredAt = now
        }
    }

    private companion object {
        const val TAG = "stream"
        const val CHECK_INTERVAL_MS = 250L
        const val WINDOW_SAMPLES = 8 // 2 s
        const val LOG_INTERVAL_MS = 2000L

        const val INITIAL_MARGIN = 0.15 // seconds of stream ahead of playback
        const val MIN_MARGIN = 0.03
        const val MAX_MARGIN = 1.0
        const val MARGIN_STEP_UP = 0.05
        const val MARGIN_STEP_DOWN = 0.01
        const val MIN_QUIET_MS = 2000L // between lowering steps before any underrun
        const val MAX_QUIET_MS = 60_000L
        const val FLOOR_CLEARANCE = 0.02
        const val FLOOR_SINK_INTERVAL_MS = 60_000L
        const val OUTPUT_UNDERRUN_EXCESS = 0.05 // queued over the target at an underrun: the output's
        const val AUDIO_OUTPUT_SETTLE_MS = 3000L // its start's underruns come within ~1 s
        const val FROZEN_VIDEO_BEHIND = 1.0 // seconds the picture may lag the sound before the stream is opened again
    }
}

/** What LiveEdge does about the lag. */
sealed interface LiveEdgeAction {
    object None : LiveEdgeAction
    /** Pause this long, for the stream to queue up. */
    data class Pause(val seconds: Double) : LiveEdgeAction
    /** Too far behind for the cache: open the stream again. */
    object Reopen : LiveEdgeAction
}

/**
 * The action for the lag behind the live edge (its minimum over the last 2 s,
 * lagMin) and the target margin, both in seconds: a pause to the target when
 * more than PAUSE_UNDER under it, reopening when more than REOPEN_OVER over,
 * else none.
 */
fun liveEdgeAction(lagMin: Double, margin: Double): LiveEdgeAction {
    val error = lagMin - margin
    return when {
        error > REOPEN_OVER -> LiveEdgeAction.Reopen
        error < -PAUSE_UNDER -> LiveEdgeAction.Pause(-error)
        else -> LiveEdgeAction.None
    }
}

private const val PAUSE_UNDER = 0.03
private const val REOPEN_OVER = 1.0

/**
 * Splits the lag behind the live edge into where the stream waits, sampled
 * with each check and reported as medians over the log interval, ms:
 *   demux:      in the demuxer's queue (cache end - reader-pts): the jitter cushion
 *   audio path: read from the demuxer, not yet heard (audio reader-pts -
 *               audio-pts): audio decoder, filters (scaletempo2), output buffer
 *   video path: read from the demuxer, not yet shown (video reader-pts -
 *               time-pos): MediaCodec's queue, the video output's queue
 *   video:      the picture shown behind the sound heard (audio-pts - time-pos)
 * The per-stream reader positions come from demuxer-cache-state's
 * ts-per-stream (mpv 0.38+), in stream timestamps: less demuxer-start-time.
 */
private class LagParts(private val mpv: MPVLib) {
    private val demux = ArrayList<Double>()
    private val audioPath = ArrayList<Double>()
    private val videoPath = ArrayList<Double>()
    private val video = ArrayList<Double>()
    private var shownState = false

    fun sample(cacheEnd: Double, position: Double) {
        val text = mpv.getPropertyString("demuxer-cache-state") ?: return
        if (!shownState) {
            shownState = true
            android.util.Log.i("stream", "demuxer-cache-state: $text")
        }
        val state = runCatching { org.json.JSONObject(text) }.getOrNull() ?: return
        val readerPts = state.optDouble("reader-pts", Double.NaN)
        val audioPts = mpv.getPropertyDouble("audio-pts") ?: return
        demux += cacheEnd - readerPts
        video += audioPts - position
        val streams = state.optJSONArray("ts-per-stream") ?: return
        // ts-per-stream is in the stream's own timestamps; the rest in playback time
        val start = mpv.getPropertyDouble("demuxer-start-time") ?: 0.0
        for (i in 0 until streams.length()) {
            val stream = streams.optJSONObject(i) ?: continue
            val reader = stream.optDouble("reader-pts", Double.NaN) - start
            when (stream.optString("type")) {
                "audio" -> audioPath += reader - audioPts
                "video" -> videoPath += reader - position
            }
        }
    }

    fun report(): String {
        fun median(values: ArrayList<Double>) = values.filter { !it.isNaN() }.sorted().let { if (it.isEmpty()) Double.NaN else it[it.size / 2] * 1000 }
        val text = "demux %.0f, audio path %.0f, video path %.0f, video behind audio %.0f ms".format(median(demux), median(audioPath), median(videoPath), median(video))
        demux.clear(); audioPath.clear(); videoPath.clear(); video.clear()
        return text
    }
}
