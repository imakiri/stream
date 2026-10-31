package one.imakiri.stream.app

import android.os.Handler
import android.os.Looper
import android.util.Log
import dev.jdtech.mpv.MPVLib
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.roundToInt

/**
 * Keeps playback close to the live edge, the end of what has come in, and
 * tunes how close at run time.
 *
 * The lag behind the edge is sampled 4 times a second; its minimum over the
 * last 2 s is the margin playback really has before it runs dry. The speed
 * follows that minimum smoothly (0.9..1.1, with pitch kept by scaletempo2):
 * faster while it is above the target margin, slower below it.
 *
 * The target margin itself adapts, as TCP's window does the other way round:
 * every audio underrun mpv reports raises it by 50 ms, and quiet time lowers
 * it by 10 ms steps, so it settles at the least delay this phone and network
 * play without breaks. An underrun is a click, so the probing is careful: the
 * margin an underrun came at is remembered as a floor the margin stays 20 ms
 * above, and the quiet time before each lowering step doubles with every
 * underrun, up to a minute. The floor sinks by 10 ms a minute without
 * underruns, for when the network gets better. A lag far behind the target that lasts (after
 * a stall) is dropped by opening the stream again, which starts at the edge
 * with the next keyframe.
 */
class LiveEdge(private val mpv: MPVLib, private val url: String) {
    private val handler = Handler(Looper.getMainLooper())
    private val underruns = AtomicInteger()
    private val lags = ArrayDeque<Double>()

    private var margin = INITIAL_MARGIN
    private var speed = 1.0
    private var floor = 0.0 // the margin of the last underrun
    private var quietBeforeLowering = MIN_QUIET_MS
    private var lastUnderrunAt = 0L
    private var lastLoweredAt = 0L
    private var lastFloorSunkAt = 0L
    private var behindSince = 0L
    private var loggedAt = 0L
    private var underrunsLogged = 0

    private val underrunObserver = object : MPVLib.LogObserver {
        override fun logMessage(prefix: String, level: Int, text: String) {
            if ("Audio device underrun" in text) underruns.incrementAndGet()
        }
    }

    private val check = object : Runnable {
        override fun run() {
            step(System.currentTimeMillis())
            handler.postDelayed(this, CHECK_INTERVAL_MS)
        }
    }

    fun start() {
        mpv.addLogObserver(underrunObserver)
        handler.postDelayed(check, CHECK_INTERVAL_MS)
    }

    fun stop() {
        handler.removeCallbacks(check)
        mpv.removeLogObserver(underrunObserver)
    }

    private fun step(now: Long) {
        adaptMargin(now)
        val position = mpv.getPropertyDouble("time-pos") ?: return
        val cacheEnd = mpv.getPropertyDouble("demuxer-cache-time") ?: return
        val lag = cacheEnd - position
        lags.addLast(lag)
        while (lags.size > WINDOW_SAMPLES) lags.removeFirst()
        val lagMin = lags.min()

        if (lagMin > margin + JUMP_EXCESS) {
            if (behindSince == 0L) behindSince = now
            if (now - behindSince >= JUMP_AFTER_MS) {
                behindSince = 0L
                lags.clear()
                setSpeed(1.0)
                mpv.command(arrayOf("loadfile", url, "replace"))
                Log.i(TAG, "live edge: %.2f s behind, opened the stream again".format(lagMin))
                return
            }
        } else {
            behindSince = 0L
        }
        setSpeed((1.0 + SPEED_GAIN * (lagMin - margin)).coerceIn(MIN_SPEED, MAX_SPEED))

        if (now - loggedAt >= LOG_INTERVAL_MS) {
            loggedAt = now
            val total = underrunsLogged
            Log.i(TAG, "live edge: %.3f s behind (min %.3f), target %.3f (floor %.3f), speed %.2f, underruns %d".format(lag, lagMin, margin, floor, speed, total))
        }
    }

    /** Raises the target margin on underruns, lowers it while there are none. */
    private fun adaptMargin(now: Long) {
        val count = underruns.getAndSet(0)
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

    private fun setSpeed(wanted: Double) {
        val rounded = (wanted * 100).roundToInt() / 100.0 // fewer property sets
        if (rounded != speed) {
            speed = rounded
            mpv.setPropertyDouble("speed", speed)
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

        // speed per second of margin above the target: 0.2 s over plays at 1.1
        const val SPEED_GAIN = 0.5
        const val MIN_SPEED = 0.9
        const val MAX_SPEED = 1.1

        const val JUMP_EXCESS = 1.5 // seconds over the target that catching up would take long over
        const val JUMP_AFTER_MS = 1000L
    }
}
