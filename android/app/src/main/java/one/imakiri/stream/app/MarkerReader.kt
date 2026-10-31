package one.imakiri.stream.app

import android.graphics.Bitmap
import android.graphics.Rect
import android.os.Handler
import android.os.HandlerThread
import android.view.PixelCopy
import android.view.SurfaceView
import dev.jdtech.mpv.MPVLib
import java.io.File
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Measures the screen-to-screen delay with player/latency-marker.html: copies
 * the top left quarter of the picture on screen 4 times a second, finds the
 * marker's barcode, reads the PC time of the frame (t0) and takes the true
 * time (NTP) of the copy. Each reading goes to file as a line
 * `seen_unix_us  marker_unix_ms  player_lag_s` and to onReading.
 */
class MarkerReader(
    private val surface: SurfaceView,
    private val mpv: MPVLib,
    private val file: File,
    private val onReading: (delayMs: Long) -> Unit,
) {
    private val thread = HandlerThread("marker").apply { start() }
    private val handler = Handler(thread.looper)
    private val out = file.bufferedWriter()
    @Volatile private var running = true

    private val tick = object : Runnable {
        override fun run() {
            if (!running) return
            copy()
            handler.postDelayed(this, INTERVAL_MS)
        }
    }

    fun start() = handler.post(tick)

    fun stop() {
        running = false
        handler.removeCallbacks(tick)
        handler.post {
            out.close()
            thread.quitSafely()
        }
    }

    private fun copy() {
        val region = markerRegion() ?: return
        val bitmap = Bitmap.createBitmap(region.width(), region.height(), Bitmap.Config.ARGB_8888)
        // the time the copied frame is on screen; the copy is of the frame shown at the request
        val seenUs = ClientSession.trueUnixMicro() ?: return
        PixelCopy.request(surface, region, bitmap, { result ->
            if (result != PixelCopy.SUCCESS || !running) return@request
            val markerMs = readMarker(bitmap, seenUs / 1000) ?: return@request
            val lag = mpv.getPropertyDouble("demuxer-cache-time")?.let { end -> mpv.getPropertyDouble("time-pos")?.let { end - it } }
            out.write("$seenUs\t$markerMs\t${lag ?: ""}\n")
            onReading(seenUs / 1000 - markerMs)
        }, handler)
    }

    /** The top left quarter of the video on the surface, where the marker is to be. */
    private fun markerRegion(): Rect? {
        val videoW = mpv.getPropertyInt("video-params/w") ?: return null
        val videoH = mpv.getPropertyInt("video-params/h") ?: return null
        val viewW = surface.width
        val viewH = surface.height
        if (videoW <= 0 || videoH <= 0 || viewW <= 0 || viewH <= 0) return null
        val scale = min(viewW.toDouble() / videoW, viewH.toDouble() / videoH)
        val left = ((viewW - videoW * scale) / 2).roundToInt()
        val top = ((viewH - videoH * scale) / 2).roundToInt()
        return Rect(left, top, left + (videoW * scale / 2).roundToInt(), top + (videoH * scale / 2).roundToInt())
    }

    private companion object {
        const val INTERVAL_MS = 250L
    }
}

private const val CELLS = 42 // row 0: alternating, starting black
private const val DATA_BITS = 40 // row 1: 32 bits of the time in ms, CRC-8

/**
 * Finds the barcode of latency-marker.html in bitmap and returns the unix
 * time in ms it carries, the high bits taken from nowMs; null if there is no
 * readable code.
 */
fun readMarker(bitmap: Bitmap, nowMs: Long): Long? {
    val w = bitmap.width
    val h = bitmap.height
    val pixels = IntArray(w * h)
    bitmap.getPixels(pixels, 0, w, 0, 0, w, h)
    val luma = IntArray(w * h) { i ->
        val p = pixels[i]
        (((p shr 16) and 0xff) * 299 + ((p shr 8) and 0xff) * 587 + (p and 0xff) * 114) / 1000
    }

    // rows where the alternating grid row shows: its left edge and cell width
    var hitFirstY = -1
    var hitLastY = -1
    var gridX = 0.0
    var cell = 0.0
    var y = 0
    while (y < h) {
        val grid = findGrid(luma, w, y)
        if (grid != null && (hitFirstY < 0 || (y - hitLastY <= 2 && kotlin.math.abs(grid.first - gridX) < cell / 2))) {
            if (hitFirstY < 0) {
                hitFirstY = y
                gridX = grid.first
                cell = grid.second
            }
            hitLastY = y
        } else if (hitFirstY >= 0) {
            break // the first grid found, and passed it
        }
        y += 1
    }
    if (hitFirstY < 0 || hitLastY - hitFirstY < cell / 3) return null

    val gridY = (hitFirstY + hitLastY) / 2.0
    val dataY = gridY + cell
    // levels of black and white from the grid row itself
    var dark = 0
    var light = 0
    for (i in 0 until CELLS step 2) dark += sample(luma, w, h, gridX + (i + 0.5) * cell, gridY)
    for (i in 1 until CELLS step 2) light += sample(luma, w, h, gridX + (i + 0.5) * cell, gridY)
    val threshold = (dark + light) / CELLS

    var value = 0L
    for (i in 0 until DATA_BITS) {
        val bit = if (sample(luma, w, h, gridX + (i + 0.5) * cell, dataY) < threshold) 1L else 0L
        value = (value shl 1) or bit
    }
    val low = value ushr 8
    val crc = (value and 0xff).toInt()
    val bytes = intArrayOf(((low shr 24) and 0xff).toInt(), ((low shr 16) and 0xff).toInt(), ((low shr 8) and 0xff).toInt(), (low and 0xff).toInt())
    if (crc8(bytes) != crc) return null

    // the full time closest to now
    val span = 1L shl 32
    var ms = (nowMs and (span - 1).inv()) or low
    if (ms - nowMs > span / 2) ms -= span
    if (nowMs - ms > span / 2) ms += span
    return ms
}

/**
 * Looks for the grid row (42 cells alternating black and white after a white
 * quiet zone) on row y; returns its left edge and cell width.
 */
private fun findGrid(luma: IntArray, w: Int, y: Int): Pair<Double, Double>? {
    // runs of dark and light along the row
    val starts = ArrayList<Int>()
    val darks = ArrayList<Boolean>()
    var prev: Boolean? = null
    for (x in 0 until w) {
        val d = luma[y * w + x] < 128
        if (d != prev) {
            starts += x
            darks += d
            prev = d
        }
    }
    starts += w
    val runs = darks.size
    // cells 0..40 are runs i..i+40; cell 41 is white and runs into the quiet zone
    val needed = CELLS - 1
    var i = 1
    while (i + needed < runs) {
        if (darks[i] && !darks[i - 1]) {
            val lengths = IntArray(needed) { starts[i + it + 1] - starts[i + it] }
            val sorted = lengths.sorted()
            val median = sorted[needed / 2]
            if (median >= 3 && lengths.all { it >= median * 0.6 && it <= median * 1.4 } &&
                starts[i] - starts[i - 1] >= median * 1.5
            ) {
                val left = starts[i].toDouble()
                return left to (starts[i + needed] - left) / needed
            }
        }
        i++
    }
    return null
}

/** Mean luma of the 3x3 pixels around (x, y). */
private fun sample(luma: IntArray, w: Int, h: Int, x: Double, y: Double): Int {
    val cx = x.toInt()
    val cy = y.toInt()
    var sum = 0
    var n = 0
    for (dy in -1..1) for (dx in -1..1) {
        val px = min(max(cx + dx, 0), w - 1)
        val py = min(max(cy + dy, 0), h - 1)
        sum += luma[py * w + px]
        n++
    }
    return sum / n
}

private fun crc8(bytes: IntArray): Int {
    var crc = 0
    for (b in bytes) {
        crc = crc xor b
        repeat(8) { crc = if (crc and 0x80 != 0) ((crc shl 1) xor 0x07) and 0xff else (crc shl 1) and 0xff }
    }
    return crc
}
