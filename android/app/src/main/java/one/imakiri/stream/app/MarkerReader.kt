package one.imakiri.stream.app

import android.graphics.Bitmap
import android.graphics.Rect
import android.os.Handler
import android.os.HandlerThread
import android.view.PixelCopy
import android.view.SurfaceView
import dev.jdtech.mpv.MPVLib
import java.io.File
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min
import kotlin.math.roundToInt

/**
 * Measures the screen-to-screen delay with player/latency-marker.html: copies
 * the picture on screen 4 times a second, finds the marker's barcode anywhere
 * in it by its finder squares, reads the PC time of the frame (t0, to 1/60 s)
 * and takes the true time (NTP) of the copy. Each reading goes to file as a
 * line `seen_unix_us  marker_unix_ms  player_lag_s` and to onReading.
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

    // used on the handler thread only: the copy and its pixels, kept between
    // copies; a copy still in flight skips the next tick
    private var bitmap: Bitmap? = null
    private var pixels = IntArray(0)
    private var copying = false

    private val tick = object : Runnable {
        override fun run() {
            if (!running) return
            if (!copying) copy()
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
        val region = videoRegion() ?: return
        // the copy at most at the video's own size: more pixels hold no more detail
        val scale = min(1.0, (mpv.getPropertyInt("video-params/w") ?: return).toDouble() / region.width())
        val w = max(1, (region.width() * scale).roundToInt())
        val h = max(1, (region.height() * scale).roundToInt())
        val target = bitmap?.takeIf { it.width == w && it.height == h }
            ?: Bitmap.createBitmap(w, h, Bitmap.Config.ARGB_8888).also { bitmap = it; pixels = IntArray(w * h) }
        // the time the copied frame is on screen; the copy is of the frame shown at the request
        val seenUs = ClientSession.trueUnixMicro() ?: return
        copying = true
        PixelCopy.request(surface, region, target, { result ->
            copying = false
            if (result != PixelCopy.SUCCESS || !running) return@request
            target.getPixels(pixels, 0, w, 0, 0, w, h)
            val markerMs = readMarker(pixels, w, h, seenUs / 1000) ?: return@request
            val lag = mpv.getPropertyDouble("demuxer-cache-time")?.let { end -> mpv.getPropertyDouble("time-pos")?.let { end - it } }
            out.write("$seenUs\t$markerMs\t${lag ?: ""}\n")
            onReading(seenUs / 1000 - markerMs)
        }, handler)
    }

    /** The video's picture on the surface (without the black bars). */
    private fun videoRegion(): Rect? {
        val videoW = mpv.getPropertyInt("video-params/w") ?: return null
        val videoH = mpv.getPropertyInt("video-params/h") ?: return null
        val viewW = surface.width
        val viewH = surface.height
        if (videoW <= 0 || videoH <= 0 || viewW <= 0 || viewH <= 0) return null
        val scale = min(viewW.toDouble() / videoW, viewH.toDouble() / videoH)
        val left = ((viewW - videoW * scale) / 2).roundToInt()
        val top = ((viewH - videoH * scale) / 2).roundToInt()
        return Rect(left, top, left + (videoW * scale).roundToInt(), top + (videoH * scale).roundToInt())
    }

    private companion object {
        const val INTERVAL_MS = 250L
    }
}

// A barcode of latency-marker.html (its header has the same layout), in
// cells from the code's top left corner.
private const val FINDER_CENTER = 3.5      // of the left finder, both x and y
private const val FINDER_DISTANCE = 51.0   // from the left finder's center to the right one's
private const val CODE_X = 8               // the first column of the grid and data rows
private const val GRID_Y = 1               // the grid row: 42 cells alternating black, white
private val DATA_Y = intArrayOf(3, 5)      // the data rows, the same 32 bits twice
private const val GRID_CELLS = 42
private const val DATA_BITS = 32           // the low bits of the slot number, Gray code
private const val SLOTS_PER_SECOND = 60    // the page's slot: the capture's frame interval
private const val MAX_DELAY_MS = 60_000L   // a reading farther from now is a misread

/** A finder square found in the picture: its center and the size of its cell, px. */
private class Finder(var x: Double, var y: Double, var cell: Double, var hits: Int = 1)

/**
 * Finds the barcode of latency-marker.html in the picture (pixels: ARGB,
 * w x h) and returns the unix time in ms of the start of the slot it shows,
 * the high bits taken from nowMs; null if there is no readable code.
 */
fun readMarker(pixels: IntArray, w: Int, h: Int, nowMs: Long): Long? {
    val finders = findFinders(pixels, w, h)
    for (left in finders) for (right in finders) {
        if (right.x <= left.x || abs(right.y - left.y) > left.cell) continue
        if (right.cell / left.cell !in 0.7..1.4) continue
        val cell = (right.x - left.x) / FINDER_DISTANCE
        if (abs(cell / ((left.cell + right.cell) / 2) - 1) > 0.2) continue
        val ms = decode(pixels, w, h, left, right, cell, nowMs)
        if (ms != null) return ms
    }
    return null
}

/** The finder squares in the picture: 1:1:3:1:1 black and white runs across, checked down. */
private fun findFinders(pixels: IntArray, w: Int, h: Int): List<Finder> {
    val finders = ArrayList<Finder>()
    val starts = IntArray(w + 1)
    var y = 0
    while (y < h) {
        // runs of dark and light along the row; the first run's color is darkFirst
        var runs = 0
        var prev = false
        var darkFirst = false
        for (x in 0 until w) {
            val d = isDark(pixels[y * w + x])
            if (x == 0 || d != prev) {
                if (x == 0) darkFirst = d
                starts[runs++] = x
                prev = d
            }
        }
        starts[runs] = w
        // dark runs are every other one from the first dark run
        var i = if (darkFirst) 0 else 1
        while (i + 4 < runs) {
            val cell = finderCell(
                starts[i + 1] - starts[i], starts[i + 2] - starts[i + 1], starts[i + 3] - starts[i + 2],
                starts[i + 4] - starts[i + 3], starts[i + 5] - starts[i + 4],
            )
            if (cell != null) {
                val cx = (starts[i + 2] + starts[i + 3]) / 2
                val cy = verticalCenter(pixels, w, h, cx, y)
                if (cy != null) addFinder(finders, cx + 0.5, cy, cell)
            }
            i += 2
        }
        y += 2
    }
    // a finder is seen on several rows; one or two hits are noise
    return finders.filter { it.hits >= 2 }
}

/** The cell size if 5 run lengths are in the finder's 1:1:3:1:1, else null. */
private fun finderCell(a: Int, b: Int, c: Int, d: Int, e: Int): Double? {
    val cell = (a + b + c + d + e) / 7.0
    if (cell < 2) return null
    val slack = cell * 0.5
    if (abs(a - cell) > slack || abs(b - cell) > slack || abs(d - cell) > slack || abs(e - cell) > slack) return null
    if (abs(c - 3 * cell) > slack * 1.5) return null
    return cell
}

/** Checks the column through a finder's core for 1:1:3:1:1 too; returns the core's center y. */
private fun verticalCenter(pixels: IntArray, w: Int, h: Int, x: Int, y: Int): Double? {
    fun dark(yy: Int) = isDark(pixels[yy * w + x])
    // from y up: the rest of the core, a light ring, the dark ring
    var top = y
    while (top > 0 && dark(top - 1)) top--
    var up = top
    while (up > 0 && !dark(up - 1)) up--
    val lightUp = top - up
    var ringUp = up
    while (ringUp > 0 && dark(ringUp - 1)) ringUp--
    val darkUp = up - ringUp
    // from y down, the same
    var bottom = y
    while (bottom < h - 1 && dark(bottom + 1)) bottom++
    var down = bottom
    while (down < h - 1 && !dark(down + 1)) down++
    val lightDown = down - bottom
    var ringDown = down
    while (ringDown < h - 1 && dark(ringDown + 1)) ringDown++
    val darkDown = ringDown - down
    finderCell(darkUp, lightUp, bottom - top + 1, lightDown, darkDown) ?: return null
    return (top + bottom + 1) / 2.0
}

/** Adds a finder hit, merging it with a finder found on the rows above. */
private fun addFinder(finders: MutableList<Finder>, x: Double, y: Double, cell: Double) {
    for (f in finders) {
        if (abs(f.x - x) < f.cell * 2 && abs(f.y - y) < f.cell * 2) {
            f.x = (f.x * f.hits + x) / (f.hits + 1)
            f.y = (f.y * f.hits + y) / (f.hits + 1)
            f.cell = (f.cell * f.hits + cell) / (f.hits + 1)
            f.hits++
            return
        }
    }
    finders += Finder(x, y, cell)
}

/** Reads the grid and data rows between two finders; null if the code does not check out. */
private fun decode(pixels: IntArray, w: Int, h: Int, left: Finder, right: Finder, cell: Double, nowMs: Long): Long? {
    val slope = (right.y - left.y) / (right.x - left.x)
    // the luma around the center of cell (column, row) of the code
    fun at(column: Int, row: Int): Int {
        val x = left.x + (column + 0.5 - FINDER_CENTER) * cell
        val y = left.y + (row + 0.5 - FINDER_CENTER) * cell + (x - left.x) * slope
        return sample(pixels, w, h, x, y)
    }

    // levels of black and white from the grid row, which must alternate
    var dark = 0
    var light = 0
    for (i in 0 until GRID_CELLS step 2) dark += at(CODE_X + i, GRID_Y)
    for (i in 1 until GRID_CELLS step 2) light += at(CODE_X + i, GRID_Y)
    val threshold = (dark + light) / GRID_CELLS
    if (light - dark < GRID_CELLS / 2 * 64) return null // under 64 levels apart: no grid
    for (i in 0 until GRID_CELLS) if ((at(CODE_X + i, GRID_Y) < threshold) != (i % 2 == 0)) return null

    // the data rows: Gray code, read only when both rows agree
    var gray = -1L
    for (row in DATA_Y) {
        var value = 0L
        for (i in 0 until DATA_BITS) {
            val bit = if (at(CODE_X + i, row) < threshold) 1L else 0L
            value = (value shl 1) or bit
        }
        if (gray >= 0 && value != gray) return null
        gray = value
    }
    var low = gray
    var shift = 1
    while (shift < DATA_BITS) {
        low = low xor (low ushr shift)
        shift *= 2
    }

    // the full slot number closest to now's, then its start in ms
    val span = 1L shl DATA_BITS
    val nowSlot = nowMs * SLOTS_PER_SECOND / 1000
    var slot = (nowSlot and (span - 1).inv()) or low
    if (slot - nowSlot > span / 2) slot -= span
    if (nowSlot - slot > span / 2) slot += span
    val ms = slot * 1000 / SLOTS_PER_SECOND
    if (abs(ms - nowMs) > MAX_DELAY_MS) return null
    return ms
}

private fun luma(p: Int): Int =
    (((p shr 16) and 0xff) * 299 + ((p shr 8) and 0xff) * 587 + (p and 0xff) * 114) / 1000

private fun isDark(p: Int) = luma(p) < 128

/** Mean luma of the 3x3 pixels around (x, y). */
private fun sample(pixels: IntArray, w: Int, h: Int, x: Double, y: Double): Int {
    val cx = x.toInt()
    val cy = y.toInt()
    var sum = 0
    var n = 0
    for (dy in -1..1) for (dx in -1..1) {
        val px = min(max(cx + dx, 0), w - 1)
        val py = min(max(cy + dy, 0), h - 1)
        sum += luma(pixels[py * w + px])
        n++
    }
    return sum / n
}
