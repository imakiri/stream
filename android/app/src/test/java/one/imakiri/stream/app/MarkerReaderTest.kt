package one.imakiri.stream.app

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test
import kotlin.random.Random

/** Draws the barcode as player/latency-marker.html does and reads it back. */
class MarkerReaderTest {
    private val black = 0xff000000.toInt()
    private val white = 0xffffffff.toInt()
    private val w = 1920
    private val h = 1080

    private fun noise() = Random(1).let { random -> IntArray(w * h) { black or random.nextInt(0x1000000) } }

    private fun gray(slot: Long): Long {
        val low = slot % (1L shl 32)
        return low xor (low ushr 1)
    }

    /**
     * Draws the code with its quiet zone into pixels, its top left corner at
     * (left, top), cells of cell px; the data rows (y 3 and 5) show the slots
     * slot3 and slot5.
     */
    private fun code(pixels: IntArray, left: Int, top: Int, cell: Int, slot3: Long, slot5: Long = slot3) {
        fun fill(cx: Int, cy: Int, cw: Int, ch: Int, color: Int) {
            for (y in top + cy * cell until top + (cy + ch) * cell) for (x in left + cx * cell until left + (cx + cw) * cell) pixels[y * w + x] = color
        }
        fill(-2, -2, 62, 11, white) // quiet zone
        for (fx in intArrayOf(0, 51)) {
            fill(fx, 0, 7, 7, black)
            fill(fx + 1, 1, 5, 5, white)
            fill(fx + 2, 2, 3, 3, black)
        }
        for (i in 0 until 42 step 2) fill(8 + i, 1, 1, 1, black)
        for ((row, slot) in listOf(3 to slot3, 5 to slot5)) {
            val value = gray(slot)
            for (i in 0 until 32) if ((value shr (31 - i)) and 1L == 1L) fill(8 + i, row, 1, 1, black)
        }
    }

    private val slot = 1_793_483_333_024L * 60 / 1000
    private val slotStartMs = slot * 1000 / 60

    @Test
    fun readsTheCodeAnywhereInThePicture() {
        for ((left, top, cell) in listOf(Triple(40, 40, 16), Triple(900, 900, 12), Triple(1100, 300, 8), Triple(300, 1000, 5))) {
            val pixels = noise().also { code(it, left, top, cell, slot) }
            assertEquals("at $left,$top cell $cell", slotStartMs, readMarker(pixels, w, h, slotStartMs + 470))
        }
    }

    @Test
    fun nextSlotChangesOneCellPerRow() {
        for (s in slot until slot + 1000) assertEquals(1, java.lang.Long.bitCount(gray(s) xor gray(s + 1)))
    }

    @Test
    fun rowsThatDisagreeAreNotRead() {
        val pixels = noise().also { code(it, 300, 300, 16, slot, slot + 1) }
        assertNull(readMarker(pixels, w, h, slotStartMs + 470))
    }

    @Test
    fun aTimeFarFromNowIsAMisread() {
        val pixels = noise().also { code(it, 300, 300, 16, slot) }
        assertNull(readMarker(pixels, w, h, slotStartMs + 61_000))
    }

    @Test
    fun noCodeInNoise() {
        assertNull(readMarker(noise(), w, h, slotStartMs))
    }
}
