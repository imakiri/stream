package one.imakiri.stream.app

import org.junit.Assert.assertEquals
import org.junit.Test

class LiveEdgeActionTest {
    private val margin = 0.12

    @Test
    fun nothingWithinTheBand() {
        for (lag in listOf(0.12, 0.091, 0.219, 0.1, 0.2, 0.12 + 0.95)) assertEquals("lag $lag", LiveEdgeAction.None, liveEdgeAction(lag, margin))
    }

    @Test
    fun pausesToTheTargetWhenUnder() {
        val action = liveEdgeAction(0.12 - 0.05, margin) as LiveEdgeAction.Pause
        assertEquals(0.05, action.seconds, 1e-9)
    }

    @Test
    fun reopensWhenFarBehind() {
        assertEquals(LiveEdgeAction.Reopen, liveEdgeAction(0.12 + 1.1, margin))
    }
}
