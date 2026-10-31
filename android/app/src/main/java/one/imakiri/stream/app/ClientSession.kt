package one.imakiri.stream.app

import android.content.Context
import android.util.Log
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import one.imakiri.stream.streammobile.Listener
import one.imakiri.stream.streammobile.Session
import one.imakiri.stream.streammobile.Streammobile
import java.io.File

/** The profile directory: client.cfg, the AmneziaWG config, client.key, client.enrollment. */
fun profileDir(context: Context): File = File(context.filesDir, "profile")

/** Name the imported AmneziaWG config gets in the profile directory. */
const val AMNEZIAWG_CONFIG_FILE = "amneziawg.conf"

/**
 * The running Go client, one per process: the player activity may be created
 * again, and two clients would take the same slot on the relay from each other.
 */
object ClientSession {
    private const val TAG = "stream"
    private const val LOG_LINES = 200

    private var session: Session? = null

    private val _log = MutableStateFlow<List<String>>(emptyList())
    /** The last lines of the client log. */
    val log: StateFlow<List<String>> = _log

    private val _speedKbit = MutableStateFlow(0L)
    /** What comes from the relay, kbit/s, updated every second. */
    val speedKbit: StateFlow<Long> = _speedKbit

    private val listener = object : Listener {
        override fun log(line: String) {
            Log.i(TAG, line)
            _log.update { (it + line).takeLast(LOG_LINES) }
        }

        override fun speed(kbitPerSecond: Long) {
            _speedKbit.value = kbitPerSecond
        }
    }

    /** Starts the client of the profile in dir, stopping a running one first. */
    @Synchronized
    fun start(dir: File) {
        session?.stop()
        session = null
        _log.value = emptyList()
        _speedKbit.value = 0
        session = Streammobile.start(dir.path, listener)
    }

    @Synchronized
    fun stop() {
        session?.stop()
        session = null
    }

    /** Starts the client's latency trace to file, see cmd/latency-report. */
    @Synchronized
    fun startTrace(file: File) {
        session?.startTrace(file.path)
    }

    @Synchronized
    fun stopTrace() {
        session?.stopTrace()
    }

    /** The NTP time in unix µs; null while the clock is not synced or no client runs. */
    fun trueUnixMicro(): Long? = try {
        session?.trueUnixMicro()
    } catch (e: Exception) {
        null
    }
}
