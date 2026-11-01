package one.imakiri.stream.app

import android.content.Context
import android.net.Uri
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.FilterChip
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import one.imakiri.stream.streammobile.Profile
import one.imakiri.stream.streammobile.Streammobile
import java.io.File

/** The form fields of a profile, as text while they are edited. */
private class ProfileForm(profile: Profile) {
    var relayAddr by mutableStateOf(profile.relayAddr)
    var relayPort by mutableStateOf(profile.relayPort.toString())
    var relayNetwork by mutableStateOf(profile.relayNetwork)
    var proxyKind by mutableStateOf(profile.proxyKind)
    var proxyURL by mutableStateOf(profile.proxyURL)
    var amneziaWGConfig by mutableStateOf(profile.amneziaWGConfig)
    var playerPort by mutableStateOf(profile.playerPort.toString())
    var decoderDispatcherSize by mutableStateOf(profile.decoderDispatcherSize.toString())
    var decoderReorderDeadline by mutableStateOf(profile.decoderReorderDeadline.toString())
    var ntpServer by mutableStateOf(profile.ntpServer)

    /** The profile of the fields; throws on a number that is not one. */
    fun profile(): Profile = Profile().also {
        it.relayAddr = relayAddr
        it.relayPort = number("relay port", relayPort)
        it.relayNetwork = relayNetwork
        it.proxyKind = proxyKind
        it.proxyURL = proxyURL
        it.amneziaWGConfig = amneziaWGConfig
        it.playerPort = number("player port", playerPort)
        it.decoderDispatcherSize = number("decoder dispatcher size", decoderDispatcherSize)
        it.decoderReorderDeadline = number("decoder reorder deadline", decoderReorderDeadline)
        it.ntpServer = ntpServer
    }

    private fun number(name: String, text: String): Long =
        text.trim().toLongOrNull() ?: throw IllegalArgumentException("$name is not a number")
}

/**
 * The profile form: imported from a client.cfg or typed in; the player and
 * decoder fields start with the defaults (the FEC layout comes in the packets). An import overwrites every field.
 */
@Composable
fun ProfileScreen(editing: Boolean, onSaved: () -> Unit) {
    val context = LocalContext.current
    val dir = profileDir(context)
    // the generation is bumped by an import, so the form is made anew from the imported profile
    var generation by remember { mutableStateOf(0) }
    var imported by remember { mutableStateOf<Profile?>(null) }
    val form = remember(generation) {
        ProfileForm(imported ?: Streammobile.loadProfile(dir.path) ?: Streammobile.defaultProfile())
    }
    var message by remember { mutableStateOf<String?>(null) }

    val importConfig = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        if (uri == null) return@rememberLauncherForActivityResult
        try {
            val profile = Streammobile.parseProfile(readText(context, uri))
            if (profile.proxyKind == Streammobile.ProxyAmneziaWG) {
                // the cfg names a file next to it, which a picked document has no way to; pick it below
                profile.amneziaWGConfig = if (File(dir, AMNEZIAWG_CONFIG_FILE).exists()) AMNEZIAWG_CONFIG_FILE else ""
            }
            imported = profile
            generation++
            message = "imported"
        } catch (e: Exception) {
            message = "import failed: ${e.message}"
        }
    }
    val importAmneziaWG = rememberLauncherForActivityResult(ActivityResultContracts.OpenDocument()) { uri ->
        if (uri == null) return@rememberLauncherForActivityResult
        try {
            dir.mkdirs()
            File(dir, AMNEZIAWG_CONFIG_FILE).writeText(readText(context, uri))
            form.amneziaWGConfig = AMNEZIAWG_CONFIG_FILE
            message = null
        } catch (e: Exception) {
            message = "failed to read the AmneziaWG config: ${e.message}"
        }
    }

    Column(
        Modifier.safeDrawingPadding().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text(if (editing) "Profile" else "New profile", style = MaterialTheme.typography.headlineSmall)
        OutlinedButton(onClick = { importConfig.launch(arrayOf("*/*")) }, Modifier.fillMaxWidth()) {
            Text("Import client.cfg")
        }
        Text("or fill in by hand", style = MaterialTheme.typography.bodySmall)

        Section("Relay")
        Field("Address", form.relayAddr) { form.relayAddr = it }
        Field("Port", form.relayPort, number = true) { form.relayPort = it }
        Choice(listOf("udp", "tcp"), form.relayNetwork) { form.relayNetwork = it }

        Section("Proxy")
        Choice(
            listOf(Streammobile.ProxyNone, Streammobile.ProxySocks5, Streammobile.ProxyHTTP, Streammobile.ProxyAmneziaWG),
            form.proxyKind,
            label = { it.ifEmpty { "none" } },
        ) { form.proxyKind = it }
        when (form.proxyKind) {
            Streammobile.ProxySocks5, Streammobile.ProxyHTTP ->
                Field("URL, e.g. ${form.proxyKind}://127.0.0.1:2080", form.proxyURL) { form.proxyURL = it }
            Streammobile.ProxyAmneziaWG -> {
                Text(if (form.amneziaWGConfig.isEmpty()) "no config chosen" else "config: ${form.amneziaWGConfig}")
                OutlinedButton(onClick = { importAmneziaWG.launch(arrayOf("*/*")) }) { Text("Choose AmneziaWG .conf") }
            }
        }

        Section("Player")
        Field("Local UDP port", form.playerPort, number = true) { form.playerPort = it }

        Section("Decoder")
        Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
            Field("Dispatcher size", form.decoderDispatcherSize, Modifier.weight(1f), number = true) { form.decoderDispatcherSize = it }
            Field("Reorder deadline, ms", form.decoderReorderDeadline, Modifier.weight(1f), number = true) { form.decoderReorderDeadline = it }
        }

        Section("Clock")
        Field("NTP server, empty: off (e.g. time.cloudflare.com)", form.ntpServer) { form.ntpServer = it }

        message?.let { Text(it, color = MaterialTheme.colorScheme.error) }
        Button(
            onClick = {
                try {
                    form.profile().save(dir.path)
                    onSaved()
                } catch (e: Exception) {
                    message = e.message
                }
            },
            Modifier.fillMaxWidth(),
        ) { Text("Save") }
    }
}

private fun readText(context: Context, uri: Uri): String =
    context.contentResolver.openInputStream(uri)?.use { it.readBytes().decodeToString() }
        ?: throw IllegalStateException("cannot open $uri")

@Composable
private fun Section(title: String) {
    Text(title, style = MaterialTheme.typography.titleMedium, modifier = Modifier.padding(top = 8.dp))
}

@Composable
private fun Field(
    label: String,
    value: String,
    modifier: Modifier = Modifier.fillMaxWidth(),
    number: Boolean = false,
    onChange: (String) -> Unit,
) {
    OutlinedTextField(
        value = value,
        onValueChange = onChange,
        label = { Text(label) },
        singleLine = true,
        keyboardOptions = if (number) KeyboardOptions(keyboardType = KeyboardType.Number) else KeyboardOptions.Default,
        modifier = modifier,
    )
}

@Composable
private fun Choice(options: List<String>, selected: String, label: (String) -> String = { it }, onSelect: (String) -> Unit) {
    Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
        for (option in options) {
            FilterChip(selected = option == selected, onClick = { onSelect(option) }, label = { Text(label(option)) })
        }
    }
}
