package one.imakiri.stream.app

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.layout.size
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Delete
import androidx.compose.material.icons.filled.PlayArrow
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.unit.dp
import one.imakiri.stream.streammobile.Streammobile

/** The profile's home: play, the profile settings, deleting the profile. */
@Composable
fun HomeScreen(onPlay: () -> Unit, onSettings: () -> Unit, onDeleted: () -> Unit) {
    val context = LocalContext.current
    val dir = profileDir(context)
    val profile = remember { savedProfile(dir) }
    val enrolledAs = remember { runCatching { Streammobile.enrolledAs(dir.path) }.getOrElse { it.message ?: "" } }
    var confirmDelete by remember { mutableStateOf(false) }

    Box(Modifier.fillMaxSize().safeDrawingPadding().padding(16.dp)) {
        Column(Modifier.align(Alignment.TopStart)) {
            Text(profile?.let { "${it.relayAddr}:${it.relayPort}" } ?: "", style = MaterialTheme.typography.titleMedium)
            Text(enrolledAs, style = MaterialTheme.typography.bodySmall)
        }
        Column(Modifier.align(Alignment.TopEnd)) {
            IconButton(onClick = onSettings) { Icon(Icons.Filled.Settings, contentDescription = "Profile settings") }
            IconButton(onClick = { confirmDelete = true }) { Icon(Icons.Filled.Delete, contentDescription = "Delete the profile") }
        }
        Button(onClick = onPlay, modifier = Modifier.align(Alignment.Center).size(160.dp)) {
            Icon(Icons.Filled.PlayArrow, contentDescription = "Play", modifier = Modifier.size(96.dp))
        }
    }

    if (confirmDelete) {
        AlertDialog(
            onDismissRequest = { confirmDelete = false },
            title = { Text("Delete the profile?") },
            text = { Text("The config and the client key go; the server owner has to sign a new key.") },
            confirmButton = {
                TextButton(onClick = {
                    confirmDelete = false
                    ClientSession.stop()
                    Streammobile.deleteProfile(dir.path)
                    onDeleted()
                }) { Text("Delete") }
            },
            dismissButton = { TextButton(onClick = { confirmDelete = false }) { Text("Cancel") } },
        )
    }
}
