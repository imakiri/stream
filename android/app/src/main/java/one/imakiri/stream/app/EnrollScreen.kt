package one.imakiri.stream.app

import android.content.ClipData
import android.content.ClipboardManager
import android.content.Context
import android.content.Intent
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeDrawingPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.selection.SelectionContainer
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedButton
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.unit.dp
import one.imakiri.stream.streammobile.Streammobile

/**
 * Enrollment: shows the client's public key for the server owner to sign with
 * server -sign and takes the enrollment they send back.
 */
@Composable
fun EnrollScreen(onEnrolled: () -> Unit, onEditProfile: () -> Unit) {
    val context = LocalContext.current
    val dir = profileDir(context)
    val publicKey = remember { runCatching { Streammobile.clientPublicKey(dir.path) } }
    var enrollment by remember { mutableStateOf("") }
    var message by remember { mutableStateOf<String?>(null) }

    Column(
        Modifier.safeDrawingPadding().verticalScroll(rememberScrollState()).padding(16.dp),
        verticalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Text("Enrollment", style = MaterialTheme.typography.headlineSmall)
        publicKey.onFailure { Text("failed to make the client key: ${it.message}", color = MaterialTheme.colorScheme.error) }
        publicKey.onSuccess { key ->
            Text("Send this public key to the server owner to sign:")
            SelectionContainer { Text(key, fontFamily = FontFamily.Monospace) }
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                OutlinedButton(onClick = { copy(context, key) }) { Text("Copy") }
                OutlinedButton(onClick = { share(context, key) }) { Text("Share") }
            }

            Text("Paste the enrollment you got back:", modifier = Modifier.padding(top = 16.dp))
            OutlinedTextField(
                value = enrollment,
                onValueChange = { enrollment = it },
                textStyle = MaterialTheme.typography.bodySmall.copy(fontFamily = FontFamily.Monospace),
                minLines = 3,
                modifier = Modifier.fillMaxWidth(),
            )
            OutlinedButton(onClick = { paste(context)?.let { enrollment = it } }) { Text("Paste") }
            message?.let { Text(it, color = MaterialTheme.colorScheme.error) }
            Button(
                onClick = {
                    try {
                        Streammobile.enroll(dir.path, enrollment)
                        onEnrolled()
                    } catch (e: Exception) {
                        message = e.message
                    }
                },
                enabled = enrollment.isNotBlank(),
                modifier = Modifier.fillMaxWidth(),
            ) { Text("Enroll") }
        }
        TextButton(onClick = onEditProfile) { Text("Edit the profile") }
    }
}

private fun copy(context: Context, text: String) {
    val clipboard = context.getSystemService(ClipboardManager::class.java)
    clipboard.setPrimaryClip(ClipData.newPlainText("stream client key", text))
}

private fun paste(context: Context): String? {
    val clipboard = context.getSystemService(ClipboardManager::class.java)
    return clipboard.primaryClip?.takeIf { it.itemCount > 0 }?.getItemAt(0)?.coerceToText(context)?.toString()?.trim()
}

private fun share(context: Context, text: String) {
    val send = Intent(Intent.ACTION_SEND).setType("text/plain").putExtra(Intent.EXTRA_TEXT, text)
    context.startActivity(Intent.createChooser(send, "Send the client key"))
}
