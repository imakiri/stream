package one.imakiri.stream.app

import android.content.Intent
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.foundation.isSystemInDarkTheme
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Surface
import androidx.compose.material3.darkColorScheme
import androidx.compose.material3.lightColorScheme
import androidx.compose.runtime.Composable
import androidx.compose.ui.Modifier
import androidx.compose.ui.platform.LocalContext
import androidx.navigation.NavHostController
import androidx.navigation.compose.NavHost
import androidx.navigation.compose.composable
import androidx.navigation.compose.rememberNavController
import one.imakiri.stream.streammobile.Streammobile

/** Routes of the setup flow: profile form, enrollment, home. */
object Route {
    const val SETUP = "setup"     // the first profile
    const val EDIT = "edit"       // the profile, from the home screen's gear
    const val ENROLL = "enroll"
    const val HOME = "home"
}

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge() // status bar icons that follow the theme
        val dir = profileDir(this)
        val start = when {
            savedProfile(dir) == null -> Route.SETUP
            !isEnrolled(dir.path) -> Route.ENROLL
            else -> Route.HOME
        }
        setContent {
            MaterialTheme(colorScheme = if (isSystemInDarkTheme()) darkColorScheme() else lightColorScheme()) {
                // the whole window: the theme behind it is the dark platform one
                Surface(Modifier.fillMaxSize()) {
                    StreamNavigation(start)
                }
            }
        }
    }
}

@Composable
private fun StreamNavigation(start: String) {
    val nav = rememberNavController()
    val context = LocalContext.current
    NavHost(nav, startDestination = start) {
        composable(Route.SETUP) {
            ProfileScreen(editing = false, onSaved = { nav.after(Route.SETUP, nextAfterProfile(context)) })
        }
        composable(Route.EDIT) {
            ProfileScreen(editing = true, onSaved = { nav.popBackStack() })
        }
        composable(Route.ENROLL) {
            EnrollScreen(
                onEnrolled = { nav.after(Route.ENROLL, Route.HOME) },
                onEditProfile = { nav.navigate(Route.EDIT) },
            )
        }
        composable(Route.HOME) {
            HomeScreen(
                onPlay = { context.startActivity(Intent(context, PlayerActivity::class.java)) },
                onSettings = { nav.navigate(Route.EDIT) },
                onDeleted = { nav.after(Route.HOME, Route.SETUP) },
            )
        }
    }
}

private fun nextAfterProfile(context: android.content.Context): String =
    if (!isEnrolled(profileDir(context).path)) Route.ENROLL else Route.HOME

/** Goes to route, dropping the screen it leaves from the back stack. */
private fun NavHostController.after(leaving: String, route: String) {
    navigate(route) { popUpTo(leaving) { inclusive = true } }
}

/** Whether dir holds an enrollment that loads; one that does not is asked for again. */
private fun isEnrolled(dir: String): Boolean =
    runCatching { Streammobile.enrolledAs(dir) }.getOrDefault("").isNotEmpty()
