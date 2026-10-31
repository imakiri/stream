plugins {
    id("com.android.application") version "9.2.1" apply false
    // AGP 9 compiles Kotlin itself; the Compose compiler plugin follows the Kotlin version
    id("org.jetbrains.kotlin.plugin.compose") version "2.4.21" apply false
}
