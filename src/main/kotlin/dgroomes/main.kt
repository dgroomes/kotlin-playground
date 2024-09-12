package dgroomes

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.Logger
import org.slf4j.LoggerFactory

val log: Logger = LoggerFactory.getLogger("dgroomes.main")

/**
 * Please see the README for more information about this program.
 */
fun main() {
    log.info("Let's learn some Kotlin ...")

    runBlocking {
        log.info("Hello from the top-level of a coroutine!")

        launch {
            log.info("Hello from another coroutine?")
            delay(2_000)
            log.info("Goodbye.")
        }

        launch {
            delay(1_000)
            log.info("Hello from yet another coroutine?")
        }
    }
}

