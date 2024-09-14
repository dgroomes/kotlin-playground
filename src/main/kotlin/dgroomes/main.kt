package dgroomes

import kotlinx.coroutines.*
import java.time.Duration
import java.time.Instant
import java.time.LocalTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * See the README for more information.
 *
 * This program is written in an unusual way. All code is expressed inside the 'main' function. I've done it this way
 * because I want the reader to read the code in an order that's close to the way it is executed. By contrast, I don't
 * want the reader to jump to a different file or class. In a normal program, I would use a more object-oriented
 * design.
 */
@OptIn(DelicateCoroutinesApi::class)
fun main() {

    /*
    Understanding the order of operations is delicate in the example code in this project. We know that logging
    frameworks will only flush their logs asynchronously, and creates a confusing picture of the order of execution
    because at the same time we'll have Thread errors being printed via "printStackTrace" which are synchronous.

    Also, we can't rely on logging frameworks for statements in or after a shutdown hook because it might have already
    been shutdown.
    */
    fun log(msg: String) {
        val fmt = DateTimeFormatter.ofPattern("mm:ss.SS") // No need for hours. Let's keep it small.
        val now = LocalTime.now()
        val thread = Thread.currentThread()
        println("${fmt.format(now)} [${thread.name}]- $msg")
    }

    log("Let's learn Kotlin coroutines ...")

    // This is how long the data thread will continue to produce data without being checked in on. In other words, it
    // describes how long the lease is until it expires.
    val leaseDur = Duration.ofSeconds(5)!!

    val monolithicLoopDelay = Duration.ofSeconds(1)

    // To make this more interesting. We simulate the data push behavior with a variable delay. Importantly, the longest
    // delay is 6 seconds which is longer than the lease duration.
    val dataPushDelays = listOf(
        Duration.ofMillis(500),
        Duration.ofMillis(1500),
        Duration.ofSeconds(3),
        Duration.ofSeconds(6)
    )

    val bufferSize = 10

    data class Packet(val payload: Int, val timestamp: Instant)

    val stopFnRef: AtomicReference<(() -> Unit)?> = AtomicReference(null)

    // The program components (threads, coroutines) will keep going until this flag is set to false.
    val go = AtomicBoolean(true)

    // The lease will expire at this time.
    val leaseExp = AtomicReference(Instant.now().plus(leaseDur))

    fun renew() {
        leaseExp.set(Instant.now().plus(leaseDur))
    }

    val dataBuffer = ArrayBlockingQueue<Packet>(bufferSize)

    // This thread simulates some external software component that is producing data. Let's say that it produces a
    // message every second. This component also needs periodic attention, or else it will stop producing data.
    // We're modelling this with a "lease".
    val pushThread = Thread {
        try {
            var i = 0
            fun act() {
                val now = Instant.now()
                if (now.isAfter(leaseExp.get())) {
                    throw IllegalStateException("Lease expired!")
                }

                val packet = Packet(i, now)
                if (!dataBuffer.offer(packet)) {
                    log("Buffer full. No push.")
                    return
                }

                i++
            }

            var nextDelay = dataPushDelays.iterator()

            while (true) {
                if (!go.get()) {
                    log("The 'go' flag is false. Breaking from the push loop.")
                    break
                }
                act()

                if (!nextDelay.hasNext()) {
                    nextDelay = dataPushDelays.iterator()
                }
                val delay = nextDelay.next()

                try {
                    Thread.sleep(delay)
                } catch (e: InterruptedException) {
                    log("Interrupted while waiting to push data. Breaking from the push loop.")
                    break
                }
            }
        } finally {
            // If an unexpected error occurs in the push thread, we no longer have a functioning program. Let's initiate
            // the structured shutdown.
            stopFnRef.get()?.invoke()
        }
    }
    pushThread.name = "Push"

    // The 'appCtxt' CoroutineContext and its underlying threading machinery encapsulates the bulk of the application's
    // execution. The application code continuously receives data and renews the lease. All of that work is done on
    // threads managed by this CoroutineContext.
    //
    // I'm trying to understand the trade-offs between creating a CoroutineContext via an executor that I created, or
    // using the shortcut 'newSingleThreadContext'. I've tried both below.
    val executor = Executors.newSingleThreadExecutor {
        Thread(it).apply { name = "app" }
    }
//    val appCtxt = executor.asCoroutineDispatcher()
    val appCtxt = newSingleThreadContext("app-ctxt")
    val appScope = CoroutineScope(appCtxt)

    /*
    A monolithic event loop that handles both the lease renewal and the data poll.

    This is unfortunate because we are coupling the rate at which we pull data to the rate at which we
    renew the lease. Say that new data comes in half a second, but we wait a full second. That's uneeded
    latency.
    */
    suspend fun monolithicEventLoop() {
        while (true) {
            if (!go.get()) {
                log("The 'go' flag is false. Breaking from the app loop.")
                break
            }

            val data = dataBuffer.poll()
            if (data == null) {
                log("Buffer empty.")
            } else {
                log("Pulled: payload=${data.payload}, age=${Duration.between(data.timestamp, Instant.now())}")
            }

            renew()
            delay(monolithicLoopDelay.toMillis())
        }
    }

    suspend fun decoupledRenewAndPoll_buggy() {
        val renewJob = appScope.launch {
            while (true) {
                // Note: If we're in a coroutine context, we might als prefer to just use the 'isActive' property?
                // On the other hand, if I wanted more fine-grained flags I would still need to do something like the
                // atomic boolean. Not sure.
                if (!go.get()) {
                    log("The 'go' flag is false. Breaking from the renew loop.")
                    break
                }
                renew()
                log("Lease renewed.")
                try {
                    delay(leaseDur.toMillis() / 2)
                } catch (e: CancellationException) {
                    log("Cancellation exception caught. Breaking from the renew loop.")
                    break
                }
            }
        }
        val pollJob = appScope.launch {
            while (true) {
                if (!go.get()) {
                    log("The 'go' flag is false. Breaking from the poll loop.")
                    break
                }

                val data: Packet
                try {
                    // This is a tricky part. We want to get data as soon as it's available and the 'BlockingQueue#take'
                    // method is designed just for that. Calling this method blocks the thread until data is available
                    // and returns it. But this has the effect that the thread can't schedule other work on other
                    // coroutines. If we wait too long for data to come, then we would fail to renew the lease.
                    //
                    // So, this code is buggy.
                    data = dataBuffer.take()
                } catch (e: InterruptedException) {
                    log("Interrupted while waiting for data. Breaking from the poll loop.")
                    break
                }
                log("Pulled: payload=${data.payload}, age=${Duration.between(data.timestamp, Instant.now())}")
            }
        }

        // Wait for both coroutines to complete
        pollJob.join()
        renewJob.join()
    }

    val appJob = appScope.launch(start = CoroutineStart.LAZY) {
//            monolithicEventLoop()
        decoupledRenewAndPoll_buggy()
    }

    val stopped = AtomicBoolean(false)
    fun stopFn() {
        if (stopped.getAndSet(true)) {
            log("Stop function invoked but already stopped.")
            return
        }
        log("Stop function invoked.")
        go.set(false)
        appCtxt.close()
        executor.shutdownNow()
        appScope.cancel()
        pushThread.interrupt()
//        appThread.interrupt()
        appJob.cancel()
    }
    stopFnRef.set(::stopFn)

    Runtime.getRuntime().addShutdownHook(Thread {
        log("Shutdown hook triggered.")
        val now = Instant.now()
        stopFn()
        pushThread.join()
//        appThread.join()
        runBlocking {
            appJob.join()
        }
        val elapsed = Duration.between(now, Instant.now())
        log("Gracefully shut down in $elapsed")
    })

    // We only launch the execution machinery (it all just boils down to Threads) near the end of the main function
    // because we need to have already wired up all the objects and references.
    appJob.start()
    pushThread.start()
}
