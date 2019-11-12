package io.datakernel.async

import io.datakernel.eventloop.Eventloop
import io.datakernel.promise.Promise
import io.datakernel.promise.Promises
import io.datakernel.promise.SettablePromise
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import java.util.stream.Stream
import java.util.stream.StreamSupport
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

fun <T> eventloop(block: suspend CoroutineScope.() -> T): T = GlobalScope.eventloop(block)

fun <T> CoroutineScope.eventloop(block: suspend CoroutineScope.() -> T): T {
    val eventloop = Eventloop.create().withCurrentThread()
    val res = async { block() }
    eventloop.run() // blocks
    if (res.isResult) {
        return res.result
    }
    if (res.isException) {
        throw res.exception
    }
    throw AssertionError("Promise not complete after eventloop finished")
}

fun <T> async(block: suspend CoroutineScope.() -> T): Promise<T> = GlobalScope.async(block)

fun <T> CoroutineScope.async(block: suspend CoroutineScope.() -> T): Promise<T> {
    val stage = SettablePromise<T>()
    launch(Dispatchers.Unconfined) {
        try {
            stage.set(block())
        } catch (t: Throwable) {
            stage.setException(t)
        }
    }
    return stage
}

suspend fun <T> Promise<T>.await(): T {
    if (isResult) {
        return result
    }
    if (isException) {
        throw exception
    }
    return suspendCoroutine {
        whenComplete { result, error ->
            if (error == null) {
                it.resume(result)
            } else {
                it.resumeWithException(error)
            }
        }
    }
}

suspend fun <T> await(vararg promises: Promise<T>): List<T> =
        promises.asList().awaitAll()

suspend fun <T> Iterable<Promise<out T>>.awaitAll(): List<T> =
        Promises.toList(StreamSupport.stream(spliterator(), false)).await()

suspend fun Promise<*>.join() {
    if (isResult) {
        return
    }
    if (isException) {
        throw exception
    }
    return suspendCoroutine {
        whenComplete { _, error ->
            if (error == null) {
                it.resume(Unit)
            } else {
                it.resumeWithException(error)
            }
        }
    }
}

suspend fun Iterator<Promise<*>>.joinAll() = Promises.all(this).join()

suspend fun Iterable<Promise<*>>.joinAll() = Promises.all(this).join()

suspend fun Stream<Promise<*>>.joinAll() = Promises.all(this).join()

suspend fun Sequence<Promise<*>>.joinAll() = Promises.all(iterator()).join()
