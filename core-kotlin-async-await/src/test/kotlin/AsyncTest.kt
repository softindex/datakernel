
import io.datakernel.async.async
import io.datakernel.async.await
import io.datakernel.async.awaitAll
import io.datakernel.async.eventloop
import io.datakernel.eventloop.Eventloop
import io.datakernel.promise.Promises
import junit.framework.TestCase.assertEquals
import org.junit.Test

class AsyncTest {
    @Test
    fun asyncAwaitTest() {
        val eventloop = Eventloop.create().withCurrentThread()
        async {
            val job = async {
                val a = Promises.delay(200, 100L)
                val b = Promises.delay(350, 200L)
                listOf(a, b).awaitAll().sum()
            }
            assertEquals(300L, job.await())
        }
        eventloop.run()
    }

    @Test
    fun eventloopTest() {
        eventloop {
            val job = async {
                val a = Promises.delay(200, 100L)
                val b = Promises.delay(350, 200L)
                listOf(a, b).awaitAll().sum()
            }
            assertEquals(300L, job.await())
        }
    }
}
