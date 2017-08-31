package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import static java.util.Arrays.asList;

public class AsyncRunnablesTest {
	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create();

		AsyncRunnable runnable1 = () -> SettableStage.immediateStage(null);
		AsyncRunnable runnable2 = () -> SettableStage.immediateStage(null);

		AsyncRunnable timeoutCallable = AsyncRunnables.runInParallel(eventloop, asList(runnable1, runnable2));
		timeoutCallable.run().whenComplete(AsyncCallbacks.assertBiConsumer($ -> {}));

		eventloop.run();
	}
}