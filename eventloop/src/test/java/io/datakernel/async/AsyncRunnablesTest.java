package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import org.junit.Test;

import static java.util.Arrays.asList;

public class AsyncRunnablesTest {
	@Test
	public void test() {
		Eventloop eventloop = Eventloop.create();

		AsyncRunnable runnable1 = new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				callback.setComplete();
			}
		};
		AsyncRunnable runnable2 = new AsyncRunnable() {
			@Override
			public void run(CompletionCallback callback) {
				callback.setComplete();
			}
		};

		AsyncRunnable timeoutCallable = AsyncRunnables.runInParallel(eventloop, asList(runnable1, runnable2));
		timeoutCallable.run(new AssertingCompletionCallback() {
			@Override
			protected void onComplete() {
			}
		});

		eventloop.run();
	}
}