package io.datakernel.promise;

import io.datakernel.async.callback.Callback;
import io.datakernel.common.ref.Ref;
import io.datakernel.common.ref.RefInt;
import io.datakernel.eventloop.Eventloop;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class AbstractPromiseTest {

	@Test
	public void testRecursionProblems() {
		int count = 1_000_000;
		SettablePromise<Void> settablePromise = new SettablePromise<>();
		RefInt refInt = new RefInt(count);
		for (int i = 0; i < count; i++) {
			settablePromise.whenResult($ -> refInt.dec());
		}

		settablePromise.set(null);

		assertEquals(0, refInt.get());
	}

	@Test
	public void testOrder() {
		SettablePromise<@Nullable Void> settablePromise = new SettablePromise<>();
		StringBuilder sb = new StringBuilder();
		settablePromise
				.whenResult($ -> sb.append("1"))
				.whenResult($ -> sb.append("2"))
				.whenResult($ -> sb.append("3"))
				.whenResult($ -> sb.append("4"))
				.whenResult($ -> sb.append("5"));

		settablePromise.set(null);

		assertEquals("12345", sb.toString());

	}

	@Test
	public void toAnotherEventloop() throws InterruptedException {
		System.setProperty("RunnableWithContext.wrapContext", "true");

		Ref<Object> contextRef = new Ref<>();
		Eventloop currentEventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler((e, context) -> contextRef.set(context));
		Eventloop anotherEventloop = Eventloop.create();

		anotherEventloop.startExternalTask();
		Thread thread = new Thread(anotherEventloop);

		SettablePromise<Integer> cb = new SettablePromise<>();
		cb.map($ -> $)
				.then(Promise::of)
				.whenResult($ -> {})
				.whenComplete(() -> {throw new RuntimeException();});

		currentEventloop.post(() -> {
			Callback<Integer> anotherEventloopCb = Callback.toAnotherEventloop(currentEventloop, cb);
			anotherEventloop.completeExternalTask();
			anotherEventloopCb.accept(123, null);
		});

		thread.start();
		currentEventloop.run();
		thread.join();

		Object caughtContext = contextRef.get();
		assertSame(cb, caughtContext);

		System.out.println(caughtContext);
	}
}
