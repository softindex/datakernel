package io.datakernel.async;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.DatakernelRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(DatakernelRunner.class)
public class AsyncSuppliersTest {
	@Test
	public void reuse() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		AtomicInteger counter = new AtomicInteger();
		AsyncSupplier<Void> reuse = AsyncSuppliers.reuse(() -> {
			System.out.println("Running reuse task");
			counter.incrementAndGet();
			SettablePromise<Void> settablePromise = new SettablePromise<>();
			eventloop.post(() -> {
				System.out.println("Completing reuse task");
				settablePromise.set(null);
			});
			return settablePromise;
		});

		System.out.println("First call of reuse");
		reuse.get().whenResult($ -> System.out.println("First reuse completed"));
		System.out.println("Second call of reuse");
		reuse.get().whenResult($ -> System.out.println("Second reuse completed"));

		eventloop.run();
		assertEquals(1, counter.get());
	}

	@Test
	public void resubscribe() {
		Eventloop eventloop = Eventloop.getCurrentEventloop();
		AtomicInteger counter = new AtomicInteger();
		AsyncSupplier<Void> resubscribe = AsyncSuppliers.subscribe(() -> {
			System.out.println("Running resubscribe task");
			counter.incrementAndGet();
			SettablePromise<Void> settablePromise = new SettablePromise<>();
			eventloop.post(() -> {
				System.out.println("Completing resubscribe task");
				settablePromise.set(null);
			});
			return settablePromise;
		});

		System.out.println("First call of resubscribe");
		resubscribe.get().whenResult($ -> System.out.println("First resubscribe completed"));
		System.out.println("Second call of resubscribe");
		resubscribe.get().whenResult($ -> System.out.println("Second resubscribe completed"));
		System.out.println("Third call of resubscribe");
		resubscribe.get().whenResult($ -> System.out.println("Third resubscribe completed"));

		eventloop.run();
		assertEquals(2, counter.get());
	}
}
