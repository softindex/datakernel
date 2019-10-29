package io.datakernel.datastream.processor;

import io.datakernel.common.ref.RefBoolean;
import io.datakernel.datastream.*;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.SettablePromise;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static io.datakernel.datastream.TestStreamConsumers.decorator;
import static io.datakernel.promise.TestUtils.await;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StreamSuspendBufferTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private void testImmediateSuspend(StreamSupplierTransformer<String, StreamSupplier<String>> suspendingModifier) {
		List<String> items = IntStream.range(0, 100).mapToObj(i -> "test_" + i).collect(toList());

		SettablePromise<List<String>> result = new SettablePromise<>();

		RefBoolean suspended = new RefBoolean(false);

		StreamSupplier<String> supplier = StreamSupplier.ofIterable(items)
				.transformWith(suspendingModifier)
				.transformWith(StreamBuffer.create());

		StreamConsumer<String> consumer = StreamConsumerToList.<String>create()
				.withResultAcceptor(v -> v.whenComplete(result))
				.transformWith(decorator((context, receiver) ->
						item -> {
							receiver.accept(item);
							// logger.info("Received: " + item);
							assertFalse("Should not be suspended when receiving new item!", suspended.get());
							suspended.set(true);
							context.suspend();
							Eventloop.getCurrentEventloop().post(() -> {
								suspended.set(false);
								context.resume();
							});
						}));

		await(supplier.streamTo(consumer));

		assertEquals(items, result.getResult());
	}

	private static <T> StreamSupplierTransformer<T, StreamSupplier<T>> suspend(BiConsumer<Integer, StreamSupplier<T>> suspend) {
		return p -> new ForwardingStreamSupplier<T>(p) {
			private int counter = 0;

			@Override
			public void suspend() {
				suspend.accept(counter++, supplier);
			}
		};
	}

	@Test
	public void testDelayedSuspend() {
		// on every 5th suspend call its actual suspend is delayed
		testImmediateSuspend(suspend((i, supplier) -> {
			if (i % 5 != 0) {
				supplier.suspend();
				return;
			}
			Eventloop.getCurrentEventloop().postLater(supplier::suspend);
		}));
	}

	@Test
	public void testVeryDelayedSuspend() {
		// on every suspend call its actual suspend is delayed
		testImmediateSuspend(suspend(($, supplier) -> Eventloop.getCurrentEventloop().postLater(supplier::suspend)));
	}

	@Test
	public void testBufferedSuspend() {
		// does not ever suspend
		testImmediateSuspend(suspend(($, supplier) -> {}));
	}
}
