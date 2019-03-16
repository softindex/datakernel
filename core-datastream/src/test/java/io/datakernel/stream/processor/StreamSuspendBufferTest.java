package io.datakernel.stream.processor;

import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.stream.TestStreamConsumers.decorator;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(DatakernelRunner.class)
public class StreamSuspendBufferTest {
	private void testImmediateSuspend(StreamSupplierTransformer<String, StreamSupplier<String>> suspendingModifier) {
		List<String> items = IntStream.range(0, 100).mapToObj(i -> "test_" + i).collect(toList());

		SettablePromise<List<String>> result = new SettablePromise<>();

		boolean[] suspended = {false};

		StreamSupplier<String> supplier = StreamSupplier.ofIterable(items)
				.transformWith(suspendingModifier)
				.transformWith(StreamBuffer.create());

		StreamConsumer<String> consumer = StreamConsumerToList.<String>create()
				.withResultAcceptor(v -> v.acceptEx(result::set))
				.transformWith(decorator((context, receiver) ->
						item -> {
							receiver.accept(item);
							//									logger.info("Received: " + item);
							assertFalse("Should not be suspended when receiving new item!", suspended[0]);
							suspended[0] = true;
							context.suspend();
							Eventloop.getCurrentEventloop().post(() -> {
								suspended[0] = false;
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
