package io.datakernel.stream.processor;

import io.datakernel.async.SettablePromise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.ForwardingStreamSupplier;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.StreamSupplierFunction;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamConsumers.decorator;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class StreamSuspendBufferTest {
	private static final Logger logger = LoggerFactory.getLogger(StreamSuspendBufferTest.class);

	private void testImmediateSuspend(StreamSupplierFunction<String, StreamSupplier<String>> suspendingModifier) {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		List<String> items = IntStream.range(0, 100).mapToObj(i -> "test_" + i).collect(toList());

		SettablePromise<List<String>> result = new SettablePromise<>();

		boolean[] suspended = {false};
		StreamSupplier.ofIterable(items)
				.apply(suspendingModifier)
				.apply(StreamBuffer.create())
				.streamTo(StreamConsumerToList.<String>create()
						.withResultAcceptor(v -> v.whenComplete(result::set))
						.apply(decorator((context, receiver) ->
								item -> {
									receiver.accept(item);
//									logger.info("Received: " + item);
									assertFalse("Should not be suspended when receiving new item!", suspended[0]);
									suspended[0] = true;
									context.suspend();
									eventloop.post(() -> {
										suspended[0] = false;
										context.resume();
									});
								})));

		eventloop.run();

		assertEquals(items, result.getResult());
	}

	private static <T> StreamSupplierFunction<T, StreamSupplier<T>> suspend(BiConsumer<Integer, StreamSupplier<T>> suspend) {
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
