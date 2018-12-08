package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamSuppliers.errorDecorator;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamSuppliersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testErrorDecorator() {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.streamTo(consumer);

		eventloop.run();

		TestUtils.assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed())
				.transformWith(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		CompletableFuture<Void> future = supplier.streamTo(consumer)
				.whenComplete(($, e) -> assertThat(e, instanceOf(IllegalArgumentException.class)))
				.thenApplyEx(($, e) -> (Void) null)
				.toCompletableFuture();
		eventloop.run();

		future.get();
		TestUtils.assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testSupplierSupplier() {
		List<Integer> actual = new ArrayList<>();
		int[] i = {0};
		StreamSupplier.ofSupplier(
				() -> {
					if (i[0] == 10) {
						return null;
					}
					return i[0]++;
				})
				.streamTo(StreamConsumerToList.create(actual));
		eventloop.run();
		assertEquals(Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), actual);
	}
}
