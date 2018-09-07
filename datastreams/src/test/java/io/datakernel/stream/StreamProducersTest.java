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
import static io.datakernel.stream.TestStreamProducers.errorDecorator;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamProducersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testErrorDecorator() {
		StreamProducer<Integer> producer = StreamProducer.ofStream(IntStream.range(1, 10).boxed())
				.apply(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		producer.streamTo(consumer);

		eventloop.run();

		TestUtils.assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamProducer<Integer> producer = StreamProducer.ofStream(IntStream.range(1, 10).boxed())
				.apply(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		CompletableFuture<Void> future = producer.streamTo(consumer)
				.whenComplete(($, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.thenApplyEx(($, throwable) -> (Void) null)
				.toCompletableFuture();
		eventloop.run();

		future.get();
		TestUtils.assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testSupplierProducer() {
		List<Integer> actual = new ArrayList<>();
		int[] i = {0};
		StreamProducer.ofSupplier(
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
