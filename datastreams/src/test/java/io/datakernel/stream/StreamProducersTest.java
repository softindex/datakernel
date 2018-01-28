package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamProducers.errorDecorator;
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
				.with(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		producer.streamTo(consumer);

		eventloop.run();

		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamProducerWithResult<Integer, Void> producer = StreamProducer.ofStream(IntStream.range(1, 10).boxed())
				.withEndOfStreamAsResult()
				.with(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		CompletableFuture<Void> future = producer.streamTo(consumer)
				.getProducerResult()
				.whenComplete((aVoid, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.exceptionally(throwable -> null)
				.toCompletableFuture();
		eventloop.run();

		future.get();
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

}