package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.stream.StreamProducers.*;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamProducersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testErrorDecorator() {
		final List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		final StreamProducer<Integer> readProducer = ofIterable(eventloop, values);
		final StreamProducer<Integer> errorProducer = errorDecorator(
				readProducer,
				k -> k.equals(5),
				IllegalArgumentException::new);

		final StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>(eventloop);
		errorProducer.streamTo(consumer);
		eventloop.run();

		assertEquals(((AbstractStreamProducer<Integer>) readProducer).getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		final List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		final StreamProducerWithResult<Integer, Void> readProducer = withEndOfStream(ofIterable(eventloop, values));
		final StreamProducerWithResult<Integer, Void> errorProducer = errorDecorator(
				readProducer,
				k -> k.equals(5),
				IllegalArgumentException::new);

		final StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>(eventloop);
		errorProducer.streamTo(consumer);
		final CompletableFuture<Void> producerFuture = readProducer
				.getResult()
				.whenComplete((aVoid, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.exceptionally(throwable -> null)
				.toCompletableFuture();
		eventloop.run();

		producerFuture.get();
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

}