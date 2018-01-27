package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamProducers.errorsInjection;
import static java.util.stream.Collectors.toList;
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
		List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());

		StreamProducer<Integer> producer = StreamProducer.ofIterable(values)
				.with(errorsInjection(k -> k.equals(5), IllegalArgumentException::new));

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();

		stream(producer, consumer);

		eventloop.run();

		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());

		StreamProducerWithResult<Integer, Void> readProducer = StreamProducer.ofIterable(values)
				.withEndOfStreamAsResult()
				.with(errorsInjection(k -> k.equals(5), IllegalArgumentException::new));

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();

		stream(readProducer, consumer);

		CompletableFuture<Void> future = readProducer
				.getResult()
				.whenComplete((aVoid, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.exceptionally(throwable -> null)
				.toCompletableFuture();
		eventloop.run();

		future.get();
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

}