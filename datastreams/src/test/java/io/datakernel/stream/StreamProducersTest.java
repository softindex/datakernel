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
		StreamProducer<Integer> readProducer = StreamProducer.ofIterable(values);
		StreamProducer<Integer> errorProducer = StreamProducers.errorDecorator(readProducer, k -> k.equals(5), IllegalArgumentException::new);

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();
		stream(errorProducer, consumer);
		eventloop.run();

		assertEquals(((AbstractStreamProducer<Integer>) readProducer).getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		StreamProducerWithResult<Integer, Void> readProducer = StreamProducer.ofIterable(values).withEndOfStreamAsResult();
		StreamProducerWithResult<Integer, Void> errorProducer = StreamProducers.errorDecorator(readProducer, k -> k.equals(5), IllegalArgumentException::new)
				.withResult(readProducer.getResult());

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();
		stream(errorProducer, consumer);
		CompletableFuture<Void> producerFuture = readProducer
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