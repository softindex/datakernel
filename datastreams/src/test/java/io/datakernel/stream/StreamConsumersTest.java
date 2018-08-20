package io.datakernel.stream;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.TestUtils.CountTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.TestStreamConsumers.errorDecorator;
import static io.datakernel.stream.TestStreamConsumers.suspendDecorator;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamConsumersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testErrorDecorator() {
		StreamProducer<Integer> producer = StreamProducer.ofStream(IntStream.range(1, 10).boxed());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		producer.streamTo(consumer.with(errorDecorator(item -> item.equals(5) ? new IllegalArgumentException() : null)));
		eventloop.run();

		assertEquals(((AbstractStreamProducer<Integer>) producer).getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamProducerWithResult<Integer, Void> producer = StreamProducer.ofStream(IntStream.range(1, 10).boxed())
				.withEndOfStreamAsResult();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumerWithResult<Integer, List<Integer>> errorConsumer =
				consumer.with(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		CompletableFuture<Void> producerFuture = producer.streamTo(errorConsumer)
				.getProducerResult()
				.whenComplete(($, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.thenApplyEx(($, throwable) -> (Void) null)
				.toCompletableFuture();
		eventloop.run();

		producerFuture.get();
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testSuspendDecorator() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);

		CountTransformer<Integer> transformer = new CountTransformer<>();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer = consumer
				.with(suspendDecorator(
						k -> true,
						context -> eventloop.delay(10, context::resume)
				));

		producer.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(errorConsumer);
		eventloop.run();

		assertEquals(values, consumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());

		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);
		CountTransformer<Integer> transformer = new CountTransformer<>();
		StreamConsumerWithResult<Integer, List<Integer>> consumer = StreamConsumerToList.create();

		producer.with(transformer).streamTo(
				consumer.with(suspendDecorator(
						item -> true,
						context -> eventloop.delay(10, context::resume))));

		CompletableFuture<List<Integer>> listFuture = consumer.getResult().toCompletableFuture();
		eventloop.run();

		assertEquals(values, listFuture.get());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testConsumerWrapper() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();
		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);
		StreamConsumer<Integer> consumer = StreamConsumer.ofSerialConsumer(SerialConsumer.of(AsyncConsumer.of(actual::add)));
		producer.streamTo(consumer);
		eventloop.run();
		assertEquals(values, actual);
	}
}
