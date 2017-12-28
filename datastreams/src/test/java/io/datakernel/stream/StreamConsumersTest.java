package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.stream.TestUtils.CountTransformer;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.withEndOfStream;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class StreamConsumersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	@Test
	public void testErrorDecorator() {
		final List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		final StreamProducer<Integer> producer = ofIterable(eventloop, values);

		final StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>(eventloop);
		final StreamConsumer<Integer> errorConsumer = StreamConsumers.errorDecorator(
				((StreamConsumer<Integer>) consumer),
				k -> k.equals(5),
				IllegalArgumentException::new);

		producer.streamTo(errorConsumer);
		eventloop.run();

		assertEquals(((AbstractStreamProducer<Integer>) producer).getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		final List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		final StreamProducerWithResult<Integer, Void> producer = withEndOfStream(ofIterable(eventloop, values));

		final StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>(eventloop);
		final StreamConsumerWithResult<Integer, List<Integer>> errorConsumer = StreamConsumers.errorDecorator(
				consumer,
				k -> k.equals(5),
				IllegalArgumentException::new);

		producer.streamTo(errorConsumer);
		final CompletableFuture<Void> producerFuture = producer
				.getResult()
				.whenComplete((aVoid, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.exceptionally(throwable -> null)
				.toCompletableFuture();
		eventloop.run();

		producerFuture.get();
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testSuspendDecorator() {
		final List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		final StreamProducer<Integer> producer = ofIterable(eventloop, values);

		final CountTransformer<Integer> transformer = new CountTransformer<>(eventloop);

		final StreamConsumerToList<Integer> realConsumer = new StreamConsumerToList<>(eventloop);
		final StreamConsumer<Integer> consumer = StreamConsumers.suspendDecorator(
				((StreamConsumer<Integer>) realConsumer),
				k -> true,
				(innerProducer, integerStreamDataReceiver) -> eventloop.post(() -> {
					if (!transformer.isEndOfStream()) {
						eventloop.schedule(eventloop.currentTimeMillis() + 100, () ->
								innerProducer.produce(integerStreamDataReceiver));
					}
				}));

		producer.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(values, realConsumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() throws ExecutionException, InterruptedException {
		final List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		final StreamProducer<Integer> producer = ofIterable(eventloop, values);

		final CountTransformer<Integer> transformer = new CountTransformer<>(eventloop);

		final StreamConsumerWithResult<Integer, List<Integer>> consumer = StreamConsumers.suspendDecorator(
				new StreamConsumerToList<>(eventloop),
				k -> true,
				(innerProducer, integerStreamDataReceiver) -> eventloop.post(() -> {
					if (!transformer.isEndOfStream()) {
						eventloop.schedule(eventloop.currentTimeMillis() + 100, () ->
								innerProducer.produce(integerStreamDataReceiver));
					}
				}));

		producer.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(consumer);
		final CompletableFuture<List<Integer>> listFuture = consumer.getResult().toCompletableFuture();
		eventloop.run();

		assertEquals(values, listFuture.get());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

}