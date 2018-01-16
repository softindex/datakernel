package io.datakernel.stream;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.TestUtils.CountTransformer;
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

public class StreamConsumersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	@Test
	public void testErrorDecorator() {
		List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();
		StreamConsumer<Integer> errorConsumer = StreamConsumers.errorDecorator(
				((StreamConsumer<Integer>) consumer),
				k -> k.equals(5),
				IllegalArgumentException::new);

		stream(producer, errorConsumer);
		eventloop.run();

		assertEquals(((AbstractStreamProducer<Integer>) producer).getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertEquals(consumer.getStatus(), StreamStatus.CLOSED_WITH_ERROR);
		assertThat(consumer.getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 10).boxed().collect(toList());
		StreamProducerWithResult<Integer, Void> producer = StreamProducer.ofIterable(values).withEndOfStreamAsResult();

		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<>();
		StreamConsumerWithResult<Integer, List<Integer>> errorConsumer = StreamConsumers.errorDecorator(
				consumer,
				k -> k.equals(5),
				IllegalArgumentException::new);

		stream(producer, errorConsumer);
		CompletableFuture<Void> producerFuture = producer
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
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);

		CountTransformer<Integer> transformer = new CountTransformer<>();

		StreamConsumerToList<Integer> realConsumer = new StreamConsumerToList<>();
		StreamConsumer<Integer> consumer = StreamConsumers.suspendDecorator(
				((StreamConsumer<Integer>) realConsumer),
				k -> true,
				(innerProducer, integerStreamDataReceiver) -> eventloop.post(() -> {
					if (!transformer.isEndOfStream()) {
						eventloop.schedule(eventloop.currentTimeMillis() + 100, () ->
								innerProducer.produce(integerStreamDataReceiver));
					}
				}));

		stream(producer, transformer.getInput());
		stream(transformer.getOutput(), consumer);
		eventloop.run();

		assertEquals(values, realConsumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		StreamProducer<Integer> producer = StreamProducer.ofIterable(values);

		CountTransformer<Integer> transformer = new CountTransformer<>();

		StreamConsumerWithResult<Integer, List<Integer>> consumer = StreamConsumers.suspendDecorator(
				new StreamConsumerToList<>(),
				k -> true,
				(innerProducer, integerStreamDataReceiver) -> eventloop.post(() -> {
					if (!transformer.isEndOfStream()) {
						eventloop.schedule(eventloop.currentTimeMillis() + 100, () ->
								innerProducer.produce(integerStreamDataReceiver));
					}
				}));

		stream(producer, transformer.getInput());
		stream(transformer.getOutput(), consumer);
		CompletableFuture<List<Integer>> listFuture = consumer.getResult().toCompletableFuture();
		eventloop.run();

		assertEquals(values, listFuture.get());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

}