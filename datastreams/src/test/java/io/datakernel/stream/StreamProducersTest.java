package io.datakernel.stream;

import io.datakernel.async.SettableStage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.FatalErrorHandlers;
import io.datakernel.stream.processor.StreamFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.datakernel.stream.StreamConsumers.toList;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.StreamProducers.ofStage;
import static org.junit.Assert.assertEquals;

public class StreamProducersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	private <T> CompletionStage<StreamProducer<T>> scheduledProducer(long delta, StreamProducer<T> consumer) {
		final SettableStage<StreamProducer<T>> stage = SettableStage.create();
		eventloop.schedule(eventloop.currentTimeMillis() + delta, () -> stage.set(consumer));
		return stage;
	}


	@Test
	public void testOfStageSimple() throws ExecutionException, InterruptedException {
		final List<Integer> values = Arrays.asList(1, 2, 3, 4);

		final StreamConsumerWithResult<Integer, List<Integer>> listConsumer = toList(eventloop);
		ofStage(scheduledProducer(100, ofIterable(eventloop, values))).streamTo(listConsumer);

		final CompletableFuture<List<Integer>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();

		assertEquals(values, listFuture.get());
	}

	@Test
	public void testOfStageInTheMiddle() throws ExecutionException, InterruptedException {
		final List<Integer> values = Arrays.asList(1, 2, 3, 4);
		final Function<Integer, Integer> intFunction = input -> input + 1;

		final StreamConsumerWithResult<Integer, List<Integer>> listConsumer = toList(eventloop);
		final StreamFunction<Integer, Integer> functionStream = StreamFunction.create(eventloop, intFunction::apply);
		final StreamProducer<Integer> producer = ofIterable(eventloop, values);
		final CompletionStage<StreamProducer<Integer>> producerStage = scheduledProducer(200, functionStream.getOutput());

		producer.streamTo(functionStream.getInput());
		ofStage(producerStage).streamTo(listConsumer);

		final CompletableFuture<List<Integer>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();

		final List<Integer> mappedValues = values.stream().map(intFunction).collect(Collectors.toList());
		assertEquals(mappedValues, listFuture.get());
	}

	@Test
	public void testOfStageInTheBeginning() throws ExecutionException, InterruptedException {
		final List<Integer> values = Arrays.asList(1, 2, 3, 4);
		final Function<Integer, Integer> intFunction = input -> input + 1;

		final StreamConsumerWithResult<Integer, List<Integer>> listConsumer = toList(eventloop);
		final StreamFunction<Integer, Integer> functionStream = StreamFunction.create(eventloop, intFunction::apply);
		final CompletionStage<StreamProducer<Integer>> producerStage = scheduledProducer(200, ofIterable(eventloop, values));

		ofStage(producerStage).streamTo(functionStream.getInput());
		functionStream.getOutput().streamTo(listConsumer);

		final CompletableFuture<List<Integer>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();

		final List<Integer> mappedValues = values.stream().map(intFunction).collect(Collectors.toList());
		assertEquals(mappedValues, listFuture.get());
	}

}