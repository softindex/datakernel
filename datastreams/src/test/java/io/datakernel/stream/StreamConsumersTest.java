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

import static io.datakernel.stream.StreamConsumers.ofStage;
import static io.datakernel.stream.StreamConsumers.toList;
import static io.datakernel.stream.StreamProducers.ofIterable;
import static org.junit.Assert.assertEquals;

public class StreamConsumersTest {

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(FatalErrorHandlers.rethrowOnAnyError());
	}

	private <T> CompletionStage<StreamConsumer<T>> scheduledConsumer(long delta, StreamConsumer<T> consumer) {
		final SettableStage<StreamConsumer<T>> stage = SettableStage.create();
		eventloop.schedule(eventloop.currentTimeMillis() + delta, () -> stage.set(consumer));
		return stage;
	}

	@Test
	public void testOfStageSimple() throws ExecutionException, InterruptedException {
		final List<Integer> values = Arrays.asList(1, 2, 3, 4);

		final StreamConsumerWithResult<Integer, List<Integer>> listConsumer = toList(eventloop);
		ofIterable(eventloop, values).streamTo(ofStage(scheduledConsumer(100, listConsumer)));

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

		final CompletionStage<StreamConsumer<Integer>> consumerStage = scheduledConsumer(200, functionStream.getInput());

		ofIterable(eventloop, values).streamTo(ofStage(consumerStage));
		functionStream.getOutput().streamTo(listConsumer);

		final CompletableFuture<List<Integer>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();

		final List<Integer> mappedValues = values.stream().map(intFunction).collect(Collectors.toList());
		assertEquals(mappedValues, listFuture.get());
	}

	@Test
	public void testOfStageInTheEnd() throws ExecutionException, InterruptedException {
		final List<Integer> values = Arrays.asList(1, 2, 3, 4);
		final Function<Integer, Integer> intFunction = input -> input + 1;

		final StreamConsumerWithResult<Integer, List<Integer>> listConsumer = toList(eventloop);
		final StreamFunction<Integer, Integer> functionStream = StreamFunction.create(eventloop, intFunction::apply);
		final CompletionStage<StreamConsumer<Integer>> consumerStage = scheduledConsumer(200, listConsumer);

		ofIterable(eventloop, values).streamTo(functionStream.getInput());
		functionStream.getOutput().streamTo(ofStage(consumerStage));

		final CompletableFuture<List<Integer>> listFuture = listConsumer.getResult().toCompletableFuture();
		eventloop.run();

		final List<Integer> mappedValues = values.stream().map(intFunction).collect(Collectors.toList());
		assertEquals(mappedValues, listFuture.get());
	}

}
