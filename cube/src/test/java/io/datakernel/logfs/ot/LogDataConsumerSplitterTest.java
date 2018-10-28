package io.datakernel.logfs.ot;

import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.stream.StreamSupplier;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogDataConsumerSplitterTest {
	private static final List<Integer> VALUES_1 = IntStream.range(1, 100).boxed().collect(Collectors.toList());
	private static final List<Integer> VALUES_2 = IntStream.range(-100, 0).boxed().collect(Collectors.toList());

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
	}

	private <T> void assertStreamResult(List<T> values, StreamConsumerWithResult<T, List<T>> consumer, Promise<List<T>> result)
			throws ExecutionException, InterruptedException {

		StreamSupplier.ofIterable(values).streamTo(consumer.getConsumer());
		CompletableFuture<List<T>> future = result.toCompletableFuture();
		eventloop.run();
		assertEquals(values, future.get());
	}

	@Test
	public void testConsumes() throws ExecutionException, InterruptedException {
		List<StreamConsumerToList<Integer>> consumers = asList(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test
	public void testConsumersWithSuspend() throws ExecutionException, InterruptedException {
		List<StreamConsumerToList<Integer>> consumers = asList(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncorrectImplementation() {
		LogDataConsumerSplitter<Integer, Integer> splitter = new LogDataConsumerSplitter<Integer, Integer>() {
			@Override
			protected StreamDataAcceptor<Integer> createSplitter() {
				return item -> {};
			}
		};

		StreamSupplier.ofIterable(VALUES_1).streamTo(splitter.consume().getConsumer());
	}

	private static class LogDataConsumerSplitterStub<T, D> extends LogDataConsumerSplitter<T, D> {
		private final LogDataConsumer<T, D> logConsumer;

		private LogDataConsumerSplitterStub(LogDataConsumer<T, D> logConsumer) {
			this.logConsumer = logConsumer;
		}

		@Override
		protected StreamDataAcceptor<T> createSplitter() {
			return addOutput(logConsumer);
		}
	}

}
