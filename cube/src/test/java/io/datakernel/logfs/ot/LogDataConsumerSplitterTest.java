package io.datakernel.logfs.ot;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerWithResult;
import io.datakernel.stream.StreamDataReceiver;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamConsumerToList.oneByOne;
import static io.datakernel.stream.StreamConsumers.toList;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogDataConsumerSplitterTest {
	private static final List<Integer> VALUES_1 = IntStream.range(1, 100).boxed().collect(Collectors.toList());
	private static final List<Integer> VALUES_2 = IntStream.range(-100, 0).boxed().collect(Collectors.toList());

	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	}

	private <T> void assertStreamResult(List<T> values, StreamConsumer<T> consume, CompletionStage<List<T>> result)
			throws ExecutionException, InterruptedException {

		StreamProducers.ofIterable(eventloop, values).streamTo(consume);
		final CompletableFuture<List<T>> future = result.toCompletableFuture();
		eventloop.run();
		assertEquals(values, future.get());
	}

	@Test
	public void testConsumes() throws ExecutionException, InterruptedException {
		final List<StreamConsumerWithResult<Integer, List<Integer>>> consumers =
				asList(toList(eventloop), toList(eventloop));

		final Iterator<StreamConsumerWithResult<Integer, List<Integer>>> iterator = consumers.iterator();
		final LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(eventloop, iterator::next);

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test
	public void testConsumersWithSuspend() throws ExecutionException, InterruptedException {
		final List<StreamConsumerWithResult<Integer, List<Integer>>> consumers =
				asList(oneByOne(eventloop), oneByOne(eventloop));

		final Iterator<StreamConsumerWithResult<Integer, List<Integer>>> iterator = consumers.iterator();
		final LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(eventloop, iterator::next);

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncorrectImplementation() {
		final LogDataConsumerSplitter<Integer, Integer> splitter = new LogDataConsumerSplitter<Integer, Integer>(eventloop) {

			@Override
			protected StreamDataReceiver<Integer> createSplitter() {
				return item -> {};
			}
		};

		StreamProducers.ofIterable(eventloop, VALUES_1).streamTo(splitter.consume());
	}

	private static class LogDataConsumerSplitterStub<T, D> extends LogDataConsumerSplitter<T, D> {
		private final LogDataConsumer<T, D> logConsumer;

		private LogDataConsumerSplitterStub(Eventloop eventloop, LogDataConsumer<T, D> logConsumer) {
			super(eventloop);
			this.logConsumer = logConsumer;
		}

		@Override
		protected StreamDataReceiver<T> createSplitter() {
			return addOutput(logConsumer);
		}
	}

}