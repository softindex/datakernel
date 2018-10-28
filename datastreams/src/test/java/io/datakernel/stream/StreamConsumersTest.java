package io.datakernel.stream;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.stream.processor.StreamTransformer;
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
import static io.datakernel.stream.TestUtils.assertClosedWithError;
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
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.streamTo(consumer.apply(errorDecorator(item -> item.equals(5) ? new IllegalArgumentException() : null)));
		eventloop.run();

		assertClosedWithError(supplier);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() throws ExecutionException, InterruptedException {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer =
				consumer.apply(errorDecorator(k -> k.equals(5) ? new IllegalArgumentException() : null));

		CompletableFuture<Void> supplierFuture = supplier.streamTo(errorConsumer)
				.whenComplete(($, throwable) -> assertThat(throwable, instanceOf(IllegalArgumentException.class)))
				.thenApplyEx(($, throwable) -> (Void) null)
				.toCompletableFuture();
		eventloop.run();

		supplierFuture.get();
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	private static class CountTransformer<T> implements StreamTransformer<T, T> {
		private final AbstractStreamConsumer<T> input;
		private final AbstractStreamSupplier<T> output;

		private boolean isEndOfStream = false;
		private int suspended = 0;
		private int resumed = 0;

		public CountTransformer() {
			this.input = new Input();
			this.output = new Output();
		}

		@Override
		public StreamConsumer<T> getInput() {
			return input;
		}

		@Override
		public StreamSupplier<T> getOutput() {
			return output;
		}

		public boolean isEndOfStream() {
			return isEndOfStream;
		}

		public int getSuspended() {
			return suspended;
		}

		public int getResumed() {
			return resumed;
		}

		protected final class Input extends AbstractStreamConsumer<T> {
			@Override
			protected Promise<Void> onEndOfStream() {
				isEndOfStream = true;
				return output.sendEndOfStream();
			}

			@Override
			protected void onError(Throwable t) {
				output.close(t);
			}

		}

		protected final class Output extends AbstractStreamSupplier<T> {
			@Override
			protected void onSuspended() {
				suspended++;
				input.getSupplier().suspend();
			}

			@Override
			protected void onError(Throwable t) {
				input.close(t);
			}

			@Override
			protected void onProduce(StreamDataAcceptor<T> dataAcceptor) {
				resumed++;
				input.getSupplier().resume(dataAcceptor);
			}
		}
	}

	@Test
	public void testSuspendDecorator() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);

		CountTransformer<Integer> transformer = new CountTransformer<>();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer = consumer
				.apply(suspendDecorator(
						k -> true,
						context -> eventloop.delay(10, context::resume)
				));

		supplier.streamTo(transformer.getInput());
		transformer.getOutput().streamTo(errorConsumer);
		eventloop.run();

		assertEquals(values, consumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() throws ExecutionException, InterruptedException {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		CountTransformer<Integer> transformer = new CountTransformer<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.apply(transformer).streamTo(
				consumer.apply(suspendDecorator(
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
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		StreamConsumer<Integer> consumer = StreamConsumer.ofSerialConsumer(SerialConsumer.of(AsyncConsumer.of(actual::add)));
		supplier.streamTo(consumer);
		eventloop.run();
		assertEquals(values, actual);
	}
}
