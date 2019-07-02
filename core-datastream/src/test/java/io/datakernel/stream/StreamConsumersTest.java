/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.stream;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.processor.StreamTransformer;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.stream.TestStreamConsumers.errorDecorator;
import static io.datakernel.stream.TestStreamConsumers.suspendDecorator;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class StreamConsumersTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@Test
	public void testErrorDecorator() {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		IllegalArgumentException exception = new IllegalArgumentException("TestException");

		Throwable e = awaitException(supplier.streamTo(consumer
				.transformWith(errorDecorator(item -> item.equals(5) ? exception : null))));

		assertSame(exception, e);

		assertClosedWithError(supplier);
		assertClosedWithError(consumer);
		assertThat(consumer.getAcknowledgement().getException(), instanceOf(IllegalArgumentException.class));
	}

	@Test
	public void testErrorDecoratorWithResult() {
		StreamSupplier<Integer> supplier = StreamSupplier.ofStream(IntStream.range(1, 10).boxed());
		IllegalArgumentException exception = new IllegalArgumentException("TestException");

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();
		StreamConsumer<Integer> errorConsumer =
				consumer.transformWith(errorDecorator(k -> k.equals(5) ? exception : null));

		Throwable e = awaitException(supplier.streamTo(errorConsumer));

		assertSame(exception, e);
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
			protected void onError(Throwable e) {
				output.close(e);
			}

		}

		protected final class Output extends AbstractStreamSupplier<T> {
			@Override
			protected void onSuspended() {
				suspended++;
				input.getSupplier().suspend();
			}

			@Override
			protected void onError(Throwable e) {
				input.close(e);
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
				.transformWith(suspendDecorator(
						k -> true,
						context -> Eventloop.getCurrentEventloop().delay(10L, context::resume)
				));

		await(
				supplier.streamTo(transformer.getInput()),
				transformer.getOutput().streamTo(errorConsumer)
		);

		assertEquals(values, consumer.getList());
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testSuspendDecoratorWithResult() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());

		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		CountTransformer<Integer> transformer = new CountTransformer<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.transformWith(transformer)
				.streamTo(consumer
						.transformWith(suspendDecorator(
								item -> true,
								context -> Eventloop.getCurrentEventloop().delay(10L, context::resume))));

		List<Integer> list = await(consumer.getResult());

		assertEquals(values, list);
		assertEquals(5, transformer.getResumed());
		assertEquals(5, transformer.getSuspended());
	}

	@Test
	public void testConsumerWrapper() {
		List<Integer> values = IntStream.range(1, 6).boxed().collect(toList());
		List<Integer> actual = new ArrayList<>();
		StreamSupplier<Integer> supplier = StreamSupplier.ofIterable(values);
		StreamConsumer<Integer> consumer = StreamConsumer.ofChannelConsumer(ChannelConsumer.of(AsyncConsumer.of(actual::add)));
		await(supplier.streamTo(consumer));

		assertEquals(values, actual);
	}
}
