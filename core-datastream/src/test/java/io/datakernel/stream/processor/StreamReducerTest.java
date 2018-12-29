/*
 * Copyright (C) 2015 SoftIndex LLC.
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

package io.datakernel.stream.processor;

import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamDataAcceptor;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.StreamReducers.Reducer;
import io.datakernel.stream.processor.StreamReducers.ReducerToAccumulator;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.stream.TestStreamConsumers.decorator;
import static io.datakernel.stream.TestStreamConsumers.randomlySuspending;
import static io.datakernel.stream.TestUtils.*;
import static io.datakernel.stream.processor.StreamReducers.mergeDistinctReducer;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(DatakernelRunner.class)
public class StreamReducerTest {
	@Test
	public void testEmpty() {
		StreamSupplier<Integer> source = StreamSupplier.of();

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.<Integer, Integer, Void>create(Integer::compareTo)
				.withBufferSize(1);
		Reducer<Integer, Integer, Integer, Void> reducer = mergeDistinctReducer();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(
				source.streamTo(streamReducer.newInput(Function.identity(), reducer)),
				streamReducer.getOutput()
						.streamTo(consumer
								.transformWith(randomlySuspending()))
		);

		assertEquals(emptyList(), consumer.getList());
		assertEndOfStream(source);
		assertEndOfStream(streamReducer.getOutput());
		assertConsumersEndOfStream(streamReducer.getInputs());
	}

	@Test
	public void testDeduplicate() {
		StreamSupplier<Integer> source0 = StreamSupplier.of();
		StreamSupplier<Integer> source1 = StreamSupplier.of(7);
		StreamSupplier<Integer> source2 = StreamSupplier.of(3, 4, 6);
		StreamSupplier<Integer> source3 = StreamSupplier.of();
		StreamSupplier<Integer> source4 = StreamSupplier.of(2, 3, 5);
		StreamSupplier<Integer> source5 = StreamSupplier.of(1, 3);
		StreamSupplier<Integer> source6 = StreamSupplier.of(1, 3);
		StreamSupplier<Integer> source7 = StreamSupplier.of();

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.<Integer, Integer, Void>create(Integer::compareTo)
				.withBufferSize(1);
		Function<Integer, Integer> keyFunction = Function.identity();
		Reducer<Integer, Integer, Integer, Void> reducer = mergeDistinctReducer();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		await(
				source0.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source1.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source2.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source3.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source4.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source5.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source6.streamTo(streamReducer.newInput(keyFunction, reducer)),
				source7.streamTo(streamReducer.newInput(keyFunction, reducer)),

				streamReducer.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer.getList());
		assertEndOfStream(source0);
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
		assertEndOfStream(source4);
		assertEndOfStream(source5);
		assertEndOfStream(source6);
		assertEndOfStream(source7);

		assertEndOfStream(streamReducer.getOutput());
		assertConsumersEndOfStream(streamReducer.getInputs());
	}

	@Test
	public void testWithError() {
		StreamSupplier<KeyValue1> source1 = StreamSupplier.of(
				new KeyValue1(1, 10.0),
				new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSupplier.of(
				new KeyValue2(1, 10.0),
				new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSupplier.of(
				new KeyValue3(2, 10.0, 20.0),
				new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>create(Integer::compareTo)
				.withBufferSize(1);

		List<KeyValueResult> list = new ArrayList<>();
		StreamConsumerToList<KeyValueResult> consumer = StreamConsumerToList.create(list);
		ExpectedException exception = new ExpectedException("Test Exception");

		Throwable e = awaitException(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput()
						.streamTo(consumer
								.transformWith(decorator((context, dataAcceptor) ->
										item -> {
											list.add(item);
											if (list.size() == 1) {
												context.closeWithError(exception);
											}
										})))
		);
//		assertEquals(1, list.size());

		assertSame(exception, e);
		assertClosedWithError(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);

		assertClosedWithError(streamReducer.getOutput());
		assertClosedWithError(streamReducer.getInput(0));
		assertClosedWithError(streamReducer.getInput(1));
		assertClosedWithError(streamReducer.getInput(2));
	}

	@Test
	public void testSupplierDisconnectWithError() {
		StreamSupplier<KeyValue1> source1 = StreamSupplier.of(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));

		Exception exception = new Exception("Test Exception");
		StreamSupplier<KeyValue2> source2 = StreamSupplier.closingWithError(exception);

		StreamSupplier<KeyValue3> source3 = StreamSupplier.of(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.create(Integer::compareTo);
		streamReducer.withBufferSize(1);

		List<KeyValueResult> list = new ArrayList<>();
		StreamConsumerToList<KeyValueResult> consumer = StreamConsumerToList.create(list);

		Throwable e = awaitException(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput().streamTo(consumer)
		);

		assertSame(exception, e);
		assertEquals(0, list.size());
		assertClosedWithError(consumer);
		assertEndOfStream(source1);
		assertClosedWithError(source2);
		assertEndOfStream(source3);
	}

	private static final class KeyValue1 {
		public int key;
		public double metric1;

		private KeyValue1(int key, double metric1) {
			this.key = key;
			this.metric1 = metric1;
		}

		public static final ReducerToAccumulator<Integer, KeyValue1, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<Integer, KeyValue1, KeyValueResult>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue1 value) {
				accumulator.metric1 += value.metric1;
				return accumulator;
			}
		};

		public static Reducer<Integer, KeyValue1, KeyValueResult, KeyValueResult> REDUCER = new Reducer<Integer, KeyValue1, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue1 firstValue) {
				return new KeyValueResult(key, firstValue.metric1, 0.0, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue1 nextValue, KeyValueResult accumulator) {
				accumulator.metric1 += nextValue.metric1;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}

		};
	}

	private static final class KeyValue2 {
		public int key;
		public double metric2;

		private KeyValue2(int key, double metric2) {
			this.key = key;
			this.metric2 = metric2;
		}

		public static final ReducerToAccumulator<Integer, KeyValue2, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<Integer, KeyValue2, KeyValueResult>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue2 value) {
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};

		public static Reducer<Integer, KeyValue2, KeyValueResult, KeyValueResult> REDUCER = new Reducer<Integer, KeyValue2, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue2 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue2 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}
		};
	}

	private static final class KeyValue3 {
		public int key;
		public double metric2;
		public double metric3;

		private KeyValue3(int key, double metric2, double metric3) {
			this.key = key;
			this.metric2 = metric2;
			this.metric3 = metric3;
		}

		public static final ReducerToAccumulator<Integer, KeyValue3, KeyValueResult> REDUCER_TO_ACCUMULATOR = new ReducerToAccumulator<Integer, KeyValue3, KeyValueResult>() {
			@Override
			public KeyValueResult createAccumulator(Integer key) {
				return new KeyValueResult(key, 0.0, 0.0, 0.0);
			}

			@Override
			public KeyValueResult accumulate(KeyValueResult accumulator, KeyValue3 value) {
				accumulator.metric2 += value.metric2;
				accumulator.metric3 += value.metric3;
				return accumulator;
			}
		};

		public static Reducer<Integer, KeyValue3, KeyValueResult, KeyValueResult> REDUCER = new Reducer<Integer, KeyValue3, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue3 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, firstValue.metric3);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValue3 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				accumulator.metric3 += nextValue.metric3;

				return accumulator;
			}

			@Override
			public void onComplete(StreamDataAcceptor<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.accept(accumulator);
			}
		};
	}

	private static final class KeyValueResult {
		public int key;
		public double metric1;
		public double metric2;
		public double metric3;

		KeyValueResult(int key, double metric1, double metric2, double metric3) {
			this.key = key;
			this.metric1 = metric1;
			this.metric2 = metric2;
			this.metric3 = metric3;
		}

		@Override
		public boolean equals(Object o) {
			KeyValueResult that = (KeyValueResult) o;

			if (key != that.key) return false;
			if (Double.compare(that.metric1, metric1) != 0) return false;
			if (Double.compare(that.metric2, metric2) != 0) return false;
			if (Double.compare(that.metric3, metric3) != 0) return false;

			return true;
		}

		@Override
		public String toString() {
			return "KeyValueResult{" +
					"key=" + key +
					", metric1=" + metric1 +
					", metric2=" + metric2 +
					", metric3=" + metric3 +
					'}';
		}
	}

	@Test
	public void test2() {
		StreamSupplier<KeyValue1> source1 = StreamSupplier.of(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSupplier.of(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSupplier.of(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>create(Integer::compareTo)
				.withBufferSize(1);

		StreamConsumerToList<KeyValueResult> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER_TO_ACCUMULATOR.inputToOutput())),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER_TO_ACCUMULATOR.inputToOutput())),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER_TO_ACCUMULATOR.inputToOutput())),

				streamReducer.getOutput().streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(asList(
				new KeyValueResult(1, 10.0, 10.0, 0.0),
				new KeyValueResult(2, 0.0, 10.0, 20.0),
				new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
	}

	@Test
	public void test3() {
		StreamSupplier<KeyValue1> source1 = StreamSupplier.of(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0));
		StreamSupplier<KeyValue2> source2 = StreamSupplier.of(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0));
		StreamSupplier<KeyValue3> source3 = StreamSupplier.of(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.<Integer, KeyValueResult, KeyValueResult>create(Integer::compareTo)
				.withBufferSize(1);

		StreamConsumerToList<KeyValueResult> consumer = StreamConsumerToList.create();

		await(
				source1.streamTo(streamReducer.newInput(input -> input.key, KeyValue1.REDUCER)),
				source2.streamTo(streamReducer.newInput(input -> input.key, KeyValue2.REDUCER)),
				source3.streamTo(streamReducer.newInput(input -> input.key, KeyValue3.REDUCER)),

				streamReducer.getOutput()
						.streamTo(consumer.transformWith(randomlySuspending()))
		);

		assertEquals(asList(
				new KeyValueResult(1, 10.0, 10.0, 0.0),
				new KeyValueResult(2, 0.0, 10.0, 20.0),
				new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEndOfStream(source1);
		assertEndOfStream(source2);
		assertEndOfStream(source3);
	}

}
