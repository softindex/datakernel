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

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Ordering;
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.StreamReducers.mergeDeduplicateReducer;
import static io.datakernel.stream.processor.Utils.assertConsumerStatuses;
import static io.datakernel.stream.processor.Utils.consumerStatuses;
import static java.util.Arrays.asList;
import static java.util.Collections.EMPTY_LIST;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class StreamReducerTest {
	@Test
	public void testEmpty() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);
		Function<Integer, Integer> keyFunction = Functions.identity();
		StreamReducers.Reducer<Integer, Integer, Integer, Void> reducer = mergeDeduplicateReducer();

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamReducer.newInput(keyFunction, reducer));
		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(EMPTY_LIST, consumer.getList());
		assertEquals(END_OF_STREAM, source.getProducerStatus());
		assertEquals(END_OF_STREAM, streamReducer.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, streamReducer.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testDeduplicate() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, asList(2, 3, 5));
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, asList(1, 3));
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, asList(1, 3));
		StreamProducer<Integer> source7 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);
		Function<Integer, Integer> keyFunction = Functions.identity();
		StreamReducers.Reducer<Integer, Integer, Integer, Void> reducer = mergeDeduplicateReducer();

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(streamReducer.newInput(keyFunction, reducer));
		source1.streamTo(streamReducer.newInput(keyFunction, reducer));
		source2.streamTo(streamReducer.newInput(keyFunction, reducer));
		source3.streamTo(streamReducer.newInput(keyFunction, reducer));
		source4.streamTo(streamReducer.newInput(keyFunction, reducer));
		source5.streamTo(streamReducer.newInput(keyFunction, reducer));
		source6.streamTo(streamReducer.newInput(keyFunction, reducer));
		source7.streamTo(streamReducer.newInput(keyFunction, reducer));
		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer.getList());
		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertEquals(END_OF_STREAM, source4.getProducerStatus());
		assertEquals(END_OF_STREAM, source5.getProducerStatus());
		assertEquals(END_OF_STREAM, source6.getProducerStatus());
		assertEquals(END_OF_STREAM, source7.getProducerStatus());

		assertEquals(END_OF_STREAM, streamReducer.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, streamReducer.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<KeyValue1> source1 = StreamProducers.ofIterable(eventloop, asList(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0)));
		StreamProducer<KeyValue2> source2 = StreamProducers.ofIterable(eventloop, asList(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0)));
		StreamProducer<KeyValue3> source3 = StreamProducers.ofIterable(eventloop, asList(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0)));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);

		final List<KeyValueResult> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<KeyValueResult> consumer = new TestStreamConsumers.TestConsumerToList<KeyValueResult>(eventloop, list) {
			@Override
			public void onData(KeyValueResult item) {
				list.add(item);
				if (list.size() == 1) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				upstreamProducer.onConsumerSuspended();
				eventloop.post(new Runnable() {
					@Override
					public void run() {
						upstreamProducer.onConsumerResumed();
					}
				});
			}
		};

		StreamConsumer<KeyValue1> streamConsumer1 = streamReducer.newInput(KeyValue1.keyFunction, KeyValue1.REDUCER);
		source1.streamTo(streamConsumer1);

		StreamConsumer<KeyValue2> streamConsumer2 = streamReducer.newInput(KeyValue2.keyFunction, KeyValue2.REDUCER);
		source2.streamTo(streamConsumer2);

		StreamConsumer<KeyValue3> streamConsumer3 = streamReducer.newInput(KeyValue3.keyFunction, KeyValue3.REDUCER);
		source3.streamTo(streamConsumer3);

		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 1);

		assertEquals(CLOSED_WITH_ERROR, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());

		assertEquals(CLOSED_WITH_ERROR, streamReducer.getOutput().getProducerStatus());
		assertArrayEquals(new StreamStatus[]{CLOSED_WITH_ERROR, END_OF_STREAM, END_OF_STREAM},
				consumerStatuses(streamReducer.getInputs()));
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testProducerDisconnectWithError() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<KeyValue1> source1 = StreamProducers.ofIterable(eventloop,
				asList(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0)));

		StreamProducer<KeyValue2> source2 = StreamProducers.closingWithError(eventloop, new Exception("Test Exception"));

		StreamProducer<KeyValue3> source3 = StreamProducers.ofIterable(eventloop,
				asList(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0)));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);

		List<KeyValueResult> list = new ArrayList<>();
		StreamConsumers.ToList<KeyValueResult> consumer = new StreamConsumers.ToList<>(eventloop, list);

		StreamConsumer<KeyValue1> streamConsumer1 = streamReducer.newInput(KeyValue1.keyFunction, KeyValue1.REDUCER);
		source1.streamTo(streamConsumer1);

		StreamConsumer<KeyValue2> streamConsumer2 = streamReducer.newInput(KeyValue2.keyFunction, KeyValue2.REDUCER);
		source2.streamTo(streamConsumer2);

		StreamConsumer<KeyValue3> streamConsumer3 = streamReducer.newInput(KeyValue3.keyFunction, KeyValue3.REDUCER);
		source3.streamTo(streamConsumer3);

		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();
		assertTrue(list.size() == 0);
		assertEquals(CLOSED_WITH_ERROR, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	private static final class KeyValue1 {
		public int key;
		public double metric1;

		private KeyValue1(int key, double metric1) {
			this.key = key;
			this.metric1 = metric1;
		}

		public static final Function<KeyValue1, Integer> keyFunction = new Function<KeyValue1, Integer>() {
			@Override
			public Integer apply(KeyValue1 input) {
				return input.key;
			}
		};

		public static final StreamReducers.ReducerToAccumulator<Integer, KeyValue1, KeyValueResult> REDUCER_TO_ACCUMULATOR = new StreamReducers.ReducerToAccumulator<Integer, KeyValue1, KeyValueResult>() {
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

		public static StreamReducers.Reducer<Integer, KeyValue1, KeyValueResult, KeyValueResult> REDUCER = new StreamReducers.Reducer<Integer, KeyValue1, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue1 firstValue) {
				return new KeyValueResult(key, firstValue.metric1, 0.0, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue1 nextValue, KeyValueResult accumulator) {
				accumulator.metric1 += nextValue.metric1;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.onData(accumulator);
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

		public static final Function<KeyValue2, Integer> keyFunction = new Function<KeyValue2, Integer>() {
			@Override
			public Integer apply(KeyValue2 input) {
				return input.key;
			}
		};

		public static final StreamReducers.ReducerToAccumulator<Integer, KeyValue2, KeyValueResult> REDUCER_TO_ACCUMULATOR = new StreamReducers.ReducerToAccumulator<Integer, KeyValue2, KeyValueResult>() {
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

		public static StreamReducers.Reducer<Integer, KeyValue2, KeyValueResult, KeyValueResult> REDUCER = new StreamReducers.Reducer<Integer, KeyValue2, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue2 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, 0.0);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue2 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.onData(accumulator);
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

		public static final Function<KeyValue3, Integer> keyFunction = new Function<KeyValue3, Integer>() {
			@Override
			public Integer apply(KeyValue3 input) {
				return input.key;
			}
		};

		public static final StreamReducers.ReducerToAccumulator<Integer, KeyValue3, KeyValueResult> REDUCER_TO_ACCUMULATOR = new StreamReducers.ReducerToAccumulator<Integer, KeyValue3, KeyValueResult>() {
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

		public static StreamReducers.Reducer<Integer, KeyValue3, KeyValueResult, KeyValueResult> REDUCER = new StreamReducers.Reducer<Integer, KeyValue3, KeyValueResult, KeyValueResult>() {
			@Override
			public KeyValueResult onFirstItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue3 firstValue) {
				return new KeyValueResult(key, 0.0, firstValue.metric2, firstValue.metric3);
			}

			@Override
			public KeyValueResult onNextItem(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValue3 nextValue, KeyValueResult accumulator) {
				accumulator.metric2 += nextValue.metric2;
				accumulator.metric3 += nextValue.metric3;

				return accumulator;
			}

			@Override
			public void onComplete(StreamDataReceiver<KeyValueResult> stream, Integer key, KeyValueResult accumulator) {
				stream.onData(accumulator);
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
			return MoreObjects.toStringHelper(this)
					.add("key", key)
					.add("metric1", metric1)
					.add("metric2", metric2)
					.add("metric3", metric3)
					.toString();
		}
	}

	@Test
	public void test2() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<KeyValue1> source1 = StreamProducers.ofIterable(eventloop, asList(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0)));
		StreamProducer<KeyValue2> source2 = StreamProducers.ofIterable(eventloop, asList(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0)));
		StreamProducer<KeyValue3> source3 = StreamProducers.ofIterable(eventloop, asList(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0)));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);

		TestStreamConsumers.TestConsumerToList<KeyValueResult> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		StreamConsumer<KeyValue1> streamConsumer1 = streamReducer.newInput(KeyValue1.keyFunction, KeyValue1.REDUCER_TO_ACCUMULATOR.inputToOutput());
		source1.streamTo(streamConsumer1);

		StreamConsumer<KeyValue2> streamConsumer2 = streamReducer.newInput(KeyValue2.keyFunction, KeyValue2.REDUCER_TO_ACCUMULATOR.inputToOutput());
		source2.streamTo(streamConsumer2);

		StreamConsumer<KeyValue3> streamConsumer3 = streamReducer.newInput(KeyValue3.keyFunction, KeyValue3.REDUCER_TO_ACCUMULATOR.inputToOutput());
		source3.streamTo(streamConsumer3);

		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(
						new KeyValueResult(1, 10.0, 10.0, 0.0),
						new KeyValueResult(2, 0.0, 10.0, 20.0),
						new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void test3() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<KeyValue1> source1 = StreamProducers.ofIterable(eventloop, asList(new KeyValue1(1, 10.0), new KeyValue1(3, 30.0)));
		StreamProducer<KeyValue2> source2 = StreamProducers.ofIterable(eventloop, asList(new KeyValue2(1, 10.0), new KeyValue2(3, 30.0)));
		StreamProducer<KeyValue3> source3 = StreamProducers.ofIterable(eventloop, asList(new KeyValue3(2, 10.0, 20.0), new KeyValue3(3, 10.0, 20.0)));

		StreamReducer<Integer, KeyValueResult, KeyValueResult> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);

		TestStreamConsumers.TestConsumerToList<KeyValueResult> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		StreamConsumer<KeyValue1> streamConsumer1 = streamReducer.newInput(KeyValue1.keyFunction, KeyValue1.REDUCER);
		source1.streamTo(streamConsumer1);

		StreamConsumer<KeyValue2> streamConsumer2 = streamReducer.newInput(KeyValue2.keyFunction, KeyValue2.REDUCER);
		source2.streamTo(streamConsumer2);

		StreamConsumer<KeyValue3> streamConsumer3 = streamReducer.newInput(KeyValue3.keyFunction, KeyValue3.REDUCER);
		source3.streamTo(streamConsumer3);

		streamReducer.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(
						new KeyValueResult(1, 10.0, 10.0, 0.0),
						new KeyValueResult(2, 0.0, 10.0, 20.0),
						new KeyValueResult(3, 30.0, 40.0, 20.0)),
				consumer.getList());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(3, 4, 6));
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, asList(2, 3, 5));
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, asList(1, 3));
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, asList(1, 3));
		StreamProducer<Integer> source7 = StreamProducers.ofIterable(eventloop, EMPTY_LIST);

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);
		Function<Integer, Integer> keyFunction = Functions.identity();
		StreamReducers.Reducer<Integer, Integer, Integer, Void> reducer = mergeDeduplicateReducer();

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source0.streamTo(streamReducer.newInput(keyFunction, reducer));
		source1.streamTo(streamReducer.newInput(keyFunction, reducer));
		source2.streamTo(streamReducer.newInput(keyFunction, reducer));
		source3.streamTo(streamReducer.newInput(keyFunction, reducer));
		source4.streamTo(streamReducer.newInput(keyFunction, reducer));
		source5.streamTo(streamReducer.newInput(keyFunction, reducer));
		source6.streamTo(streamReducer.newInput(keyFunction, reducer));
		source7.streamTo(streamReducer.newInput(keyFunction, reducer));
		eventloop.run();

		streamReducer.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer.getList());
		assertEquals(END_OF_STREAM, source0.getProducerStatus());
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertEquals(END_OF_STREAM, source3.getProducerStatus());
		assertEquals(END_OF_STREAM, source4.getProducerStatus());
		assertEquals(END_OF_STREAM, source5.getProducerStatus());
		assertEquals(END_OF_STREAM, source6.getProducerStatus());
		assertEquals(END_OF_STREAM, source7.getProducerStatus());

		assertEquals(END_OF_STREAM, streamReducer.getOutput().getProducerStatus());
		assertConsumerStatuses(END_OF_STREAM, streamReducer.getInputs());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutProducer() {
		Eventloop eventloop = Eventloop.create();

		StreamReducer<Integer, Integer, Void> streamReducer = StreamReducer.create(eventloop, Ordering.<Integer>natural(), 1);
		CheckCallCallback checkCallCallback = new CheckCallCallback();
		StreamConsumers.ToList<Integer> toList = StreamConsumers.toList(eventloop);
		toList.setCompletionCallback(checkCallCallback);

		streamReducer.getOutput().streamTo(toList);
		eventloop.run();

		assertTrue(checkCallCallback.isCall());
		assertThat(eventloop, doesntHaveFatals());
	}

	class CheckCallCallback implements CompletionCallback {
		private int onComplete;
		private int onException;

		@Override
		public void onComplete() {
			onComplete++;
		}

		@Override
		public void onException(Exception exception) {
			onException++;
		}

		public int getOnComplete() {
			return onComplete;
		}

		public int getOnException() {
			return onException;
		}

		public boolean isCall() {
			return (onComplete == 1 && onException == 0) || (onComplete == 0 && onException == 1);
		}
	}
}
