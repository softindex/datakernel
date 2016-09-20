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
import io.datakernel.async.CompletionCallback;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.consumerStatuses;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public class StreamMemoryReducerTest {

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<DataItem1> source1 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));
		StreamProducer<DataItem1> source2 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));

		StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult> reducer = new StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult>() {
			@Override
			public DataItemResult createAccumulator(DataItemKey key) {
				return new DataItemResult(key.key1, key.key2, 0, 0, 0);
			}

			@Override
			public DataItemResult accumulate(DataItemResult accumulator, DataItem1 value) {
				accumulator.metric1 += value.metric1;
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = StreamMemoryReducer.create(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		TestStreamConsumers.TestConsumerToList<DataItemResult> consumer = TestStreamConsumers.toListOneByOne(eventloop);

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		sorter.getOutput().streamTo(consumer);

		eventloop.run();

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());

		List<DataItemResult> result = consumer.getList();
		Collections.sort(result, new Comparator<DataItemResult>() {
			@Override
			public int compare(DataItemResult o1, DataItemResult o2) {
				int result = Integer.compare(o1.key1, o2.key1);
				if (result != 0)
					return result;
				return Integer.compare(o1.key2, o2.key2);
			}
		});

		assertArrayEquals(new DataItemResult[]{new DataItemResult(1, 1, 30, 60, 0), new DataItemResult(1, 2, 60, 90, 0)},
				result.toArray(new DataItemResult[0]));
		assertThat(eventloop, doesntHaveFatals());
	}

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void testWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();
		List<DataItemResult> list = new ArrayList<>();

		StreamProducer<DataItem1> source1 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));
		StreamProducer<DataItem1> source2 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));

		StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult> reducer = new StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult>() {
			@Override
			public DataItemResult createAccumulator(DataItemKey key) {
				return new DataItemResult(key.key1, key.key2, 0, 0, 0);
			}

			@Override
			public DataItemResult accumulate(DataItemResult accumulator, DataItem1 value) {
				accumulator.metric1 += value.metric1;
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = StreamMemoryReducer.create(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		TestStreamConsumers.TestConsumerToList<DataItemResult> consumer = new TestStreamConsumers.TestConsumerToList<DataItemResult>(eventloop, list) {
			@Override
			public void onData(DataItemResult item) {
				list.add(item);
				if (item.equals(new DataItemResult(1, 2, 60, 90, 0))) {
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

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		sorter.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 2);
		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void testProducerWithError() throws Exception {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<DataItem1> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30)),
				StreamProducers.<DataItem1>closingWithError(eventloop, new Exception("Test Exception"))
		);
		StreamProducer<DataItem1> source2 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30))
		);

		StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult> reducer = new StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult>() {
			@Override
			public DataItemResult createAccumulator(DataItemKey key) {
				return new DataItemResult(key.key1, key.key2, 0, 0, 0);
			}

			@Override
			public DataItemResult accumulate(DataItemResult accumulator, DataItem1 value) {
				accumulator.metric1 += value.metric1;
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = StreamMemoryReducer.create(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		List<DataItemResult> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<DataItemResult> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		sorter.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertArrayEquals(new StreamStatus[]{CLOSED_WITH_ERROR, END_OF_STREAM},
				consumerStatuses(sorter.getInputs()));
		assertEquals(CLOSED_WITH_ERROR, sorter.getOutput().getProducerStatus());
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create();

		StreamProducer<DataItem1> source1 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));
		StreamProducer<DataItem1> source2 = StreamProducers.ofIterable(eventloop, asList(
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30),
				new DataItem1(1, 1, 10, 20),
				new DataItem1(1, 2, 20, 30)));

		StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult> reducer = new StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult>() {
			@Override
			public DataItemResult createAccumulator(DataItemKey key) {
				return new DataItemResult(key.key1, key.key2, 0, 0, 0);
			}

			@Override
			public DataItemResult accumulate(DataItemResult accumulator, DataItem1 value) {
				accumulator.metric1 += value.metric1;
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = StreamMemoryReducer.create(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		TestStreamConsumers.TestConsumerToList<DataItemResult> consumer = TestStreamConsumers.toListOneByOne(eventloop);

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		eventloop.run();

		sorter.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());

		List<DataItemResult> result = consumer.getList();
		Collections.sort(result, new Comparator<DataItemResult>() {
			@Override
			public int compare(DataItemResult o1, DataItemResult o2) {
				int result = Integer.compare(o1.key1, o2.key1);
				if (result != 0)
					return result;
				return Integer.compare(o1.key2, o2.key2);
			}
		});

		assertArrayEquals(new DataItemResult[]{new DataItemResult(1, 1, 30, 60, 0), new DataItemResult(1, 2, 60, 90, 0)},
				result.toArray(new DataItemResult[0]));
		assertThat(eventloop, doesntHaveFatals());
	}

	@Test
	public void testWithoutProducer() {
		Eventloop eventloop = Eventloop.create();

		StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult> reducer = new StreamReducers.ReducerToAccumulator<DataItemKey, DataItem1, DataItemResult>() {
			@Override
			public DataItemResult createAccumulator(DataItemKey key) {
				return new DataItemResult(key.key1, key.key2, 0, 0, 0);
			}

			@Override
			public DataItemResult accumulate(DataItemResult accumulator, DataItem1 value) {
				accumulator.metric1 += value.metric1;
				accumulator.metric2 += value.metric2;
				return accumulator;
			}
		};
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = StreamMemoryReducer.create(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);
		CheckCallCallback checkCallCallback = new CheckCallCallback();
		StreamConsumers.ToList<DataItemResult> toList = StreamConsumers.toList(eventloop);
		toList.setCompletionCallback(checkCallCallback);

		sorter.getOutput().streamTo(toList);
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
