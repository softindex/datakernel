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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class StreamMemoryReducerTest {

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

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
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = new StreamMemoryReducer<>(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		StreamConsumers.ToList<DataItemResult> consumer = StreamConsumers.toListOneByOne(eventloop);

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		sorter.streamTo(consumer);

		eventloop.run();

		System.out.println(consumer.getList());

		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);

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
	}

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void testWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();
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
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = new StreamMemoryReducer<>(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		StreamConsumers.ToList<DataItemResult> consumer = new StreamConsumers.ToList<DataItemResult>(eventloop, list) {
			@Override
			public void onData(DataItemResult item) {
				super.onData(item);
				if (item.equals(new DataItemResult(1, 2, 60, 90, 0))) {
					onProducerError(new Exception());
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
		sorter.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 2);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void test() throws Exception {
		NioEventloop eventloop = new NioEventloop();
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
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = new StreamMemoryReducer<>(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		StreamConsumers.ToList<DataItemResult> consumer = new StreamConsumers.ToList<DataItemResult>(eventloop, list) {
			@Override
			public void onData(DataItemResult item) {
				super.onData(item);
				if (item.equals(new DataItemResult(1, 2, 60, 90, 0))) {
					onProducerEndOfStream();
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
		sorter.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 2);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED);
	}

	@SuppressWarnings("ToArrayCallWithZeroLengthArrayArgument")
	@Test
	public void testProducerWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<DataItem1> source1 = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 1, 10, 20)),
				StreamProducers.ofValue(eventloop, new DataItem1(1, 2, 20, 30)),
				StreamProducers.<DataItem1>closingWithError(eventloop, new Exception())
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
		StreamMemoryReducer<DataItemKey, DataItem1, DataItemResult, DataItemResult> sorter = new StreamMemoryReducer<>(eventloop,
				reducer,
				new Function<DataItem1, DataItemKey>() {
					@Override
					public DataItemKey apply(DataItem1 input) {
						return new DataItemKey(input.key1, input.key2);
					}
				}
		);

		List<DataItemResult> list = new ArrayList<>();
		StreamConsumers.ToList<DataItemResult> consumer = StreamConsumers.toListOneByOne(eventloop, list);

		source1.streamTo(sorter.newInput());
		source2.streamTo(sorter.newInput());
		sorter.streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 0);
		assertTrue(source1.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
		assertTrue(source2.getStatus() == StreamProducer.CLOSED_WITH_ERROR);
	}

}
