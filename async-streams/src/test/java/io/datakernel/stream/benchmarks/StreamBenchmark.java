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

package io.datakernel.stream.benchmarks;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.processor.*;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;

@Warmup(iterations = 4)
@Measurement(iterations = 8)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
@Fork(2)
@Threads(1)
public class StreamBenchmark {

	@Param({"1", "10", "10000000"})
	private int suspendInterval;

	private static final int sequenceLength = 1_000_000;

	@Benchmark
	public void baseline() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void simpleFilter() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return true;
			}
		};
		StreamFilter<Integer> filter = new StreamFilter<>(eventloop, predicate);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(filter);
		filter.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void simpleFilterSequence() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return true;
			}
		};
		StreamFilter<Integer> filter1 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter2 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter3 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter4 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter5 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter6 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter7 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter8 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter9 = new StreamFilter<>(eventloop, predicate);
		StreamFilter<Integer> filter10 = new StreamFilter<>(eventloop, predicate);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(filter1);
		filter1.streamTo(filter2);
		filter2.streamTo(filter3);
		filter3.streamTo(filter4);
		filter4.streamTo(filter5);
		filter5.streamTo(filter6);
		filter6.streamTo(filter7);
		filter7.streamTo(filter8);
		filter8.streamTo(filter9);
		filter9.streamTo(filter10);
		filter10.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void simpleOddFilter() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return input % 2 == 1;
			}
		};
		StreamFilter<Integer> filter = new StreamFilter<>(eventloop, predicate);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(filter);
		filter.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void testForwarder() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		StreamForwarder<Integer> forwarder = new StreamForwarder<>(eventloop);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(forwarder);
		forwarder.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void simpleTransformer() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		StreamFunction<Integer, Integer> transformer = new StreamFunction<>(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input;
			}
		});

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(transformer);
		transformer.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void transformerAndFilter() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return true;
			}
		};
		StreamFilter<Integer> filter = new StreamFilter<>(eventloop, predicate);

		StreamFunction<Integer, Integer> transformer = new StreamFunction<>(eventloop, new Function<Integer, Integer>() {
			@Override
			public Integer apply(Integer input) {
				return input;
			}
		});

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);
		generator.streamTo(filter);
		filter.streamTo(transformer);
		transformer.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void streamSorter() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		StreamMergeSorterStorage<Integer> storage = new StreamMergeSorterStorageStub<>(eventloop);
		StreamSorter<Integer, Integer> sorter = new StreamSorter<>(eventloop,
				storage, Functions.<Integer>identity(), Ordering.<Integer>natural(), true, 1000);
		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(sorter);
		sorter.getSortedStream().streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void streamSplitter() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);
		StreamSplitter<Integer> streamConcat = new StreamSplitter<>(eventloop);

		SuspendConsumer consumer1 = new SuspendConsumer(eventloop, suspendInterval);
		SuspendConsumer consumer2 = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(streamConcat);
		streamConcat.newOutput().streamTo(consumer1);
		streamConcat.newOutput().streamTo(consumer2);
		eventloop.run();
	}

	@Benchmark
	public void streamSplitUnion() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);
		StreamSplitter<Integer> streamConcat = new StreamSplitter<>(eventloop);

		StreamUnion<Integer> streamUnion = new StreamUnion<>(eventloop);
		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(streamConcat);
		streamConcat.newOutput().streamTo(streamUnion.newInput());
		streamConcat.newOutput().streamTo(streamUnion.newInput());
		streamUnion.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void streamMap() {
		NioEventloop eventloop = new NioEventloop();

		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);

		StreamMap.MapperProjection<Integer, KeyValueWrapper> functionToKeyValue = new StreamMap.MapperProjection<Integer, KeyValueWrapper>() {
			@Override
			protected KeyValueWrapper apply(Integer input) {
				return new KeyValueWrapper(input, input);
			}
		};

		StreamMap.MapperProjection<KeyValueWrapper, Integer> functionFromKeyValue = new StreamMap.MapperProjection<KeyValueWrapper, Integer>() {
			@Override
			protected Integer apply(KeyValueWrapper input) {
				return input.value;
			}
		};

		StreamMap<Integer, KeyValueWrapper> toKeyValueMap = new StreamMap<>(eventloop, functionToKeyValue);
		StreamMap<KeyValueWrapper, Integer> fromKeyValueMap = new StreamMap<>(eventloop, functionFromKeyValue);

		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(toKeyValueMap);
		toKeyValueMap.streamTo(fromKeyValueMap);
		fromKeyValueMap.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void reducers() {
		NioEventloop eventloop = new NioEventloop();
		SequenceGenerator generator = new SequenceGenerator(eventloop, sequenceLength);
		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		StreamMap.MapperProjection<Integer, KeyValueWrapper> functionToKeyValue = new StreamMap.MapperProjection<Integer, KeyValueWrapper>() {
			@Override
			protected KeyValueWrapper apply(Integer input) {
				return new KeyValueWrapper(input, input);
			}
		};

		StreamMap<Integer, KeyValueWrapper> toKeyValueMap = new StreamMap<>(eventloop, functionToKeyValue);

		StreamReducer<Integer, KeyValueWrapper, KeyValueWrapper> streamReducer = new StreamReducer<>(eventloop, Ordering.<Integer>natural());
		StreamConsumer<KeyValueWrapper> streamConsumer = streamReducer.newInput(new Function<KeyValueWrapper, Integer>() {
			@Override
			public Integer apply(KeyValueWrapper input) {
				return input.key;
			}
		}, new StreamReducers.Reducer<Integer, KeyValueWrapper, KeyValueWrapper, KeyValueWrapper>() {
			@Override
			public KeyValueWrapper onFirstItem(StreamDataReceiver<KeyValueWrapper> stream, Integer key, KeyValueWrapper firstValue) {
				return firstValue;
			}

			@Override
			public KeyValueWrapper onNextItem(StreamDataReceiver<KeyValueWrapper> stream, Integer key, KeyValueWrapper nextValue, KeyValueWrapper accumulator) {
				accumulator.value += nextValue.value;
				return accumulator;
			}

			@Override
			public void onComplete(StreamDataReceiver<KeyValueWrapper> stream, Integer key, KeyValueWrapper accumulator) {
				stream.onData(accumulator);
			}
		});

		StreamMap.MapperProjection<KeyValueWrapper, Integer> functionFromKeyValue = new StreamMap.MapperProjection<KeyValueWrapper, Integer>() {
			@Override
			protected Integer apply(KeyValueWrapper input) {
				return input.value;
			}
		};
		StreamMap<KeyValueWrapper, Integer> fromKeyValueMap = new StreamMap<>(eventloop, functionFromKeyValue);

		generator.streamTo(toKeyValueMap);
		toKeyValueMap.streamTo(streamConsumer);

		streamReducer.streamTo(fromKeyValueMap);
		fromKeyValueMap.streamTo(consumer);

		eventloop.run();
	}

	@Benchmark
	public void serializers() {
		NioEventloop eventloop = new NioEventloop();
		StreamBenchmark.SequenceGenerator generator = new StreamBenchmark.SequenceGenerator(eventloop, sequenceLength);

		StreamBinarySerializer<Integer> serializerStream = new StreamBinarySerializer<>(eventloop, intSerializer(), 1024, StreamBinarySerializer.MAX_SIZE, 0, false);
		StreamBinaryDeserializer<Integer> deserializerStream = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 12);
		SuspendConsumer consumer = new SuspendConsumer(eventloop, suspendInterval);

		generator.streamTo(serializerStream);
		serializerStream.streamTo(deserializerStream);
		deserializerStream.streamTo(consumer);

		eventloop.run();
	}

	public static class KeyValueWrapper {
		final int key;
		int value;

		public KeyValueWrapper(int key, int value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			KeyValueWrapper that = (KeyValueWrapper) o;

			if (key != that.key) return false;
			return value == that.value;

		}

		@Override
		public int hashCode() {
			int result = key;
			result = 31 * result + value;
			return result;
		}
	}

	public static class SequenceGenerator extends AbstractStreamProducer<Integer> {

		private final int maxValue;
		private int currentValue;

		protected SequenceGenerator(Eventloop eventloop, int maxValue) {
			super(eventloop);
			this.maxValue = maxValue;
		}

		@Override
		protected void doProduce() {

			while (currentValue < maxValue) {
				if (status != READY) {
					return;
				}

				send(currentValue);
				currentValue++;
			}

			sendEndOfStream();
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@Override
		protected void onSuspended() {

		}

//		@Override
//		protected void onProducerStarted() {
//			produce();
//		}

		@Override
		protected void onResumed() {
			resumeProduce();
		}

		@Override
		protected void onError(Exception e) {

		}
	}

	public static class SuspendConsumer extends AbstractStreamConsumer<Integer> implements StreamDataReceiver<Integer> {

		private final int suspendPeriod;
		private int receivedValues = 0;

		public SuspendConsumer(Eventloop eventloop, int suspendPeriod) {
			super(eventloop);
			this.suspendPeriod = suspendPeriod;
		}

		@Override
		public StreamDataReceiver<Integer> getDataReceiver() {
			return this;
		}

		@Override
		protected void onStarted() {

		}

//		@Override
//		public void onProducerEndOfStream() {
////			upstreamProducer.close();
//			close();
//		}

		@Override
		protected void onEndOfStream() {
			close();
		}

		@Override
		protected void onError(Exception e) {

		}

		@Override
		public void onData(Integer item) {
			if (receivedValues == suspendPeriod) {
				receivedValues = 0;
//				suspendUpstream();
				suspend();
				this.eventloop.post(new Runnable() {
					@Override
					public void run() {
//						resumeUpstream();
						resume();
					}
				});

			}
			receivedValues++;
		}
	}

}
