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
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.StreamReducer;
import io.datakernel.stream.processor.StreamReducers;

import java.util.ArrayList;

import static io.datakernel.stream.processor.StreamReducers.mergeDeduplicateReducer;

public class StreamReduceBenchmark implements Runnable {
	private long bestTime;
	private long worstTime;
	private int benchmarkRounds;
	private long avgTime;

	public long getBestTime() {
		return bestTime;
	}

	public long getWorstTime() {
		return worstTime;
	}

	public long getAvgTime() {
		return avgTime;
	}

	public StreamReduceBenchmark(int benchmarkRounds) {
		this.benchmarkRounds = benchmarkRounds;
	}

	@SuppressWarnings("unchecked")
	private void setUp(Eventloop eventloop) {
		ArrayList testList = new ArrayList();
		for (int i = 0; i < 1000000; i++) {
			testList.add(i);
		}

		StreamProducer<Integer> source0 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source3 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source4 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source5 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source6 = StreamProducers.ofIterable(eventloop, testList);
		StreamProducer<Integer> source7 = StreamProducers.ofIterable(eventloop, testList);

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
	}

	@Override
	public void run() {
		System.out.println("Benchmark running...");
		long time = 0;
		this.bestTime = -1;
		this.worstTime = -1;

		Eventloop eventloop = Eventloop.create();

		for (int i = 0; i < this.benchmarkRounds; i++) {
			setUp(eventloop);
			long roundTime = System.currentTimeMillis();
			eventloop.run();
			roundTime = System.currentTimeMillis() - roundTime;
			time += roundTime;
			if (this.bestTime == -1 || roundTime < this.bestTime) {
				this.bestTime = roundTime;
			}
			if (this.worstTime == -1 || roundTime > this.worstTime) {
				this.worstTime = roundTime;
			}
			System.out.println("round:" + i + ", time:" + roundTime);
		}
		avgTime = time / benchmarkRounds;
		System.out.println("best time: " + getBestTime());
		System.out.println("worst time: " + getWorstTime());
		System.out.println("Avg time: " + getAvgTime());
	}

	public static void main(String[] args) {
		StreamReduceBenchmark benchmark = new StreamReduceBenchmark(10);
		benchmark.run();
	}

}
