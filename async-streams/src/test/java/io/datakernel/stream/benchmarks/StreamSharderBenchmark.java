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

import com.google.common.base.Functions;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.Sharder;
import io.datakernel.stream.processor.StreamSharder;

import java.util.ArrayList;

public class StreamSharderBenchmark implements Runnable {
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

	public StreamSharderBenchmark(int benchmarkRounds) {
		this.benchmarkRounds = benchmarkRounds;
	}

	@SuppressWarnings("unchecked")
	private void setUp(Eventloop eventloop) {
		final Sharder<Integer> SHARDER = new Sharder<Integer>() {
			@Override
			public int shard(Integer object) {
				return object % 5;
			}
		};

		StreamSharder<Integer, Integer> streamSharder = StreamSharder.create(eventloop, SHARDER, Functions.<Integer>identity());
		ArrayList testList = new ArrayList();
		for (int i = 0; i < 10000000; i++) {
			testList.add(i);
		}
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, testList);
		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer3 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer4 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer5 = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(streamSharder.getInput());
		streamSharder.newOutput().streamTo(consumer1);
		streamSharder.newOutput().streamTo(consumer2);
		streamSharder.newOutput().streamTo(consumer3);
		streamSharder.newOutput().streamTo(consumer4);
		streamSharder.newOutput().streamTo(consumer5);

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
		StreamSharderBenchmark benchmark = new StreamSharderBenchmark(10);
		benchmark.run();
	}

}
