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

import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.StreamBinaryDeserializer;
import io.datakernel.stream.processor.StreamBinarySerializer;

import java.util.ArrayList;

import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;

public class StreamSerializerBenchmark implements Runnable {

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

	public StreamSerializerBenchmark(int benchmarkRounds) {
		this.benchmarkRounds = benchmarkRounds;
	}

	@SuppressWarnings("unchecked")
	private void setUp(Eventloop eventloop) {
		ArrayList list = new ArrayList();
		for (int i = 0; i < 1000000; i++) {
			list.add(i);

		}
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, list);
		StreamBinarySerializer<Integer> serializerStream = StreamBinarySerializer.create(eventloop, intSerializer(), 1, StreamBinarySerializer.MAX_SIZE, 0, false);
		StreamBinaryDeserializer<Integer> deserializerStream = StreamBinaryDeserializer.create(eventloop, intSerializer(), 12);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop);

		source.streamTo(serializerStream.getInput());
		serializerStream.getOutput().streamTo(deserializerStream.getInput());
		deserializerStream.getOutput().streamTo(consumer);
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
		StreamSerializerBenchmark benchmark = new StreamSerializerBenchmark(10);
		benchmark.run();
	}
}
