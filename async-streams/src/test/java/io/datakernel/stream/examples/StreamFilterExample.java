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

package io.datakernel.stream.examples;

import com.google.common.base.Predicate;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.stream.processor.StreamFilter;

import static io.datakernel.stream.StreamProducers.ofIterable;
import static io.datakernel.stream.TestStreamConsumers.TestConsumerToList;
import static java.util.Arrays.asList;

/**
 * Example 1.
 * Example of working with StreamFilter.
 */
public class StreamFilterExample {
	public static void main(String[] args) {
		Eventloop eventloop = new Eventloop();

		StreamProducer<Integer> source = ofIterable(eventloop, asList(1, 2, 3));

		/* Predicate, which returns true when applied to even integers. */
		Predicate<Integer> predicate = new Predicate<Integer>() {
			@Override
			public boolean apply(Integer input) {
				return input % 2 == 1;
			}
		};
		StreamFilter<Integer> filter = new StreamFilter<>(eventloop, predicate);

		TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(filter.getInput());
		filter.getOutput().streamTo(consumer);

		eventloop.run();

		System.out.println(consumer.getList());
	}
}
