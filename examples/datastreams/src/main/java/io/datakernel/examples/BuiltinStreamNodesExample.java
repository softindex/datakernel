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

package io.datakernel.examples;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.logger.LoggerConfigurer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamSupplier;
import io.datakernel.stream.processor.Sharders.HashSharder;
import io.datakernel.stream.processor.StreamFilter;
import io.datakernel.stream.processor.StreamMapper;
import io.datakernel.stream.processor.StreamSharder;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;

/**
 * Example of some simple builtin stream nodes.
 */
public class BuiltinStreamNodesExample {
	static {
		LoggerConfigurer.enableLogging();
	}

	private static void filter() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		supplier.transformWith(filter).streamTo(consumer);

		consumer.getResult().whenResult(System.out::println);
	}

	private static void sharder() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		StreamSharder<Integer> sharder = StreamSharder.create(new HashSharder<>(3));

		StreamConsumerToList<Integer> first = StreamConsumerToList.create();
		StreamConsumerToList<Integer> second = StreamConsumerToList.create();
		StreamConsumerToList<Integer> third = StreamConsumerToList.create();

		sharder.newOutput().streamTo(first);
		sharder.newOutput().streamTo(second);
		sharder.newOutput().streamTo(third);

		supplier.streamTo(sharder.getInput());

		first.getResult().whenResult(x -> System.out.println("first: " + x));
		second.getResult().whenResult(x -> System.out.println("second: " + x));
		third.getResult().whenResult(x -> System.out.println("third: " + x));
	}

	private static void mapper() {
		StreamSupplier<Integer> supplier = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

		StreamMapper<Integer, String> simpleMap = StreamMapper.create(x -> x + " times ten = " + x * 10);

		StreamConsumerToList<String> consumer = StreamConsumerToList.create();

		supplier.transformWith(simpleMap).streamTo(consumer);

		consumer.getResult().whenResult(System.out::println);
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());

		filter();
		sharder();
		mapper();

		eventloop.run();
	}
}
