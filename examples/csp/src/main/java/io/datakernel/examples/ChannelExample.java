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

import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.eventloop.Eventloop;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

public class ChannelExample {
	private static void supplierOfValues() {
		ChannelSupplier.of("1", "2", "3", "4", "5")
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void supplierOfList(List<String> list) {
		ChannelSupplier.ofIterable(list)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void map() {
		ChannelSupplier.of(1, 2, 3, 4, 5)
				.map(integer -> integer + " times 10 = " + integer * 10)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	private static void toCollector() {
		ChannelSupplier.of(1, 2, 3, 4, 5)
				.toCollector(Collectors.toList())
				.whenResult(System.out::println);
	}

	private static void filter() {
		ChannelSupplier.of(1, 2, 3, 4, 5, 6)
				.filter(integer -> integer % 2 == 0)
				.streamTo(ChannelConsumer.ofConsumer(System.out::println));
	}

	public static void main(String[] args) {
		Eventloop eventloop = Eventloop.create().withCurrentThread();
		supplierOfValues();
		supplierOfList(asList("One", "Two", "Three"));
		map();
		toCollector();
		filter();
		eventloop.run();
	}
}
