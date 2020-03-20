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

package io.datakernel.etl;

import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamConsumerWithResult;
import io.datakernel.datastream.StreamDataAcceptor;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.EventloopRule;
import org.jetbrains.annotations.NotNull;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.datakernel.promise.TestUtils.await;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class LogDataConsumerSplitterTest {
	private static final List<Integer> VALUES_1 = IntStream.range(1, 100).boxed().collect(Collectors.toList());
	private static final List<Integer> VALUES_2 = IntStream.range(-100, 0).boxed().collect(Collectors.toList());

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	private <T> void assertStreamResult(List<T> values, StreamConsumerWithResult<T, List<T>> consumer, Promise<List<T>> result) {
		await(StreamSupplier.ofIterable(values).streamTo(consumer.getConsumer()));
		List<T> list = await(result);
		assertEquals(values, list);
	}

	@Test
	public void testConsumes() {
		List<StreamConsumerToList<Integer>> consumers = asList(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test
	public void testConsumersWithSuspend() {
		List<StreamConsumerToList<Integer>> consumers = asList(
				StreamConsumerToList.create(),
				StreamConsumerToList.create());

		Iterator<StreamConsumerToList<Integer>> iterator = consumers.iterator();
		LogDataConsumerSplitter<Integer, Integer> splitter =
				new LogDataConsumerSplitterStub<>(() -> {
					StreamConsumerToList<Integer> next = iterator.next();
					return StreamConsumerWithResult.of(next, next.getResult());
				});

		assertStreamResult(VALUES_1, splitter.consume(), consumers.get(0).getResult());
		assertStreamResult(VALUES_2, splitter.consume(), consumers.get(1).getResult());
	}

	@Test(expected = IllegalStateException.class)
	public void testIncorrectImplementation() {
		LogDataConsumerSplitter<Integer, Integer> splitter = new LogDataConsumerSplitter<Integer, Integer>() {
			@Override
			protected StreamDataAcceptor<Integer> createSplitter(@NotNull Context ctx) {
				return item -> {};
			}
		};

		StreamSupplier.ofIterable(VALUES_1).streamTo(splitter.consume().getConsumer());
	}

	private static class LogDataConsumerSplitterStub<T, D> extends LogDataConsumerSplitter<T, D> {
		private final LogDataConsumer<T, D> logConsumer;

		private LogDataConsumerSplitterStub(LogDataConsumer<T, D> logConsumer) {
			this.logConsumer = logConsumer;
		}

		@Override
		protected StreamDataAcceptor<T> createSplitter(@NotNull Context ctx) {
			return ctx.addOutput(logConsumer);
		}
	}

}
