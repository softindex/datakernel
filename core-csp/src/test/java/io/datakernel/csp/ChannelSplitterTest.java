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

package io.datakernel.csp;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.csp.process.ChannelSplitter;
import io.datakernel.exception.StacklessException;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class ChannelSplitterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	@Test
	public void simpleCase() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		List<String> theList = new ArrayList<>();

		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
				.withInput(ChannelSupplier.ofIterable(expected));

		for (int i = 0; i < n; i++) {
			splitter.addOutput()
					.set(ChannelConsumer.of(AsyncConsumer.<String>of(theList::add)).async());
		}

		await(splitter.startProcess());

		assertEquals(expected.stream().flatMap(x -> Stream.generate(() -> x).limit(n)).collect(toList()), theList);
	}

	@Test
	public void inputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		StacklessException exception = new StacklessException(ChannelSplitterTest.class, "test exception");
		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
				.withInput(ChannelSuppliers.concat(ChannelSupplier.ofIterable(expected), ChannelSupplier.ofException(exception)));

		for (int i = 0; i < n; i++) {
			splitter.addOutput()
					.set(ChannelConsumer.of(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
		}

		assertSame(exception, awaitException(splitter.startProcess()));
	}

	@Test
	public void oneOutputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		ChannelSplitter<String> splitter = ChannelSplitter.<String>create()
				.withInput(ChannelSupplier.ofIterable(expected));
		StacklessException exception = new StacklessException(ChannelSplitterTest.class, "test exception");

		for (int i = 0; i < n; i++) {
			if (i == n / 2) {
				splitter.addOutput()
						.set(ChannelConsumer.ofException(exception));
			} else {
				splitter.addOutput()
						.set(ChannelConsumer.of(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
			}
		}

		assertSame(exception, awaitException(splitter.startProcess()));
	}
}
