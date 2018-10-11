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

package io.datakernel.serial;

import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Stage;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.StacklessException;
import io.datakernel.serial.processor.SerialSplitter;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SerialSplitterTest {

	private Eventloop eventloop;

	@Before
	public void setUp() {
		eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
	}

	@Test
	public void simpleCase() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		List<String> theList = new ArrayList<>();

		SerialSplitter<String> splitter = SerialSplitter.<String>create()
				.withInput(SerialSupplier.ofIterable(expected));

		for (int i = 0; i < n; i++) {
			splitter.addOutput().set(SerialConsumer.of(AsyncConsumer.<String>of(theList::add)).async());
		}

		splitter.startProcess().whenComplete(assertComplete());

		eventloop.run();

		assertEquals(expected.stream().flatMap(x -> Stream.generate(() -> x).limit(n)).collect(toList()), theList);
	}

	@Test
	public void inputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		SerialSplitter<String> splitter = SerialSplitter.<String>create()
				.withInput(SerialSuppliers.concat(SerialSupplier.ofIterable(expected), SerialSupplier.ofException(new StacklessException("test exception"))));

		for (int i = 0; i < n; i++) {
			splitter.addOutput().set(SerialConsumer.of(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
		}

		splitter.startProcess().whenComplete(assertFailure("test exception"));

		eventloop.run();
	}

	@Test
	public void oneOutputFailure() {
		int n = 10;

		List<String> expected = new ArrayList<>();
		expected.add("first");
		expected.add("second");
		expected.add("third");

		SerialSplitter<String> splitter = SerialSplitter.<String>create()
				.withInput(SerialSupplier.ofIterable(expected));

		for (int i = 0; i < n; i++) {
			if (i == n / 2) {
				splitter.addOutput().set(SerialConsumer.ofException(new StacklessException("test exception")));
			} else {
				splitter.addOutput().set(SerialConsumer.of(AsyncConsumer.of((String s) -> { /*noop*/ })).async());
			}
		}

		Stage<Void> stage = splitter.startProcess()
				.whenComplete(($, e) -> System.out.println("completed"))
				.whenComplete(assertFailure("test exception"));

		eventloop.run();

		assertTrue(stage.isComplete());
	}
}
