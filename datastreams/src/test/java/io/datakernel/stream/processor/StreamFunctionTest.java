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

package io.datakernel.stream.processor;

import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamProducers.concat;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamFunctionTest {

	@Test
	public void testFunction() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, input -> input * input);

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertStatus(END_OF_STREAM, source1);
		assertStatus(END_OF_STREAM, streamFunction.getInput());
		assertStatus(END_OF_STREAM, streamFunction.getOutput());
		assertStatus(END_OF_STREAM, consumer);
	}

	@Test
	public void testFunctionConsumerError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, input -> input * input);

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new ExpectedException("Test Exception"));
					return;
				}
				getProducer().suspend();
				eventloop.post(() -> getProducer().produce(this));
			}
		};

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4), list);

		assertStatus(CLOSED_WITH_ERROR, source1);
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getOutput());
	}

	@Test
	public void testFunctionProducerError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, input -> input * input);

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source1 = concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.closingWithError(eventloop, new ExpectedException("Test Exception")));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop, list);

		source1.streamTo(streamFunction.getInput());
		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9, 16, 25, 36), list);

		assertStatus(CLOSED_WITH_ERROR, consumer.getProducer());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFunction.getOutput());
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamFunction<Integer, Integer> streamFunction = StreamFunction.create(eventloop, input -> input * input);

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source1.streamTo(streamFunction.getInput());
		eventloop.run();

		streamFunction.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 4, 9), consumer.getList());

		assertStatus(END_OF_STREAM, source1);
		assertStatus(END_OF_STREAM, streamFunction.getInput());
		assertStatus(END_OF_STREAM, streamFunction.getOutput());
		assertStatus(END_OF_STREAM, consumer);
	}
}
