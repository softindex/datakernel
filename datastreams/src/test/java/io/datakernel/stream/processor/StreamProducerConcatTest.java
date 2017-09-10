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

import com.google.common.base.Functions;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamProducerConcatTest {

	@Test
	public void testSequence() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
		assertEquals(END_OF_STREAM, consumer.getStatus());
	}

	@Test
	public void testSequenceException() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.closingWithError(eventloop, new ExpectedException("Test Exception")),
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)));
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop, list);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), list);
		assertStatus(CLOSED_WITH_ERROR, consumer);
	}

	@Test
	public void testConcat() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.closing(eventloop));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
	}

	@Test
	public void testConcatException() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.closingWithError(eventloop, new ExpectedException("Test Exception")));

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		producer.streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), list);

	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(1, 2, 3)),
				StreamProducers.ofIterable(eventloop, asList(4, 5, 6)),
				StreamProducers.closing(eventloop));

		StreamConsumers.ToList<Integer> consumer = StreamConsumers.toList(eventloop);
		StreamFunction<Integer, Integer> function = StreamFunction.create(eventloop, Functions.<Integer>identity());

		producer.streamTo(function.getInput());
		eventloop.run();

		function.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6), consumer.getList());
	}

}