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
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducerDecorator;
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

public class StreamProducerDecoratorTest {
	@SuppressWarnings("unchecked")
	@Test
	public void test2() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		List<Integer> list = new ArrayList<>();

		TestStreamConsumers.TestConsumerToList consumer = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				if (item == 3) {
					getProducer().closeWithError(new Exception("Test Exception"));
					return;
				}
				list.add(item);
				getProducer().suspend();
				eventloop.post(() -> getProducer().produce(this));
			}
		};

		final StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(producer) {};

		producerDecorator.streamTo(consumer);

		eventloop.run();

		assertEquals(list, asList(1, 2));
		assertStatus(CLOSED_WITH_ERROR, producer);
		assertStatus(CLOSED_WITH_ERROR, consumer);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);
		final StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(producer) {};

		producerDecorator.streamTo(consumer);

		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3, 4, 5));
		assertStatus(END_OF_STREAM, producer);
		assertStatus(END_OF_STREAM, consumer);
	}

	@Test
	public void testWithoutConsumer() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);
		final StreamProducer<Integer> producer = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));
		StreamProducerDecorator<Integer> producerDecorator = new StreamProducerDecorator<Integer>(producer) {};
		StreamFunction<Integer, Integer> function = StreamFunction.create(eventloop, Functions.<Integer>identity());

		producerDecorator.streamTo(function.getInput());
		eventloop.run();

		function.getOutput().streamTo(consumer);
		eventloop.run();

		assertEquals(consumer.getList(), asList(1, 2, 3, 4, 5));
		assertStatus(END_OF_STREAM, producer);
		assertStatus(END_OF_STREAM, consumer);
	}
}
