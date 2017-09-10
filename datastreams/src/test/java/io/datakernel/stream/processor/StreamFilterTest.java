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
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamStatus.*;
import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFilterTest {
	@Test
	public void test1() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));

		StreamFilter<Integer> filter = StreamFilter.create(eventloop, input -> input % 2 == 1);

		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(filter.getInput());
		filter.getOutput().streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 3), consumer.getList());
		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, filter.getInput());
		assertStatus(END_OF_STREAM, filter.getOutput());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5));

		StreamFilter<Integer> streamFilter = StreamFilter.create(eventloop, input -> input % 2 != 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 3) {
					closeWithError(new ExpectedException("Test Exception"));
					return;
				}
				getProducer().suspend();
				eventloop.post(() -> getProducer().produce(this));
			}
		};

		source.streamTo(streamFilter.getInput());
		streamFilter.getOutput().streamTo(consumer1);

		eventloop.run();

		assertEquals(asList(1, 2, 3), list);
		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, consumer1);
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getOutput());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProducerDisconnectWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, Arrays.asList(1, 2, 3)),
				StreamProducers.closingWithError(eventloop, new ExpectedException("Test Exception")));

		StreamFilter<Integer> streamFilter = StreamFilter.create(eventloop, input -> input % 2 != 2);

		List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source.streamTo(streamFilter.getInput());
		streamFilter.getOutput().streamTo(consumer);

		eventloop.run();

		assertTrue(list.size() == 3);
		assertStatus(CLOSED_WITH_ERROR, consumer.getProducer());
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getOutput());
	}

}
