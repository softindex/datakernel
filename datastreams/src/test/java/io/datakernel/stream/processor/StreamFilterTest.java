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
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.StreamConsumers.decorator;
import static io.datakernel.stream.StreamConsumers.randomlySuspending;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamFilterTest {
	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> producer = StreamProducer.of(1, 2, 3);
		StreamFilter<Integer> filter = StreamFilter.create(input -> input % 2 == 1);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create();

		producer.with(filter).streamTo(
				consumer.with(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(1, 3), consumer.getList());
		assertStatus(END_OF_STREAM, producer);
		assertStatus(END_OF_STREAM, filter.getInput());
		assertStatus(END_OF_STREAM, filter.getOutput());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		List<Integer> list = new ArrayList<>();

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3, 4, 5);
		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 2);
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.with(streamFilter).streamTo(
				consumer.with(decorator((context, dataReceiver) ->
						item -> {
							dataReceiver.onData(item);
							if (item == 3) {
								context.closeWithError(new ExpectedException("Test Exception"));
							}
						})));

		eventloop.run();

		assertEquals(asList(1, 2, 3), list);
		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getOutput());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProducerDisconnectWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamProducer<Integer> source = StreamProducer.concat(
				StreamProducer.ofIterable(Arrays.asList(1, 2, 3)),
				StreamProducer.closingWithError(new ExpectedException("Test Exception")));

		StreamFilter<Integer> streamFilter = StreamFilter.create(input -> input % 2 != 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList consumer = StreamConsumerToList.create(list);

		source.with(streamFilter).streamTo(
				consumer.with(StreamConsumers.oneByOne()));

		eventloop.run();

		assertTrue(list.size() == 3);
		assertStatus(CLOSED_WITH_ERROR, consumer);
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getInput());
		assertStatus(CLOSED_WITH_ERROR, streamFilter.getOutput());
	}

}
