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
import io.datakernel.stream.StreamConsumer;
import io.datakernel.stream.StreamConsumerToList;
import io.datakernel.stream.StreamProducer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamConsumers.*;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertProducerStatuses;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSharderTest {

	private static final Sharder<Integer> SHARDER = object -> object % 2;

	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		stream(source, streamSharder.getInput());

		stream(streamSharder.newOutput(), consumer1.with(randomlySuspending()));
		stream(streamSharder.newOutput(), consumer2.with(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, streamSharder.getInput());
		assertProducerStatuses(END_OF_STREAM, streamSharder.getOutputs());
		assertStatus(END_OF_STREAM, consumer1);
		assertStatus(END_OF_STREAM, consumer2);
	}

	@Test
	public void test2() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3, 4);
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();

		stream(source, streamSharder.getInput());
		stream(streamSharder.newOutput(), consumer1.with(randomlySuspending()));
		stream(streamSharder.newOutput(), consumer2.with(randomlySuspending()));

		eventloop.run();
		assertEquals(asList(2, 4), consumer1.getList());
		assertEquals(asList(1, 3), consumer2.getList());

		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, streamSharder.getInput());
		assertProducerStatuses(END_OF_STREAM, streamSharder.getOutputs());
		assertStatus(END_OF_STREAM, consumer1);
		assertStatus(END_OF_STREAM, consumer2);
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamProducer<Integer> source = StreamProducer.of(1, 2, 3, 4);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create(list1);

		List<Integer> list2 = new ArrayList<>();
		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create(list2);

		stream(source, streamSharder.getInput());
		stream(streamSharder.newOutput(), consumer1);
		stream(streamSharder.newOutput(),
				consumer2.with(decorator((context, dataReceiver) ->
						item -> {
							dataReceiver.onData(item);
							if (item == 3) {
								context.closeWithError(new ExpectedException("Test Exception"));
							}
						})));

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);
		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, streamSharder.getInput());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamSharder.getOutputs());
		assertStatus(CLOSED_WITH_ERROR, consumer1);
		assertStatus(CLOSED_WITH_ERROR, consumer2);
	}

	@Test
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamSharder<Integer> streamSharder = StreamSharder.create(SHARDER);

		StreamProducer<Integer> source = StreamProducer.concat(
				StreamProducer.of(1),
				StreamProducer.of(2),
				StreamProducer.of(3),
				StreamProducer.closingWithError(new ExpectedException("Test Exception"))
		);

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = StreamConsumerToList.create(list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = StreamConsumerToList.create(list2);

		stream(source, streamSharder.getInput());
		stream(streamSharder.newOutput(), consumer1.with(oneByOne()));
		stream(streamSharder.newOutput(), consumer2.with(oneByOne()));

		eventloop.run();

		assertTrue(list1.size() == 1);
		assertTrue(list2.size() == 2);

		assertStatus(CLOSED_WITH_ERROR, streamSharder.getInput());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamSharder.getOutputs());
		assertStatus(CLOSED_WITH_ERROR, consumer1);
		assertStatus(CLOSED_WITH_ERROR, consumer2);
	}
}
