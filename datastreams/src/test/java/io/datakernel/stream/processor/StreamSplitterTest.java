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
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.assertProducerStatuses;
import static io.datakernel.stream.TestUtils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSplitterTest {
	private Eventloop eventloop;

	@Before
	public void before() {
		eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
	}

	@Test
	public void test1() {
		StreamProducer<Integer> source = StreamProducers.of(1, 2, 3);
		StreamSplitter<Integer> streamConcat = StreamSplitter.create();
		StreamConsumerToList<Integer> consumerToList1 = StreamConsumerToList.randomlySuspending();
		StreamConsumerToList<Integer> consumerToList2 = StreamConsumerToList.randomlySuspending();

		stream(source, streamConcat.getInput());
		stream(streamConcat.newOutput(), consumerToList1);
		stream(streamConcat.newOutput(), consumerToList2);
		eventloop.run();
		assertEquals(asList(1, 2, 3), consumerToList1.getList());
		assertEquals(asList(1, 2, 3), consumerToList2.getList());
		assertStatus(END_OF_STREAM, source);
		assertStatus(END_OF_STREAM, streamConcat.getInput());
		assertProducerStatuses(END_OF_STREAM, streamConcat.getOutputs());
	}

	@Test
	public void testConsumerDisconnectWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());

		StreamProducer<Integer> source = StreamProducers.of(1, 2, 3, 4, 5);
		StreamSplitter<Integer> streamConcat = StreamSplitter.create();

		List<Integer> toList1 = new ArrayList<>();
		StreamConsumerToList<Integer> consumerToList1 = StreamConsumerToList.oneByOne(toList1);
		List<Integer> toList2 = new ArrayList<>();
		StreamConsumerToList<Integer> consumerToList2 = StreamConsumerToList.oneByOne(toList2);

		List<Integer> toBadList = new ArrayList<>();
		StreamConsumerToList<Integer> badConsumer = new StreamConsumerToList<Integer>(toBadList) {
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

		stream(source, streamConcat.getInput());

		stream(streamConcat.newOutput(), consumerToList1);
		stream(streamConcat.newOutput(), badConsumer);
		stream(streamConcat.newOutput(), consumerToList2);

		eventloop.run();

		assertTrue(toList1.size() == 3);
		assertTrue(toList2.size() == 3);
		assertTrue(toBadList.size() == 3);

		assertStatus(CLOSED_WITH_ERROR, source);
		assertStatus(CLOSED_WITH_ERROR, streamConcat.getInput());
		assertProducerStatuses(CLOSED_WITH_ERROR, streamConcat.getOutputs());
	}

	@Test
	public void testProducerDisconnectWithError() {
		StreamProducer<Integer> source = StreamProducers.concat(
				StreamProducers.of(1),
				StreamProducers.of(2),
				StreamProducers.of(3),
				StreamProducers.closingWithError(new ExpectedException("Test Exception"))
		);

		StreamSplitter<Integer> splitter = StreamSplitter.create();

		List<Integer> list1 = new ArrayList<>();
		StreamConsumer<Integer> consumer1 = StreamConsumerToList.oneByOne(list1);
		List<Integer> list2 = new ArrayList<>();
		StreamConsumer<Integer> consumer2 = StreamConsumerToList.oneByOne(list2);
		List<Integer> list3 = new ArrayList<>();
		StreamConsumer<Integer> consumer3 = StreamConsumerToList.oneByOne(list3);

		stream(source, splitter.getInput());
		stream(splitter.newOutput(), consumer1);
		stream(splitter.newOutput(), consumer2);
		stream(splitter.newOutput(), consumer3);

		eventloop.run();

		assertTrue(list1.size() == 3);
		assertTrue(list2.size() == 3);
		assertTrue(list3.size() == 3);

		assertStatus(CLOSED_WITH_ERROR, splitter.getInput());
		assertProducerStatuses(CLOSED_WITH_ERROR, splitter.getOutputs());
	}

	@Test(expected = IllegalStateException.class)
	public void testNoOutputs() throws ExecutionException, InterruptedException {
		final StreamSplitter<Integer> splitter = StreamSplitter.create();

		Future<Void> future = stream(StreamProducers.of(1, 2, 3, 4), splitter.getInput())
				.toCompletableFuture();

		eventloop.run();
		future.get();
	}
}