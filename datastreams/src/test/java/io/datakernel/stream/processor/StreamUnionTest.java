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
import io.datakernel.stream.StreamStatus;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.stream.DataStreams.stream;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.TestUtils.*;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

@SuppressWarnings("unchecked")
public class StreamUnionTest {
	@Test
	public void test1() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamUnion<Integer> streamUnion = StreamUnion.create();

		StreamProducer<Integer> source0 = StreamProducer.of();
		StreamProducer<Integer> source1 = StreamProducer.of(1);
		StreamProducer<Integer> source2 = StreamProducer.of(2, 3);
		StreamProducer<Integer> source3 = StreamProducer.of();
		StreamProducer<Integer> source4 = StreamProducer.of(4, 5);
		StreamProducer<Integer> source5 = StreamProducer.of(6);
		StreamProducer<Integer> source6 = StreamProducer.of();

		StreamConsumerToList<Integer> consumer = StreamConsumerToList.randomlySuspending();

		stream(source0, streamUnion.newInput());
		stream(source1, streamUnion.newInput());
		stream(source2, streamUnion.newInput());
		stream(source3, streamUnion.newInput());
		stream(source4, streamUnion.newInput());
		stream(source5, streamUnion.newInput());
		stream(source6, streamUnion.newInput());
		stream(streamUnion.getOutput(), consumer);
		eventloop.run();

		List<Integer> result = consumer.getList();
		Collections.sort(result);
		assertEquals(asList(1, 2, 3, 4, 5, 6), result);

		assertStatus(END_OF_STREAM, source0);
		assertStatus(END_OF_STREAM, source1);
		assertStatus(END_OF_STREAM, source2);
		assertStatus(END_OF_STREAM, source3);
		assertStatus(END_OF_STREAM, source4);
		assertStatus(END_OF_STREAM, source5);
		assertStatus(END_OF_STREAM, source6);

		assertStatus(END_OF_STREAM, streamUnion.getOutput());
		assertConsumerStatuses(END_OF_STREAM, streamUnion.getInputs());
	}

	@Test
	public void testWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamUnion<Integer> streamUnion = StreamUnion.create();

		StreamProducer<Integer> source0 = StreamProducer.of(1, 2, 3);
		StreamProducer<Integer> source1 = StreamProducer.of(4, 5);
		StreamProducer<Integer> source2 = StreamProducer.of(6, 7);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = new StreamConsumerToList<Integer>(list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (item == 1) {
					closeWithError(new ExpectedException("Test Exception"));
					return;
				}
				getProducer().suspend();
				eventloop.post(() -> getProducer().produce(this));
			}
		};

		stream(source0, streamUnion.newInput());
		stream(source1, streamUnion.newInput());
		stream(source2, streamUnion.newInput());

		stream(streamUnion.getOutput(), consumer);
		eventloop.run();

		assertEquals(Arrays.asList(6, 7, 4, 5, 1), list);
		assertStatus(CLOSED_WITH_ERROR, source0);
		assertStatus(END_OF_STREAM, source1);
		assertStatus(END_OF_STREAM, source2);

		assertStatus(CLOSED_WITH_ERROR, streamUnion.getOutput());
		assertArrayEquals(new StreamStatus[]{CLOSED_WITH_ERROR, END_OF_STREAM, END_OF_STREAM},
				consumerStatuses(streamUnion.getInputs()));
	}

	@Test
	public void testProducerWithError() {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamUnion<Integer> streamUnion = StreamUnion.create();

		StreamProducer<Integer> source0 = StreamProducer.concat(
				StreamProducer.ofIterable(Arrays.asList(1, 2)),
				StreamProducer.closingWithError(new ExpectedException("Test Exception"))
		);
		StreamProducer<Integer> source1 = StreamProducer.concat(
				StreamProducer.ofIterable(Arrays.asList(7, 8, 9)),
				StreamProducer.closingWithError(new ExpectedException("Test Exception"))
		);

		List<Integer> list = new ArrayList<>();
		StreamConsumer<Integer> consumer = StreamConsumerToList.oneByOne(list);

		stream(source0, streamUnion.newInput());
		stream(source1, streamUnion.newInput());

		stream(streamUnion.getOutput(), consumer);
		eventloop.run();

		assertTrue(list.size() == 3);
		assertStatus(CLOSED_WITH_ERROR, streamUnion.getOutput());
		assertConsumerStatuses(CLOSED_WITH_ERROR, streamUnion.getInputs());
	}

	@Test
	public void testWithoutProducer() throws ExecutionException, InterruptedException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();

		StreamUnion<Integer> streamUnion = StreamUnion.create();
		StreamConsumerToList<Integer> toList = StreamConsumerToList.create();
		stream(streamUnion.getOutput(), toList);
		CompletableFuture<Void> future = toList.getEndOfStream().toCompletableFuture();
		eventloop.run();
		future.get();
	}

}
