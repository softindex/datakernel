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
import com.google.common.collect.Ordering;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.exception.ExpectedException;
import io.datakernel.stream.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamStorage() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5, 6, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(111));

		StreamSorterStorageImpl<Integer> storage = StreamSorterStorageImpl.create(eventloop, executor, intSerializer(), tempFolder.getRoot().toPath())
				.withWriteBlockSize(64);

		StreamConsumerWithResult<Integer, Integer> writer1 = StreamConsumerWithResult.ofStage(storage.write());
		StreamConsumerWithResult<Integer, Integer> writer2 = StreamConsumerWithResult.ofStage(storage.write());
		source1.streamTo(writer1);
		source2.streamTo(writer2);

		CompletableFuture<Integer> chunk1 = writer1.getResult().toCompletableFuture();
		CompletableFuture<Integer> chunk2 = writer2.getResult().toCompletableFuture();

		eventloop.run();

		assertStatus(END_OF_STREAM, source1);
		assertStatus(END_OF_STREAM, source2);

		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListOneByOne(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		StreamProducerWithResult.ofStage(storage.read(chunk1.get())).streamTo(consumer1);
		StreamProducerWithResult.ofStage(storage.read(chunk2.get())).streamTo(consumer2);
		eventloop.run();
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
		assertEquals(asList(111), consumer2.getList());

		storage.cleanup(Arrays.asList(chunk1.get(), chunk2.get()));
		eventloop.run();

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void test() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(eventloop, executor, intSerializer(), tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(eventloop,
				storage, Functions.identity(), Ordering.natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertStatus(END_OF_STREAM, source);
//		assertStatus(END_OF_STREAM, sorter.getOutput());
		assertStatus(END_OF_STREAM, sorter.getInput());
	}

	@Test
	public void testWithoutConsumer() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(eventloop, executor, intSerializer(), tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(eventloop,
				storage, Functions.identity(), Ordering.natural(), true, 2);

		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(sorter.getInput());
		eventloop.run();

		sorter.getOutput().streamTo(consumerToList);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertStatus(END_OF_STREAM, source);
//		assertStatus(END_OF_STREAM, sorter.getOutput());
		assertStatus(END_OF_STREAM, sorter.getInput());
	}

	@Test
	public void testErrorOnConsumer() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2, 5, 1, 4, 3, 2));

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(eventloop, executor, intSerializer(), tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(eventloop,
				storage, Functions.identity(), Ordering.natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		TestStreamConsumers.TestConsumerToList<Integer> consumerToList = new TestStreamConsumers.TestConsumerToList<Integer>(eventloop, list) {
			@Override
			public void onData(Integer item) {
				list.add(item);
				if (list.size() == 2) {
					closeWithError(new ExpectedException());
				}
			}
		};

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();

//		assertTrue(list.size() == 2);
		assertStatus(END_OF_STREAM, source);
//		assertStatus(CLOSED_WITH_ERROR, sorter.getOutput());
		assertStatus(END_OF_STREAM, sorter.getInput());
		assertStatus(CLOSED_WITH_ERROR, consumerToList);
	}

	@Test
	public void testErrorOnProducer() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducers.concat(eventloop,
				StreamProducers.ofIterable(eventloop, asList(3, 1, 3, 2)),
				StreamProducers.closingWithError(eventloop, new ExpectedException())
		);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(eventloop, executor, intSerializer(), tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(eventloop,
				storage, Functions.identity(), Ordering.natural(), true, 2);

		final List<Integer> list = new ArrayList<>();
		StreamConsumers.ToList<Integer> consumerToList = new StreamConsumers.ToList<>(eventloop, list);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();

		assertTrue(list.size() == 0);

		assertStatus(CLOSED_WITH_ERROR, consumerToList);
//		assertStatus(CLOSED_WITH_ERROR, sorter.getOutput());
		assertStatus(CLOSED_WITH_ERROR, sorter.getInput());
	}
}