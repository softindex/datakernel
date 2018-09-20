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
import io.datakernel.stream.TestStreamConsumers;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.serializer.asm.BufferSerializers.INT_SERIALIZER;
import static io.datakernel.stream.TestUtils.assertClosedWithError;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamStorage() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source1 = StreamProducer.of(1, 2, 3, 4, 5, 6, 7);
//		StreamProducer<Integer> source2 = StreamProducer.of(111);

		StreamSorterStorageImpl<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.getRoot().toPath())
				.withWriteBlockSize(MemSize.of(64));

		StreamConsumer<Integer> writer1 = storage.writeStream(1);
//		StreamConsumer<Integer> writer2 = storage.writeStream(2);
		source1.streamTo(writer1);
//		source2.streamTo(writer2);

		eventloop.run();

		assertEndOfStream(source1);
//		assertEndOfStream(source2);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
//		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();
		storage.readStream(1).streamTo(consumer1.apply(TestStreamConsumers.oneByOne()));
//		storage.readStream(2).streamTo(consumer2.with(TestStreamConsumers.randomlySuspending()));
		eventloop.run();
		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
//		assertEquals(asList(111), consumer2.getList());

		storage.cleanup(Arrays.asList(1, 2));
		eventloop.run();
	}

	@Test
	public void test() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducer.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList.apply(TestStreamConsumers.randomlySuspending()));

		eventloop.run();

		assertEndOfStream(source);
//		assertEndOfStream(sorter.getOutput());
		assertEndOfStream(sorter.getInput());
		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
	}

	@Test
	public void testErrorOnConsumer() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducer.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(
				consumer.apply(TestStreamConsumers.decorator((context, dataAcceptor) ->
						item -> {
							dataAcceptor.accept(item);
							if (list.size() == 2) {
								context.closeWithError(new ExpectedException());
							}
						})));

		eventloop.run();

//		assertTrue(list.size() == 2);
		assertEndOfStream(source);
		assertClosedWithError(sorter.getOutput());
		assertClosedWithError(sorter.getInput());
		assertClosedWithError(consumer);
	}

	@Test
	public void testErrorOnProducer() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source = StreamProducer.concat(
				StreamProducer.of(3, 1, 3, 2),
				StreamProducer.closingWithError(new ExpectedException())
		);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		source.streamTo(sorter.getInput());
		sorter.getOutput().streamTo(consumerToList);

		eventloop.run();

		assertTrue(consumerToList.getList().size() == 0);

		assertClosedWithError(consumerToList);
//		assertClosedWithError(sorter.getOutput());
		assertClosedWithError(sorter.getInput());
	}
}
