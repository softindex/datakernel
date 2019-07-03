/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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
import io.datakernel.stream.StreamSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.test.rules.ExecutorRule;
import io.datakernel.util.MemSize;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.serializer.util.BinarySerializers.INT_SERIALIZER;
import static io.datakernel.stream.TestStreamConsumers.*;
import static io.datakernel.stream.TestUtils.assertEndOfStream;
import static io.datakernel.test.rules.ExecutorRule.getExecutor;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public final class StreamSorterTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufPool = new ByteBufRule();

	@ClassRule
	public static final ExecutorRule executorRule = new ExecutorRule();

	@Test
	public void testStreamStorage() {
		StreamSupplier<Integer> source1 = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7);
		//		StreamSupplier<Integer> source2 = StreamSupplier.of(111);

		Executor executor = getExecutor();
		StreamSorterStorageImpl<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.getRoot().toPath())
				.withWriteBlockSize(MemSize.of(64));

		StreamConsumer<Integer> writer1 = storage.writeStream(1);
//		StreamConsumer<Integer> writer2 = storage.writeStream(2);
		source1.streamTo(writer1);
//		source2.streamTo(writer2);

		Eventloop.getCurrentEventloop().run();

		assertEndOfStream(source1);
//		assertEndOfStream(source2);

		StreamConsumerToList<Integer> consumer1 = StreamConsumerToList.create();
//		StreamConsumerToList<Integer> consumer2 = StreamConsumerToList.create();
		storage.readStream(1).streamTo(consumer1.transformWith(oneByOne()));
//		storage.readStream(2).streamTo(consumer2.with(TestStreamConsumers.randomlySuspending()));
		Eventloop.getCurrentEventloop().run();

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
//		assertEquals(asList(111), consumer2.getList());

		storage.cleanup(Arrays.asList(1, 2));
	}

	@Test
	public void test() throws Exception {
		StreamSupplier<Integer> source = StreamSupplier.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = getExecutor();
		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(storage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		await(source.transformWith(sorter)
				.streamTo(consumerToList.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
	}

	@Test
	public void testErrorOnConsumer() throws IOException {
		StreamSupplier<Integer> source = StreamSupplier.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = getExecutor();
		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);
		ExpectedException exception = new ExpectedException();

		Throwable e = awaitException(
				source.streamTo(sorter.getInput()),
				sorter.getOutput()
						.streamTo(consumer
								.transformWith(decorator((context, dataAcceptor) ->
										item -> {
											dataAcceptor.accept(item);
											if (list.size() == 2) {
												context.closeWithError(exception);
											}
										})))
		);

		assertSame(exception, e);
	}

	@Test
	public void testErrorOnSupplier() throws IOException {
		Executor executor = getExecutor();
		ExpectedException exception = new ExpectedException();

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(3, 1, 3, 2),
				StreamSupplier.closingWithError(exception)
		);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		Throwable e = awaitException(source.transformWith(sorter)
				.streamTo(consumerToList));

		assertSame(exception, e);
		assertEquals(0, consumerToList.getList().size());
	}
}
