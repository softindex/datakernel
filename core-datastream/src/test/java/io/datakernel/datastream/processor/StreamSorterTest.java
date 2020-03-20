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

package io.datakernel.datastream.processor;

import io.datakernel.common.MemSize;
import io.datakernel.common.exception.ExpectedException;
import io.datakernel.datastream.StreamConsumer;
import io.datakernel.datastream.StreamConsumerToList;
import io.datakernel.datastream.StreamSupplier;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.promise.Promise;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static io.datakernel.datastream.TestStreamTransformers.*;
import static io.datakernel.datastream.TestUtils.assertClosedWithError;
import static io.datakernel.datastream.TestUtils.assertEndOfStream;
import static io.datakernel.datastream.processor.FailingStreamSorterStorageStub.STORAGE_EXCEPTION;
import static io.datakernel.promise.TestUtils.await;
import static io.datakernel.promise.TestUtils.awaitException;
import static io.datakernel.serializer.BinarySerializers.INT_SERIALIZER;
import static java.util.Arrays.asList;
import static org.junit.Assert.*;

public final class StreamSorterTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufPool = new ByteBufRule();

	@Test
	public void testStreamStorage() {
		StreamSupplier<Integer> source1 = StreamSupplier.of(1, 2, 3, 4, 5, 6, 7);
		//		StreamSupplier<Integer> source2 = StreamSupplier.of(111);

		Executor executor = Executors.newSingleThreadExecutor();
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

		Executor executor = Executors.newSingleThreadExecutor();
		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(storage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		await(source.transformWith(sorter)
				.streamTo(consumerToList.transformWith(randomlySuspending())));

		assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		assertEndOfStream(source, consumerToList);
		assertEndOfStream(sorter.getOutput(), sorter.getInput());
	}

	@Test
	public void testErrorOnConsumer() throws IOException {
		StreamSupplier<Integer> source = StreamSupplier.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = Executors.newSingleThreadExecutor();
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
								.transformWith(decorate(promise -> promise.then(
										item -> item == 5 ? Promise.ofException(exception) : Promise.of(item)))))
		);

		assertSame(exception, e);
		assertEndOfStream(source);
		assertClosedWithError(exception, consumer);
		assertClosedWithError(exception, sorter.getOutput(), sorter.getInput());
	}

	@Test
	public void testErrorOnSupplier() throws IOException {
		Executor executor = Executors.newSingleThreadExecutor();
		ExpectedException exception = new ExpectedException();

		StreamSupplier<Integer> source = StreamSupplier.concat(
				StreamSupplier.of(3, 1, 3, 2),
				StreamSupplier.closingWithError(exception)
		);

		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, tempFolder.newFolder().toPath());
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(
				storage, Function.identity(), Integer::compareTo, true, 10);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		Throwable e = awaitException(source.transformWith(sorter)
				.streamTo(consumerToList));

		assertSame(exception, e);
		assertEquals(0, consumerToList.getList().size());
		assertClosedWithError(exception, source, consumerToList);
		assertClosedWithError(exception, sorter.getOutput(), sorter.getInput());
	}

	@Test
	public void testCleanup() throws IOException {
		StreamSupplier<Integer> source = StreamSupplier.of(6, 5, 4, 3, 2, 1);

		Executor executor = Executors.newSingleThreadExecutor();
		Path storagePath = tempFolder.newFolder().toPath();
		StreamSorterStorage<Integer> storage = StreamSorterStorageImpl.create(executor, INT_SERIALIZER, storagePath);
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(storage, Function.identity(), Integer::compareTo, true, 0);

		List<Integer> list = new ArrayList<>();
		StreamConsumerToList<Integer> consumer = StreamConsumerToList.create(list);

		assertFalse(Files.list(storagePath).findAny().isPresent());

		Promise<Void> inputPromise = source.streamTo(sorter.getInput());

		// wait some time till files are actually created
		await(Promise.complete().async());

		assertEquals(6, Files.list(storagePath).count());

		await(inputPromise, sorter.getOutput().streamTo(consumer.transformWith(randomlySuspending())));

		assertFalse(Files.list(storagePath).findAny().isPresent());
	}

	@Test
	public void testErrorsOnStorage() throws IOException {
		FailingStreamSorterStorageStub<Integer> failingNewPartitionStorage = FailingStreamSorterStorageStub.<Integer>create().withFailNewPartition();
		doTestFailingStorage(failingNewPartitionStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Throwable exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter.getOutput(), sorter.getInput());
			assertClosedWithError(STORAGE_EXCEPTION, supplier, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStreamSorterStorageStub<Integer> failingWriteStorage = FailingStreamSorterStorageStub.<Integer>create().withFailWrite();
		doTestFailingStorage(failingWriteStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Throwable exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter.getOutput(), sorter.getInput());
			assertClosedWithError(STORAGE_EXCEPTION, supplier, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStreamSorterStorageStub<Integer> failingReadStorage = FailingStreamSorterStorageStub.<Integer>create().withFailRead();
		doTestFailingStorage(failingReadStorage, (streamPromise, sorter, supplier, consumerToList) -> {
			Throwable exception = awaitException(streamPromise);
			assertSame(STORAGE_EXCEPTION, exception);
			assertClosedWithError(STORAGE_EXCEPTION, sorter.getOutput(), sorter.getInput());
			assertEndOfStream(supplier);
			assertClosedWithError(STORAGE_EXCEPTION, consumerToList);
			assertTrue(consumerToList.getList().isEmpty());
		});

		FailingStreamSorterStorageStub<Integer> failingCleanup = FailingStreamSorterStorageStub.<Integer>create().withFailCleanup();
		doTestFailingStorage(failingCleanup, (streamPromise, sorter, supplier, consumerToList) -> {
			await(streamPromise);
			assertEndOfStream(sorter.getOutput(), sorter.getInput());
			assertEndOfStream(supplier, consumerToList);
			assertEquals(asList(1, 2, 3, 4, 5), consumerToList.getList());
		});
	}

	private void doTestFailingStorage(FailingStreamSorterStorageStub<Integer> failingStorage, StreamSorterValidator<Integer, Integer> validator) throws IOException {
		StreamSupplier<Integer> source = StreamSupplier.of(3, 1, 3, 2, 5, 1, 4, 3, 2);

		Executor executor = Executors.newSingleThreadExecutor();
		Path path = tempFolder.newFolder().toPath();
		failingStorage.setStorage(StreamSorterStorageImpl.create(executor, INT_SERIALIZER, path));
		StreamSorter<Integer, Integer> sorter = StreamSorter.create(failingStorage, Function.identity(), Integer::compareTo, true, 2);

		StreamConsumerToList<Integer> consumerToList = StreamConsumerToList.create();

		Promise<Void> streamPromise = source.transformWith(sorter)
				.streamTo(consumerToList.transformWith(randomlySuspending()));

		validator.validate(streamPromise, sorter, source, consumerToList);

		assertEquals(failingStorage.failCleanup, Files.list(path).findAny().isPresent());
	}

	private interface StreamSorterValidator<K, T> {
		void validate(Promise<Void> streamPromise, StreamSorter<K, T> sorter, StreamSupplier<T> supplier, StreamConsumerToList<T> consumerToList);
	}

}
