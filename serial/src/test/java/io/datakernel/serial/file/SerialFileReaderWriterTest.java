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

package io.datakernel.serial.file;

import io.datakernel.annotation.Nullable;
import io.datakernel.async.AsyncConsumer;
import io.datakernel.async.Promise;
import io.datakernel.async.SettablePromise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.AbstractSerialConsumer;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.ActivePromisesRule;
import io.datakernel.stream.processor.ByteBufRule;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.eventloop.FatalErrorHandlers.rethrowOnAnyError;
import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static org.junit.Assert.*;

public class SerialFileReaderWriterTest {

	@Rule
	public ActivePromisesRule activePromisesRule = new ActivePromisesRule();

	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamFileReader() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		byte[] fileBytes = Files.readAllBytes(Paths.get("test_data/in.dat"));
		SerialFileReader reader = SerialFileReader.readFile(executor, Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1));

		List<ByteBuf> list = new ArrayList<>();
		reader.toList()
				.whenComplete(assertComplete(list::addAll));
		eventloop.run();

		ByteBufQueue byteQueue = new ByteBufQueue();
		for (ByteBuf buf : list) {
			byteQueue.add(buf);
		}

		ByteBuf buf = byteQueue.takeRemaining();

		assertArrayEquals(fileBytes, buf.asArray());
	}

	@Test
	public void testStreamFileReaderWithDelay() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();

		byte[] fileBytes = Files.readAllBytes(Paths.get("test_data/in.dat"));
		SerialFileReader reader = SerialFileReader.readFile(executor, Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1));

		List<ByteBuf> list = new ArrayList<>();

		class MockConsumer extends AbstractSerialConsumer<ByteBuf> {
			@Override
			protected Promise<Void> doAccept(@Nullable ByteBuf value) {
				if (value == null) {
					return Promise.complete();
				}
				SettablePromise<Void> promise = new SettablePromise<>();
				eventloop.delay(10, () -> {
					list.add(value);
					promise.set(null);
				});
				return promise;
			}
		}

		SerialConsumer<ByteBuf> consumer = new MockConsumer();

		reader.streamTo(consumer)
				.whenComplete(assertComplete());
		eventloop.run();

		ByteBufQueue byteQueue = new ByteBufQueue();
		for (ByteBuf buf : list) {
			byteQueue.add(buf);
		}

		ByteBuf buf = ByteBuf.wrapForWriting(new byte[byteQueue.remainingBytes()]);
		byteQueue.drainTo(buf);

		assertArrayEquals(fileBytes, buf.array());
	}

	@Test
	public void testStreamFileWriter() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = new byte[]{'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		SerialSupplier<ByteBuf> producer = SerialSupplier.of(ByteBuf.wrapForReading(bytes));

		SerialFileWriter writer = SerialFileWriter.create(executor, tempPath);

		producer.streamTo(writer)
				.whenComplete(assertComplete());
		eventloop.run();

		byte[] fileBytes = Files.readAllBytes(tempPath);
		assertArrayEquals(bytes, fileBytes);
	}

	@Test
	public void testStreamFileWriterRecycle() throws IOException {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		ExecutorService executor = Executors.newCachedThreadPool();
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = new byte[]{'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};
		new Random().nextBytes(bytes);

		SerialSupplier<ByteBuf> producer = SerialSupplier.of(ByteBuf.wrapForReading(bytes));
		SerialFileWriter writer = SerialFileWriter.create(executor, tempPath);

		producer.streamTo(writer)
				.thenCompose($ -> writer.accept(ByteBuf.wrapForReading("abc".getBytes())))
				.whenComplete(assertFailure(Exception.class, "Test Exception"));
		writer.close(new Exception("Test Exception"));

		eventloop.run();
	}

	@Test
	public void testStreamFileReaderWhenFileMultipleOfBuffer() throws IOException {
		Path folder = tempFolder.newFolder().toPath();
		byte[] data = new byte[3 * SerialFileReader.DEFAULT_BUFFER_SIZE.toInt()];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 256 - 127);
		}
		Path file = folder.resolve("test.bin");
		Files.write(file, data);

		Eventloop eventloop = Eventloop.create().withCurrentThread().withFatalErrorHandler(rethrowOnAnyError());
		ExecutorService executor = Executors.newSingleThreadExecutor();

		AsyncConsumer<ByteBuf> asyncConsumer = buf -> {
			assertTrue("Received byte buffer is empty", buf.canRead());
			buf.recycle();
			return Promise.complete();
		};

		SerialFileReader.readFile(executor, file)
				.streamTo(SerialConsumer.of(asyncConsumer))
				.whenComplete(assertComplete());

		eventloop.run();
		executor.shutdown();
	}

	@Test
	public void testClose() throws Exception {
		Eventloop eventloop = Eventloop.create().withFatalErrorHandler(rethrowOnAnyError()).withCurrentThread();
		File file = tempFolder.newFile("2Mb");
		byte[] data = new byte[2 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		new Random().nextBytes(data);
		Files.write(file.toPath(), data);
		Path srcPath = file.toPath();
		Exception testException = new Exception("Test Exception");

		SerialFileReader serialFileReader = SerialFileReader.readFile(Executors.newCachedThreadPool(), srcPath);
		serialFileReader.toList()
				.whenComplete((res, e) -> {
					System.out.println("COMPLETED");
					assertSame(testException, e);
				});
		serialFileReader.close(testException);

		eventloop.run();
	}
}
