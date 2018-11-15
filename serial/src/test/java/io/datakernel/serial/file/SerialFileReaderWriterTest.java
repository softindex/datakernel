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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.serial.SerialConsumer;
import io.datakernel.serial.SerialSupplier;
import io.datakernel.stream.processor.DatakernelRunner;
import io.datakernel.util.MemSize;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.test.TestUtils.assertComplete;
import static io.datakernel.test.TestUtils.assertFailure;
import static org.junit.Assert.*;

@RunWith(DatakernelRunner.class)
public final class SerialFileReaderWriterTest {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamFileReader() throws IOException {
		SerialFileReader.readFile(Executors.newSingleThreadExecutor(), Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1))
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), buf.asArray())));
	}

	@Test
	public void testStreamFileReaderWithDelay() throws IOException {
		SerialFileReader.readFile(Executors.newSingleThreadExecutor(), Paths.get("test_data/in.dat"))
				.withBufferSize(MemSize.of(1))
				.<ByteBuf>transformAsync(buf -> Promise.ofCallback(cb -> Eventloop.getCurrentEventloop().delay(10, () -> cb.set(buf))))
				.toCollector(ByteBufQueue.collector())
				.whenComplete(assertComplete(buf -> assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), buf.asArray())));
	}

	@Test
	public void testStreamFileWriter() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		SerialSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(SerialFileWriter.create(Executors.newSingleThreadExecutor(), tempPath))
				.whenComplete(assertComplete($ -> assertArrayEquals(bytes, Files.readAllBytes(tempPath))));
	}

	@Test
	public void testStreamFileWriterRecycle() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		SerialFileWriter writer = SerialFileWriter.create(Executors.newSingleThreadExecutor(), tempPath);

		SerialSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(writer)
				.thenCompose($ -> writer.accept(ByteBuf.wrapForReading("abc".getBytes())))
				.whenComplete(assertFailure(Exception.class, "Test Exception"));
		writer.close(new Exception("Test Exception"));
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

		SerialFileReader.readFile(Executors.newSingleThreadExecutor(), file)
				.streamTo(SerialConsumer.of(buf -> {
					assertTrue("Received byte buffer is empty", buf.canRead());
					buf.recycle();
					return Promise.complete();
				}))
				.whenComplete(assertComplete());
	}

	@Test
	public void testClose() throws Exception {
		File file = tempFolder.newFile("2Mb");
		byte[] data = new byte[2 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		ThreadLocalRandom.current().nextBytes(data);

		Path srcPath = file.toPath();
		Files.write(srcPath, data);
		Exception testException = new Exception("Test Exception");

		SerialFileReader serialFileReader = SerialFileReader.readFile(Executors.newCachedThreadPool(), srcPath);
		serialFileReader.toList().whenComplete(assertFailure(e -> assertSame(testException, e)));
		serialFileReader.close(testException);
	}
}
