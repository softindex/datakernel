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

package io.datakernel.csp.file;

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.ChannelConsumer;
import io.datakernel.csp.ChannelSupplier;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.EventloopRule;
import io.datakernel.util.MemSize;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static io.datakernel.async.TestUtils.await;
import static io.datakernel.async.TestUtils.awaitException;
import static io.datakernel.eventloop.Eventloop.getCurrentEventloop;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public final class ChannelFileReaderWriterTest {

	@ClassRule
	public static final EventloopRule eventloopRule = new EventloopRule();

	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void streamFileReader() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.readFile(Paths.get("test_data/in.dat"))
				.then(cfr -> cfr.toCollector(ByteBufQueue.collector())));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void streamFileReaderWithDelay() throws IOException {
		ByteBuf byteBuf = await(ChannelFileReader.readFile(Paths.get("test_data/in.dat"))
				.then(cfr -> cfr.withBufferSize(MemSize.of(1))
						.mapAsync(buf -> Promise.<ByteBuf>ofCallback(cb -> getCurrentEventloop().delay(10, () -> cb.set(buf))))
						.toCollector(ByteBufQueue.collector())));

		assertArrayEquals(Files.readAllBytes(Paths.get("test_data/in.dat")), byteBuf.asArray());
	}

	@Test
	public void streamFileWriter() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		await(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(ChannelFileWriter.create(tempPath)));

		assertArrayEquals(bytes, Files.readAllBytes(tempPath));
	}

	@Test
	public void streamFileWriterRecycle() throws IOException {
		Path tempPath = tempFolder.getRoot().toPath().resolve("out.dat");
		byte[] bytes = {'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		ChannelFileWriter writer = await(ChannelFileWriter.create(tempPath));

		Exception exception = new Exception("Test Exception");

		writer.close(exception);
		Throwable e = awaitException(ChannelSupplier.of(ByteBuf.wrapForReading(bytes))
				.streamTo(writer)
				.then($ -> writer.accept(ByteBuf.wrapForReading("abc".getBytes()))));
		assertSame(exception, e);
	}

	@Test
	public void streamFileReaderWhenFileMultipleOfBuffer() throws IOException {
		Path folder = tempFolder.newFolder().toPath();
		byte[] data = new byte[3 * ChannelFileReader.DEFAULT_BUFFER_SIZE.toInt()];
		for (int i = 0; i < data.length; i++) {
			data[i] = (byte) (i % 256 - 127);
		}
		Path file = folder.resolve("test.bin");
		Files.write(file, data);

		await(ChannelFileReader.readFile(file)
				.then(cfr -> cfr.streamTo(ChannelConsumer.of(buf -> {
					assertTrue("Received byte buffer is empty", buf.canRead());
					buf.recycle();
					return Promise.complete();
				}))));
	}

	@Test
	public void close() throws Exception {
		File file = tempFolder.newFile("2Mb");
		byte[] data = new byte[2 * 1024 * 1024]; // the larger the file the less chance that it will be read fully before close completes
		ThreadLocalRandom.current().nextBytes(data);

		Path srcPath = file.toPath();
		Files.write(srcPath, data);
		Exception testException = new Exception("Test Exception");

		ChannelFileReader serialFileReader = await(ChannelFileReader.readFile(srcPath));

		serialFileReader.close(testException);
		assertSame(testException, awaitException(serialFileReader.toList()));
	}

	@Test
	public void readOverFile() throws IOException {
		ChannelFileReader cfr = await(ChannelFileReader.readFile(Paths.get("test_data/in.dat")));

		ByteBuf byteBuf = await(cfr.withOffset(Files.size(Paths.get("test_data/in.dat")) + 100)
				.withBufferSize(MemSize.of(1))
				.toCollector(ByteBufQueue.collector()));

		assertEquals("", byteBuf.asString(UTF_8));
	}

	@Test
	public void testReaderNotRegularFile() throws IOException {
		File file = tempFolder.newFile();
		ChannelFileReader cfr = await(ChannelFileReader.readFile(file.toPath()));
		cfr.close();

		File dir = tempFolder.newFolder();
		Throwable e = awaitException(ChannelFileReader.readFile(dir.toPath()));
		assertSame(ChannelFileReader.NOT_A_REGULAR_FILE, e);
	}

}
