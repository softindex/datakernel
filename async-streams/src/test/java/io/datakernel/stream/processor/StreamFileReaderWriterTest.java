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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.stream.StreamStatus.*;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.*;

public class StreamFileReaderWriterTest {
	NioEventloop eventloop;
	ExecutorService executor;
	StreamFileReader reader;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new NioEventloop();
		executor = Executors.newCachedThreadPool();
		reader = new StreamFileReader(eventloop, executor,
				1, Paths.get("test_data/in.dat"), 0, Long.MAX_VALUE) {
			@Override
			public void send(ByteBuf item) {
				if (item.toString().equals("1")) {
					closeWithError(new Exception("Intentionally closed with exception"));
					return;
				}
				super.send(item);
			}
		};
	}

	@Test
	public void testStreamWriterOnError() throws IOException {
		File tempFile = tempFolder.newFile("outWriterWithError.dat");
		StreamFileWriter writer = new StreamFileWriter(eventloop, executor, Paths.get(tempFile.getAbsolutePath()),
				new OpenOption[]{WRITE, TRUNCATE_EXISTING}, true);

		reader.streamTo(writer);

		eventloop.run();
		assertEquals(CLOSED_WITH_ERROR, reader.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, writer.getConsumerStatus());
		assertEquals(Files.exists(Paths.get("test/outWriterWithError.dat")), false);
	}

	@Test
	public void testStreamReaderOnCloseWithError() throws IOException {
		final File tempFile = tempFolder.newFile("outReaderWithError.dat");
		final StreamFileWriter writer = StreamFileWriter.createFile(eventloop, executor,
				Paths.get(tempFile.getAbsolutePath()));

		reader.streamTo(writer);
		eventloop.run();

		assertEquals(CLOSED_WITH_ERROR, reader.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, writer.getConsumerStatus());
		assertArrayEquals(com.google.common.io.Files.toByteArray(tempFile), "Test".getBytes());
	}

	@Test
	public void testStreamFileReader() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		byte[] fileBytes = Files.readAllBytes(Paths.get("test_data/in.dat"));
		StreamFileReader reader = StreamFileReader.readFileFully(eventloop, executor,
				1, Paths.get("test_data/in.dat"));

		List<ByteBuf> list = new ArrayList<>();
		StreamConsumers.ToList<ByteBuf> consumer = StreamConsumers.toList(eventloop, list);

		reader.streamTo(consumer);
		eventloop.run();

		ByteBufQueue byteQueue = new ByteBufQueue();
		for (ByteBuf buf : list) {
			byteQueue.add(buf);
		}

		ByteBuf buf = ByteBuf.allocate(byteQueue.remainingBytes());
		byteQueue.drainTo(buf);

		assertArrayEquals(fileBytes, buf.array());
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamFileReaderWithSuspends() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		byte[] fileBytes = Files.readAllBytes(Paths.get("test_data/in.dat"));
		StreamFileReader reader = StreamFileReader.readFileFully(eventloop, executor,
				1, Paths.get("test_data/in.dat"));

		final List<ByteBuf> list = new ArrayList<>();

		class MockConsumer extends AbstractStreamConsumer<ByteBuf> implements StreamDataReceiver<ByteBuf> {
			protected MockConsumer(Eventloop eventloop) {
				super(eventloop);
			}

			@Override
			protected void onStarted() {
				suspend();
				eventloop.schedule(eventloop.currentTimeMillis() + 10, new Runnable() {
					@Override
					public void run() {
						resume();
					}
				});
			}

			@Override
			protected void onEndOfStream() {

			}

			@Override
			protected void onError(Exception e) {

			}

			@Override
			public StreamDataReceiver<ByteBuf> getDataReceiver() {
				return this;
			}

			@Override
			public void onData(ByteBuf item) {
				list.add(item);
				suspend();
				eventloop.schedule(eventloop.currentTimeMillis() + 10, new Runnable() {
					@Override
					public void run() {
						resume();
					}
				});
			}
		}

		StreamConsumer<ByteBuf> consumer = new MockConsumer(eventloop);

		reader.streamTo(consumer);
		eventloop.run();

		ByteBufQueue byteQueue = new ByteBufQueue();
		for (ByteBuf buf : list) {
			byteQueue.add(buf);
		}

		ByteBuf buf = ByteBuf.allocate(byteQueue.remainingBytes());
		byteQueue.drainTo(buf);

		assertArrayEquals(fileBytes, buf.array());
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamFileWriter() throws IOException {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = Executors.newCachedThreadPool();
		File tempFile = tempFolder.newFile("out.dat");
		byte[] bytes = new byte[]{'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrap(bytes));

		StreamFileWriter writer = new StreamFileWriter(eventloop, executor, Paths.get(tempFile.getAbsolutePath()),
				new OpenOption[]{WRITE, TRUNCATE_EXISTING}, false);

		producer.streamTo(writer);
		eventloop.run();

		byte[] fileBytes = com.google.common.io.Files.toByteArray(tempFile);
		assertArrayEquals(bytes, fileBytes);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}