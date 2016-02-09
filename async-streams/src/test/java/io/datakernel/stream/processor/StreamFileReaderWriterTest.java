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

import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.file.AsyncFile;
import io.datakernel.stream.*;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class StreamFileReaderWriterTest {
	Eventloop eventloop;
	ExecutorService executor;
	StreamFileReaderWithError reader;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testStreamWriterOnError() throws IOException {
		File tempFile = tempFolder.newFile("outWriterWithError.dat");
		StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, Paths.get(tempFile.getAbsolutePath()));

		reader.streamTo(writer);

		eventloop.run();
		assertEquals(CLOSED_WITH_ERROR, reader.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, writer.getConsumerStatus());
		assertEquals(Files.exists(Paths.get("test/outWriterWithError.dat")), false);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamReaderOnCloseWithError() throws IOException {
		final File tempFile = tempFolder.newFile("outReaderWithError.dat");
		final StreamFileWriter writer = StreamFileWriter.create(eventloop, executor,
				Paths.get(tempFile.getAbsolutePath()));

		reader.streamTo(writer);
		eventloop.run();

		assertEquals(CLOSED_WITH_ERROR, reader.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, writer.getConsumerStatus());
		assertArrayEquals(com.google.common.io.Files.toByteArray(tempFile), "Test".getBytes());
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamFileReader() throws IOException {
		Eventloop eventloop = new Eventloop();
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
		Eventloop eventloop = new Eventloop();
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
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();
		File tempFile = tempFolder.newFile("out.dat");
		byte[] bytes = new byte[]{'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		StreamProducer<ByteBuf> producer = StreamProducers.ofValue(eventloop, ByteBuf.wrap(bytes));

		StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, Paths.get(tempFile.getAbsolutePath()));

		producer.streamTo(writer);
		eventloop.run();

		byte[] fileBytes = com.google.common.io.Files.toByteArray(tempFile);
		assertArrayEquals(bytes, fileBytes);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamFileWriterRecycle() throws IOException {
		Eventloop eventloop = new Eventloop();
		ExecutorService executor = Executors.newCachedThreadPool();
		File tempFile = tempFolder.newFile("out.dat");
		byte[] bytes = new byte[]{'T', 'e', 's', 't', '1', ' ', 'T', 'e', 's', 't', '2', ' ', 'T', 'e', 's', 't', '3', '\n', 'T', 'e', 's', 't', '\n'};

		StreamProducer<ByteBuf> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, ByteBuf.wrap(bytes)),
				StreamProducers.<ByteBuf>closingWithError(eventloop, new Exception("Test Exception")));

		StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, Paths.get(tempFile.getAbsolutePath()));

		producer.streamTo(writer);
		eventloop.run();

		byte[] fileBytes = com.google.common.io.Files.toByteArray(tempFile);
		assertArrayEquals(bytes, fileBytes);
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testStreamReaderRecycle() throws IOException {
		final TestStreamConsumers.TestConsumerToList<ByteBuf> writer = new TestStreamConsumers.TestConsumerToList<ByteBuf>(eventloop) {
			@Override
			public void onData(ByteBuf item) {
				super.onData(item);
				item.recycle();
			}
		};

		reader.streamTo(writer);
		eventloop.run();

		assertEquals(CLOSED_WITH_ERROR, reader.getProducerStatus());
		assertEquals(CLOSED_WITH_ERROR, writer.getConsumerStatus());
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		eventloop = new Eventloop();
		executor = Executors.newCachedThreadPool();
		reader = new StreamFileReaderWithError(eventloop, executor, 1, Paths.get("test_data/in.dat"), 0, Long.MAX_VALUE);

	}

	// override send(ByteBuf item) with error
	public final static class StreamFileReaderWithError extends AbstractStreamProducer<ByteBuf> {
		private static final Logger logger = LoggerFactory.getLogger(StreamFileReader.class);

		private final ExecutorService executor;
		protected AsyncFile asyncFile;
		private Path path;

		protected final int bufferSize;
		protected long position;
		protected long length;

		protected boolean pendingAsyncOperation;

		public StreamFileReaderWithError(Eventloop eventloop, ExecutorService executor,
		                                 int bufferSize,
		                                 Path path, long position, long length) {
			super(eventloop);
			this.executor = checkNotNull(executor);
			this.bufferSize = bufferSize;
			this.path = path;
			this.position = position;
			this.length = length;
		}

		/**
		 * Returns new StreamFileReader for reading file segment
		 *
		 * @param eventloop  event loop in which it will work
		 * @param executor   executor in which file will be opened
		 * @param bufferSize size of buffer, size of data which can be read at once
		 * @param path       location of file
		 * @param position   position after which reader will read file
		 * @param length     number of elements for reading
		 */
		public static StreamFileReaderWithError readFileSegment(Eventloop eventloop, ExecutorService executor,
		                                                        int bufferSize,
		                                                        Path path, long position, long length) {
			return new StreamFileReaderWithError(eventloop, executor, bufferSize, path, position, length);
		}

		/**
		 * Returns new StreamFileReader for full reading file
		 *
		 * @param eventloop  event loop in which it will work
		 * @param executor   executor it which file will be opened
		 * @param bufferSize size of buffer, size of data which can be read at once
		 * @param path       location of file
		 */
		public static StreamFileReaderWithError readFileFully(Eventloop eventloop, ExecutorService executor,
		                                                      int bufferSize,
		                                                      Path path) {
			return new StreamFileReaderWithError(eventloop, executor, bufferSize, path, 0, Long.MAX_VALUE);
		}

		/**
		 * Returns new StreamFileReader for reading file after some position
		 *
		 * @param eventloop  event loop in which it will work
		 * @param executor   executor it which file will be opened
		 * @param bufferSize size of buffer, size of data which can be read at once
		 * @param path       location of file
		 * @param position   position after which reader will read file
		 */
		public static StreamFileReaderWithError readFileFrom(Eventloop eventloop, ExecutorService executor,
		                                                     int bufferSize,
		                                                     Path path, long position) {
			return new StreamFileReaderWithError(eventloop, executor, bufferSize, path, position, Long.MAX_VALUE);
		}

		protected void doFlush() {
			if (getProducerStatus().isClosed() || asyncFile == null)
				return;

			if (length == 0L) {
				doCleanup();
				sendEndOfStream();
				return;
			}

			final ByteBuf buf = ByteBufPool.allocate((int) min(bufferSize, length));

			asyncFile.read(buf, position, new ResultCallback<Integer>() {
				@Override
				public void onResult(Integer result) {
					if (getProducerStatus().isClosed()) {
						buf.recycle();
						doCleanup();
						return;
					}
					pendingAsyncOperation = false;
					if (result == -1) {
						buf.recycle();
						doCleanup();
						sendEndOfStream();
						return;
					} else {
						position += result;
						send(buf);
						if (length != Long.MAX_VALUE)
							length -= result;
					}
					if (isStatusReady())
						postFlush();
				}

				@Override
				public void onException(Exception e) {
					buf.recycle();
					doCleanup();
					closeWithError(e);
				}
			});
		}

		@Override
		protected void send(ByteBuf item) {
			item.flip();
			if (item.toString().equals("1")) {
				item.recycle();
				closeWithError(new Exception("Intentionally closed with exception"));
				return;
			}
			super.send(item);
		}

		protected void postFlush() {
			if (asyncFile == null || pendingAsyncOperation)
				return;
			pendingAsyncOperation = true;
			eventloop.post(new Runnable() {
				@Override
				public void run() {
					doFlush();
				}
			});
		}

		@Override
		public void onSuspended() {
			logger.trace("{}: downstream consumer {} suspended.", this, downstreamConsumer);
		}

		@Override
		public void onResumed() {
			postFlush();
		}

		@Override
		protected void onStarted() {
			if (asyncFile != null || pendingAsyncOperation)
				return;
			pendingAsyncOperation = true;
			AsyncFile.open(eventloop, executor, path, new OpenOption[]{READ}, new ResultCallback<AsyncFile>() {
				@Override
				public void onResult(AsyncFile file) {
					pendingAsyncOperation = false;
					asyncFile = file;
					postFlush();
				}

				@Override
				public void onException(Exception exception) {
					closeWithError(exception);
				}
			});
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onError(Exception e) {
			logger.error("{}: downstream consumer {} exception.", this, downstreamConsumer);
		}

		@Override
		protected void doCleanup() {
			if (asyncFile != null) {
				asyncFile.close(ignoreCompletionCallback());
				asyncFile = null;
			}
		}

		public long getPosition() {
			return position;
		}
	}
}