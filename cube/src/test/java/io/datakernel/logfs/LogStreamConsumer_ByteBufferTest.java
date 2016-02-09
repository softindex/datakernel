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

package io.datakernel.logfs;

import io.datakernel.async.CompletionCallback;
import io.datakernel.async.ForwardingResultCallback;
import io.datakernel.async.ResultCallback;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.*;
import io.datakernel.stream.file.StreamFileReader;
import io.datakernel.stream.file.StreamFileWriter;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.postExceptionConcurrently;
import static io.datakernel.async.AsyncCallbacks.postResultConcurrently;
import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.logfs.LogManagerImpl.DEFAULT_FILE_SWITCH_PERIOD;
import static org.junit.Assert.assertEquals;

public class LogStreamConsumer_ByteBufferTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor = Executors.newCachedThreadPool();
	private final List<StreamFileWriter> listWriter = new ArrayList<>();
	private Path testDir;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		testDir = temporaryFolder.newFolder().toPath();
		clearTestDir(testDir);

		listWriter.clear();
		executor = Executors.newCachedThreadPool();
	}

	@After
	public void after() {
		clearTestDir(testDir);
	}

	@Test
	public void testProducerWithError() throws InterruptedException {
		final SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider();
		final Eventloop eventloop = new Eventloop(timeProvider);
		timeProvider.setTime(new LocalDateTime("1970-01-01T00:59:59").toDateTime(DateTimeZone.UTC).getMillis());
		final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);

		final LogFileSystem fileSystem = new SimpleLogFileSystem(eventloop, executor, testDir, listWriter);

		final StreamProducer<ByteBuf> producer = new ScheduledProducer(eventloop) {
			@Override
			protected void doProduce() {
				if (++nom == 9) {
					closeWithError(new Exception("Test Exception"));
					return;
				}
				if (nom == 4) {
					timeProvider.setTime(new LocalDateTime("1970-01-01T01:00:00").toDateTime(DateTimeZone.UTC).getMillis());
				}
				send(ByteBuf.wrap(new byte[]{1}));
				onConsumerSuspended();
				eventloop.schedule(5L, new Runnable() {
					@Override
					public void run() {
						onConsumerResumed();
					}
				});
			}
		};

		String streamId = "newId";
		final CallbackСallCount callbackСallCount = new CallbackСallCount();
		CompletionCallback completionCallback = new CompletionCallback() {
			@Override
			public void onException(Exception exception) {
				callbackСallCount.incrementOnError();
			}

			@Override
			public void onComplete() {
				callbackСallCount.incrementOnComplite();
			}
		};
		LogStreamConsumer_ByteBuffer logStreamConsumerByteBuffer =
				new LogStreamConsumer_ByteBuffer(eventloop, DATE_TIME_FORMATTER, DEFAULT_FILE_SWITCH_PERIOD,
						fileSystem, streamId);
		logStreamConsumerByteBuffer.setCompletionCallback(completionCallback);

		producer.streamTo(logStreamConsumerByteBuffer);
		eventloop.run();

		assertEquals(callbackСallCount.isCalledOnce(), true);
		assertEquals(callbackСallCount.isCalledOnError(), true);
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, producer.getProducerStatus());
		assertEquals(StreamStatus.CLOSED_WITH_ERROR, logStreamConsumerByteBuffer.getConsumerStatus());
		assertEquals(listWriter.size(), 2);

		assertEquals(getLast(listWriter).getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testFileSystemWithError() throws InterruptedException {
		final Eventloop eventloop = new Eventloop();
		final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd_HH").withZone(DateTimeZone.UTC);

		final LogFileSystem fileSystem = new SimpleLogFileSystem(eventloop, executor, testDir, listWriter) {
			@Override
			public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
				try {
					StreamFileWriter writer = StreamFileWriter.create(eventloop, executor, path(logPartition, logFile));
					listWriter.add(writer);
					writer.onProducerError(new Exception("Test Exception"));
					producer.streamTo(writer);
					writer.setFlushCallback(callback);
				} catch (IOException e) {
					callback.onException(e);
				}
			}
		};

		StreamProducer<ByteBuf> producer = new ScheduledProducer(eventloop) {

			@Override
			protected void doProduce() {
				if (++nom == 9) {
					sendEndOfStream();
					return;
				}
				send(ByteBuf.wrap(new byte[]{1}));
				onConsumerSuspended();
				eventloop.schedule(100L, new Runnable() {
					@Override
					public void run() {
						onConsumerResumed();
					}
				});
			}
		};

		String streamId = "newId";
		final CallbackСallCount callbackСallCount = new CallbackСallCount();
		CompletionCallback completionCallback = new CompletionCallback() {
			@Override
			public void onException(Exception exception) {
				callbackСallCount.incrementOnError();
			}

			@Override
			public void onComplete() {
				callbackСallCount.incrementOnComplite();
			}
		};
		LogStreamConsumer_ByteBuffer logStreamConsumerByteBuffer =
				new LogStreamConsumer_ByteBuffer(eventloop, DATE_TIME_FORMATTER, DEFAULT_FILE_SWITCH_PERIOD,
						fileSystem, streamId);
		logStreamConsumerByteBuffer.setCompletionCallback(completionCallback);

		producer.streamTo(logStreamConsumerByteBuffer);
		eventloop.run();

		assertEquals(callbackСallCount.isCalledOnce(), true);
		assertEquals(callbackСallCount.isCalledOnComplite(), true);
		assertEquals(StreamStatus.END_OF_STREAM, producer.getProducerStatus());
		assertEquals(StreamStatus.END_OF_STREAM, logStreamConsumerByteBuffer.getConsumerStatus());
		for (int i = 0; i < listWriter.size() - 1; i++) {
			assertEquals(listWriter.get(i).getConsumerStatus(), StreamStatus.END_OF_STREAM);
		}
		assertEquals(getLast(listWriter).getConsumerStatus(), StreamStatus.CLOSED_WITH_ERROR);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());

	}

	public static <T> T getLast(List<T> list) {
		return list.get(list.size() - 1);
	}

	private static void clearTestDir(Path testDir) {
		if (testDir == null)
			return;
		File directory = testDir.toFile();
		if (directory == null || !directory.isDirectory())
			return;
		File[] files = directory.listFiles();
		if (files == null)
			return;

		for (File file : files) {
			file.delete();
		}
		directory.delete();
	}

	public static class SimpleLogFileSystem implements LogFileSystem {
		private final Eventloop eventloop;
		private final ExecutorService executorService;
		private final Path dir;
		private final List<StreamFileWriter> listWriter;

		/**
		 * Constructs a log file system, that runs in the given event loop, runs blocking IO operations in the specified executor,
		 * stores logs in the given directory.
		 *
		 * @param eventloop       event loop, which log file system is to run
		 * @param executorService executor for blocking IO operations
		 * @param dir             directory for storing log files
		 * @param listWriter
		 */
		public SimpleLogFileSystem(Eventloop eventloop, ExecutorService executorService,
		                           Path dir, List<StreamFileWriter> listWriter) {
			this.eventloop = eventloop;
			this.executorService = executorService;
			this.dir = dir;
			this.listWriter = listWriter;
		}

		public SimpleLogFileSystem(Eventloop eventloop, ExecutorService executorService, Path dir, String logName, List<StreamFileWriter> listWriter) {
			this.eventloop = eventloop;
			this.executorService = executorService;
			this.listWriter = listWriter;
			this.dir = dir.resolve(logName);
		}

		private static final class PartitionAndFile {
			private final String logPartition;
			private final LogFile logFile;

			private PartitionAndFile(String logPartition, LogFile logFile) {
				this.logPartition = logPartition;
				this.logFile = logFile;
			}
		}

		private static PartitionAndFile parse(Path path) {
			String s = path.getFileName().toString();
			int index1 = s.indexOf('.');
			if (index1 == -1)
				return null;
			String name = s.substring(0, index1);
			if (name.isEmpty())
				return null;
			s = s.substring(index1 + 1);
			if (!s.endsWith(".log"))
				return null;
			s = s.substring(0, s.length() - 4);
			int n = 0;
			int index2 = s.indexOf('-');
			String logPartition;
			if (index2 != -1) {
				logPartition = s.substring(0, index2);
				try {
					n = Integer.parseInt(s.substring(index2 + 1));
				} catch (NumberFormatException e) {
					return null;
				}
			} else {
				logPartition = s;
			}
			if (logPartition.isEmpty())
				return null;
			return new PartitionAndFile(logPartition, new LogFile(name, n));
		}

		protected Path path(String logPartition, LogFile logFile) {
			String filename = logFile.getName() + "." + logPartition + (logFile.getN() != 0 ? "-" + logFile.getN() : "") + ".log";
			return dir.resolve(filename);
		}

		@Override
		public void makeUniqueLogFile(String logPartition, final String logName, final ResultCallback<LogFile> callback) {
			list(logPartition, new ForwardingResultCallback<List<LogFile>>(callback) {
				@Override
				public void onResult(List<LogFile> logFiles) {
					int chunkN = 0;
					for (LogFile logFile : logFiles) {
						if (logFile.getName().equals(logName)) {
							chunkN = Math.max(chunkN, logFile.getN() + 1);
						}
					}
					callback.onResult(new LogFile(logName, chunkN));
				}
			});
		}

		@Override
		public void list(final String logPartition, final ResultCallback<List<LogFile>> callback) {
			final Eventloop.ConcurrentOperationTracker concurrentOperationTracker = eventloop.startConcurrentOperation();
			executorService.execute(new Runnable() {
				@Override
				public void run() {
					final List<LogFile> entries = new ArrayList<>();

					try {
						Files.createDirectories(dir);
						Files.walkFileTree(dir, new FileVisitor<Path>() {
							@Override
							public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
								return FileVisitResult.CONTINUE;
							}

							@Override
							public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
								PartitionAndFile partitionAndFile = parse(file);
								if (partitionAndFile != null && partitionAndFile.logPartition.equals(logPartition)) {
									entries.add(partitionAndFile.logFile);
								}
								return FileVisitResult.CONTINUE;
							}

							@Override
							public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
								if (exc != null) {
//									logger.error("visitFileFailed error", exc);
								}
								return FileVisitResult.CONTINUE;
							}

							@Override
							public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
								if (exc != null) {
//									logger.error("postVisitDirectory error", exc);
								}
								return FileVisitResult.CONTINUE;
							}
						});
						postResultConcurrently(eventloop, callback, entries);
					} catch (IOException e) {
						// TODO ?
//						logger.error("walkFileTree error", e);
						postExceptionConcurrently(eventloop, callback, e);
					}

					concurrentOperationTracker.complete();
				}
			});
		}

		@Override
		public void read(String logPartition, LogFile logFile, long startPosition, StreamConsumer<ByteBuf> consumer) {
			try {
				StreamFileReader reader = StreamFileReader.readFileFrom(eventloop, executorService, 1024 * 1024,
						path(logPartition, logFile), startPosition);
				reader.streamTo(consumer);
			} catch (IOException e) {
				StreamProducers.<ByteBuf>closingWithError(eventloop, e).streamTo(consumer);
			}
		}

		@Override
		public void write(String logPartition, LogFile logFile, StreamProducer<ByteBuf> producer, CompletionCallback callback) {
			try {
				StreamFileWriter writer = StreamFileWriter.create(eventloop, executorService, path(logPartition, logFile));
				producer.streamTo(writer);
				writer.setFlushCallback(callback);
				listWriter.add(writer);
			} catch (IOException e) {
				callback.onException(e);
			}
		}
	}

	class ScheduledProducer extends AbstractStreamProducer<ByteBuf> {
		protected int nom;

		protected ScheduledProducer(Eventloop eventloop) {
			super(eventloop);
		}

		@Override
		protected void onResumed() {
			resumeProduce();
		}

		@Override
		protected void onStarted() {
			produce();
		}

		@Override
		protected void onDataReceiverChanged() {

		}

		@Override
		protected void onSuspended() {

		}
	}

	class CallbackСallCount {
		private int onError;
		private int onComplite;

		public CallbackСallCount() {
			this.onError = 0;
			this.onComplite = 0;
		}

		public void incrementOnError() {
			onError++;
		}

		public void incrementOnComplite() {
			onComplite++;
		}

		public boolean isCalledOnce() {
			return (onComplite == 1 && onError == 0) || (onComplite == 0 && onError == 1);
		}

		public boolean isCalledOnError() {
			return onError == 1;
		}

		public boolean isCalledOnComplite() {
			return onComplite == 1;
		}
	}
}
