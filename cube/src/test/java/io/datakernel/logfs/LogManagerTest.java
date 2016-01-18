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

import io.datakernel.asm.DefiningClassLoader;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.*;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogManagerTest {
	private static final long ONE_SECOND = 1000L;
	private static final long ONE_HOUR = 60 * 60 * ONE_SECOND;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private ExecutorService executor = Executors.newCachedThreadPool();
	private Path testDir;

	@Before
	public void setUp() throws Exception {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);

		testDir = temporaryFolder.newFolder().toPath();
		clearTestDir(testDir);

		executor = Executors.newCachedThreadPool();
	}

	@After
	public void after() {
		clearTestDir(testDir);
	}

	@Test
	public void testConsumer() throws Exception {
		SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider();
		NioEventloop eventloop = new NioEventloop(timeProvider);
		timeProvider.setTime(new LocalDateTime("1970-01-01T00:00:00").toDateTime(DateTimeZone.UTC).getMillis());
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, testDir);
		LogManager<String> logManager = new LogManagerImpl<>(eventloop, fileSystem, BufferSerializers.stringSerializer());
		new StreamProducers.OfIterator<>(eventloop, Arrays.asList("1", "2", "3").iterator())
				.streamTo(logManager.consumer("p1"));
		eventloop.run();
		timeProvider.setTime(new LocalDateTime("1970-01-01T00:50:00").toDateTime(DateTimeZone.UTC).getMillis());
		new StreamProducers.OfIterator<>(eventloop, Arrays.asList("4", "5", "6").iterator())
				.streamTo(logManager.consumer("p1"));
		eventloop.run();
		timeProvider.setTime(new LocalDateTime("1970-01-01T01:50:00").toDateTime(DateTimeZone.UTC).getMillis());
		new StreamProducers.OfIterator<>(eventloop, Arrays.asList("7", "8", "9").iterator())
				.streamTo(logManager.consumer("p1"));
		eventloop.run();

		StreamProducer<String> p1 = logManager.producer("p1", new LogFile("1970-01-01_00", 1), 0L, null);
		StreamConsumers.ToList<String> consumerToList = new StreamConsumers.ToList<>(eventloop);
		p1.streamTo(consumerToList);
		eventloop.run();
		System.out.println(consumerToList.getList());
//		System.out.println(p1.getLogPosition());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	public static class TestItem {
		@Serialize(order = 0)
		public String s;

		public TestItem(@Deserialize("s") String s) {
			this.s = s;
		}
	}

	@Test
	public void testSerializationError() throws Exception {
		SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider();
		NioEventloop eventloop = new NioEventloop(timeProvider);
		timeProvider.setTime(new LocalDateTime("1970-01-01T00:00:00").toDateTime(DateTimeZone.UTC).getMillis());
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, testDir);
		BufferSerializer<TestItem> serializer = SerializerBuilder.newDefaultInstance(new DefiningClassLoader()).create(TestItem.class);
		LogManager<TestItem> logManager = new LogManagerImpl<>(eventloop, fileSystem, serializer);
		LogStreamConsumer<TestItem> logConsumer = logManager.consumer("p1");
		new StreamProducers.OfIterator<>(eventloop, asList(new TestItem("a"), new TestItem(null),
				new TestItem("b")).iterator()).streamTo(logConsumer);
		eventloop.run();

		StreamProducer<TestItem> p1 = logManager.producer("p1", new LogFile("1970-01-01_00", 0), 0L, null);
		StreamConsumers.ToList<TestItem> consumerToList = new StreamConsumers.ToList<>(eventloop);
		p1.streamTo(consumerToList);
		eventloop.run();
		List<TestItem> resultList = consumerToList.getList();

		assertEquals(StreamStatus.END_OF_STREAM, logConsumer.getConsumerStatus()); // not closed with error
		assertEquals(1, resultList.size());
		assertEquals("a", resultList.get(0).s);
	}

	@Test
	public void test() throws Exception {
		final LocalDate testDate = new LocalDate(0);
		final String logPartition = "testLog";

		final SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider();
		final NioEventloop eventloop = new NioEventloop(timeProvider);

		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, testDir);
		final LogManagerImpl<String> logManager = new LogManagerImpl<>(eventloop, fileSystem, BufferSerializers.stringSerializer());
		final StreamSender<String> streamSender = new StreamSender<>(eventloop, false);
		streamSender.streamTo(logManager.consumer(logPartition));

		Map<Long, String> testData = new LinkedHashMap<>();
		testData.put(ONE_HOUR - 2 * ONE_SECOND, "1");
		testData.put(ONE_HOUR - ONE_SECOND, "2");
		testData.put(ONE_HOUR + ONE_SECOND, "3");
		testData.put(ONE_HOUR + 2 * ONE_SECOND, "4");
		testData.put(ONE_HOUR + ONE_HOUR / 2, "5");

		eventloop.keepAlive(true);
		executor.submit(eventloop);

		for (final Entry<Long, String> entry : testData.entrySet()) {
			eventloop.postConcurrently(new Runnable() {
				@Override
				public void run() {
					timeProvider.setTime(entry.getKey());
					long timestamp = eventloop.refreshTimestampAndGet();
					streamSender.trySend(entry.getValue());
					timeProvider.setTime(timestamp + LogManagerImpl.DEFAULT_FLUSH_DELAY + 1);
					eventloop.refreshTimestampAndGet();
				}
			});
			Thread.sleep(200L);
		}
		timeProvider.setTime(eventloop.currentTimeMillis() + LogManagerImpl.DEFAULT_FLUSH_DELAY * 2);
		eventloop.refreshTimestampAndGet();
		eventloop.postConcurrently(new Runnable() {
			@Override
			public void run() {
				streamSender.endStreaming();
			}
		});

		// Wait for ends Eventloop
		eventloop.keepAlive(false);
		while (eventloop.getThreadName() != null) {
			Thread.sleep(100L);
		}

		final StreamConsumers.ToList<String> consumerToList0 = new StreamConsumers.ToList<>(eventloop);
		final StreamConsumers.ToList<String> consumerToList1 = new StreamConsumers.ToList<>(eventloop);
		final StreamConsumers.ToList<String> consumerToList2 = new StreamConsumers.ToList<>(eventloop);

		logManager.producer(logPartition, new LogFile(testDate.toString() + "_00", 0), 0L, null)
				.streamTo(consumerToList0);
		eventloop.run();

		logManager.producer(logPartition, new LogFile(testDate.toString() + "_01", 0), 0L, null)
				.streamTo(consumerToList1);
		eventloop.run();

		logManager.producer(logPartition, new LogFile(testDate.toString() + "_02", 0), 0L, null)
				.streamTo(consumerToList2);
		eventloop.run();

		List<String> list00 = consumerToList0.getList();
		List<String> list01 = consumerToList1.getList();
		List<String> list02 = consumerToList2.getList();

		System.out.println(list00);
		System.out.println(list01);
		System.out.println(list02);

		assertTrue(list02.isEmpty());
		assertEquals(Arrays.asList("1", "2", "3", "4", "5"), list00);
		assertEquals(Arrays.asList("3", "4", "5"), list01);
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
}