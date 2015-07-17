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

import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.serializer.asm.BufferSerializers;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.time.SettableCurrentTimeProvider;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LogManagerTest {
	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testConsumer() throws Exception {
		SettableCurrentTimeProvider timeProvider = new SettableCurrentTimeProvider();
		NioEventloop eventloop = new NioEventloop(timeProvider);
		timeProvider.setTime(new LocalDateTime("1970-01-01T00:00:00").toDateTime(DateTimeZone.UTC).getMillis());
		ExecutorService executor = Executors.newCachedThreadPool();
		Path dir = temporaryFolder.newFolder().toPath();
		LogFileSystemImpl fileSystem = new LogFileSystemImpl(eventloop, executor, dir);
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
	}

	@Test
	public void testProducer() throws Exception {

	}
}