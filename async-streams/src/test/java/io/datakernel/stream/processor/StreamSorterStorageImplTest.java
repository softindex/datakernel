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

import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.AbstractStreamProducer;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamSorterStorageImplTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void testStreamStorage() throws Exception {
		NioEventloop eventloop = new NioEventloop();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5, 6, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(111));

		File workPath = tempFolder.newFolder("tmp");
		StreamMergeSorterStorageImpl<Integer> storage = new StreamMergeSorterStorageImpl<>(eventloop, executor, intSerializer(), Paths.get(workPath.getAbsolutePath(), "%d.part"), 64);

		int firstStorage = storage.nextPartition();
		source1.streamTo(storage.streamWriter());

		int secondStorage = storage.nextPartition();
		source2.streamTo(storage.streamWriter());

		eventloop.run();

		assertTrue(((AbstractStreamProducer)source1).getStatus() == AbstractStreamProducer.CLOSED);
		assertTrue(((AbstractStreamProducer)source2).getStatus() == AbstractStreamProducer.CLOSED);

		StreamConsumers.ToList<Integer> consumer1 = StreamConsumers.toListOneByOne(eventloop);
		StreamConsumers.ToList<Integer> consumer2 = StreamConsumers.toListRandomlySuspending(eventloop);
		storage.streamReader(firstStorage).streamTo(consumer1);
		storage.streamReader(secondStorage).streamTo(consumer2);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
		assertEquals(asList(111), consumer2.getList());
		storage.cleanup();
		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

}