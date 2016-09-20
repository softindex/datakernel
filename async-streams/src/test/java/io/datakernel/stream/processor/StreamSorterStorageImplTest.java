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
import io.datakernel.eventloop.Eventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.datakernel.async.AsyncCallbacks.ignoreCompletionCallback;
import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.helper.TestUtils.doesntHaveFatals;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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
		Eventloop eventloop = Eventloop.create();
		ExecutorService executor = Executors.newCachedThreadPool();

		StreamProducer<Integer> source1 = StreamProducers.ofIterable(eventloop, asList(1, 2, 3, 4, 5, 6, 7));
		StreamProducer<Integer> source2 = StreamProducers.ofIterable(eventloop, asList(111));

		File workPath = tempFolder.newFolder("tmp");
		StreamMergeSorterStorageImpl<Integer> storage = StreamMergeSorterStorageImpl.create(eventloop, executor, intSerializer(), Paths.get(workPath.getAbsolutePath(), "%d.part"), 64);

		int firstStorage = storage.write(source1, ignoreCompletionCallback());
		int secondStorage = storage.write(source2, ignoreCompletionCallback());

		eventloop.run();

		assertEquals(END_OF_STREAM, source1.getProducerStatus());
		assertEquals(END_OF_STREAM, source2.getProducerStatus());

		TestStreamConsumers.TestConsumerToList<Integer> consumer1 = TestStreamConsumers.toListOneByOne(eventloop);
		TestStreamConsumers.TestConsumerToList<Integer> consumer2 = TestStreamConsumers.toListRandomlySuspending(eventloop);
		storage.read(firstStorage, ignoreCompletionCallback()).streamTo(consumer1);
		storage.read(secondStorage, ignoreCompletionCallback()).streamTo(consumer2);
		eventloop.run();

		assertEquals(asList(1, 2, 3, 4, 5, 6, 7), consumer1.getList());
		assertEquals(asList(111), consumer2.getList());
		storage.cleanup(Arrays.asList(firstStorage, secondStorage));
		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
		assertThat(eventloop, doesntHaveFatals());
	}

}