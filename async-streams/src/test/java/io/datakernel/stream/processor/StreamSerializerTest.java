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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import io.datakernel.stream.TestStreamConsumers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static io.datakernel.stream.AbstractStreamConsumer.StreamConsumerStatus;
import static io.datakernel.stream.AbstractStreamProducer.StreamProducerStatus;
import static io.datakernel.stream.processor.Utils.assertStatus;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamSerializerTest {

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test1() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(10, 20, 30, 40));
		StreamBinarySerializer<Integer> serializerStream = new StreamBinarySerializer<>(eventloop, intSerializer(), 14, 14, 0, false);
		TestStreamConsumers.TestConsumerToList<ByteBuf> consumer = TestStreamConsumers.toListRandomlySuspending(eventloop);

		source.streamTo(serializerStream);
		serializerStream.streamTo(consumer);

		eventloop.run();
		List<ByteBuf> result = consumer.getList();
		Assert.assertEquals(2, result.size());
		Assert.assertEquals(15, result.get(0).limit());
		Assert.assertEquals(5, result.get(1).limit());
		Assert.assertEquals(10, result.get(0).array()[4]);
		Assert.assertEquals(20, result.get(0).array()[9]);
		Assert.assertEquals(30, result.get(0).array()[14]);
		Assert.assertEquals(40, result.get(1).array()[4]);
		for (ByteBuf buf : result) {
			buf.recycle();
		}

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
		assertStatus(StreamConsumerStatus.CLOSED, serializerStream.getUpstreamConsumer());
		assertStatus(StreamProducerStatus.END_OF_STREAM, serializerStream.getDownstreamProducer());
	}

	@Test
	public void test2() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		List<Integer> list = new ArrayList<>();
		StreamProducer<Integer> source = StreamProducers.ofIterable(eventloop, asList(1, 2, 3));
		StreamBinarySerializer<Integer> serializerStream = new StreamBinarySerializer<>(eventloop, intSerializer(), 1, StreamBinarySerializer.MAX_SIZE, 0, false);
		StreamBinaryDeserializer<Integer> deserializerStream = new StreamBinaryDeserializer<>(eventloop, intSerializer(), 12);
		TestStreamConsumers.TestConsumerToList<Integer> consumer = TestStreamConsumers.toListOneByOne(eventloop, list);

		source.streamTo(serializerStream);
		serializerStream.streamTo(deserializerStream);
		deserializerStream.streamTo(consumer);

		eventloop.run();
		assertEquals(asList(1, 2, 3), consumer.getList());
		assertStatus(StreamProducerStatus.END_OF_STREAM, source);

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());

		assertStatus(StreamConsumerStatus.CLOSED, serializerStream.getUpstreamConsumer());
		assertStatus(StreamProducerStatus.END_OF_STREAM, serializerStream.getDownstreamProducer());

		assertStatus(StreamConsumerStatus.CLOSED, deserializerStream.getUpstreamConsumer());
		assertStatus(StreamProducerStatus.END_OF_STREAM, deserializerStream.getDownstreamProducer());
	}

}
