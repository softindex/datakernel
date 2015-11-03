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

import com.google.gson.Gson;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducer;
import io.datakernel.stream.StreamProducers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.getPoolItemsString;
import static io.datakernel.stream.StreamStatus.CLOSED_WITH_ERROR;
import static io.datakernel.stream.StreamStatus.END_OF_STREAM;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

public class StreamGsonSerializerTest {

	public static class TestItem {
		public int x;
		public String str;

		public TestItem() {
		}

		public TestItem(int x, String str) {
			this.x = x;
			this.str = str;
		}

		@Override
		public String toString() {
			return "TestItem{x=" + x + ", str='" + str + '\'' + '}';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestItem testItem = (TestItem) o;

			if (x != testItem.x) return false;
			if (str != null ? !str.equals(testItem.str) : testItem.str != null) return false;

			return true;
		}

		@Override
		public int hashCode() {
			int result = x;
			result = 31 * result + (str != null ? str.hashCode() : 0);
			return result;
		}
	}

	@Before
	public void before() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(0, Integer.MAX_VALUE);
	}

	@Test
	public void test1() throws Exception {
		List<TestItem> items = asList(new TestItem(1, "item1"), new TestItem(1, "item2"), new TestItem(1, "item3"));

		NioEventloop eventloop = new NioEventloop();

		StreamGsonSerializer<TestItem> serializerStream = new StreamGsonSerializer<>(eventloop, new Gson(), TestItem.class, 1, 50, 0);

		StreamProducers.ofIterable(eventloop, items).streamTo(serializerStream.getInput());

		StreamGsonDeserializer<TestItem> deserializerStream = new StreamGsonDeserializer<>(eventloop, new Gson(), TestItem.class, 10);
		serializerStream.getOutput().streamTo(deserializerStream.getInput());

		StreamConsumers.ToList<TestItem> consumerToList = StreamConsumers.toList(eventloop);
		deserializerStream.getOutput().streamTo(consumerToList);
		eventloop.run();

		assertEquals(items, consumerToList.getList());
		assertEquals(END_OF_STREAM, serializerStream.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, serializerStream.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, deserializerStream.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, deserializerStream.getOutput().getProducerStatus());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testWithoutConsumer() {
		List<TestItem> items = asList(new TestItem(1, "item1"), new TestItem(1, "item2"), new TestItem(1, "item3"));

		NioEventloop eventloop = new NioEventloop();

		StreamGsonSerializer<TestItem> serializerStream = new StreamGsonSerializer<>(eventloop, new Gson(), TestItem.class, 1, 50, 0);

		StreamProducers.ofIterable(eventloop, items).streamTo(serializerStream.getInput());
		eventloop.run();

		StreamGsonDeserializer<TestItem> deserializerStream = new StreamGsonDeserializer<>(eventloop, new Gson(), TestItem.class, 10);
		serializerStream.getOutput().streamTo(deserializerStream.getInput());
		eventloop.run();

		StreamConsumers.ToList<TestItem> consumerToList = StreamConsumers.toList(eventloop);
		deserializerStream.getOutput().streamTo(consumerToList);
		eventloop.run();

		assertEquals(items, consumerToList.getList());
		assertEquals(END_OF_STREAM, serializerStream.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, serializerStream.getOutput().getProducerStatus());

		assertEquals(END_OF_STREAM, deserializerStream.getInput().getConsumerStatus());
		assertEquals(END_OF_STREAM, deserializerStream.getOutput().getProducerStatus());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}

	@Test
	public void testProducerWithError() throws Exception {
		NioEventloop eventloop = new NioEventloop();

		List<TestItem> list = new ArrayList<>();

		StreamGsonSerializer<TestItem> serializerStream = new StreamGsonSerializer<>(eventloop, new Gson(), TestItem.class, 1, 50, 0);

		StreamProducer<TestItem> producer = StreamProducers.concat(eventloop,
				StreamProducers.ofValue(eventloop, new TestItem(1, "item1")),
				StreamProducers.ofValue(eventloop, new TestItem(1, "item2")),
				StreamProducers.<TestItem>closingWithError(eventloop, new Exception("Test Exception"))
		);
		producer.streamTo(serializerStream.getInput());

		StreamGsonDeserializer<TestItem> deserializerStream = new StreamGsonDeserializer<>(eventloop, new Gson(), TestItem.class, 10);
		serializerStream.getOutput().streamTo(deserializerStream.getInput());

		StreamConsumers.ToList<TestItem> consumerToList = StreamConsumers.toList(eventloop, list);
		deserializerStream.getOutput().streamTo(consumerToList);

		eventloop.run();

		assertEquals(CLOSED_WITH_ERROR, serializerStream.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, serializerStream.getOutput().getProducerStatus());

		assertEquals(CLOSED_WITH_ERROR, deserializerStream.getInput().getConsumerStatus());
		assertEquals(CLOSED_WITH_ERROR, deserializerStream.getOutput().getProducerStatus());

		assertEquals(getPoolItemsString(), ByteBufPool.getCreatedItems(), ByteBufPool.getPoolItems());
	}
}
