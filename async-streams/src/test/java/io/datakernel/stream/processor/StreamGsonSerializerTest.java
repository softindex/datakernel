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
import io.datakernel.eventloop.NioEventloop;
import io.datakernel.stream.StreamConsumers;
import io.datakernel.stream.StreamProducers;
import org.junit.Test;

import java.util.List;

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

	@Test
	public void test1() throws Exception {
		List<TestItem> items = asList(new TestItem(1, "item1"), new TestItem(1, "item2"), new TestItem(1, "item3"));

		NioEventloop eventloop = new NioEventloop();

		StreamGsonSerializer<TestItem> serializerStream = new StreamGsonSerializer<>(eventloop, new Gson(), TestItem.class, 1, 50, 0);

		StreamProducers.ofIterable(eventloop, items).streamTo(serializerStream);

		StreamGsonDeserializer<TestItem> deserializerStream = new StreamGsonDeserializer<>(eventloop, new Gson(), TestItem.class, 10);
		serializerStream.streamTo(deserializerStream);

		StreamConsumers.ToList<TestItem> consumerToList = StreamConsumers.toList(eventloop);
		deserializerStream.streamTo(consumerToList);
		eventloop.run();

		assertEquals(items, consumerToList.getList());
	}

}