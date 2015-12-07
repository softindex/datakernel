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

package io.datakernel.serializer.asm;

import com.carrotsearch.hppc.*;
import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodeGenSerializerGenHppcSetTest {

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return serializer.deserialize(input);
	}

	private static <T, V> BufferSerializer<T> getBufferSerializer(Class<?> collectionType, Class<V> valueClass) {
		return SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(collectionType, SerializerGenHppcSet.serializerGenBuilder(collectionType, valueClass))
				.create(collectionType);
	}

	@Test
	public void testByteSet() throws Exception {
		BufferSerializer<ByteSet> serializer = getBufferSerializer(ByteSet.class, byte.class);

		ByteSet test1 = new ByteOpenHashSet();
		ByteSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add((byte) 10);
		test1.add((byte) 11);

		ByteSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testCharSet() throws Exception {
		BufferSerializer<CharSet> serializer = getBufferSerializer(CharSet.class, char.class);
		CharSet test1 = new CharOpenHashSet();
		CharSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add('a');
		test1.add('b');
		CharSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testShortSet() throws Exception {
		BufferSerializer<ShortSet> serializer = getBufferSerializer(ShortSet.class, short.class);

		ShortSet test1 = new ShortOpenHashSet();
		ShortSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add((short) 10);
		test1.add((short) 11);
		ShortSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testIntSet() throws Exception {
		BufferSerializer<IntSet> serializer = getBufferSerializer(IntSet.class, int.class);
		IntSet test1 = new IntOpenHashSet();
		IntSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);
		IntSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testLongSet() throws Exception {
		BufferSerializer<LongSet> serializer = getBufferSerializer(LongSet.class, long.class);
		LongSet test1 = new LongOpenHashSet();
		LongSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);
		LongSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testFloatSet() throws Exception {
		BufferSerializer<FloatSet> serializer = getBufferSerializer(FloatSet.class, float.class);
		FloatSet test1 = new FloatOpenHashSet();
		FloatSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);
		FloatSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testDoubleSet() throws Exception {
		BufferSerializer<DoubleSet> serializer = getBufferSerializer(DoubleSet.class, double.class);
		DoubleSet test1 = new DoubleOpenHashSet();
		DoubleSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);
		DoubleSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	public static class ObjectSetStringHolder {
		@Serialize(order = 0)
		public ObjectSet<String> set;
	}

	@Test
	public void testObjectSet() throws Exception {
		BufferSerializer<ObjectSetStringHolder> bufferSerializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ObjectSet.class, SerializerGenHppcSet.serializerGenBuilder(ObjectSet.class, Object.class))
				.create(ObjectSetStringHolder.class);

		ObjectSetStringHolder testData1 = new ObjectSetStringHolder();
		testData1.set = new ObjectOpenHashSet<>();
		ObjectSetStringHolder testData2 = doTest(testData1, bufferSerializer);
		assertNotNull(testData2.set);
		assertEquals(testData1.set, testData2.set);

	}

	public static class TestObject {
		@Serialize(order = 0)
		public String i;

		public TestObject(String i) {
			this.i = i;
		}

		public TestObject() {}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestObject that = (TestObject) o;

			return !(i != null ? !i.equals(that.i) : that.i != null);

		}

		@Override
		public int hashCode() {
			return i != null ? i.hashCode() : 0;
		}
	}

	public static class ManyMapHolder {
		@Serialize(order = 0)
		public ObjectSet<String> set;
		@Serialize(order = 1)
		public ObjectSet<Integer> setI;
		@Serialize(order = 2)
		public ObjectSet<TestObject> setO;
	}

	@Test
	public void testMult() {
		BufferSerializer<ManyMapHolder> bufferSerializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ObjectSet.class, SerializerGenHppcSet.serializerGenBuilder(ObjectSet.class, Object.class))
				.create(ManyMapHolder.class);

		ManyMapHolder testMap1 = new ManyMapHolder();
		testMap1.set = new ObjectOpenHashSet<>();
		testMap1.setI = new ObjectOpenHashSet<>();
		testMap1.setO = new ObjectOpenHashSet<>();
		ManyMapHolder testMap2 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap1.set);
		assertNotNull(testMap1.setI);
		assertNotNull(testMap1.setO);
		assertEquals(testMap1.set, testMap2.set);
		assertEquals(testMap1.setI, testMap2.setI);
		assertEquals(testMap1.setO, testMap2.setO);

		testMap1.set.add("123");
		testMap1.set.add("345");

		testMap1.setI.add(5);
		testMap1.setI.add(25);

		testMap1.setO.add(new TestObject("2"));
		testMap1.setO.add(new TestObject("22"));
		ManyMapHolder testMap3 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap3.set);
		assertNotNull(testMap3.setI);
		assertNotNull(testMap3.setO);
		assertEquals(testMap1.set, testMap3.set);
		assertEquals(testMap1.setI, testMap3.setI);
		assertEquals(testMap1.setO, testMap3.setO);

	}
}
