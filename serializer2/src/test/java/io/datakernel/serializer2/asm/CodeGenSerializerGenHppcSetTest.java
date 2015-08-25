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

package io.datakernel.serializer2.asm;

import com.carrotsearch.hppc.*;
import io.datakernel.serializer2.*;
import io.datakernel.serializer2.annotations.Serialize;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodeGenSerializerGenHppcSetTest {
	private static final SerializerCodeGenFactory bufferSerializerFactory = SerializerCodeGenFactory.createBufferSerializerFactory();

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return serializer.deserialize(input);
	}

	private static <T, V> BufferSerializer<T> getBufferSerializer(Class<?> collectionType, Class<V> valueClass) {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(collectionType, SerializerGenHppcSet.serializerGenBuilder(collectionType, valueClass));
		SerializerGen serializerGen = registry.serializer(collectionType);
		return bufferSerializerFactory.createBufferSerializer(serializerGen);
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
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(ObjectSet.class, SerializerGenHppcSet.serializerGenBuilder(ObjectSet.class, Object.class));
		SerializerGen serializerGen = registry.serializer(ObjectSetStringHolder.class);
		BufferSerializer<ObjectSetStringHolder> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		ObjectSetStringHolder testData1 = new ObjectSetStringHolder();
		testData1.set = new ObjectOpenHashSet<>();
		ObjectSetStringHolder testData2 = doTest(testData1, bufferSerializer);
		assertNotNull(testData2.set);
		assertEquals(testData1.set, testData2.set);

	}
}
