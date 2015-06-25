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
import com.google.common.reflect.TypeToken;
import io.datakernel.serializer.*;
import org.junit.Test;

import static io.datakernel.serializer.SerializerFactory.createBufferSerializerFactory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SerializerGenHppcMapTest {
	private static final SerializerFactory bufferSerializerFactory = createBufferSerializerFactory();

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return serializer.deserialize(input);
	}

	private static <T, K, V> BufferSerializer<T> getBufferSerializer(TypeToken<T> collectionTypeToken, Class<K> keyClass, Class<V> valueClass) {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGenHppcMap hppcMapSerializer = new SerializerGenHppcMap(collectionTypeToken.getRawType(), keyClass, valueClass);
		registry.register(collectionTypeToken.getRawType(), hppcMapSerializer);
		SerializerGen serializerGen = registry.serializer(collectionTypeToken);
		return bufferSerializerFactory.createBufferSerializer(serializerGen);
	}

	@Test
	public void testIntByteMap() throws Exception {
		BufferSerializer<IntByteMap> serializer = getBufferSerializer(new TypeToken<IntByteMap>() {
		}, int.class, byte.class);

		IntByteMap testMap1 = new IntByteOpenHashMap();
		IntByteMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, (byte) 10);
		testMap1.put(1, (byte) 11);
		IntByteMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testIntCharMap() throws Exception {
		BufferSerializer<IntCharMap> serializer = getBufferSerializer(new TypeToken<IntCharMap>() {
		}, int.class, char.class);

		IntCharMap testMap1 = new IntCharOpenHashMap();
		IntCharMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, '0');
		testMap1.put(1, '1');
		IntCharMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testByteIntMap() throws Exception {
		BufferSerializer<ByteIntMap> serializer = getBufferSerializer(new TypeToken<ByteIntMap>() {
		}, byte.class, int.class);
		ByteIntMap testMap1 = new ByteIntOpenHashMap();
		ByteIntMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put((byte) 0, 10);
		testMap1.put((byte) 1, 11);
		ByteIntMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testShortByteMap() throws Exception {
		BufferSerializer<ShortByteMap> serializer = getBufferSerializer(new TypeToken<ShortByteMap>() {
		}, short.class, byte.class);
		ShortByteMap testMap1 = new ShortByteOpenHashMap();
		ShortByteMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put((short) 0, (byte) 10);
		testMap1.put((short) 1, (byte) 11);
		ShortByteMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testByteLongMap() throws Exception {
		BufferSerializer<ByteLongMap> serializer = getBufferSerializer(new TypeToken<ByteLongMap>() {
		}, byte.class, long.class);

		ByteLongMap testMap1 = new ByteLongOpenHashMap();
		ByteLongMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put((byte) 0, 10);
		testMap1.put((byte) 1, 11);
		ByteLongMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testLongByteMap() throws Exception {
		BufferSerializer<LongByteMap> serializer = getBufferSerializer(new TypeToken<LongByteMap>() {
		}, long.class, byte.class);
		LongByteMap testMap1 = new LongByteOpenHashMap();
		LongByteMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, (byte) 10);
		testMap1.put(1, (byte) 11);
		LongByteMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testLongLongMap() throws Exception {
		BufferSerializer<LongLongMap> serializer = getBufferSerializer(new TypeToken<LongLongMap>() {
		}, long.class, long.class);
		LongLongMap testMap1 = new LongLongOpenHashMap();
		LongLongMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, 10);
		testMap1.put(1, 11);
		LongLongMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testLongFloatMap() throws Exception {
		BufferSerializer<LongFloatMap> serializer = getBufferSerializer(new TypeToken<LongFloatMap>() {
		}, long.class, float.class);
		LongFloatMap testMap1 = new LongFloatOpenHashMap();
		LongFloatMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, 10);
		testMap1.put(1, 11);
		LongFloatMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testDoubleDoubleMap() throws Exception {
		BufferSerializer<DoubleDoubleMap> serializer = getBufferSerializer(new TypeToken<DoubleDoubleMap>() {
		}, double.class, double.class);
		DoubleDoubleMap testMap1 = new DoubleDoubleOpenHashMap();
		DoubleDoubleMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, 10);
		testMap1.put(1, 11);
		DoubleDoubleMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test(expected = NullPointerException.class)
	public void testIntObjectMap() throws Exception {
		@SuppressWarnings("unused")
		BufferSerializer<IntObjectMap<String>> serializer = getBufferSerializer(new TypeToken<IntObjectMap<String>>() {
		}, int.class, Object.class);
	}
}
