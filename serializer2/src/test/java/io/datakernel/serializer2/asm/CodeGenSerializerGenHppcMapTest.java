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

public class CodeGenSerializerGenHppcMapTest {
	private static final SerializerCodeGenFactory bufferSerializerFactory = SerializerCodeGenFactory.createBufferSerializerFactory();

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return serializer.deserialize(input);
	}

	private static <T, K, V> BufferSerializer<T> getBufferSerializer(Class<?> collectionType, Class<K> keyClass, Class<V> valueClass) {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(collectionType, SerializerGenHppcMap.serializerGenBuilder(collectionType, keyClass, valueClass));
		SerializerGen serializerGen = registry.serializer(collectionType);
		return bufferSerializerFactory.createBufferSerializer(serializerGen);
	}

	@Test
	public void testIntByteMap() throws Exception {
		BufferSerializer<IntByteMap> serializer = getBufferSerializer(IntByteMap.class, int.class, byte.class);

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
		BufferSerializer<IntCharMap> serializer = getBufferSerializer(IntCharMap.class, int.class, char.class);

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
		BufferSerializer<ByteIntMap> serializer = getBufferSerializer(ByteIntMap.class, byte.class, int.class);
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
		BufferSerializer<ShortByteMap> serializer = getBufferSerializer(ShortByteMap.class, short.class, byte.class);
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
		BufferSerializer<ByteLongMap> serializer = getBufferSerializer(ByteLongMap.class, byte.class, long.class);

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
		BufferSerializer<LongByteMap> serializer = getBufferSerializer(LongByteMap.class, long.class, byte.class);
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
		BufferSerializer<LongLongMap> serializer = getBufferSerializer(LongLongMap.class, long.class, long.class);
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
		BufferSerializer<LongFloatMap> serializer = getBufferSerializer(LongFloatMap.class, long.class, float.class);
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
		BufferSerializer<DoubleDoubleMap> serializer = getBufferSerializer(DoubleDoubleMap.class, double.class, double.class);
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

	public static class MapIntObjectHolder {
		@Serialize(order = 0)
		public IntObjectMap<String> map;
	}

	@Test
	public void testIntObjectMap() {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(IntObjectMap.class, SerializerGenHppcMap.serializerGenBuilder(IntObjectMap.class, int.class, Object.class));
		SerializerGen serializerGen = registry.serializer(MapIntObjectHolder.class);
		BufferSerializer<MapIntObjectHolder> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		MapIntObjectHolder testMap1 = new MapIntObjectHolder();
		testMap1.map = new IntObjectOpenHashMap<>();
		MapIntObjectHolder testMap2 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap2.map);
		assertEquals(testMap1.map, testMap2.map);

		testMap1.map.put(10, "123");
		testMap1.map.put(11, "345");
		MapIntObjectHolder testMap3 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap3.map);
		assertEquals(testMap1.map, testMap3.map);
	}

	public static class MapObjectShortHolder {
		@Serialize(order = 0)
		public ObjectShortMap<String> map;
	}

	@Test
	public void testObjectShortMap() throws Exception {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(ObjectShortMap.class, SerializerGenHppcMap.serializerGenBuilder(ObjectShortMap.class, Object.class, short.class));
		SerializerGen serializerGen = registry.serializer(MapObjectShortHolder.class);
		BufferSerializer<MapObjectShortHolder> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		MapObjectShortHolder testMap1 = new MapObjectShortHolder();
		testMap1.map = new ObjectShortOpenHashMap<>();
		MapObjectShortHolder testMap2 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap2.map);
		assertEquals(testMap1.map, testMap2.map);

		testMap1.map.put("0", (short) 10);
		testMap1.map.put("1", (short) 11);
		MapObjectShortHolder testMap3 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap3.map);
		assertEquals(testMap1.map, testMap3.map);
	}

	public static class MapObjectObjectHolder {
		@Serialize(order = 0)
		public ObjectObjectMap<String, String> map;
	}

	@Test
	public void testObjectObjectMap() throws Exception {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.register(ObjectObjectMap.class, SerializerGenHppcMap.serializerGenBuilder(ObjectObjectMap.class, Object.class, Object.class));
		SerializerGen serializerGen = registry.serializer(MapObjectObjectHolder.class);
		BufferSerializer<MapObjectObjectHolder> bufferSerializer = bufferSerializerFactory.createBufferSerializer(serializerGen);

		MapObjectObjectHolder testMap1 = new MapObjectObjectHolder();
		testMap1.map = new ObjectObjectOpenHashMap<>();
		MapObjectObjectHolder testMap2 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap2.map);
		assertEquals(testMap1.map, testMap2.map);

		testMap1.map.put("0", "10");
		testMap1.map.put("1", "11");
		MapObjectObjectHolder testMap3 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap3.map);
		assertEquals(testMap1.map, testMap3.map);
	}
}
