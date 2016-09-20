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

package io.datakernel.serializer;

import com.carrotsearch.hppc.*;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.asm.SerializerGenHppcMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodeGenSerializerGenHppcMapTest {

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer) {
		byte[] array = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(array);
		serializer.serialize(buf, testData1);
		return serializer.deserialize(buf);
	}

	private static <T> BufferSerializer<T> getBufferSerializer(Class<T> collectionType) {
		return SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withHppcSupport()
				.build(collectionType);
	}

	@Test
	public void testIntByteMap() throws Exception {
		BufferSerializer<IntByteMap> serializer = getBufferSerializer(IntByteMap.class);

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
		BufferSerializer<IntCharMap> serializer = getBufferSerializer(IntCharMap.class);

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
		BufferSerializer<ByteIntMap> serializer = getBufferSerializer(ByteIntMap.class);
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
		BufferSerializer<ShortByteMap> serializer = getBufferSerializer(ShortByteMap.class);
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
		BufferSerializer<ByteLongMap> serializer = getBufferSerializer(ByteLongMap.class);

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
		BufferSerializer<LongByteMap> serializer = getBufferSerializer(LongByteMap.class);
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
		BufferSerializer<LongLongMap> serializer = getBufferSerializer(LongLongMap.class);
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
		BufferSerializer<LongFloatMap> serializer = getBufferSerializer(LongFloatMap.class);
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
		BufferSerializer<DoubleDoubleMap> serializer = getBufferSerializer(DoubleDoubleMap.class);
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
		BufferSerializer<MapIntObjectHolder> bufferSerializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializerFor(IntObjectMap.class, SerializerGenHppcMap.serializerGenBuilder(IntObjectMap.class, int.class, Object.class))
				.build(MapIntObjectHolder.class);

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
		BufferSerializer<MapObjectShortHolder> bufferSerializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializerFor(ObjectShortMap.class, SerializerGenHppcMap.serializerGenBuilder(ObjectShortMap.class, Object.class, short.class))
				.build(MapObjectShortHolder.class);

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
		BufferSerializer<MapObjectObjectHolder> bufferSerializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializerFor(ObjectObjectMap.class, SerializerGenHppcMap.serializerGenBuilder(ObjectObjectMap.class, Object.class, Object.class))
				.build(MapObjectObjectHolder.class);

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
		public IntObjectMap<String> map;
		@Serialize(order = 1)
		public IntObjectMap<Integer> mapI;
		@Serialize(order = 2)
		public IntObjectMap<TestObject> mapO;
	}

	@Test
	public void testMult() {
		BufferSerializer<ManyMapHolder> bufferSerializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializerFor(IntObjectMap.class, SerializerGenHppcMap.serializerGenBuilder(IntObjectMap.class, int.class, Object.class))
				.build(ManyMapHolder.class);

		ManyMapHolder testMap1 = new ManyMapHolder();
		testMap1.map = new IntObjectOpenHashMap<>();
		testMap1.mapI = new IntObjectOpenHashMap<>();
		testMap1.mapO = new IntObjectOpenHashMap<>();
		ManyMapHolder testMap2 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap2.map);
		assertEquals(testMap1.map, testMap2.map);
		assertEquals(testMap1.mapI, testMap2.mapI);
		assertEquals(testMap1.mapO, testMap2.mapO);

		testMap1.map.put(10, "123");
		testMap1.map.put(11, "345");

		testMap1.mapI.put(20, 5);
		testMap1.mapI.put(25, 25);

		testMap1.mapO.put(42, new TestObject("2"));
		testMap1.mapO.put(42, new TestObject("22"));
		ManyMapHolder testMap3 = doTest(testMap1, bufferSerializer);
		assertNotNull(testMap3.map);
		assertNotNull(testMap3.mapI);
		assertNotNull(testMap3.mapO);
		assertEquals(testMap1.map, testMap3.map);
		assertEquals(testMap1.mapI, testMap3.mapI);
		assertEquals(testMap1.mapO, testMap3.mapO);

	}
}
