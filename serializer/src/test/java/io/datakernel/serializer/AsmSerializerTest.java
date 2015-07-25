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

import com.google.common.reflect.TypeToken;
import io.datakernel.serializer.annotations.*;
import io.datakernel.serializer.asm.SerializerGen;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class AsmSerializerTest {
	private static final SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory();

	private static <T> T doTest(TypeToken<T> typeToken, T testData1) {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(typeToken);
		BufferSerializer<T> serializer = bufferSerializerFactory.createBufferSerializer(serializerGen);
		return doTest(testData1, serializer, serializer);
	}

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer, BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return deserializer.deserialize(input);
	}

	public static class TestDataScalars {
		public enum TestEnum {
			ONE(1), TWO(2), THREE(3);

			TestEnum(@SuppressWarnings("UnusedParameters") int id) {
			}
		}

		@Serialize(order = 1)
		public boolean z;
		@Serialize(order = 2)
		public char c;
		@Serialize(order = 3)
		public byte b;
		@Serialize(order = 4)
		public short s;
		@Serialize(order = 5)
		public int i;
		@Serialize(order = 6)
		public long l;
		@Serialize(order = 7)
		public float f;
		@Serialize(order = 8)
		public double d;

		@Serialize(order = 9)
		public Boolean zBoxed;
		@Serialize(order = 10)
		public Character cBoxed;
		@Serialize(order = 11)
		public Byte bBoxed;
		@Serialize(order = 12)
		public Short sBoxed;
		@Serialize(order = 13)
		public Integer iBoxed;
		@Serialize(order = 14)
		public Long lBoxed;
		@Serialize(order = 15)
		public Float fBoxed;
		@Serialize(order = 16)
		public Double dBoxed;

		@Serialize(order = 17)
		public byte[] bytes;

		@Serialize(order = 18)
		public String string;
		@Serialize(order = 19)
		public TestEnum testEnum;
		@Serialize(order = 20)
		public InetAddress address;
	}

	@Test
	public void testScalars() throws UnknownHostException {
		TestDataScalars testData1 = new TestDataScalars();

		testData1.z = true;
		testData1.c = Character.MAX_VALUE;
		testData1.b = Byte.MIN_VALUE;
		testData1.s = Short.MIN_VALUE;
		testData1.i = Integer.MIN_VALUE;
		testData1.l = Long.MIN_VALUE;
		testData1.f = Float.MIN_VALUE;
		testData1.d = Double.MIN_VALUE;

		testData1.zBoxed = true;
		testData1.cBoxed = Character.MAX_VALUE;
		testData1.bBoxed = Byte.MIN_VALUE;
		testData1.sBoxed = Short.MIN_VALUE;
		testData1.iBoxed = Integer.MIN_VALUE;
		testData1.lBoxed = Long.MIN_VALUE;
		testData1.fBoxed = Float.MIN_VALUE;
		testData1.dBoxed = Double.MIN_VALUE;
		testData1.bytes = new byte[] { 1, 2, 3 };

		testData1.string = "abc";
		testData1.testEnum = TestDataScalars.TestEnum.TWO;
		testData1.address = InetAddress.getByName("127.0.0.1");

		TestDataScalars testData2 = doTest(TypeToken.of(TestDataScalars.class), testData1);

		assertEquals(testData1.z, testData2.z);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.b, testData2.b);
		assertEquals(testData1.s, testData2.s);
		assertEquals(testData1.i, testData2.i);
		assertEquals(testData1.l, testData2.l);
		assertEquals(testData1.f, testData2.f, Double.MIN_VALUE);
		assertEquals(testData1.d, testData2.d, Double.MIN_VALUE);

		assertEquals(testData1.zBoxed, testData2.zBoxed);
		assertEquals(testData1.cBoxed, testData2.cBoxed);
		assertEquals(testData1.bBoxed, testData2.bBoxed);
		assertEquals(testData1.sBoxed, testData2.sBoxed);
		assertEquals(testData1.iBoxed, testData2.iBoxed);
		assertEquals(testData1.lBoxed, testData2.lBoxed);
		assertEquals(testData1.fBoxed, testData2.fBoxed);
		assertEquals(testData1.dBoxed, testData2.dBoxed);

		assertArrayEquals(testData1.bytes, testData2.bytes);
		assertEquals(testData1.string, testData2.string);
		assertEquals(testData1.testEnum, testData2.testEnum);
		assertEquals(testData1.address, testData2.address);
	}

	public static class TestDataDeserialize {
		public TestDataDeserialize(@Deserialize("finalInt") int finalInt, @Deserialize("finalString") String finalString) {
			this.finalInt = finalInt;
			this.finalString = finalString;
		}

		@Serialize(order = 0)
		public final int finalInt;

		@Serialize(order = 1)
		public final String finalString;

		private int i;
		private int iBoxed;

		private int getterInt;
		private String getterString;

		@Serialize(order = 2)
		public int getI() {
			return i;
		}

		public void setI(int i) {
			this.i = i;
		}

		@Serialize(order = 3)
		public int getIBoxed() {
			return iBoxed;
		}

		public void setIBoxed(int iBoxed) {
			this.iBoxed = iBoxed;
		}

		@Serialize(order = 4)
		public int getGetterInt() {
			return getterInt;
		}

		@Serialize(order = 5)
		public String getGetterString() {
			return getterString;
		}

		public void setMultiple(@Deserialize("getterInt") int getterInt, @Deserialize("getterString") String getterString) {
			this.getterInt = getterInt;
			this.getterString = getterString;
		}
	}

	@Test
	public void testDeserialize() {
		TestDataDeserialize testData1 = new TestDataDeserialize(10, "abc");
		testData1.setI(20);
		testData1.setIBoxed(30);
		testData1.setMultiple(40, "123");

		TestDataDeserialize testData2 = doTest(TypeToken.of(TestDataDeserialize.class), testData1);
		assertEquals(testData1.finalInt, testData2.finalInt);
		assertEquals(testData1.finalString, testData2.finalString);
		assertEquals(testData1.getI(), testData2.getI());
		assertEquals(testData1.getIBoxed(), testData2.getIBoxed());
		assertEquals(testData1.getGetterInt(), testData2.getGetterInt());
		assertEquals(testData1.getGetterString(), testData2.getGetterString());
	}

	public static class TestDataNested {
		private final int a;

		private int b;

		public TestDataNested(@Deserialize("a") int a) {
			this.a = a;
		}

		@Serialize(order = 0)
		public int getA() {
			return a;
		}

		@Serialize(order = 1)
		public int getB() {
			return b;
		}

		public void setB(int b) {
			this.b = b;
		}
	}

	public static class TestDataComplex {
		@Serialize(order = 0)
		public TestDataNested nested;
		@Serialize(order = 1)
		public TestDataNested[] nestedArray;
		@Serialize(order = 2)
		public TestDataNested[][] nestedArrayArray;
		@Serialize(order = 3)
		public List<TestDataNested> nestedList;
		@Serialize(order = 4)
		public List<List<TestDataNested>> nestedListList;

		@Serialize(order = 5)
		public int[] ints;

		@Serialize(order = 6)
		public int[][] intsArray;
	}

	private void assertEqualNested(TestDataNested testDataNested1, TestDataNested testDataNested2) {
		assertEquals(testDataNested1.getA(), testDataNested2.getA());
		assertEquals(testDataNested1.getB(), testDataNested2.getB());
	}

	@Test
	public void testComplex() {
		TestDataComplex testData1 = new TestDataComplex();

		testData1.ints = new int[] { 1, 2, 3 };
		testData1.intsArray = new int[][] {
				new int[] { 1, 2 },
				new int[] { 3, 4, 5 } };

		testData1.nested = new TestDataNested(11);
		testData1.nestedArray = new TestDataNested[] { new TestDataNested(12), new TestDataNested(13) };
		testData1.nestedArrayArray = new TestDataNested[][] {
				new TestDataNested[] { new TestDataNested(14), new TestDataNested(15) },
				new TestDataNested[] { new TestDataNested(16) } };
		testData1.nestedList = Arrays.asList(new TestDataNested(1), new TestDataNested(2));
		testData1.nestedListList = Arrays.asList(
				Arrays.asList(new TestDataNested(20), new TestDataNested(21)),
				Collections.singletonList(new TestDataNested(22)));

		TestDataComplex testData2 = doTest(TypeToken.of(TestDataComplex.class), testData1);

		assertArrayEquals(testData1.ints, testData2.ints);

		assertEquals(testData1.intsArray.length, testData2.intsArray.length);
		for (int i = 0; i < testData1.intsArray.length; i++) {
			assertArrayEquals(testData1.intsArray[i], testData2.intsArray[i]);
		}

		assertEqualNested(testData1.nested, testData2.nested);

		assertEquals(testData1.nestedArray.length, testData2.nestedArray.length);
		for (int i = 0; i < testData1.nestedArray.length; i++) {
			assertEqualNested(testData1.nestedArray[i], testData2.nestedArray[i]);
		}

		assertEquals(testData1.nestedArrayArray.length, testData2.nestedArrayArray.length);
		for (int i = 0; i < testData1.nestedArrayArray.length; i++) {
			TestDataNested[] nested1 = testData1.nestedArrayArray[i];
			TestDataNested[] nested2 = testData2.nestedArrayArray[i];
			assertEquals(nested1.length, nested2.length);
			for (int j = 0; j < nested1.length; j++) {
				assertEqualNested(nested1[j], nested2[j]);
			}
		}

		assertEquals(testData1.nestedList.size(), testData2.nestedList.size());
		for (int i = 0; i < testData1.nestedList.size(); i++) {
			assertEqualNested(testData1.nestedList.get(i), testData2.nestedList.get(i));
		}

		assertEquals(testData1.nestedListList.size(), testData2.nestedListList.size());
		for (int i = 0; i < testData1.nestedListList.size(); i++) {
			List<TestDataNested> nested1 = testData1.nestedListList.get(i);
			List<TestDataNested> nested2 = testData2.nestedListList.get(i);
			assertEquals(nested1.size(), nested2.size());
			for (int j = 0; j < nested1.size(); j++) {
				assertEqualNested(nested1.get(j), nested2.get(j));
			}
		}
	}

	public static class TestDataNullables {
		@Serialize(order = 0)
		@SerializeNullable
		public String nullableString1;

		@Serialize(order = 1)
		@SerializeNullable
		public String nullableString2;

		@Serialize(order = 2)
		@SerializeNullable(path = 0)
		public List<String> listOfNullableStrings;

		@Serialize(order = 3)
		@SerializeNullableEx({ @SerializeNullable, @SerializeNullable(path = { 0 }), @SerializeNullable(path = { 0, 0 }) })
		public String[][] nullableArrayOfNullableArrayOfNullableStrings;

		@Serialize(order = 4)
		@SerializeNullableEx({ @SerializeNullable(path = { 0 }), @SerializeNullable(path = { 1 }) })
		public Map<Integer, String> mapOfNullableInt2NullableString;
	}

	@Test
	public void testNullables() {
		TestDataNullables testData1 = new TestDataNullables();

		testData1.nullableString1 = null;
		testData1.nullableString2 = "abc";
		testData1.listOfNullableStrings = Arrays.asList("a", null, "b");
		testData1.nullableArrayOfNullableArrayOfNullableStrings = new String[][] {
				new String[] { "a", null },
				null };
		testData1.mapOfNullableInt2NullableString = new LinkedHashMap<>();
		testData1.mapOfNullableInt2NullableString.put(1, "abc");
		testData1.mapOfNullableInt2NullableString.put(2, null);
		testData1.mapOfNullableInt2NullableString.put(null, "xyz");

		TestDataNullables testData2 = doTest(TypeToken.of(TestDataNullables.class), testData1);

		assertEquals(testData1.nullableString1, testData2.nullableString1);
		assertEquals(testData1.nullableString2, testData2.nullableString2);

		assertEquals(testData1.listOfNullableStrings, testData2.listOfNullableStrings);

		assertEquals(
				testData1.nullableArrayOfNullableArrayOfNullableStrings.length,
				testData2.nullableArrayOfNullableArrayOfNullableStrings.length);
		for (int i = 0; i < testData1.nullableArrayOfNullableArrayOfNullableStrings.length; i++) {
			assertArrayEquals(
					testData1.nullableArrayOfNullableArrayOfNullableStrings[i],
					testData2.nullableArrayOfNullableArrayOfNullableStrings[i]);
		}

		assertEquals(testData1.mapOfNullableInt2NullableString, testData2.mapOfNullableInt2NullableString);

	}

	public interface TestDataInterface {
		@Serialize(order = 0)
		int getI();

		@Serialize(order = 1)
		Integer getIBoxed();
	}

	public class TestDataInterfaceImpl implements TestDataInterface {
		private int i;
		private Integer iBoxed;

		@Override
		public int getI() {
			return i;
		}

		@Override
		public Integer getIBoxed() {
			return iBoxed;
		}
	}

	@Test
	public void testInterface() {
		TestDataInterfaceImpl testData1 = new TestDataInterfaceImpl();
		testData1.i = 10;
		testData1.iBoxed = 20;

		TestDataInterface testData2 = doTest(TypeToken.of(TestDataInterface.class), testData1);
		assertEquals(testData1.getI(), testData2.getI());
		assertEquals(testData1.getIBoxed(), testData2.getIBoxed());
	}

	@Test
	public void testList() {
		List<String> testData1 = Arrays.asList("a", "b", "c");
		List<String> testData2 = doTest(new TypeToken<List<String>>() {
		}, testData1);
		assertEquals(testData1, testData2);
	}

	@Test
	public void testMap() {
		Map<Integer, String> testData1 = new HashMap<>();
		testData1.put(1, "abc");
		testData1.put(2, "xyz");
		Map<Integer, String> testData2 = doTest(new TypeToken<Map<Integer, String>>() {
		}, testData1);
		assertEquals(testData1, testData2);
	}

	public interface TestDataGenericNestedInterface<K, V> {
		@Serialize(order = 0)
		K getKey();

		@Serialize(order = 1)
		V getValue();
	}

	public static class TestDataGenericNested<K, V> implements TestDataGenericNestedInterface<K, V> {
		private K key;

		private V value;

		@SuppressWarnings("UnusedDeclaration")
		public TestDataGenericNested() {
		}

		public TestDataGenericNested(K key, V value) {
			this.key = key;
			this.value = value;
		}

		@Serialize(order = 0)
		@Override
		public K getKey() {
			return key;
		}

		public void setKey(K key) {
			this.key = key;
		}

		@Serialize(order = 1)
		@Override
		public V getValue() {
			return value;
		}

		public void setValue(V value) {
			this.value = value;
		}
	}

	public interface TestDataGenericInterface<K, V> {
		@Serialize(order = 0)
		List<TestDataGenericNested<K, V>> getList();
	}

	public static class TestDataGeneric<K, V> implements TestDataGenericInterface<K, V> {
		private List<TestDataGenericNested<K, V>> list;

		@Serialize(order = 0)
		@Override
		public List<TestDataGenericNested<K, V>> getList() {
			return list;
		}

		public void setList(List<TestDataGenericNested<K, V>> list) {
			this.list = list;
		}
	}

	private static void assertEqualsGenericNested(TestDataGenericNested<Integer, String> item1, TestDataGenericNested<Integer, String> item2) {
		if (item1 != null || item2 != null) {
			assertNotNull(item1);
			assertNotNull(item2);
			assertEquals(item1.getKey(), item2.getKey());
			assertEquals(item1.getValue(), item2.getValue());
		}
	}

	@Test
	public void testGeneric() {
		TestDataGeneric<Integer, String> testData1 = new TestDataGeneric<>();
		testData1.setList(Arrays.asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")));
		TestDataGeneric<Integer, String> testData2 = doTest(new TypeToken<TestDataGeneric<Integer, String>>() {
		}, testData1);
		assertEquals(testData1.list.size(), testData2.list.size());
		for (int i = 0; i < testData1.list.size(); i++) {
			assertEqualsGenericNested(testData1.list.get(i), testData2.list.get(i));
		}
	}

	public static class TestDataGenericParameters {
		@Serialize(order = 0)
		@SerializeNullableEx({ @SerializeNullable(path = { 0 }), @SerializeNullable(path = { 0, 0 }), @SerializeNullable(path = { 0, 1 }) })
		@SerializeVarLength(path = { 0, 0 })
		@SerializeUtf16(path = { 0, 1 })
		public List<TestDataGenericNested<Integer, String>> list;
	}

	@Test
	public void testGenericParameters() {
		TestDataGenericParameters testData1 = new TestDataGenericParameters();
		testData1.list = Arrays.asList(
				null,
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<Integer, String>(null, null));
		TestDataGenericParameters testData2 = doTest(new TypeToken<TestDataGenericParameters>() {
		}, testData1);
		assertEquals(testData1.list.size(), testData2.list.size());
		for (int i = 0; i < testData1.list.size(); i++) {
			assertEqualsGenericNested(testData1.list.get(i), testData2.list.get(i));
		}
	}

	@Test
	public void testGenericInterface() {
		TestDataGeneric<Integer, String> testData1 = new TestDataGeneric<>();
		testData1.setList(Arrays.asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")));
		TestDataGenericInterface<Integer, String> testData2 = doTest(new TypeToken<TestDataGenericInterface<Integer, String>>() {
		}, testData1);
		assertEquals(testData1.getList().size(), testData2.getList().size());
		for (int i = 0; i < testData1.getList().size(); i++) {
			assertEqualsGenericNested(testData1.getList().get(i), testData2.getList().get(i));
		}
	}

	@Test
	public void testGenericNested() {
		TestDataGenericNested<String, Integer> testData1 = new TestDataGenericNested<>("a", 10);
		TestDataGenericNested<String, Integer> testData2 = doTest(new TypeToken<TestDataGenericNested<String, Integer>>() {
		}, testData1);
		assertEquals(testData1.key, testData2.key);
		assertEquals(testData1.value, testData2.value);
	}

	@Test
	public void testGenericNestedInterface() {
		TestDataGenericNested<String, Integer> testData1 = new TestDataGenericNested<>("a", 10);
		TestDataGenericNestedInterface<String, Integer> testData2 = doTest(new TypeToken<TestDataGenericNestedInterface<String, Integer>>() {
		}, testData1);
		assertEquals(testData1.getKey(), testData2.getKey());
		assertEquals(testData1.getValue(), testData2.getValue());
	}

	public static class TestDataGenericSuperclass<A, B> {
		@Serialize(order = 0)
		public A a;

		@Serialize(order = 1)
		public B b;
	}

	public static class TestDataGenericSubclass<X, Y> extends TestDataGenericSuperclass<Integer, X> {
		@Serialize(order = 0)
		public Y c;
	}

	@Test
	public void testGenericSubclass() {
		TestDataGenericSubclass<String, Boolean> testData1 = new TestDataGenericSubclass<>();
		testData1.a = 10;
		testData1.b = "abc";
		testData1.c = true;
		TestDataGenericSubclass<String, Boolean> testData2 = doTest(new TypeToken<TestDataGenericSubclass<String, Boolean>>() {
		}, testData1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(testData1.c, testData2.c);
	}

	public static class TestDataSuperclassHolder {
		@Serialize(order = 0)
		@SerializeSubclasses({ TestDataSubclass1.class, TestDataSubclass2.class })
		@SerializeNullable
		public TestDataSuperclass data;
	}

	public static class TestDataSuperclass {
		public TestDataSuperclass() {
			initByCons = 123;
		}

		@Serialize(order = 0)
		public int a;

		public int initByCons;
	}

	public static class TestDataSubclass1 extends TestDataSuperclass {
		@Serialize(order = 0)
		public boolean b;
	}

	public static class TestDataSubclass2 extends TestDataSuperclass {
		@Serialize(order = 0)
		@SerializeNullable
		public String s;
	}

	@Test
	public void testSubclasses1() {
		TestDataSuperclassHolder testData1 = new TestDataSuperclassHolder();
		testData1.data = null;
		TestDataSuperclassHolder testData2 = doTest(new TypeToken<TestDataSuperclassHolder>() {
		}, testData1);
		assertEquals(testData1.data, testData2.data);

		TestDataSubclass1 subclass1 = new TestDataSubclass1();
		testData1.data = subclass1;
		subclass1.a = 10;
		subclass1.b = true;
		testData2 = doTest(new TypeToken<TestDataSuperclassHolder>() {
		}, testData1);
		TestDataSubclass1 subclass2 = (TestDataSubclass1) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.b, subclass2.b);
		assertEquals(subclass1.initByCons, subclass2.initByCons);
	}

	@Test
	public void testSubclasses2() {
		TestDataSuperclassHolder testData1 = new TestDataSuperclassHolder();
		TestDataSubclass2 subclass1 = new TestDataSubclass2();
		testData1.data = subclass1;
		subclass1.a = 10;
		subclass1.s = "abc";
		TestDataSuperclassHolder testData2 = doTest(new TypeToken<TestDataSuperclassHolder>() {
		}, testData1);
		TestDataSubclass2 subclass2 = (TestDataSubclass2) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.s, subclass2.s);

		subclass1.s = null;
		testData2 = doTest(new TypeToken<TestDataSuperclassHolder>() {
		}, testData1);
		subclass2 = (TestDataSubclass2) testData2.data;
		assertEquals(subclass1.a, subclass2.a);
		assertEquals(subclass1.s, subclass2.s);
		assertEquals(subclass1.initByCons, subclass2.initByCons);
	}

	public static class TestDataSerializerUtf16 {
		@Serialize(order = 0)
		@SerializeUtf16(path = { 0 })
		@SerializeNullable(path = { 0 })
		public List<String> strings;
	}

	@Test
	public void testSerializerUtf16() {
		TestDataSerializerUtf16 testData1 = new TestDataSerializerUtf16();
		testData1.strings = Arrays.asList("abc", null, "123");
		TestDataSerializerUtf16 testData2 = doTest(new TypeToken<TestDataSerializerUtf16>() {
		}, testData1);
		assertEquals(testData1.strings, testData2.strings);
	}

	public static class TestDataFixedSize {
		@Serialize(order = 0)
		@SerializeFixedSize(3)
		@SerializeNullable(path = { 0 })
		public String[] strings;

		@Serialize(order = 1)
		@SerializeFixedSize(4)
		public byte[] bytes;
	}

	@Test
	public void testFixedSize() {
		TestDataFixedSize testData1 = new TestDataFixedSize();
		testData1.strings = new String[] { "abc", null, "123" };
		testData1.bytes = new byte[] { 1, 2, 3, 4 };
		TestDataFixedSize testData2 = doTest(new TypeToken<TestDataFixedSize>() {
		}, testData1);
		assertArrayEquals(testData1.strings, testData2.strings);
		assertArrayEquals(testData1.bytes, testData2.bytes);
	}

	public static class TestDataVersions {
		@Serialize(order = 0, added = 0)
		public int a;

		@Serialize(order = 1, added = 1, removed = 2)
		public int b;

		@Serialize(order = 2, added = 2)
		public int c;
	}

	@Test
	public void testVersions() {
		TypeToken<TestDataVersions> typeToken = new TypeToken<TestDataVersions>() {
		};
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(typeToken);
		BufferSerializer<TestDataVersions> serializer0 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(serializerGen, 0);
		BufferSerializer<TestDataVersions> serializer1 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(serializerGen, 1);
		BufferSerializer<TestDataVersions> serializer2 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(serializerGen, 2);

		TestDataVersions testData1 = new TestDataVersions();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;

		TestDataVersions testData2;

		testData2 = doTest(testData1, serializer0, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer0, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer0, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer0);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
	}

	public static class TestDataProfiles {
		@Serialize(order = 0)
		public int a;

		@Serialize(order = 1)
		@SerializeProfiles("profile1")
		public int b;

		@Serialize(order = 2)
		@SerializeProfiles({ "profile1", "profile2" })
		public int c;
	}

	@Test
	public void testProfiles() {
		TypeToken<TestDataProfiles> typeToken = new TypeToken<TestDataProfiles>() {
		};
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerScanner registry1 = SerializerScanner.defaultScanner("profile1");
		SerializerScanner registry2 = SerializerScanner.defaultScanner("profile2");
		BufferSerializer<TestDataProfiles> serializer = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(registry.serializer(typeToken));
		BufferSerializer<TestDataProfiles> serializer1 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(registry1.serializer(typeToken));
		BufferSerializer<TestDataProfiles> serializer2 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(registry2.serializer(typeToken));

		TestDataProfiles testData1 = new TestDataProfiles();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;

		TestDataProfiles testData2;

		testData2 = doTest(testData1, serializer, serializer);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(testData2.c, testData2.c);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
	}

	public static class TestDataProfiles2 {
		@Serialize(order = 0, added = 1)
		public int a;

		@Serialize(order = 1, added = 1)
		@SerializeProfiles(value = "profile", added = { 2 })
		public int b;

		@SerializeProfiles(value = { "profile", SerializeProfiles.COMMON_PROFILE }, added = { 1 }, removed = { 2 })
		@Serialize(order = 2, added = 2)
		public int c;

		@Serialize(order = 3, added = 2)
		public int d;

		@Serialize(order = 4, added = 1)
		@SerializeProfiles(value = "profile")
		public int e;

		public int f;
	}

	@Test
	public void testProfilesVersions() {
		TypeToken<TestDataProfiles2> typeToken = new TypeToken<TestDataProfiles2>() {
		};
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerScanner registryProfile = SerializerScanner.defaultScanner("profile");
		BufferSerializer<TestDataProfiles2> serializer1 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(registry.serializer(typeToken),
				1);
		BufferSerializer<TestDataProfiles2> serializer2 = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(registry.serializer(typeToken),
				2);
		BufferSerializer<TestDataProfiles2> serializer1Profile = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(
				registryProfile.serializer(typeToken), 1);
		BufferSerializer<TestDataProfiles2> serializer2Profile = SerializerFactory.createBufferSerializerFactory().createBufferSerializer(
				registryProfile.serializer(typeToken), 2);

		TestDataProfiles2 testData1 = new TestDataProfiles2();
		testData1.a = 10;
		testData1.b = 20;
		testData1.c = 30;
		testData1.d = 40;
		testData1.e = 50;
		testData1.f = 60;

		TestDataProfiles2 testData2;

		testData2 = doTest(testData1, serializer1, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer1, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2, serializer2);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2, serializer1);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(0, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer1Profile, serializer1Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
		testData2 = doTest(testData1, serializer1Profile, serializer2Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(0, testData2.b);
		assertEquals(testData1.c, testData2.c);
		assertEquals(0, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);

		testData2 = doTest(testData1, serializer2Profile, serializer2Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
		testData2 = doTest(testData1, serializer2Profile, serializer1Profile);
		assertEquals(testData1.a, testData2.a);
		assertEquals(testData1.b, testData2.b);
		assertEquals(0, testData2.c);
		assertEquals(testData1.d, testData2.d);
		assertEquals(testData1.e, testData2.e);
		assertEquals(0, testData2.f);
	}

	public static class TestDataRecursive {
		@Serialize(order = 0)
		public String s;

		@Serialize(order = 1)
		@SerializeNullable
		public TestDataRecursive next;

		@SuppressWarnings("UnusedDeclaration")
		public TestDataRecursive() {
		}

		public TestDataRecursive(String s) {
			this.s = s;
		}
	}

	@Test
	public void testDataRecursive() {
		TestDataRecursive testData1 = new TestDataRecursive("a");
		testData1.next = new TestDataRecursive("b");
		testData1.next.next = new TestDataRecursive("c");
		TestDataRecursive testData2 = doTest(new TypeToken<TestDataRecursive>() {
		}, testData1);
		assertTrue(testData1 != testData2 && testData1.next != testData2.next && testData1.next.next != testData2.next.next);
		assertEquals(testData1.s, testData2.s);
		assertEquals(testData1.next.s, testData2.next.s);
		assertEquals(testData1.next.next.s, testData2.next.next.s);
		assertEquals(null, testData2.next.next.next);
	}

	public static class TestDataExtraSubclasses {
		@Serialize(order = 0)
		@SerializeSubclasses(value = { String.class }, extraSubclassesId = "extraSubclasses1")
		public Object object1;

		@Serialize(order = 1)
		@SerializeSubclasses(value = { String.class }, extraSubclassesId = "extraSubclasses2")
		public Object object2;
	}

	@Test
	public void testDataExtraSubclasses() {
		TestDataExtraSubclasses testData1 = new TestDataExtraSubclasses();
		testData1.object1 = 10;
		testData1.object2 = "object2";

		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.setExtraSubclasses("extraSubclasses1", Integer.class);
		SerializerGen serializerGen = registry.serializer(TypeToken.of(TestDataExtraSubclasses.class));
		BufferSerializer<TestDataExtraSubclasses> serializer = bufferSerializerFactory.createBufferSerializer(serializerGen);
		TestDataExtraSubclasses testData2 = doTest(testData1, serializer, serializer);

		assertEquals(testData1.object1, testData2.object1);
		assertEquals(testData1.object2, testData2.object2);
	}

	@SerializeSubclasses(value = { TestDataExtraSubclasses1.class }, extraSubclassesId = "extraSubclasses")
	public interface TestDataExtraSubclassesInterface {}

	public static class TestDataExtraSubclasses1 implements TestDataExtraSubclassesInterface {
		@Serialize(order = 0)
		public boolean z;
	}

	public static class TestDataExtraSubclasses2 implements TestDataExtraSubclassesInterface {
		@Serialize(order = 1)
		public int i;
	}

	@Test
	public void testDataExtraSubclassesInterface() {
		TestDataExtraSubclasses2 testData1 = new TestDataExtraSubclasses2();
		testData1.i = 10;

		SerializerScanner registry = SerializerScanner.defaultScanner();
		registry.setExtraSubclasses("extraSubclasses", TestDataExtraSubclasses2.class);
		SerializerGen serializerGen = registry.serializer(TypeToken.of(TestDataExtraSubclassesInterface.class));
		BufferSerializer<TestDataExtraSubclassesInterface> serializer = bufferSerializerFactory.createBufferSerializer(serializerGen);
		TestDataExtraSubclassesInterface testData2 = doTest(testData1, serializer, serializer);

		assertEquals(testData1.i, ((TestDataExtraSubclasses2) testData2).i);
	}

	public interface TestInheritAnnotationsInterface1 {
		@Serialize(order = 1)
		int getIntValue();
	}

	public interface TestInheritAnnotationsInterface2 {
		@Serialize(order = 1)
		double getDoubleValue();
	}

	@SerializeInterface
	public interface TestInheritAnnotationsInterface3 extends TestInheritAnnotationsInterface1, TestInheritAnnotationsInterface2 {
		@Serialize(order = 1)
		String getStringValue();
	}

	public static class TestInheritAnnotationsInterfacesImpl implements TestInheritAnnotationsInterface3 {
		public int i;
		public double d;
		public String s;

		@Override
		public String getStringValue() {
			return s;
		}

		@Override
		public int getIntValue() {
			return i;
		}

		@Override
		public double getDoubleValue() {
			return d;
		}

		public void setIntValue(int i) {
			this.i = i;
		}

		public void setDoubleValue(double d) {
			this.d = d;
		}

		public void setStringValue(String s) {
			this.s = s;
		}

	}

	@Test
	public void testInheritSerialize() {
		TestInheritAnnotationsInterfacesImpl testData1 = new TestInheritAnnotationsInterfacesImpl();
		testData1.setIntValue(10);
		testData1.setDoubleValue(1.23);
		testData1.setStringValue("test");

		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(TypeToken.of(TestInheritAnnotationsInterface3.class));
		BufferSerializer<TestInheritAnnotationsInterface3> serializer = bufferSerializerFactory.createBufferSerializer(serializerGen);
		TestInheritAnnotationsInterface3 testData2 = doTest(testData1, serializer, serializer);

		assertEquals(testData1.getIntValue(), testData2.getIntValue());
		assertEquals(testData1.getDoubleValue(), testData2.getDoubleValue(), Double.MIN_VALUE);
		assertEquals(testData1.getStringValue(), testData2.getStringValue());

		SerializerScanner registry2 = SerializerScanner.defaultScanner();
		SerializerGen serializerGen2 = registry2.serializer(TypeToken.of(TestInheritAnnotationsInterfacesImpl.class));
		BufferSerializer<TestInheritAnnotationsInterfacesImpl> serializer2 = bufferSerializerFactory.createBufferSerializer(serializerGen2);
		TestInheritAnnotationsInterfacesImpl testData3 = doTest(testData1, serializer2, serializer2);

		assertEquals(0, testData3.getIntValue());
		assertEquals(0.0, testData3.getDoubleValue(), Double.MIN_VALUE);
		assertEquals(null, testData3.getStringValue());
	}

	public static class TestDataMaxLength {
		@Serialize(order = 0)
		@SerializeMaxLength(3)
		public String s1;

		@Serialize(order = 1)
		@SerializeNullable
		@SerializeMaxLength(4)
		public String s2;

		@Serialize(order = 2)
		@SerializeNullable
		@SerializeMaxLength(5)
		public String s3;
	}

	@Test
	public void testMaxLength() {
		TestDataMaxLength expected = new TestDataMaxLength();
		expected.s1 = "abcdefg";
		expected.s2 = "1234";
		expected.s3 = "QWE";

		TestDataMaxLength actual = doTest(new TypeToken<TestDataMaxLength>() {
		}, expected);
		assertEquals(expected.s1.substring(0, 3), actual.s1);
		assertEquals(expected.s2, actual.s2);
		assertEquals(expected.s3, actual.s3);
	}

	public static class TestDataDeserializeFactory0 {
		String str;
		int hash;

		@Serialize(order = 0)
		public String getStr() {
			return str;
		}

		public int getHash() {
			return hash;
		}

		public static TestDataDeserializeFactory0 create(@Deserialize("str") String str) {
			TestDataDeserializeFactory0 testData = new TestDataDeserializeFactory0();
			testData.str = str;
			testData.hash = str.hashCode();
			return testData;
		}
	}

	@DeserializeFactory(TestFactory.class)
	public static class TestDataDeserializeFactory1 {
		String str;
		int hash;

		@Serialize(order = 0)
		public String getStr() {
			return str;
		}

		public int getHash() {
			return hash;
		}

	}

	@SerializeSubclasses(TestDataDeserializeFactory2.class)
	public interface TestDataFactoryInterface {
		String getStr();

		int getHash();
	}

	@DeserializeFactory(TestFactory.class)
	public static class TestDataDeserializeFactory2 implements TestDataFactoryInterface {
		private final String str;
		private final int hash;
		private String test;

		public TestDataDeserializeFactory2(String str) {
			this.str = str;
			this.hash = str.hashCode();
		}

		@Override
		@Serialize(order = 1)
		public String getStr() {
			return str;
		}

		@Override
		public int getHash() {
			return hash;
		}

		@Serialize(order = 2)
		@SerializeNullable
		public String getTest() {
			return test;
		}

		public void setTest(String test) {
			this.test = test;
		}
	}

	public static class TestFactory {
		public static TestDataDeserializeFactory1 create(@Deserialize("str") String str) {
			TestDataDeserializeFactory1 testData = new TestDataDeserializeFactory1();
			testData.str = str;
			testData.hash = str.hashCode();
			return testData;
		}

		public static TestDataDeserializeFactory2 create2(@Deserialize("str") String str) {
			return new TestDataDeserializeFactory2(str);
		}
	}

	@Test
	public void testDeserializeFactory() {
		TestDataDeserializeFactory0 sourceTestData0 = TestDataDeserializeFactory0.create("abcdef");
		TestDataDeserializeFactory0 resultTestData0 = doTest(new TypeToken<TestDataDeserializeFactory0>() {
		}, sourceTestData0);
		assertEquals(sourceTestData0.getStr(), resultTestData0.getStr());
		assertEquals(sourceTestData0.getHash(), resultTestData0.getHash());

		TestDataDeserializeFactory1 sourceTestData1 = TestFactory.create("abcdef");
		TestDataDeserializeFactory1 resultTestData1 = doTest(new TypeToken<TestDataDeserializeFactory1>() {
		}, sourceTestData1);
		assertEquals(sourceTestData1.getStr(), resultTestData1.getStr());
		assertEquals(sourceTestData1.getHash(), resultTestData1.getHash());

		TestDataDeserializeFactory2 sourceTestData2 = TestFactory.create2("asdfg");
		sourceTestData2.setTest("12341242");
		TestDataDeserializeFactory2 resultTestData2 = doTest(new TypeToken<TestDataDeserializeFactory2>() {
		}, sourceTestData2);
		assertEquals(sourceTestData2.getStr(), resultTestData2.getStr());
		assertEquals(sourceTestData2.getHash(), resultTestData2.getHash());
		assertEquals(sourceTestData2.getTest(), resultTestData2.getTest());

		TestDataFactoryInterface resultTestData3 = doTest(new TypeToken<TestDataFactoryInterface>() {
		}, sourceTestData2);
		assertEquals(sourceTestData2.getStr(), resultTestData3.getStr());
		assertEquals(sourceTestData2.getHash(), resultTestData3.getHash());
	}

	public enum TestEnum {
		ONE(1), TWO(2), THREE(3);

		private final int value;

		TestEnum(int value) {
			this.value = value;
		}

		@Serialize(order = 1)
		public int getValue() {
			return value;
		}

		private static final Map<Integer, TestEnum> CACHE = new ConcurrentHashMap<>();

		static {
			for (TestEnum c : TestEnum.values()) {
				if (CACHE.put(c.getValue(), c) != null)
					throw new IllegalStateException("Duplicate code " + c.getValue());
			}
		}

		public static TestEnum of(@Deserialize("value") int value) {
			return CACHE.get(value);
		}
	}

	@Test
	public void testCustomSerializeEnum() {
		TestEnum sourceData = TestEnum.TWO;
		TestEnum resultData = doTest(new TypeToken<TestEnum>() {
		}, sourceData);
		assertEquals(sourceData, resultData);
	}

	@Test
	public void testListEnums() {
		List<TestEnum> testData1 = Arrays.asList(TestEnum.ONE, TestEnum.THREE, TestEnum.TWO);
		List<TestEnum> testData2 = doTest(new TypeToken<List<TestEnum>>() {
		}, testData1);
		assertEquals(testData1, testData2);
	}

	@Test
	public void testMapEnums() {
		Map<TestEnum, String> testData1 = new HashMap<>();
		testData1.put(TestEnum.ONE, "abc");
		testData1.put(TestEnum.TWO, "xyz");
		Map<TestEnum, String> testData2 = doTest(new TypeToken<Map<TestEnum, String>>() {
		}, testData1);
		assertEquals(testData1, testData2);
		assertEquals(testData1, testData2);
		assertTrue(testData2 instanceof EnumMap);
	}

	public enum TestEnum2 {
		ONE, TWO, THREE
	}

	@Test
	public void testEnums() {
		TestEnum2 testData1 = TestEnum2.ONE;
		TestEnum2 testData2 = doTest(new TypeToken<TestEnum2>() {
		}, testData1);
		assertEquals(testData1, testData2);
	}

	@Test
	public void testListEnums2() {
		List<TestEnum2> testData1 = Arrays.asList(TestEnum2.ONE, TestEnum2.THREE, TestEnum2.TWO);
		List<TestEnum2> testData2 = doTest(new TypeToken<List<TestEnum2>>() {
		}, testData1);
		assertEquals(testData1, testData2);
	}

	@Test
	public void testMapEnums2() {
		Map<TestEnum2, TestEnum2> testData1 = new HashMap<>();
		testData1.put(TestEnum2.ONE, TestEnum2.THREE);
		testData1.put(TestEnum2.TWO, TestEnum2.ONE);
		Map<TestEnum2, TestEnum2> testData2 = doTest(new TypeToken<Map<TestEnum2, TestEnum2>>() {
		}, testData1);
		assertEquals(testData1, testData2);
		assertTrue(testData2 instanceof EnumMap);
	}

	@Test
	public void testSet() {
		Set<Integer> testData1 = new HashSet<>();
		testData1.add(1);
		testData1.add(2);
		Set<Integer> testData2 = doTest(new TypeToken<Set<Integer>>() {
		}, testData1);
		assertEquals(testData1, testData2);
		assertTrue(testData2 instanceof LinkedHashSet);
	}

	@Test
	public void testEnumSet() {
		Set<TestEnum> testData1 = EnumSet.copyOf(Arrays.asList(TestEnum.THREE, TestEnum.ONE));
		Set<TestEnum> testData2 = doTest(new TypeToken<Set<TestEnum>>() {
		}, testData1);
		assertEquals(testData1, testData2);
		assertTrue(testData2 instanceof EnumSet);
	}

}
