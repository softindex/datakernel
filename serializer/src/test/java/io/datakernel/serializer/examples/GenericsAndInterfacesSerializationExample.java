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

package io.datakernel.serializer.examples;

import io.datakernel.serializer.*;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.Arrays;
import java.util.List;

/**
 * Example of using generics and interfaces with serializers and deserializers.
 */
public class GenericsAndInterfacesSerializationExample {
	private static final SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory();

	public static void main(String[] args) {
		// Create a test object
		TestDataGenericInterfaceImpl testData1 = new TestDataGenericInterfaceImpl();

		testData1.setList(Arrays.asList(
				new TestDataGenericNested<>(10, "a"),
				new TestDataGenericNested<>(20, "b")));

		// Serialize testData1 and then deserialize it to testData2
		TestDataGenericInterfaceImpl testData2 =
				serializeAndDeserialize(TestDataGenericInterfaceImpl.class, testData1);

		// Compare them
		System.out.println(testData1.getList().size() + " " + testData2.getList().size());
		for (int i = 0; i < testData1.getList().size(); i++) {
			System.out.println(testData1.getList().get(i).getKey() + " " + testData1.getList().get(i).getValue()
					+ ", " + testData2.getList().get(i).getKey() + " " + testData2.getList().get(i).getValue());
		}
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

	public static class TestDataGenericInterfaceImpl extends TestDataGeneric<Integer, String> {
	}

	private static <T> T serializeAndDeserialize(Class<T> typeToken, T testData1) {
		SerializerScanner registry = SerializerScanner.defaultScanner();
		SerializerGen serializerGen = registry.serializer(typeToken);
		BufferSerializer<T> serializer = bufferSerializerFactory.createBufferSerializer(serializerGen);
		return serializeAndDeserialize(testData1, serializer, serializer);
	}

	private static <T> T serializeAndDeserialize(T testData1, BufferSerializer<T> serializer,
	                                             BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return deserializer.deserialize(input);
	}
}
