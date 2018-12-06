/*
 * Copyright (C) 2015-2018 SoftIndex LLC.
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

package io.datakernel.examples;

import io.datakernel.serializer.BinarySerializer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeFixedSize;
import io.datakernel.serializer.annotations.SerializeNullable;

import java.util.Arrays;

/**
 * Example of serialization and deserialization of an object with fixed size fields.
 */
public class FixedSizeFieldsSerializationExample {

	public static void main(String[] args) {
		// Create a test object
		TestDataFixedSize testData1 = new TestDataFixedSize();

		// Fourth element will be discarded because the size of "strings" is fixed and is equal to 3
		testData1.strings = new String[]{"abc", null, "123", "superfluous"};

		testData1.bytes = new byte[]{1, 2, 3, 4};

		/* The following would cause exception to be thrown (because the size of "bytes" is fixed and is equal to 4):
		testData1.bytes = new byte[]{1, 2, 3}; */

		// Serialize testData1 and then deserialize it to testData2
		TestDataFixedSize testData2 = serializeAndDeserialize(TestDataFixedSize.class, testData1);

		// Compare them
		System.out.println(Arrays.toString(testData1.strings) + " " + Arrays.toString(testData2.strings));
		System.out.println(Arrays.toString(testData1.bytes) + " " + Arrays.toString(testData2.bytes));
	}

	public static class TestDataFixedSize {
		@Serialize(order = 0)
		@SerializeFixedSize(3)
		@SerializeNullable(path = {0})
		public String[] strings;

		@Serialize(order = 1)
		@SerializeFixedSize(4)
		public byte[] bytes;
	}

	private static <T> T serializeAndDeserialize(Class<T> typeToken, T testData1) {
		BinarySerializer<T> serializer = SerializerBuilder
				.create(getContextClassLoader())
				.build(typeToken);
		return serializeAndDeserialize(testData1, serializer, serializer);
	}

	private static <T> T serializeAndDeserialize(T testData1, BinarySerializer<T> serializer,
			BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}

	private static ClassLoader getContextClassLoader() {
		return Thread.currentThread().getContextClassLoader();
	}
}
