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
import io.datakernel.serializer.annotations.SerializeFixedSize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.asm.SerializerGen;

import java.util.Arrays;

import static io.datakernel.serializer.SerializerFactory.createBufferSerializerFactory;

/**
 * Example of serialization and deserialization of an object with fixed size fields.
 */
public class FixedSizeFieldsSerializationExample {
	private static final SerializerFactory bufferSerializerFactory = createBufferSerializerFactory();

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
