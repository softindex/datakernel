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

import com.google.common.reflect.TypeToken;
import io.datakernel.serializer.*;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.asm.SerializerGen;

import java.net.UnknownHostException;

/**
 * Example 1
 * Example of serialization and deserialization of a simple object (no null fields, generics or complex objects, such as maps or arrays, as fields).
 */
public class SimpleObjectSerializationExample {
	private static final SerializerFactory bufferSerializerFactory = SerializerFactory.createBufferSerializerFactory();

	public static void main(String[] args) throws UnknownHostException {
		// Create a test object
		TestDataSimple testData1 = new TestDataSimple(10, "abc");
		testData1.setI(20);
		testData1.setIBoxed(30);
		testData1.setMultiple(40, "123");

		// Serialize testData1 and then deserialize it to testData2
		TestDataSimple testData2 = serializeAndDeserialize(TypeToken.of(TestDataSimple.class), testData1);

		// Compare them
		System.out.println(testData1.finalInt + " " + testData2.finalInt);
		System.out.println(testData1.finalString + " " + testData2.finalString);
		System.out.println(testData1.getI() + " " + testData2.getI());
		System.out.println(testData1.getIBoxed() + " " + testData2.getIBoxed());
		System.out.println(testData1.getGetterInt() + " " + testData2.getGetterInt());
		System.out.println(testData1.getGetterString() + " " + testData2.getGetterString());
	}

	private static <T> T serializeAndDeserialize(TypeToken<T> typeToken, T testData1) {
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

	public static class TestDataSimple {
		public TestDataSimple(@Deserialize("finalInt") int finalInt,
		                      @Deserialize("finalString") String finalString) {
			this.finalInt = finalInt;
			this.finalString = finalString;
		}

		@Serialize(order = 0)
		public final int finalInt;

		@Serialize(order = 1)
		public final String finalString;

		private int i;
		private Integer iBoxed;

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
		public Integer getIBoxed() {
			return iBoxed;
		}

		public void setIBoxed(Integer iBoxed) {
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

		public void setMultiple(@Deserialize("getterInt") int getterInt,
		                        @Deserialize("getterString") String getterString) {
			this.getterInt = getterInt;
			this.getterString = getterString;
		}
	}
}
