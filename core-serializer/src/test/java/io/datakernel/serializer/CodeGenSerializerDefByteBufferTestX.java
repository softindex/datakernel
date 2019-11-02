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

import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.impl.SerializerDefByteBuffer;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CodeGenSerializerDefByteBufferTestX {

	private static <T> T doTest(T testData1, BinarySerializer<T> serializer, BinarySerializer<T> deserializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return deserializer.decode(array, 0);
	}

	@Test
	public void test() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BinarySerializer<ByteBuffer> serializerByteBuffer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.build(ByteBuffer.class);
		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void testWrap() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BinarySerializer<ByteBuffer> serializerByteBuffer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuffer.class, new SerializerDefByteBuffer(true))
				.build(ByteBuffer.class);

		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void test2() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);

		BinarySerializer<ByteBuffer> serializer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.build(ByteBuffer.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		ByteBuffer testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertEquals(testBuffer1, testBuffer3);

		int position = testBuffer3.position();
		assertEquals(10, testBuffer3.get(position));
		buffer[position] = 123;
		assertEquals(10, testBuffer3.get(position));
	}

	@Test
	public void testWrap2() {
		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);

		BinarySerializer<ByteBuffer> serializer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuffer.class, new SerializerDefByteBuffer(true))
				.build(ByteBuffer.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		ByteBuffer testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertEquals(testBuffer1, testBuffer3);

		int position = testBuffer3.position();
		assertEquals(10, testBuffer3.get(position));
		buffer[position] = 123;
		assertEquals(123, testBuffer3.get(position));
	}

	public static final class TestByteBufferData {
		private final ByteBuffer buffer;

		public TestByteBufferData(@Deserialize("buffer") ByteBuffer buffer) {
			this.buffer = buffer;
		}

		public TestByteBufferData() {
			this.buffer = null;
		}

		@Serialize(order = 0)
		@SerializeNullable
		public ByteBuffer getBuffer() {
			return buffer;
		}
	}

	@Test
	public void test3() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 2));

		BinarySerializer<TestByteBufferData> serializer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.build(TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		TestByteBufferData testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
	}

	@Test
	public void testWrap3() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 100));

		BinarySerializer<TestByteBufferData> serializer = SerializerBuilder.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuffer.class, new SerializerDefByteBuffer(true))
				.build(TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		serializer.encode(buffer, 0, testBuffer1);
		TestByteBufferData testBuffer3 = serializer.decode(buffer, 0);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
	}

}
