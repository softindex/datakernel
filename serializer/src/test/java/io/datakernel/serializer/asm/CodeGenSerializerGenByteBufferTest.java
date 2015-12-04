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

import io.datakernel.serializer.BufferSerializer;
import io.datakernel.serializer.SerializationInputBuffer;
import io.datakernel.serializer.SerializationOutputBuffer;
import io.datakernel.serializer.SerializerBuilder;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class CodeGenSerializerGenByteBufferTest {

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer, BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(array);
		serializer.serialize(output, testData1);
		SerializationInputBuffer input = new SerializationInputBuffer(array, 0);
		return deserializer.deserialize(input);
	}

	@Test
	public void test() throws Exception {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BufferSerializer<ByteBuffer> serializerByteBuffer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer())
				.create(ByteBuffer.class);
		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void testWrap() throws Exception {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array);

		BufferSerializer<ByteBuffer> serializerByteBuffer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer(true))
				.create(ByteBuffer.class);

		ByteBuffer testBuffer2 = doTest(testBuffer1, serializerByteBuffer, serializerByteBuffer);

		assertNotNull(testBuffer2);
		assertEquals(testBuffer1, testBuffer2);
	}

	@Test
	public void test2() throws Exception {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);
		ByteBuffer testBuffer2 = ByteBuffer.wrap(array, 110, 100);

		BufferSerializer<ByteBuffer> serializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer())
				.create(ByteBuffer.class);

		byte[] buffer = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(buffer);
		serializer.serialize(output, testBuffer1);
		serializer.serialize(output, testBuffer2);

		SerializationInputBuffer input = new SerializationInputBuffer(buffer, 0);
		ByteBuffer testBuffer3 = serializer.deserialize(input);
		ByteBuffer testBuffer4 = serializer.deserialize(input);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer4);
		assertEquals(testBuffer1, testBuffer3);
		assertEquals(testBuffer2, testBuffer4);

		int position = testBuffer3.position();
		assertEquals(10, testBuffer3.get(position));
		buffer[position] = 123;
		assertEquals(10, testBuffer3.get(position));
	}

	@Test
	public void testWrap2() throws Exception {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuffer testBuffer1 = ByteBuffer.wrap(array, 10, 100);
		ByteBuffer testBuffer2 = ByteBuffer.wrap(array, 110, 100);

		BufferSerializer<ByteBuffer> serializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer(true))
				.create(ByteBuffer.class);

		byte[] buffer = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(buffer);
		serializer.serialize(output, testBuffer1);
		serializer.serialize(output, testBuffer2);

		SerializationInputBuffer input = new SerializationInputBuffer(buffer, 0);
		ByteBuffer testBuffer3 = serializer.deserialize(input);
		ByteBuffer testBuffer4 = serializer.deserialize(input);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer4);
		assertEquals(testBuffer1, testBuffer3);
		assertEquals(testBuffer2, testBuffer4);

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
	public void test3() throws Exception {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 2));
		TestByteBufferData testBuffer0 = new TestByteBufferData(null);
		TestByteBufferData testBuffer2 = new TestByteBufferData(ByteBuffer.wrap(array, 110, 3));

		BufferSerializer<TestByteBufferData> serializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer())
				.create(TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(buffer);
		serializer.serialize(output, testBuffer1);
		serializer.serialize(output, testBuffer0);
		serializer.serialize(output, testBuffer2);

		SerializationInputBuffer input = new SerializationInputBuffer(buffer, 0);
		TestByteBufferData testBuffer3 = serializer.deserialize(input);
		TestByteBufferData testBuffer00 = serializer.deserialize(input);
		TestByteBufferData testBuffer4 = serializer.deserialize(input);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer00);
		assertNotNull(testBuffer4);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
		assertNull(testBuffer00.getBuffer());
		assertNotNull(testBuffer4.getBuffer());
		assertEquals(testBuffer2.getBuffer(), testBuffer4.getBuffer());
	}

	@Test
	public void testWrap3() throws Exception {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufferData testBuffer1 = new TestByteBufferData(ByteBuffer.wrap(array, 10, 100));
		TestByteBufferData testBuffer0 = new TestByteBufferData(null);
		TestByteBufferData testBuffer2 = new TestByteBufferData(ByteBuffer.wrap(array, 110, 100));

		BufferSerializer<TestByteBufferData> serializer = SerializerBuilder
				.newDefaultInstance(ClassLoader.getSystemClassLoader())
				.register(ByteBuffer.class, new SerializerGenByteBuffer(true))
				.create(TestByteBufferData.class);

		byte[] buffer = new byte[1000];
		SerializationOutputBuffer output = new SerializationOutputBuffer(buffer);
		serializer.serialize(output, testBuffer1);
		serializer.serialize(output, testBuffer0);
		serializer.serialize(output, testBuffer2);

		SerializationInputBuffer input = new SerializationInputBuffer(buffer, 0);
		TestByteBufferData testBuffer3 = serializer.deserialize(input);
		TestByteBufferData testBuffer00 = serializer.deserialize(input);
		TestByteBufferData testBuffer4 = serializer.deserialize(input);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer00);
		assertNotNull(testBuffer4);
		assertNotNull(testBuffer3.getBuffer());
		assertEquals(testBuffer1.getBuffer(), testBuffer3.getBuffer());
		assertNull(testBuffer00.getBuffer());
		assertNotNull(testBuffer4.getBuffer());
		assertEquals(testBuffer2.getBuffer(), testBuffer4.getBuffer());
	}

}
