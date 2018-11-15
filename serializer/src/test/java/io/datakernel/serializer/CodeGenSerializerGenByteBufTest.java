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

package io.datakernel.serializer;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import io.datakernel.serializer.annotations.SerializeNullable;
import io.datakernel.serializer.asm.SerializerGenByteBuf;
import io.datakernel.stream.processor.ByteBufRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public final class CodeGenSerializerGenByteBufTest {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	private static <T> T doTest(T testData1, BufferSerializer<T> serializer, BufferSerializer<T> deserializer) {
		byte[] array = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(array);
		serializer.serialize(buf, testData1);
		return deserializer.deserialize(buf);
	}

	@Test
	public void test() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuf testBuffer1 = ByteBuf.wrapForReading(array);

		BufferSerializer<ByteBuf> serializerByteBuf = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.build(ByteBuf.class);
		ByteBuf testBuffer2 = doTest(testBuffer1.slice(), serializerByteBuf, serializerByteBuf);

		assertNotNull(testBuffer2);
		assertEqualsBufs(testBuffer1, testBuffer2);

		testBuffer2.recycle();
	}

	@Test
	public void testWrap() {
		byte[] array = new byte[100];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuf testBuffer1 = ByteBuf.wrapForWriting(array);

		BufferSerializer<ByteBuf> serializerByteBuf = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuf.class, new SerializerGenByteBuf(true, true))
				.build(ByteBuf.class);

		ByteBuf testBuffer2 = doTest(testBuffer1.slice(), serializerByteBuf, serializerByteBuf);

		assertNotNull(testBuffer2);
		assertEqualsBufs(testBuffer1, testBuffer2);
	}

	@Test
	public void test2() {
		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuf testBuffer1 = ByteBuf.wrap(array, 10, 10 + 100);
		ByteBuf testBuffer2 = ByteBuf.wrap(array, 110, 110 + 100);

		BufferSerializer<ByteBuf> serializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.build(ByteBuf.class);

		byte[] buffer = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(buffer);
		serializer.serialize(buf, testBuffer1.slice());
		serializer.serialize(buf, testBuffer2.slice());

		ByteBuf testBuffer3 = serializer.deserialize(buf);
		ByteBuf testBuffer4 = serializer.deserialize(buf);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer4);
		assertEqualsBufs(testBuffer1, testBuffer3);
		assertEqualsBufs(testBuffer2, testBuffer4);

		assertEquals(10, testBuffer3.peek(0));
		buffer[testBuffer3.readPosition()] = 123;
		assertEquals(10, testBuffer3.peek(0));

		testBuffer3.recycle();
		testBuffer4.recycle();
	}

	@Test
	public void testWrap2() {

		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		ByteBuf testBuffer1 = ByteBuf.wrap(array, 10, 10 + 100);
		ByteBuf testBuffer2 = ByteBuf.wrap(array, 110, 110 + 100);

		BufferSerializer<ByteBuf> serializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuf.class, new SerializerGenByteBuf(true, true))
				.build(ByteBuf.class);

		byte[] buffer = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(buffer);
		serializer.serialize(buf, testBuffer1.slice());
		serializer.serialize(buf, testBuffer2.slice());

		ByteBuf testBuffer3 = serializer.deserialize(buf);
		ByteBuf testBuffer4 = serializer.deserialize(buf);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer4);
		assertEqualsBufs(testBuffer1, testBuffer3);
		assertEqualsBufs(testBuffer2, testBuffer4);

		assertEquals(10, testBuffer3.peek(0));
		buffer[testBuffer3.readPosition()] = 123;
		assertEquals(123, testBuffer3.peek(0));
	}

	public static final class TestByteBufData {
		private final ByteBuf buffer;

		public TestByteBufData(@Deserialize("buffer") ByteBuf buffer) {
			this.buffer = buffer;
		}

		public TestByteBufData() {
			this.buffer = null;
		}

		@Serialize(order = 0)
		@SerializeNullable
		public ByteBuf getBuffer() {
			return buffer;
		}
	}

	@Test
	public void test3() {
		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufData testBuffer1 = new TestByteBufData(ByteBuf.wrap(array, 10, 10 + 2));
		TestByteBufData testBuffer0 = new TestByteBufData(null);
		TestByteBufData testBuffer2 = new TestByteBufData(ByteBuf.wrap(array, 110, 110 + 3));

		BufferSerializer<TestByteBufData> serializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.build(TestByteBufData.class);

		byte[] buffer = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(buffer);
		serializer.serialize(buf, testBuffer1);
		serializer.serialize(buf, testBuffer0);
		serializer.serialize(buf, testBuffer2);

		TestByteBufData testBuffer3 = serializer.deserialize(buf);
		TestByteBufData testBuffer00 = serializer.deserialize(buf);
		TestByteBufData testBuffer4 = serializer.deserialize(buf);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer00);
		assertNotNull(testBuffer4);
		assertNotNull(testBuffer3.getBuffer());
		assertEqualsBufs(testBuffer1.getBuffer(), testBuffer3.getBuffer());
		assertNull(testBuffer00.getBuffer());
		assertNotNull(testBuffer4.getBuffer());
		assertEqualsBufs(testBuffer2.getBuffer(), testBuffer4.getBuffer());
		if (testBuffer3.buffer != null) {
			testBuffer3.buffer.recycle();
		}
		if (testBuffer00.buffer != null) {
			testBuffer00.buffer.recycle();
		}
		if (testBuffer4.buffer != null) {
			testBuffer4.buffer.recycle();
		}
	}

	@Test
	public void testWrap3() {
		byte[] array = new byte[1024];
		for (int i = 0; i < array.length; i++)
			array[i] = (byte) i;

		TestByteBufData testBuffer1 = new TestByteBufData(ByteBuf.wrap(array, 10, 10 + 100));
		TestByteBufData testBuffer0 = new TestByteBufData(null);
		TestByteBufData testBuffer2 = new TestByteBufData(ByteBuf.wrap(array, 110, 110 + 100));

		BufferSerializer<TestByteBufData> serializer = SerializerBuilder
				.create(ClassLoader.getSystemClassLoader())
				.withSerializer(ByteBuf.class, new SerializerGenByteBuf(true, true))
				.build(TestByteBufData.class);

		byte[] buffer = new byte[1000];
		ByteBuf buf = ByteBuf.wrapForWriting(buffer);
		serializer.serialize(buf, testBuffer1);
		serializer.serialize(buf, testBuffer0);
		serializer.serialize(buf, testBuffer2);

		TestByteBufData testBuffer3 = serializer.deserialize(buf);
		TestByteBufData testBuffer00 = serializer.deserialize(buf);
		TestByteBufData testBuffer4 = serializer.deserialize(buf);

		assertNotNull(testBuffer3);
		assertNotNull(testBuffer00);
		assertNotNull(testBuffer4);
		assertNotNull(testBuffer3.getBuffer());
		assertEqualsBufs(testBuffer1.getBuffer(), testBuffer3.getBuffer());
		assertNull(testBuffer00.getBuffer());
		assertNotNull(testBuffer4.getBuffer());
		assertEqualsBufs(testBuffer2.getBuffer(), testBuffer4.getBuffer());
	}

	static void assertEqualsBufs(ByteBuf buf1, ByteBuf buf2) {
		assertEquals(buf1.readRemaining(), buf2.readRemaining());
		for (int i = 0; i < buf1.readRemaining(); i++) {
			assertEquals(buf1.peek(i), buf2.peek(i));
		}
	}

}
