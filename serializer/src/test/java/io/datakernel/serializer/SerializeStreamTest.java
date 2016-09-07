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

import io.datakernel.codegen.utils.DefiningClassLoader;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

import static io.datakernel.serializer.DataOutputStreamEx.MAX_SIZE_127;
import static io.datakernel.serializer.asm.BufferSerializers.*;
import static org.junit.Assert.*;

public class SerializeStreamTest {

	@Test
	public void test() throws IOException, SerializeException, DeserializeException {
		BufferSerializer<String> bufferSerializer = SerializerBuilder.newDefaultInstance(new DefiningClassLoader())
				.create(String.class);

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream, 30);

		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};
		for (String string : strings) {
			dataOutputStream.serialize(bufferSerializer, string, MAX_SIZE_127);
		}
		dataOutputStream.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteInputStream, 50);

		for (String string : strings) {
			assertEquals(string, dataInputStream.deserialize(bufferSerializer));
		}
		assertTrue(dataInputStream.isEndOfStream());
	}

	@Test
	public void testLittleBuffer() throws IOException, SerializeException, DeserializeException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream, 30);

		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};
		for (String string : strings) {
			dataOutputStream.serialize(utf8Serializer(), string, MAX_SIZE_127);
		}
		dataOutputStream.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteInputStream, 3);

		for (String string : strings) {
			assertEquals(string, dataInputStream.deserialize(utf8Serializer()));
		}
		assertTrue(dataInputStream.isEndOfStream());
	}

	@Test
	public void testInteger() throws IOException, SerializeException, DeserializeException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream, 30);

		final Integer[] integers = new Integer[]{10, 20, 30, 42};
		for (Integer integer : integers) {
			dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
		}
		dataOutputStream.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteInputStream, 10);

		for (Integer integer : integers) {
			assertEquals(integer, dataInputStream.deserialize(intSerializer()));
		}
		assertTrue(dataInputStream.isEndOfStream());
	}

	@Test
	public void testChangeOutputStream() throws IOException, SerializeException, DeserializeException {
		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream1, 30);

		final Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		final Integer[] integers2 = new Integer[]{100, 200, 300, 420};
		for (Integer integer : integers1) {
			dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
		}

		dataOutputStream.changeOutputStream(byteOutputStream2);
		for (Integer integer : integers2) {
			dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
		}
		dataOutputStream.close();

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		DataInputStreamEx dataInputStream1 = new DataInputStreamEx(byteInputStream1, 10);

		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());
		DataInputStreamEx dataInputStream2 = new DataInputStreamEx(byteInputStream2, 10);

		for (Integer integer : integers1) {
			assertEquals(integer, dataInputStream1.deserialize(intSerializer()));
		}

		for (Integer integer : integers2) {
			assertEquals(integer, dataInputStream2.deserialize(intSerializer()));
		}
		assertTrue(dataInputStream1.isEndOfStream());
		assertTrue(dataInputStream2.isEndOfStream());
	}

	@Test
	public void testChangeInputStream() throws IOException, SerializeException, DeserializeException {
		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream1 = new DataOutputStreamEx(byteOutputStream1, 30);

		final Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		final Integer[] integers2 = new Integer[]{10, 20, 30, 42};

		for (Integer integer : integers1) {
			dataOutputStream1.serialize(intSerializer(), integer, MAX_SIZE_127);
		}
		dataOutputStream1.close();

		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream2 = new DataOutputStreamEx(byteOutputStream2, 30);
		for (Integer integer : integers2) {
			dataOutputStream2.serialize(intSerializer(), integer, MAX_SIZE_127);
		}
		dataOutputStream2.close();

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteInputStream1, 10);

		for (Integer integer : integers1) {
			assertEquals(integer, dataInputStream.deserialize(intSerializer()));
		}

		dataInputStream.changeInputStream(byteInputStream2);
		for (Integer integer : integers2) {
			assertEquals(integer, dataInputStream.deserialize(intSerializer()));
		}

		assertTrue(dataInputStream.isEndOfStream());
	}

	@Test
	public void testRestorePositionAfterSizeException() throws Exception {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream);
		byte[] array1 = createTestByteArray(100, (byte) 10);
		byte[] array2 = createTestByteArray(100, (byte) 20);
		byte[] tooBigArray = createTestByteArray(150, (byte) 30);
		dataOutputStream.serialize(bytesSerializer(), array1, MAX_SIZE_127);
		try {
			dataOutputStream.serialize(bytesSerializer(), tooBigArray, MAX_SIZE_127);
		} catch (SerializeException ignored) {
		}
		dataOutputStream.serialize(bytesSerializer(), array2, MAX_SIZE_127);
		dataOutputStream.close();

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteArrayInputStream);
		byte[] readArray1 = dataInputStream.deserialize(bytesSerializer());
		byte[] readArray2 = dataInputStream.deserialize(bytesSerializer());
		assertTrue(dataInputStream.isEndOfStream());
		assertArrayEquals(array1, readArray1);
		assertArrayEquals(array2, readArray2);
	}

	private static byte[] createTestByteArray(int length, byte fillValue) {
		byte[] array = new byte[length];
		Arrays.fill(array, fillValue);
		return array;
	}

	public static class TestClass {
		@Serialize(order = 0)
		public final String str;

		public TestClass(@Deserialize("str") String str) {
			this.str = str;
		}
	}

	@Test
	public void testRestorePositionAfterException() throws Exception {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		DataOutputStreamEx dataOutputStream = new DataOutputStreamEx(byteOutputStream);
		TestClass validObj1 = new TestClass("abc");
		TestClass validObj2 = new TestClass("=xyz=");
		TestClass invalidObj = new TestClass(null);
		BufferSerializer<TestClass> serializer = SerializerBuilder.newDefaultInstance(new DefiningClassLoader()).create(TestClass.class);
		dataOutputStream.serialize(serializer, validObj1, MAX_SIZE_127);
		try {
			dataOutputStream.serialize(serializer, invalidObj, MAX_SIZE_127);
		} catch (SerializeException ignored) {
		}
		dataOutputStream.serialize(serializer, validObj2, MAX_SIZE_127);
		dataOutputStream.close();

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		DataInputStreamEx dataInputStream = new DataInputStreamEx(byteArrayInputStream);
		TestClass readObj1 = dataInputStream.deserialize(serializer);
		TestClass readObj2 = dataInputStream.deserialize(serializer);
		assertTrue(dataInputStream.isEndOfStream());
		assertEquals(validObj1.str, readObj1.str);
		assertEquals(validObj2.str, readObj2.str);
	}
}
