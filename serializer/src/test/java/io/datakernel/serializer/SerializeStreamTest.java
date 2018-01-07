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

import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.serializer.annotations.Deserialize;
import io.datakernel.serializer.annotations.Serialize;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufPool.*;
import static io.datakernel.serializer.DataOutputStreamEx.*;
import static io.datakernel.serializer.asm.BufferSerializers.*;
import static org.junit.Assert.*;

public class SerializeStreamTest {

	@Test
	public void test() throws IOException, SerializeException, DeserializeException {
		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};
		BufferSerializer<String> bufferSerializer = SerializerBuilder.create(DefiningClassLoader.create())
				.build(String.class);

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 30)) {
			for (String string : strings) {
				dataOutputStream.serialize(bufferSerializer, string, MAX_SIZE_127);
			}
		}

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteInputStream, 50)) {
			List<String> newStrings = new ArrayList<>(4);
			while (!dataInputStream.isEndOfStream()) {
				newStrings.add(dataInputStream.deserialize(bufferSerializer));
			}

			assertArrayEquals(strings, newStrings.toArray(new String[4]));
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testLittleBuffer() throws IOException, SerializeException, DeserializeException {
		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 30)) {
			for (String string : strings) {
				dataOutputStream.serialize(utf8Serializer(), string, MAX_SIZE_127);
			}
		}

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteInputStream, 3)) {
			for (String string : strings) {
				assertEquals(string, dataInputStream.deserialize(utf8Serializer()));
			}

			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testInteger() throws IOException, SerializeException, DeserializeException {
		Integer[] integers = new Integer[]{10, 20, 30, 42};

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 30)) {
			for (Integer integer : integers) {
				dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
			}
		}

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteInputStream, 10)) {
			for (Integer integer : integers) {
				assertEquals(integer, dataInputStream.deserialize(intSerializer()));
			}
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testChangeOutputStream() throws IOException, SerializeException, DeserializeException {
		Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		Integer[] integers2 = new Integer[]{100, 200, 300, 420};

		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream1, 30)) {
			for (Integer integer : integers1) {
				dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
			}

			dataOutputStream.changeOutputStream(byteOutputStream2);
			for (Integer integer : integers2) {
				dataOutputStream.serialize(intSerializer(), integer, MAX_SIZE_127);
			}
		}

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());

		try (DataInputStreamEx dataInputStream1 = DataInputStreamEx.create(byteInputStream1, 10);
		     DataInputStreamEx dataInputStream2 = DataInputStreamEx.create(byteInputStream2, 10)) {
			for (Integer integer : integers1) {
				assertEquals(integer, dataInputStream1.deserialize(intSerializer()));
			}

			for (Integer integer : integers2) {
				assertEquals(integer, dataInputStream2.deserialize(intSerializer()));
			}
			assertTrue(dataInputStream1.isEndOfStream());
			assertTrue(dataInputStream2.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testChangeInputStream() throws IOException, SerializeException, DeserializeException {
		Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		Integer[] integers2 = new Integer[]{10, 20, 30, 42};

		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream1 = DataOutputStreamEx.create(byteOutputStream1, 30)) {
			for (Integer integer : integers1) {
				dataOutputStream1.serialize(intSerializer(), integer, MAX_SIZE_127);
			}
		}

		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream2 = DataOutputStreamEx.create(byteOutputStream2, 30)) {
			for (Integer integer : integers2) {
				dataOutputStream2.serialize(intSerializer(), integer, MAX_SIZE_127);
			}
		}

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteInputStream1, 10)) {
			for (Integer integer : integers1) {
				assertEquals(integer, dataInputStream.deserialize(intSerializer()));
			}

			dataInputStream.changeInputStream(byteInputStream2);
			for (Integer integer : integers2) {
				assertEquals(integer, dataInputStream.deserialize(intSerializer()));
			}

			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testRestorePositionAfterSizeException() throws Exception {
		byte[] array1 = createTestByteArray(100, (byte) 10);
		byte[] array2 = createTestByteArray(100, (byte) 20);
		byte[] tooBigArray = createTestByteArray(150, (byte) 30);

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream)) {
			dataOutputStream.serialize(bytesSerializer(), array1, MAX_SIZE_127);
			try {
				dataOutputStream.serialize(bytesSerializer(), tooBigArray, MAX_SIZE_127);
			} catch (SerializeException ignored) {
			}
			dataOutputStream.serialize(bytesSerializer(), array2, MAX_SIZE_127);
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteArrayInputStream)) {
			assertArrayEquals(array1, dataInputStream.deserialize(bytesSerializer()));
			assertArrayEquals(array2, dataInputStream.deserialize(bytesSerializer()));
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testRestorePositionAfterException() throws Exception {
		TestClass validObj1 = new TestClass("abc");
		TestClass validObj2 = new TestClass("=xyz=");
		TestClass invalidObj = new TestClass(null);
		BufferSerializer<TestClass> serializer = SerializerBuilder.create(DefiningClassLoader.create()).build(TestClass.class);

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream)) {
			dataOutputStream.serialize(serializer, validObj1, MAX_SIZE_127);
			try {
				dataOutputStream.serialize(serializer, invalidObj, MAX_SIZE_127);
			} catch (SerializeException ignored) {
			}
			dataOutputStream.serialize(serializer, validObj2, MAX_SIZE_127);
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteArrayInputStream)) {
			assertEquals(validObj1.str, dataInputStream.deserialize(serializer).str);
			assertEquals(validObj2.str, dataInputStream.deserialize(serializer).str);
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStartBufferLessThanMessage() throws IOException, SerializeException, DeserializeException {
		BufferSerializer<TestClass> serializer = SerializerBuilder.create(DefiningClassLoader.create()).build(TestClass.class);
		TestClass validObj1 = new TestClass("22222");

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (final DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 1)) {
			dataOutputStream.serialize(serializer, validObj1, MAX_SIZE_127);
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (final DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteArrayInputStream)) {
			assertEquals(validObj1, dataInputStream.deserialize(serializer));
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testHeaderSize() throws IOException, SerializeException, DeserializeException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (final DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 1)) {
			dataOutputStream.serialize(intSerializer(), 42, MAX_SIZE_127);
			dataOutputStream.serialize(intSerializer(), 42, MAX_SIZE_16K);
			dataOutputStream.serialize(intSerializer(), 42, MAX_SIZE_2M);
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (final DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteArrayInputStream)) {
			assertEquals(Integer.valueOf(42), dataInputStream.deserialize(intSerializer()));
			assertEquals(Integer.valueOf(42), dataInputStream.deserialize(intSerializer()));
			assertEquals(Integer.valueOf(42), dataInputStream.deserialize(intSerializer()));
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test
	public void testStandardWrite() throws IOException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		try (final DataOutputStreamEx dataOutputStream = DataOutputStreamEx.create(byteOutputStream, 1)) {
			dataOutputStream.writeInt(42);
			dataOutputStream.writeBoolean(false);
			dataOutputStream.writeByte((byte) 43);
			dataOutputStream.writeChar((char) 44);
			dataOutputStream.writeDouble(45.46);
			dataOutputStream.writeFloat(47.48f);
			dataOutputStream.writeIso88591("49");
			dataOutputStream.writeIso88591("");// check specific situation
			dataOutputStream.writeLong(50L);
			dataOutputStream.writeShort((short) 51);
			dataOutputStream.writeUTF8("test 52");
			dataOutputStream.writeUTF8("");// check specific situation
			dataOutputStream.writeUTF16("тест 53");
			dataOutputStream.writeUTF16("");// check specific situation
			dataOutputStream.writeVarInt(-54);
			dataOutputStream.writeVarLong(-55);
			dataOutputStream.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
			dataOutputStream.write(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, 5, 2);
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		try (final DataInputStreamEx dataInputStream = DataInputStreamEx.create(byteArrayInputStream)) {
			assertEquals(42, dataInputStream.readInt());
			assertEquals(false, dataInputStream.readBoolean());
			assertEquals((byte) 43, dataInputStream.readByte());
			assertEquals((char) 44, dataInputStream.readChar());
			assertEquals(45.46, dataInputStream.readDouble(), 1e-6);
			assertEquals(47.48f, dataInputStream.readFloat(), 1e-6);
			assertEquals("49", dataInputStream.readIso88591());
			assertEquals("", dataInputStream.readIso88591()); // check specific situation
			assertEquals(50L, dataInputStream.readLong());
			assertEquals((short) 51, dataInputStream.readShort());
			assertEquals("test 52", dataInputStream.readUTF8());
			assertEquals("", dataInputStream.readUTF8()); // check specific situation
			assertEquals("тест 53", dataInputStream.readUTF16());
			assertEquals("", dataInputStream.readUTF16()); // check specific situation
			assertEquals(-54, dataInputStream.readVarInt());
			assertEquals(-55, dataInputStream.readVarLong());
			assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, read(dataInputStream, new byte[10]));
			assertArrayEquals(new byte[]{0, 0, 0, 0, 0, 6, 7, 0, 0, 0}, read(dataInputStream, new byte[10], 5, 2));
			assertTrue(dataInputStream.isEndOfStream());
		}

		assertEquals(getPoolItemsString(), getCreatedItems(), getPoolItems());
	}

	@Test(expected = IOException.class)
	public void testReadInvalidHeader() throws IOException, DeserializeException {
		DataInputStreamEx.create(new ByteArrayInputStream(new byte[]{-1, -1, -1, -1})).deserialize(intSerializer());
	}

	private static byte[] read(DataInputStreamEx dataInputStream, byte[] bytes) throws IOException {
		dataInputStream.read(bytes);
		return bytes;
	}

	private static byte[] read(DataInputStreamEx dataInputStream, byte[] bytes, int off, int len) throws IOException {
		dataInputStream.read(bytes, off, len);
		return bytes;
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

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestClass testClass = (TestClass) o;

			return str != null ? str.equals(testClass.str) : testClass.str == null;

		}

		@Override
		public int hashCode() {
			return str != null ? str.hashCode() : 0;
		}
	}
}
