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

import io.datakernel.asm.DefiningClassLoader;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static io.datakernel.serializer.asm.BufferSerializers.intSerializer;
import static io.datakernel.serializer.asm.BufferSerializers.stringSerializer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SerializeStreamTest {

	@Test
	public void test() throws IOException {
		BufferSerializer<String> bufferSerializer = SerializerBuilder.newDefaultInstance(new DefiningClassLoader())
				.create(String.class);

		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		OutputStreamSerializer<String> outputStreamSerializer =
				new OutputStreamSerializer<>(byteOutputStream, bufferSerializer, 30, 50, false);

		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};
		for (String string : strings) {
			outputStreamSerializer.write(string);
		}
		outputStreamSerializer.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		InputStreamDeserializer<String> inputStreamDeserializer =
				new InputStreamDeserializer<>(byteInputStream, bufferSerializer, 30, 50);

		for (String string : strings) {
			assertEquals(string, inputStreamDeserializer.read());
		}
		assertNull(inputStreamDeserializer.read());
	}

	@Test
	public void testLittleBuffer() throws IOException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		final OutputStreamSerializer<String> outputStreamSerializer =
				new OutputStreamSerializer<>(byteOutputStream, stringSerializer(), 30, 3, false);

		String[] strings = new String[]{"test1-string", "test2-int", "test3-t", "test4-str"};
		for (String string : strings) {
			outputStreamSerializer.write(string);
		}
		outputStreamSerializer.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		InputStreamDeserializer<String> inputStreamDeserializer =
				new InputStreamDeserializer<>(byteInputStream, stringSerializer(), 30, 3);

		for (String string : strings) {
			assertEquals(string, inputStreamDeserializer.read());
		}
		assertNull(inputStreamDeserializer.read());
	}

	@Test
	public void testInteger() throws IOException {
		ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
		final OutputStreamSerializer<Integer> outputStreamSerializer =
				new OutputStreamSerializer<>(byteOutputStream, intSerializer(), 30, 10, false);

		final Integer[] integers = new Integer[]{10, 20, 30, 42};
		for (Integer integer : integers) {
			outputStreamSerializer.write(integer);
		}
		outputStreamSerializer.close();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(byteOutputStream.toByteArray());
		InputStreamDeserializer<Integer> inputStreamDeserializer =
				new InputStreamDeserializer<>(byteInputStream, intSerializer(), 30, 10);

		for (Integer integer : integers) {
			assertEquals(integer, inputStreamDeserializer.read());
		}
		assertNull(inputStreamDeserializer.read());
	}

	@Test
	public void testChangeOutputStream() throws IOException {
		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		final OutputStreamSerializer<Integer> outputStreamSerializer =
				new OutputStreamSerializer<>(byteOutputStream1, intSerializer(), 30, 10, false);

		final Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		final Integer[] integers2 = new Integer[]{100, 200, 300, 420};
		for (Integer integer : integers1) {
			outputStreamSerializer.write(integer);
		}

		outputStreamSerializer.changeOutputStream(byteOutputStream2);
		for (Integer integer : integers2) {
			outputStreamSerializer.write(integer);
		}
		outputStreamSerializer.close();

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		InputStreamDeserializer<Integer> inputStreamDeserializer1 =
				new InputStreamDeserializer<>(byteInputStream1, intSerializer(), 30, 10);

		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());
		InputStreamDeserializer<Integer> inputStreamDeserializer2 =
				new InputStreamDeserializer<>(byteInputStream2, intSerializer(), 30, 10);

		for (Integer integer : integers1) {
			assertEquals(integer, inputStreamDeserializer1.read());
		}

		for (Integer integer : integers2) {
			assertEquals(integer, inputStreamDeserializer2.read());
		}
		assertNull(inputStreamDeserializer1.read());
		assertNull(inputStreamDeserializer2.read());
	}

	@Test
	public void testChangeInputStream() throws IOException {
		ByteArrayOutputStream byteOutputStream1 = new ByteArrayOutputStream();
		final OutputStreamSerializer<Integer> outputStreamSerializer1 =
				new OutputStreamSerializer<>(byteOutputStream1, intSerializer(), 30, 10, false);

		final Integer[] integers1 = new Integer[]{10, 20, 30, 42};
		final Integer[] integers2 = new Integer[]{10, 20, 30, 42};

		for (Integer integer : integers1) {
			outputStreamSerializer1.write(integer);
		}
		outputStreamSerializer1.close();

		ByteArrayOutputStream byteOutputStream2 = new ByteArrayOutputStream();
		final OutputStreamSerializer<Integer> outputStreamSerializer2 =
				new OutputStreamSerializer<>(byteOutputStream2, intSerializer(), 30, 10, false);
		for (Integer integer : integers2) {
			outputStreamSerializer2.write(integer);
		}
		outputStreamSerializer2.close();

		ByteArrayInputStream byteInputStream1 = new ByteArrayInputStream(byteOutputStream1.toByteArray());
		ByteArrayInputStream byteInputStream2 = new ByteArrayInputStream(byteOutputStream2.toByteArray());
		InputStreamDeserializer<Integer> inputStreamDeserializer =
				new InputStreamDeserializer<>(byteInputStream1, intSerializer(), 30, 10);

		for (Integer integer : integers1) {
			assertEquals(integer, inputStreamDeserializer.read());
		}

		inputStreamDeserializer.changeInputStream(byteInputStream2);
		for (Integer integer : integers2) {
			assertEquals(integer, inputStreamDeserializer.read());
		}

		assertNull(inputStreamDeserializer.read());
	}
}
