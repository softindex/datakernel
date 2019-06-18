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

package io.datakernel.serializer.examples;

import com.carrotsearch.hppc.*;
import io.datakernel.codegen.DefiningClassLoader;
import io.datakernel.serializer.BinarySerializer;
import io.datakernel.test.rules.ByteBufRule;
import io.datakernel.test.rules.LoggingRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public final class SerializerGenHppc7Example {
	@Rule
	public ByteBufRule byteBufRule = new ByteBufRule();

	@ClassRule
	public static final LoggingRule loggingRule = new LoggingRule();

	private static <T> T doTest(T testData1, BinarySerializer<T> serializer) {
		byte[] array = new byte[1000];
		serializer.encode(array, 0, testData1);
		return serializer.decode(array, 0);
	}

	private static <T> BinarySerializer<T> getBufferSerializer(Class<T> collectionType) {
		return SerializerBuilderUtils.createWithHppc7Support(DefiningClassLoader.create())
				.build(collectionType);
	}

	@Test
	public void testIntByteMap() {
		BinarySerializer<IntByteMap> serializer = getBufferSerializer(IntByteMap.class);

		IntByteMap testMap1 = new IntByteHashMap();

		IntByteMap testMap2 = doTest(testMap1, serializer);
		assertNotNull(testMap2);
		assertEquals(testMap1, testMap2);

		testMap1.put(0, (byte) 10);
		testMap1.put(1, (byte) 11);
		IntByteMap testMap3 = doTest(testMap1, serializer);
		assertNotNull(testMap3);
		assertEquals(testMap1, testMap3);
	}

	@Test
	public void testByteSet() {
		BinarySerializer<ByteSet> serializer = getBufferSerializer(ByteSet.class);

		ByteSet test1 = new ByteHashSet();
		ByteSet test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add((byte) 10);
		test1.add((byte) 11);

		ByteSet test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}

	@Test
	public void testIntArrayList() {
		BinarySerializer<IntArrayList> serializer = getBufferSerializer(IntArrayList.class);

		IntArrayList test1 = new IntArrayList();
		IntArrayList test2 = doTest(test1, serializer);
		assertNotNull(test2);
		assertEquals(test1, test2);

		test1.add(10);
		test1.add(11);

		IntArrayList test3 = doTest(test1, serializer);
		assertNotNull(test3);
		assertEquals(test1, test3);
	}
}

