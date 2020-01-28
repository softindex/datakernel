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

package io.datakernel.common;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertEquals;

public class MemSizeTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testMemSize() {
		long bytes;

		bytes = 0;
		assertEquals(bytes + "", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("0 b").toLong());

		bytes = 512;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("512").toLong());

		bytes = 1024;
		assertEquals("1Kb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1kb").toLong());

		bytes = 2048;
		assertEquals("2Kb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 k 1024b").toLong());

		bytes = 1025;
		assertEquals("" + bytes, MemSize.of(bytes).format());

		bytes = 1024L * 1024L;
		assertEquals("1Mb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 mb").toLong());
		assertEquals(bytes, MemSize.valueOf("1024kb").toLong());

		bytes = 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 m 15").toLong());

		bytes = 1024L * 1024L * 10;
		assertEquals("10Mb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("10mb").toLong());

		bytes = 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9 m 1023kb 1023b").toLong());

		bytes = 1024L * 1024L * 1024L;
		assertEquals("1Gb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1gb").toLong());

		bytes = 1024L * 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1g  15 b").toLong());

		bytes = 1024L * 1024L * 1024L * 10;
		assertEquals("10Gb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("10gb").toLong());

		bytes = 1024L * 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9gb 1023 b 1023mb 1023kb").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L;
		assertEquals("1Tb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1 TB").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L + 15;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("1Tb 15B").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L * 10;
		assertEquals("10Tb", MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9tB 1024 G").toLong());

		bytes = 1024L * 1024L * 1024L * 1024L * 10 - 1;
		assertEquals("" + bytes, MemSize.of(bytes).format());
		assertEquals(bytes, MemSize.valueOf("9 tb 1023 G 1023 mB 1023KB 1023B").toLong());

		assertEquals(228, MemSize.valueOf("228").toLong());
		assertEquals(1024 + 228, MemSize.valueOf("228 1 kb").toLong());
		assertEquals(1536, MemSize.valueOf("1.5kb").toLong());
		assertEquals(1024 * 1024 + 512 * 1024, MemSize.valueOf("1.5 mB").toLong());
		assertEquals(1024L * 1024L * 1024L + 512L * 1024L * 1024L, MemSize.valueOf("1.5 Gb").toLong());
		assertEquals(1024L * 1024L * 1024L * 1024L + 512L * 1024L * 1024L * 1024L, MemSize.valueOf("1.5 TB").toLong());
		assertEquals("2000000", MemSize.of(2000000L).toString());

		//      2 tb                                3 gb                        228 mb                1 b
		bytes = 1024L * 1024L * 1024L * 1024L * 2 + 1024L * 1024L * 1024L * 3 + 1024L * 1024L * 228 + 1;
		assertEquals(MemSize.valueOf("2 Tb 3gb 1b 228mb").format(), MemSize.of(bytes).format());

		MemSize memSize = MemSize.kilobytes(1423998);
		assertEquals(1458173952L, memSize.toLong());
		assertEquals("1423998Kb", StringFormatUtils.formatMemSize(memSize));
	}

	@Test
	public void testParsingExceptions() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("MemSize unit bytes cannot be fractional");
		MemSize.valueOf("2.2b");
	}

	@Test
	public void testLongOverflow() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Resulting number of bytes exceeds Long.MAX_VALUE");
		MemSize.kilobytes(Long.MAX_VALUE);
	}

	@Test
	public void testMemSizeOfNegative() {
		exception.expect(IllegalArgumentException.class);
		exception.expectMessage("Cannot create MemSize of negative value");
		MemSize.kilobytes(-1);

	}
}
