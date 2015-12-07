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

package io.datakernel.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MemSizeTest {

	@Test
	public void testMemSize() {
		long bytes;

		bytes = 0;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 512;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024;
		assertEquals("1Kb", MemSize.of(bytes).toString());

		bytes = 2048;
		assertEquals("2Kb", MemSize.of(bytes).toString());

		bytes = 1025;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L;
		assertEquals("1Mb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L + 15;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 10;
		assertEquals("10Mb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 10 - 1;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L;
		assertEquals("1Gb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L + 15;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 10;
		assertEquals("10Gb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 10 - 1;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 1024L;
		assertEquals("1Tb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 1024L + 15;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 1024L * 10;
		assertEquals("10Tb", MemSize.of(bytes).toString());

		bytes = 1024L * 1024L * 1024L * 1024L * 10 - 1;
		assertEquals(bytes + "b", MemSize.of(bytes).toString());
	}
}
