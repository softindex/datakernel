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

package io.datakernel.bytebuf;

import io.datakernel.util.ConcurrentStack;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteArraySlabPoolTest {

	private void checkAllocate(int size, int expectedSize, int[] poolSizes) {
		ByteBufPool.clear();

		ByteBuf bytes = ByteBufPool.allocate(size);
		assertEquals(expectedSize, bytes.array().length);
		for (int i = 0; i < poolSizes.length; i++) {
			assertTrue(ByteBufPool.getSlabs()[i].isEmpty());
		}
		bytes.recycle();

		assertTrue(poolSizes.length <= ByteBufPool.getSlabs().length);
		for (int i = 0; i < poolSizes.length; i++) {
			ConcurrentStack<ByteBuf> slab = ByteBufPool.getSlabs()[i];
			assertEquals(poolSizes[i] == 0 ? 0 : 1, slab.size());
			if (!slab.isEmpty()) {
				assertTrue(slab.peek().isRecycled());
				assertEquals(poolSizes[i], slab.peek().limit());
			}
		}
	}

	@Test
	public void testAllocate() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(16, 64);

		checkAllocate(0, 0, new int[]{0, 0, 0, 0, 0});
		checkAllocate(1, 1, new int[]{0, 0, 0, 0, 0});
		checkAllocate(2, 2, new int[]{0, 0, 0, 0, 0});
		checkAllocate(8, 8, new int[]{0, 0, 0, 0, 0});
		checkAllocate(9, 9, new int[]{0, 0, 0, 0, 0});
		checkAllocate(15, 15, new int[]{0, 0, 0, 0, 0});
		checkAllocate(16, 16, new int[]{0, 0, 0, 0, 16});
		checkAllocate(17, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(31, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(32, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(33, 64, new int[]{0, 0, 0, 0, 0, 0, 64});
		checkAllocate(63, 64, new int[]{0, 0, 0, 0, 0, 0, 64});
		checkAllocate(64, 64, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
		checkAllocate(65, 65, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
		checkAllocate(100, 100, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
	}

	private void checkReallocate(int size1, int size2, boolean equals) {
		ByteBufPool.clear();

		ByteBuf bytes1 = ByteBufPool.allocate(size1);
		assertTrue(size1 <= bytes1.limit());

		ByteBuf bytes2 = ByteBufPool.ensureWriteRemaining(bytes1, size2);
		assertTrue(size2 <= bytes2.limit());

		assertEquals(equals, bytes1 == bytes2);
	}

	@Test
	public void testReallocate() {
		ByteBufPool.clear();
		ByteBufPool.setSizes(16, 64);

		checkReallocate(0, 0, true);
		checkReallocate(0, 1, false);
		checkReallocate(0, 2, false);

		checkReallocate(1, 0, true);
		checkReallocate(1, 1, true);
		checkReallocate(1, 2, false);

		checkReallocate(2, 0, true);
		checkReallocate(2, 1, true);
		checkReallocate(2, 2, true);
		checkReallocate(2, 3, false);

		checkReallocate(15, 14, true);
		checkReallocate(15, 15, true);
		checkReallocate(15, 16, false);
		checkReallocate(15, 17, false);

		checkReallocate(16, 15, true);
		checkReallocate(16, 16, true);
		checkReallocate(16, 17, false);
		checkReallocate(16, 31, false);
		checkReallocate(16, 32, false);

		checkReallocate(31, 30, true);
		checkReallocate(31, 31, true);
		checkReallocate(31, 32, true);
		checkReallocate(31, 33, false);

		checkReallocate(32, 31, true);
		checkReallocate(32, 32, true);
		checkReallocate(32, 33, false);

		checkReallocate(33, 31, true);
		checkReallocate(33, 32, true);
		checkReallocate(33, 33, true);
		checkReallocate(33, 34, true);

		checkReallocate(63, 62, true);
		checkReallocate(63, 63, true);
		checkReallocate(63, 64, true);
		checkReallocate(63, 65, false);

		checkReallocate(64, 63, true);
		checkReallocate(64, 64, true);
		checkReallocate(64, 65, false);
		checkReallocate(64, 66, false);

		checkReallocate(65, 63, true);
		checkReallocate(65, 64, true);
		checkReallocate(65, 65, true);
		checkReallocate(65, 66, false);

		checkReallocate(100, 50, true);
		checkReallocate(100, 99, true);
		checkReallocate(100, 100, true);
		checkReallocate(100, 101, false);
	}

}
