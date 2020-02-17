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

package io.datakernel.bytebuf;

import org.junit.Test;

import static io.datakernel.bytebuf.ByteBufTest.initByteBufPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteArraySlabPoolTest {
	static {
		initByteBufPool();
	}

	private void checkAllocate(int size, int expectedSize, int[] poolSizes) {
		ByteBufPool.clear();

		ByteBuf bytes = ByteBufPool.allocate(size);
		assertEquals(expectedSize, bytes.array().length);
		for (int i = 0; i < poolSizes.length; i++) {
			assertTrue(ByteBufPool.slabs[i].isEmpty());
		}
		bytes.recycle();

		assertTrue(poolSizes.length <= ByteBufPool.slabs.length);
		for (int i = 0; i < poolSizes.length; i++) {
			ByteBufConcurrentQueue slab = ByteBufPool.slabs[i];
			assertEquals(poolSizes[i] == 0 ? 0 : 1, slab.size());
			if (!slab.isEmpty()) {
				assertTrue(slab.getBufs().stream().allMatch(ByteBuf::isRecycled));
				assertEquals(poolSizes[i], slab.getBufs().get(0).limit());
			}
		}
	}

	@Test
	public void testAllocate() {
		ByteBufPool.clear();

		checkAllocate(0, 1, new int[]{});
		checkAllocate(1, 1, new int[]{1, 0, 0, 0, 0});
		checkAllocate(2, 2, new int[]{0, 2, 0, 0, 0});
		checkAllocate(8, 8, new int[]{0, 0, 0, 8, 0});
		checkAllocate(9, 16, new int[]{0, 0, 0, 0, 16});
		checkAllocate(15, 16, new int[]{0, 0, 0, 0, 16});
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

		checkReallocate(0, 0, true);
		checkReallocate(0, 1, true);
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
		checkReallocate(15, 16, true);
		checkReallocate(15, 17, false);

		checkReallocate(16, 15, true);
		checkReallocate(16, 16, true);
		checkReallocate(16, 17, false);
		checkReallocate(16, 31, false);
		checkReallocate(16, 32, false);

		checkReallocate(31, 30, true);
		checkReallocate(31, 31, true);
	}

}
