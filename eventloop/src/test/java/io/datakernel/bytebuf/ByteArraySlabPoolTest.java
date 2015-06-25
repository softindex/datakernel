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
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ByteArraySlabPoolTest {

	private void checkAllocate(ByteBufPool pool, int size, int expectedSize, int[] poolSizes) {
		pool.clear();

		ByteBuf bytes = pool.allocate(size);
		assertEquals(expectedSize, bytes.array().length);
		for (int i = 0; i < poolSizes.length; i++) {
			assertTrue(pool.getPool()[i].isEmpty());
		}
		bytes.recycle();

		assertTrue(poolSizes.length <= pool.getPool().length);
		for (int i = 0; i < poolSizes.length; i++) {
			ConcurrentStack<ByteBuf> slab = pool.getPool()[i];
			assertEquals(poolSizes[i] == 0 ? 0 : 1, slab.size());
			if (!slab.isEmpty()) {
				assertTrue(slab.peek().isRecycled());
				assertEquals(poolSizes[i], slab.peek().capacity());
			}
		}
	}

	@Test
	public void testAllocate() {
		ByteBufPool pool = new ByteBufPool(16, 64);

		checkAllocate(pool, 0, 0, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 1, 1, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 2, 2, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 8, 8, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 9, 9, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 15, 15, new int[]{0, 0, 0, 0, 0});
		checkAllocate(pool, 16, 16, new int[]{0, 0, 0, 0, 16});
		checkAllocate(pool, 17, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(pool, 31, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(pool, 32, 32, new int[]{0, 0, 0, 0, 0, 32});
		checkAllocate(pool, 33, 64, new int[]{0, 0, 0, 0, 0, 0, 64});
		checkAllocate(pool, 63, 64, new int[]{0, 0, 0, 0, 0, 0, 64});
		checkAllocate(pool, 64, 64, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
		checkAllocate(pool, 65, 65, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
		checkAllocate(pool, 100, 100, new int[]{0, 0, 0, 0, 0, 0, 0, 0});
	}

	private void checkReallocate(ByteBufPool pool, int size1, int size2, boolean equals) {
		pool.clear();

		ByteBuf bytes1 = pool.allocate(size1);
		assertTrue(size1 <= bytes1.array().length);

		ByteBuf bytes2 = pool.reallocate(bytes1, size2);
		assertTrue(size2 <= bytes2.array().length);

		assertEquals(equals, bytes1 == bytes2);
	}

	@Test
	public void testReallocate() {
		ByteBufPool pool = new ByteBufPool(16, 64);

		checkReallocate(pool, 0, 0, true);
		checkReallocate(pool, 0, 1, false);
		checkReallocate(pool, 0, 2, false);

		checkReallocate(pool, 1, 0, true);
		checkReallocate(pool, 1, 1, true);
		checkReallocate(pool, 1, 2, false);

		checkReallocate(pool, 2, 0, true);
		checkReallocate(pool, 2, 1, true);
		checkReallocate(pool, 2, 2, true);
		checkReallocate(pool, 2, 3, false);

		checkReallocate(pool, 15, 14, true);
		checkReallocate(pool, 15, 15, true);
		checkReallocate(pool, 15, 16, false);
		checkReallocate(pool, 15, 17, false);

		checkReallocate(pool, 16, 15, true);
		checkReallocate(pool, 16, 16, true);
		checkReallocate(pool, 16, 17, false);
		checkReallocate(pool, 16, 31, false);
		checkReallocate(pool, 16, 32, false);

		checkReallocate(pool, 31, 30, true);
		checkReallocate(pool, 31, 31, true);
		checkReallocate(pool, 31, 32, true);
		checkReallocate(pool, 31, 33, false);

		checkReallocate(pool, 32, 31, true);
		checkReallocate(pool, 32, 32, true);
		checkReallocate(pool, 32, 33, false);

		checkReallocate(pool, 33, 31, false);
		checkReallocate(pool, 33, 32, false);
		checkReallocate(pool, 33, 33, true);
		checkReallocate(pool, 33, 34, true);

		checkReallocate(pool, 63, 62, true);
		checkReallocate(pool, 63, 63, true);
		checkReallocate(pool, 63, 64, true);
		checkReallocate(pool, 63, 65, false);

		checkReallocate(pool, 64, 63, true);
		checkReallocate(pool, 64, 64, true);
		checkReallocate(pool, 64, 65, false);
		checkReallocate(pool, 64, 66, false);

		checkReallocate(pool, 65, 63, false);
		checkReallocate(pool, 65, 64, false);
		checkReallocate(pool, 65, 65, true);
		checkReallocate(pool, 65, 66, false);

		checkReallocate(pool, 100, 50, false);
		checkReallocate(pool, 100, 99, true);
		checkReallocate(pool, 100, 100, true);
		checkReallocate(pool, 100, 101, false);
	}

	// TODO (nkhokhlov): test that show that function source.drainTo(ByteBuf target) fails if source.remaining() < target.remaining()
	@Test
	@Ignore
	public void testDrainTo() {
		ByteBuf source = ByteBuf.allocate(10);
		ByteBuf target = ByteBuf.allocate(10);

		byte someByte = 65;
		for (int i = 0; i < 5; i++) {
			source.put(someByte);
		}
		source.flip();

		assertTrue(source.remaining() == 5);
		assertTrue(target.remaining() == 10);

		source.drainTo(target);
	}

}
