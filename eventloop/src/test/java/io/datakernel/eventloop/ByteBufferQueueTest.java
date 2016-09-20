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

package io.datakernel.eventloop;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ByteBufferQueueTest {

	private final Random random = new Random();

	@Test
	public void test() {
		byte[] test = new byte[200];
		for (int i = 0; i < test.length; i++) {
			test[i] = (byte) (i + 1);
		}

		ByteBufQueue queue = ByteBufQueue.create();

		int left = test.length;
		int pos = 0;
		while (left > 0) {
			int bufSize = random.nextInt(Math.min(10, left) + 1);
			ByteBuf buf = ByteBuf.wrap(test, pos, pos + bufSize);
			queue.add(buf);
			left -= bufSize;
			pos += bufSize;
		}

		Assert.assertEquals(test.length, queue.remainingBytes());

		left = test.length;
		pos = 0;
		while (left > 0) {
			int requested = random.nextInt(50);
			byte[] dest = new byte[100];
			int drained = queue.drainTo(dest, 10, requested);

			Assert.assertTrue(drained <= requested);

			for (int i = 0; i < drained; i++) {
				Assert.assertEquals(test[i + pos], dest[i + 10]);
			}

			left -= drained;
			pos += drained;
		}

		Assert.assertEquals(0, queue.remainingBytes());
	}

}
