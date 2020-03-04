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

package io.datakernel.csp.binary;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.common.parse.ParseException;
import io.datakernel.test.rules.ByteBufRule;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public final class ByteBufsDecoderTest {
	@ClassRule
	public static final ByteBufRule byteBufRule = new ByteBufRule();
	public final ByteBufQueue queue = new ByteBufQueue();

	@SuppressWarnings("ConstantConditions")
	@Test
	public void testOfNullTerminatedBytes() throws ParseException {
		ByteBufsDecoder<ByteBuf> decoder = ByteBufsDecoder.ofNullTerminatedBytes();
		queue.add(ByteBuf.wrapForReading(new byte[]{1, 2, 3, 0, 4, 5, 6}));
		ByteBuf beforeNull = decoder.tryDecode(queue);
		ByteBuf afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{1, 2, 3}, beforeNull.asArray());
		assertArrayEquals(new byte[]{4, 5, 6}, afterNull.asArray());

		queue.add(ByteBuf.wrapForReading(new byte[]{0, 1, 2, 3}));
		beforeNull = decoder.tryDecode(queue);
		afterNull = queue.takeRemaining();

		assertArrayEquals(new byte[]{}, beforeNull.asArray());
		assertArrayEquals(new byte[]{1, 2, 3}, afterNull.asArray());
	}
}
