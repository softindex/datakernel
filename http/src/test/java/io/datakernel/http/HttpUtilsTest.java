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

package io.datakernel.http;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufStrings.decodeUtf8;
import static io.datakernel.http.HttpUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class HttpUtilsTest {
	private static final Random RANDOM = new Random();

	@Test
	public void testEncodeUnsignedDecimal() throws ParseException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		ecodeUnsignedDecimalTest(buf, Long.MAX_VALUE);

		// Test random values
		long value = Math.abs(RANDOM.nextLong());
		byte[] bytes = String.valueOf(value).getBytes();
		Arrays.fill(bytes, (byte) 0);
		buf = ByteBuf.wrapForWriting(bytes);
		ecodeUnsignedDecimalTest(buf, value);
	}

	@Test
	public void testdecodeUnsignedInt() throws ParseException {
		// Test edge cases
		decodeUnsignedIntTest(Integer.MAX_VALUE);
		decodeUnsignedIntTest(0);

		// Test random values
		decodeUnsignedIntTest(Math.abs(RANDOM.nextInt()));
	}

	@Test
	public void testdecodeUnsignedLong() throws ParseException {
		// Test edge cases
		decodeUnsignedLongTest(Long.MAX_VALUE);
		decodeUnsignedLongTest(0L);

		// Test random values
		decodeUnsignedLongTest(Math.abs(RANDOM.nextLong()));

		// Test with offset
		String string = "  \t\t  1234567891234 \t\t\t\t   ";
		byte[] bytesRepr = string.getBytes();
		long decoded = decodeUnsignedLong(bytesRepr, 0, string.length());
		assertEquals(1234567891234L, decoded);

		// Test bigger than long
		try {
			string = "  \t\t  92233720368547758081242123 \t\t\t\t   ";
			bytesRepr = string.getBytes();
			decodeUnsignedLong(bytesRepr, 0, string.length());
			fail();
		} catch (ParseException e) {
			assertEquals(e.getMessage(), "Bigger than max long value: 92233720368547758081242123");
		}
	}

	// region helpers
	private void ecodeUnsignedDecimalTest(ByteBuf buf, long value) throws ParseException {
		buf.rewind();
		buf.moveWritePosition(encodeUnsignedDecimal(buf.array(), buf.readPosition(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals(String.valueOf(value), stringRepr);
	}

	private void decodeUnsignedIntTest(int value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		int decoded = decodeUnsignedInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void decodeUnsignedLongTest(long value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		long decoded = decodeUnsignedLong(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}
	// endregion
}
