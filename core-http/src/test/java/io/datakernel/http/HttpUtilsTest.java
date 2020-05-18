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
import io.datakernel.common.exception.parse.ParseException;
import io.datakernel.test.rules.LambdaStatement.ThrowingRunnable;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static io.datakernel.http.HttpUtils.trimAndDecodePositiveInt;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.*;

public class HttpUtilsTest {
	private static final Random RANDOM = new Random();

	@Test
	public void testEncodePositiveDecimal() throws ParseException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		encodePositiveIntTest(buf, Integer.MAX_VALUE);

		// Test random values
		int value = Math.abs(RANDOM.nextInt());
		byte[] bytes = String.valueOf(value).getBytes();
		Arrays.fill(bytes, (byte) 0);
		buf = ByteBuf.wrapForWriting(bytes);
		encodePositiveIntTest(buf, value);
	}

	@Test
	public void testDecodePositiveInt() throws ParseException {
		// Test edge cases
		decodeUnsignedIntTest(Integer.MAX_VALUE);
		decodeUnsignedIntTest(0);

		// Test random values
		decodeUnsignedIntTest(Math.abs(RANDOM.nextInt()));
	}

	@Test
	public void testDecodePositiveInt2() throws ParseException {
		// Test edge cases
		decodeUnsignedLongTest(Integer.MAX_VALUE);
		decodeUnsignedLongTest(0);

		// Test random values
		decodeUnsignedLongTest(Math.abs(RANDOM.nextInt()));

		// Test with offset
		String string = "  \t\t  123456789 \t\t\t\t   ";
		byte[] bytesRepr = string.getBytes();
		int decoded = trimAndDecodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(123456789, decoded);

		// Test bigger than long
		try {
			string = "  \t\t  92233720368547758081242123 \t\t\t\t   ";
			bytesRepr = string.getBytes();
			trimAndDecodePositiveInt(bytesRepr, 0, string.length());
			fail();
		} catch (ParseException e) {
			assertEquals(e.getMessage(), "Bigger than max int value: 92233720368547758081242123");
		}
	}

	@Test
	public void testNegativeValueWithOffset() {
		String text = "Content-Length: -1";
		byte[] bytes = text.getBytes();
		assertNegativeSizeException(() -> HttpUtils.trimAndDecodePositiveInt(bytes, 15, 3));
		assertNegativeSizeException(() -> HttpUtils.trimAndDecodePositiveInt(bytes, 16, 2));
	}

	// region helpers
	private void encodePositiveIntTest(ByteBuf buf, int value) throws ParseException {
		buf.rewind();
		buf.moveTail(encodePositiveInt(buf.array(), buf.head(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals(String.valueOf(value), stringRepr);
	}

	private void decodeUnsignedIntTest(int value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		int decoded = trimAndDecodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void decodeUnsignedLongTest(int value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		long decoded = decodePositiveInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void assertNegativeSizeException(ThrowingRunnable runnable) {
		try {
			runnable.run();
			fail();
		} catch (Throwable e) {
			assertThat(e, instanceOf(ParseException.class));
			assertThat(e.getMessage(), containsString("Not a decimal value"));
		}

	}
	// endregion
}
