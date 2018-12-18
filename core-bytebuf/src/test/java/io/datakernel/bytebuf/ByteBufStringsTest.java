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

import io.datakernel.exception.ParseException;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

import static io.datakernel.bytebuf.ByteBufStrings.decodeUtf8;
import static io.datakernel.bytebuf.ByteBufStrings.encodeLong;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ByteBufStringsTest {
	private static final Random RANDOM = new Random();

	@Test
	public void testEncodeLong() throws ParseException {
		ByteBuf buf = ByteBuf.wrapForWriting(new byte[20]);

		// Test edge cases
		encodeLongTest(buf, Long.MAX_VALUE);
		encodeLongTest(buf, Long.MIN_VALUE);

		// Test random values
		long value = RANDOM.nextLong();
		byte[] bytes = String.valueOf(value).getBytes();
		Arrays.fill(bytes, (byte) 0);
		buf = ByteBuf.wrapForWriting(bytes);
		encodeLongTest(buf, value);
	}

	@Test
	public void testDecodeInt() throws ParseException {
		// Test edge cases
		decodeIntTest(Integer.MAX_VALUE);
		decodeIntTest(Integer.MIN_VALUE);

		// Test random values
		decodeIntTest(RANDOM.nextInt());
	}

	@Test
	public void testDecodeLong() throws ParseException {
		// Test edge cases
		decodeLongTest(Long.MAX_VALUE);
		decodeLongTest(Long.MIN_VALUE);

		// Test random values
		decodeLongTest(RANDOM.nextLong());

		// Test with offset
		String string = "-1234567891234";
		byte[] bytesRepr = string.getBytes();
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals(-1234567891234L, decoded);

		// Test bigger than long
		try {
			string = "92233720368547758081242123";
			bytesRepr = string.getBytes();
			ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
			fail();
		} catch (ParseException e) {
			assertEquals(e.getMessage(), "Bigger than max long value: 92233720368547758081242123");
		}
	}

	@Test
	public void testWrapLong() throws ParseException {
		// Test edge cases
		ByteBuf byteBuf = ByteBufStrings.wrapLong(Long.MAX_VALUE);
		assertEquals(String.valueOf(Long.MAX_VALUE), ByteBufStrings.decodeUtf8(byteBuf));

		byteBuf = ByteBufStrings.wrapLong(Long.MIN_VALUE);
		assertEquals(String.valueOf(Long.MIN_VALUE), ByteBufStrings.decodeUtf8(byteBuf));

		long value = RANDOM.nextLong();
		byteBuf = ByteBufStrings.wrapLong(value);
		assertEquals(String.valueOf(value), ByteBufStrings.decodeUtf8(byteBuf));
	}

	// region helpers
	private void encodeLongTest(ByteBuf buf, long value) throws ParseException {
		buf.rewind();
		buf.moveWritePosition(encodeLong(buf.array, buf.readPosition(), value));
		String stringRepr = decodeUtf8(buf);
		assertEquals(String.valueOf(value), stringRepr);
	}

	private void decodeIntTest(int value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		int decoded = ByteBufStrings.decodeInt(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}

	private void decodeLongTest(long value) throws ParseException {
		String string = String.valueOf(value);
		byte[] bytesRepr = string.getBytes();
		long decoded = ByteBufStrings.decodeLong(bytesRepr, 0, string.length());
		assertEquals(value, decoded);
	}
	// endregion
}
