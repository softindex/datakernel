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

package io.datakernel.http;

import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.exception.ParseException;
import io.datakernel.http.stream.BufsConsumer;

import java.io.*;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.test.TestUtils.assertComplete;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.*;

public class TestUtils {

	public static byte[] toByteArray(InputStream is) {
		byte[] bytes = null;

		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();

			byte data[] = new byte[1024];
			int count;

			while ((count = is.read(data)) != -1) {
				bos.write(data, 0, count);
			}

			bos.flush();
			bos.close();
			is.close();

			bytes = bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			fail();
		}
		return bytes;
	}

	public static void readFully(InputStream is, byte[] bytes) throws UnsupportedEncodingException {
		DataInputStream dis = new DataInputStream(is);
		try {
			dis.readFully(bytes);
		} catch (IOException e) {
			throw new RuntimeException("Could not read " + new String(bytes, UTF_8), e);
		}
	}

	public static class AssertingBufsConsumer implements BufsConsumer {
		private byte[] expectedByteArray;
		private String expectedString;
		private ByteBuf expectedBuf;
		private ParseException expectedException;

		public void setExpectedByteArray(byte[] expectedByteArray) {
			this.expectedByteArray = expectedByteArray;
		}

		public void setExpectedString(String expectedString) {
			this.expectedString = expectedString;
		}

		public void setExpectedBuf(ByteBuf expectedBuf) {
			this.expectedBuf = expectedBuf;
		}

		public void setExpectedException(ParseException expectedException) {
			this.expectedException = expectedException;
		}

		public void reset() {
			expectedBuf =null;
			expectedByteArray = null;
			expectedException = null;
			expectedString = null;
		}
		@Override
		public Stage<Boolean> push(ByteBufQueue inputBufs, boolean endOfStream) {
			if (endOfStream) {
				ByteBuf actualBuf = inputBufs.takeRemaining();
				if (expectedByteArray != null) {
					byte[] actualByteArray = actualBuf.asArray();
					assertArrayEquals(expectedByteArray, actualByteArray);
				}
				if (expectedString != null) {
					String actualString = decodeAscii(actualBuf);
					assertEquals(expectedString, actualString);
				}
				if (expectedBuf != null) {
					assertEquals(expectedBuf, actualBuf);
					expectedBuf.recycle();
				}

				actualBuf.recycle();
				return Stage.of(true)
						.whenComplete(assertComplete());
			} else {
				return Stage.of(false)
						.whenComplete(assertComplete());
			}
		}

		@Override
		public void closeWithError(Throwable e) {
			if (expectedException != null){
				assertEquals(e, expectedException);
				return;
			}
			throw new AssertionError(e);
		}
	}
}
