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

import io.datakernel.annotation.Nullable;
import io.datakernel.async.Stage;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.serial.AbstractSerialConsumer;

import java.io.*;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
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

	public static class AssertingConsumer extends AbstractSerialConsumer<ByteBuf> {
		public boolean executed = false;
		private byte[] expectedByteArray;
		private String expectedString;
		private ByteBuf expectedBuf;
		private Exception expectedException;
		private ByteBufQueue queue = new ByteBufQueue();

		public void setExpectedByteArray(byte[] expectedByteArray) {
			this.expectedByteArray = expectedByteArray;
		}

		public void setExpectedString(String expectedString) {
			this.expectedString = expectedString;
		}

		public void setExpectedBuf(ByteBuf expectedBuf) {
			this.expectedBuf = expectedBuf;
		}

		public void setExpectedException(Exception expectedException) {
			this.expectedException = expectedException;
		}

		public void reset() {
			expectedBuf = null;
			expectedByteArray = null;
			expectedException = null;
			expectedString = null;
			executed = false;
		}

		@Override
		public Stage<Void> accept(@Nullable ByteBuf value) {
			if (value != null) {
				queue.add(value);
			} else {
				ByteBuf actualBuf = queue.takeRemaining();
				if (expectedByteArray != null) {
					byte[] actualByteArray = actualBuf.getArray();
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
				executed = true;
				actualBuf.recycle();
			}
			return Stage.complete().async();
		}

		@Override
		protected void onClosed(Throwable e) {
			executed = true;
			queue.recycle();
			if (expectedBuf != null) {
				expectedBuf.recycle();
			}
			if (expectedException != null) {
				assertEquals(expectedException, e);
				return;
			}
			throw new AssertionError(e);
		}
	}
}
