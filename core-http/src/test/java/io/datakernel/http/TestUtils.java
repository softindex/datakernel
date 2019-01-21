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

import io.datakernel.async.Promise;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufQueue;
import io.datakernel.csp.AbstractChannelConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestUtils {

	public static byte[] toByteArray(InputStream is) {
		byte[] bytes;

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
			throw new AssertionError(e);
		}
		return bytes;
	}

	public static void readFully(InputStream is, byte[] bytes) {
		DataInputStream dis = new DataInputStream(is);
		try {
			dis.readFully(bytes);
		} catch (IOException e) {
			throw new RuntimeException("Could not read " + new String(bytes, UTF_8), e);
		}
	}

	public static class AssertingConsumer extends AbstractChannelConsumer<ByteBuf> {
		private final ByteBufQueue queue = new ByteBufQueue();

		public boolean executed = false;
		@Nullable
		private byte[] expectedByteArray;
		@Nullable
		private String expectedString;
		@Nullable
		private ByteBuf expectedBuf;
		@Nullable
		private Exception expectedException;

		public void setExpectedByteArray(@Nullable byte[] expectedByteArray) {
			this.expectedByteArray = expectedByteArray;
		}

		public void setExpectedString(@Nullable String expectedString) {
			this.expectedString = expectedString;
		}

		public void setExpectedBuf(@Nullable ByteBuf expectedBuf) {
			this.expectedBuf = expectedBuf;
		}

		public void setExpectedException(@Nullable Exception expectedException) {
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
		protected Promise<Void> doAccept(@Nullable ByteBuf value) {
			if (value != null) {
				queue.add(value);
			} else {
				ByteBuf actualBuf = queue.takeRemaining();

				try {
					if (expectedByteArray != null) {
						byte[] actualByteArray = actualBuf.getArray();
						assertArrayEquals(expectedByteArray, actualByteArray);
					}
					if (expectedString != null) {
						String actualString = decodeAscii(actualBuf.array(), actualBuf.readPosition(), actualBuf.readRemaining());
						assertEquals(expectedString, actualString);
					}
					if (expectedBuf != null) {
						assertArrayEquals(expectedBuf.getArray(), actualBuf.getArray());
						expectedBuf.recycle();
						expectedBuf = null;
					}
				} finally {
					actualBuf.recycle();
				}

				executed = true;
			}
			return Promise.complete();
		}

		@Override
		protected void onClosed(@NotNull Throwable e) {
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
