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

package io.datakernel;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.http.HttpCookie;
import io.datakernel.http.HttpHeader;
import io.datakernel.http.HttpHeaderValue;

import java.util.Date;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

public class Test {
	private static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
		private final byte[] array;
		private final int offset;
		private final int size;

		private HttpHeaderValueOfBytes(HttpHeader key, byte[] array, int offset, int size) {
			super(key);
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		public byte[] array() {
			return array;
		}

		@Override
		public int estimateSize() {
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			buf.put(array, offset, size);
		}

		@Override
		public String toString() {
			return decodeAscii(array, offset, size);
		}
	}

	private static final class HttpHeaderValueOfList extends HttpHeaderValue {
		private final List<HttpHeaderValue> values;
		private final byte separator;

		private HttpHeaderValueOfList(HttpHeader key, List<HttpHeaderValue> values, char separator) {
			super(key);
			this.values = values;
			this.separator = (byte) separator;
		}

		@Override
		public int estimateSize() {
			int result = values.size();
			for (HttpHeaderValue value : values) {
				result += value.estimateSize();
			}
			return result;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			boolean first = true;
			for (HttpHeaderValue value : values) {
				if (!first) {
					buf.put(separator);
				}
				first = false;
				value.writeTo(buf);
			}
		}

		@Override
		public String toString() {
			return null; // TODO decodeString(bytes, offset, size);
		}
	}

	private static final class HttpHeaderValueOfUnsignedDecimal extends HttpHeaderValue {
		private final int value;

		private HttpHeaderValueOfUnsignedDecimal(HttpHeader header, int value) {
			super(header);
			this.value = value;
		}

		@Override
		public int estimateSize() {
			return 10; // Integer.toString(Integer.MAX_VALUE).length();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			putDecimal(buf, value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	private static final class HttpHeaderValueOfString extends HttpHeaderValue {
		private final String string;

		private HttpHeaderValueOfString(HttpHeader key, String string) {
			super(key);
			this.string = string;
		}

		@Override
		public int estimateSize() {
			return string.length();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			putAscii(buf, string);
		}

		@Override
		public String toString() {
			return string;
		}
	}

	public static void main(String[] args) {
		HttpCookie cookie = new HttpCookie("token", "dev");
		cookie.setExpirationDate(new Date());
		cookie.setDomain("www.google.com");
		cookie.setHttpOnly(true);
		cookie.setMaxAge(123456);

		String c = cookie.toString();

		System.out.println(c);

		List<HttpCookie> cookies = HttpCookie.parse(c);

//		for (HttpCookie httpCookie : cookies) {
//			System.out.println(httpCookie);
//		}

		System.out.println(cookie);
	}
}
