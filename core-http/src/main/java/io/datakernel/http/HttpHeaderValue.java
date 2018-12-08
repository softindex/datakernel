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
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;

import java.time.Instant;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.*;
import static java.util.Arrays.asList;

public abstract class HttpHeaderValue {
	abstract int estimateSize();

	abstract void writeTo(ByteBuf buf);

	void recycle() {
	}

	public static HttpHeaderValue of(String string) {
		return new HttpHeaderValueOfString(string);
	}

	public static HttpHeaderValue ofBytes(byte[] array, int offset, int size) {
		return new HttpHeaderValueOfBytes(array, offset, size);
	}

	public static HttpHeaderValue ofBytes(byte[] array) {
		return ofBytes(array, 0, array.length);
	}

	public static HttpHeaderValue ofDecimal(int value) {
		return new HttpHeaderValueOfUnsignedDecimal(value);
	}

	public static HttpHeaderValue ofAcceptCharsets(List<AcceptCharset> charsets) {
		return new HttpHeaderValueOfCharsets(charsets);
	}

	public static HttpHeaderValue ofAcceptCharsets(AcceptCharset... charsets) {
		return ofAcceptCharsets(asList(charsets));
	}

	public static HttpHeaderValue ofInstant(Instant date) {
		return new HttpHeaderValueOfDate(date.getEpochSecond());
	}

	public static HttpHeaderValue ofTimestamp(long timestamp) {
		return new HttpHeaderValueOfDate(timestamp / 1000L);
	}

	public static HttpHeaderValue ofAcceptMediaTypes(List<AcceptMediaType> type) {
		return new HttpHeaderValueOfAcceptMediaTypes(type);
	}

	public static HttpHeaderValue ofAcceptMediaTypes(AcceptMediaType... types) {
		return ofAcceptMediaTypes(asList(types));
	}

	public static HttpHeaderValue ofContentType(ContentType type) {
		return new HttpHeaderValueOfContentType(type);
	}

	public static int toInt(ByteBuf buf) throws ParseException {
		return decodeInt(buf.array(), buf.readPosition(), buf.readRemaining());
	}

	public static ContentType toContentType(ByteBuf buf) throws ParseException {
		return ContentType.parse(buf.array(), buf.readPosition(), buf.readRemaining());
	}

	public static Instant toInstant(ByteBuf buf) throws ParseException {
		return Instant.ofEpochSecond(HttpDate.parse(buf.array(), buf.readPosition()));
	}

	@FunctionalInterface
	public interface ParserIntoList<T> {
		void parse(ByteBuf buf, List<T> into) throws ParseException;
	}

	public static void toAcceptContentTypes(ByteBuf buf, List<AcceptMediaType> into) throws ParseException {
		AcceptMediaType.parse(buf.array(), buf.readPosition(), buf.readRemaining(), into);
	}

	public static void toAcceptCharsets(ByteBuf buf, List<AcceptCharset> into) throws ParseException {
		AcceptCharset.parse(buf.array(), buf.readPosition(), buf.readRemaining(), into);
	}

	static void toSimpleCookies(ByteBuf buf, List<HttpCookie> into) throws ParseException {
		HttpCookie.parseSimple(buf.array(), buf.readPosition(), buf.writePosition(), into);
	}

	static void toFullCookies(ByteBuf buf, List<HttpCookie> into) throws ParseException {
		HttpCookie.parseFull(buf.array(), buf.readPosition(), buf.writePosition(), into);
	}

	static final class HttpHeaderValueOfContentType extends HttpHeaderValue {
		private ContentType type;

		HttpHeaderValueOfContentType(ContentType type) {
			this.type = type;
		}

		@Override
		public int estimateSize() {
			return type.size();
		}

		@Override
		public void writeTo(ByteBuf buf) {
			ContentType.render(type, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			ContentType.render(type, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfAcceptMediaTypes extends HttpHeaderValue {
		private final List<AcceptMediaType> types;

		HttpHeaderValueOfAcceptMediaTypes(List<AcceptMediaType> types) {
			this.types = types;
		}

		@Override
		public int estimateSize() {
			int size = 0;
			for (AcceptMediaType type : types) {
				size += type.estimateSize() + 2;
			}
			return size;
		}

		@Override
		public void writeTo(ByteBuf buf) {
			AcceptMediaType.render(types, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptMediaType.render(types, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfSimpleCookies extends HttpHeaderValue {
		private final List<HttpCookie> cookies;

		HttpHeaderValueOfSimpleCookies(List<HttpCookie> cookies) {
			this.cookies = cookies;
		}

		@Override
		int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue() == null ? 0 : cookie.getValue().length() + 1;
			}
			size += (cookies.size() - 1) * 2; // semicolons and spaces
			return size;
		}

		@Override
		void writeTo(ByteBuf buf) {
			HttpCookie.renderSimple(cookies, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpCookie.renderSimple(cookies, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfFullCookies extends HttpHeaderValue {
		private final List<HttpCookie> cookies;

		HttpHeaderValueOfFullCookies(List<HttpCookie> cookies) {
			this.cookies = cookies;
		}

		@Override
		int estimateSize() {
			int size = 0;
			for (HttpCookie cookie : cookies) {
				size += cookie.getName().length();
				size += cookie.getValue() == null ? 0 : cookie.getValue().length() + 1;
				size += cookie.getDomain() == null ? 0 : cookie.getDomain().length() + 10;
				size += cookie.getPath() == null ? 0 : cookie.getPath().length() + 6;
				size += cookie.getExtension() == null ? 0 : cookie.getExtension().length();
				size += 102;
			}
			size += (cookies.size() - 1) * 2;
			return size;
		}

		@Override
		void writeTo(ByteBuf buf) {
			HttpCookie.renderFull(cookies, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpCookie.renderFull(cookies, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfCharsets extends HttpHeaderValue {
		private final List<AcceptCharset> charsets;

		HttpHeaderValueOfCharsets(List<AcceptCharset> charsets) {
			this.charsets = charsets;
		}

		@Override
		int estimateSize() {
			int size = 0;
			for (AcceptCharset charset : charsets) {
				size += charset.estimateSize() + 2;
			}
			return size;
		}

		@Override
		void writeTo(ByteBuf buf) {
			AcceptCharset.render(charsets, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			AcceptCharset.render(charsets, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfDate extends HttpHeaderValue {
		private final long epochSeconds;

		HttpHeaderValueOfDate(long epochSeconds) {
			this.epochSeconds = epochSeconds;
		}

		@Override
		int estimateSize() {
			return 29;
		}

		@Override
		void writeTo(ByteBuf buf) {
			HttpDate.render(epochSeconds, buf);
		}

		@Override
		public String toString() {
			ByteBuf buf = ByteBufPool.allocate(estimateSize());
			HttpDate.render(epochSeconds, buf);
			return ByteBufStrings.asAscii(buf);
		}
	}

	static final class HttpHeaderValueOfUnsignedDecimal extends HttpHeaderValue {
		private final int value;

		HttpHeaderValueOfUnsignedDecimal(int value) {
			this.value = value;
		}

		@Override
		int estimateSize() {
			return 10; // Integer.toString(Integer.MAX_VALUE).length();
		}

		@Override
		void writeTo(ByteBuf buf) {
			putDecimal(buf, value);
		}

		@Override
		public String toString() {
			return Integer.toString(value);
		}
	}

	static final class HttpHeaderValueOfString extends HttpHeaderValue {
		private final String string;

		HttpHeaderValueOfString(String string) {
			this.string = string;
		}

		@Override
		int estimateSize() {
			return string.length();
		}

		@Override
		void writeTo(ByteBuf buf) {
			putAscii(buf, string);
		}

		@Override
		public String toString() {
			return string;
		}
	}

	static final class HttpHeaderValueOfBytes extends HttpHeaderValue {
		private final byte[] array;
		private final int offset;
		private final int size;

		HttpHeaderValueOfBytes(byte[] array, int offset, int size) {
			this.array = array;
			this.offset = offset;
			this.size = size;
		}

		@Override
		int estimateSize() {
			return size;
		}

		@Override
		void writeTo(ByteBuf buf) {
			buf.put(array, offset, size);
		}

		@Override
		public String toString() {
			return decodeAscii(array, offset, size);
		}
	}

	static final class ParsedHttpHeaderValue extends HttpHeaderValue {
		int size;
		ByteBuf buf;
		ByteBuf[] bufs;

		public void add(ByteBuf buf) {
			if (size == 0) {
				this.buf = buf;
				size = 1;
				return;
			}
			if (bufs == null) {
				bufs = new ByteBuf[4];
				bufs[0] = buf;
			}
			if (size == bufs.length) {
				ByteBuf[] newBufs = new ByteBuf[bufs.length * 2];
				System.arraycopy(bufs, 0, newBufs, 0, bufs.length);
				bufs = newBufs;
			}
			bufs[size++] = buf;
		}

		@Override
		int estimateSize() {
			throw new UnsupportedOperationException();
		}

		@Override
		void writeTo(ByteBuf buf) {
			throw new UnsupportedOperationException();
		}

		@Override
		void recycle() {
			buf.recycle();
			if (bufs != null) {
				for (int i = 1; i < size; i++) {
					bufs[i].recycle();
				}
			}
			buf = null;
		}

		public String[] toStrings() {
			assert size != 0;
			String[] strings = new String[size];
			strings[0] = decodeAscii(buf.array(), buf.readPosition(), buf.readRemaining());
			for (int i = 1; i < size; i++) {
				ByteBuf buf = bufs[i - 1];
				strings[i] = decodeAscii(buf.array(), buf.readPosition(), buf.readRemaining());
			}
			return strings;
		}

		@Override
		public String toString() {
			return decodeAscii(buf.array(), buf.readPosition(), buf.readRemaining());
		}
	}
}
