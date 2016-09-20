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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static java.nio.charset.Charset.forName;

// maximum of 40 characters, us-ascii, see rfc2978,
// http://www.iana.org/assignments/character-sets/character-sets.txt
// case insensitive
final class HttpCharset extends CaseInsensitiveTokenMap.Token {
	private final static CaseInsensitiveTokenMap<HttpCharset> charsets = new CaseInsensitiveTokenMap<HttpCharset>(256, 2, HttpCharset.class) {
		@Override
		protected HttpCharset create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
			return new HttpCharset(bytes, offset, length, lowerCaseBytes, lowerCaseHashCode);
		}
	};
	private final static Map<Charset, HttpCharset> java2http = new HashMap<>();

	public static final HttpCharset UTF_8 = charsets.register("utf-8").addCharset(StandardCharsets.UTF_8);
	public static final HttpCharset US_ASCII = charsets.register("us-ascii").addCharset(StandardCharsets.US_ASCII);
	public static final HttpCharset LATIN_1 = charsets.register("iso-8859-1").addCharset(StandardCharsets.ISO_8859_1);

	private final byte[] bytes;
	private final int offset;
	private final int length;
	private Charset javaCharset;

	private HttpCharset(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		this.lowerCaseBytes = lowerCaseBytes;
		this.lowerCaseHashCode = lowerCaseHashCode;
	}

	static HttpCharset of(Charset charset) {
		HttpCharset hCharset = java2http.get(charset);
		if (hCharset != null) {
			return hCharset;
		} else {
			byte[] bytes = encodeAscii(charset.name());
			return parse(bytes, 0, bytes.length);
		}
	}

	static HttpCharset parse(byte[] bytes, int pos, int length) {
		return charsets.getOrCreate(bytes, pos, length);
	}

	static int render(HttpCharset charset, byte[] container, int pos) {
		System.arraycopy(charset.bytes, charset.offset, container, pos, charset.length);
		return charset.length;
	}

	private HttpCharset addCharset(Charset charset) {
		this.javaCharset = charset;
		java2http.put(charset, this);
		return this;
	}

	Charset toJavaCharset() {
		if (javaCharset != null) {
			return javaCharset;
		} else {
			javaCharset = forName(decodeAscii(bytes, offset, length));
			return javaCharset;
		}
	}

	int size() {
		return length;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}