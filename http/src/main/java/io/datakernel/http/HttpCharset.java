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

import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.util.ByteBufStrings.decodeAscii;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static java.nio.charset.Charset.forName;

// maximum of 40 characters, us-ascii, see rfc2978,
// http://www.iana.org/assignments/character-sets/character-sets.txt
final class HttpCharset extends CaseInsensitiveTokenMap.Token {
	private final static CaseInsensitiveTokenMap<HttpCharset> charsets = new CaseInsensitiveTokenMap<HttpCharset>(256, 2, HttpCharset.class) {
		@Override
		protected HttpCharset create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
			return new HttpCharset(bytes, offset, length, lowerCaseBytes, lowerCaseHashCode);
		}
	};
	private final static Map<Charset, HttpCharset> lookup = new HashMap<>();

	public static final HttpCharset UTF_8 = charsets.register("utf-8").addCharset(forName("UTF-8"));
	public static final HttpCharset US_ASCII = charsets.register("us-ascii").addCharset(forName("US-ASCII"));
	public static final HttpCharset ISO_8859_5 = charsets.register("iso-8859-5").addCharset(forName("ISO-8859-5"));
	public static final HttpCharset UNICODE_1_1 = charsets.register("unicode-1-1").addCharset(forName("UNICODE"));

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

	static HttpCharset parse(byte[] bytes, int pos, int length) {
		return charsets.get(bytes, pos, length, ByteBufStrings.hashCodeLowerCaseAscii(bytes, pos, length));
	}

	int render(byte[] container, int pos) {
		System.arraycopy(bytes, offset, container, pos, length);
		return length;
	}

	private HttpCharset addCharset(Charset charset) {
		this.javaCharset = charset;
		lookup.put(charset, this);
		return this;
	}

	static HttpCharset toHttpCharset(Charset jCharset) {
		HttpCharset hCharset = lookup.get(jCharset);
		if (hCharset != null) {
			return hCharset;
		} else {
			byte[] bytes = encodeAscii(jCharset.name());
			int hash = ByteBufStrings.hashCodeLowerCaseAscii(bytes);
			return charsets.get(bytes, 0, bytes.length, hash);
		}
	}

	Charset toJavaCharset() {
		if (javaCharset != null) {
			return javaCharset;
		} else {
			return forName(decodeAscii(bytes, offset, length));
		}
	}

	int estimateSize() {
		return length;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}