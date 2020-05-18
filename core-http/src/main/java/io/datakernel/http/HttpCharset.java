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

import io.datakernel.common.exception.parse.ParseException;
import io.datakernel.http.CaseInsensitiveTokenMap.Token;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;
import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;
import static io.datakernel.common.Utils.arraysEquals;
import static java.nio.charset.Charset.forName;

/**
 * This is a specialized token to be used in {@link CaseInsensitiveTokenMap} for charset header values.
 */
public final class HttpCharset extends Token {
	private final static CaseInsensitiveTokenMap<HttpCharset> charsets = new CaseInsensitiveTokenMap<>(256, 2, HttpCharset.class, HttpCharset::new);
	private final static Map<Charset, HttpCharset> java2http = new HashMap<>();

	public static final HttpCharset UTF_8 = charsets.register("utf-8").addCharset(StandardCharsets.UTF_8);
	public static final HttpCharset US_ASCII = charsets.register("us-ascii").addCharset(StandardCharsets.US_ASCII);
	public static final HttpCharset LATIN_1 = charsets.register("iso-8859-1").addCharset(StandardCharsets.ISO_8859_1);

	// maximum of 40 characters, us-ascii, see rfc2978,
	// http://www.iana.org/assignments/character-sets/character-sets.txt
	private final byte[] bytes;
	private final int offset;
	private final int length;
	private Charset javaCharset;

	private HttpCharset(byte[] bytes, int offset, int length, @Nullable byte[] lowerCaseBytes, int lowerCaseHashCode) {
		super(lowerCaseBytes, lowerCaseHashCode);
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
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
		javaCharset = charset;
		java2http.put(charset, this);
		return this;
	}

	Charset toJavaCharset() throws ParseException {
		if (javaCharset != null) {
			return javaCharset;
		} else {
			String charsetName = decodeAscii(bytes, offset, length);
			try {
				if (charsetName.startsWith("\"") || charsetName.startsWith("\'")) {
					charsetName = charsetName.substring(1, charsetName.length() - 1);
				}
				javaCharset = forName(charsetName);
			} catch (Exception e) {
				throw new ParseException(HttpCharset.class, "Can't fetch charset for " + charsetName, e);
			}
			return javaCharset;
		}
	}

	int size() {
		return length;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		HttpCharset that = (HttpCharset) o;
		return arraysEquals(bytes, offset, length, that.bytes, that.offset, that.length);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(bytes);
		result = 31 * result + offset;
		result = 31 * result + length;
		return result;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}
