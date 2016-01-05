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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;

import static io.datakernel.http.HttpUtils.skipSpaces;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

@SuppressWarnings("unused")
public class ContentType {
	private static final byte[] CHARSET_KEY = encodeAscii("charset");

	private final MediaType type;
	private HttpCharset charset;

	private ContentType(MediaType type) {
		this(type, null);
	}

	private ContentType(MediaType type, HttpCharset charset) {
		this.type = type;
		this.charset = charset;
	}

	public static ContentType of(MediaType type, Charset charset) {
		return new ContentType(type, HttpCharset.toHttpCharset(charset));
	}

	static ContentType of(MediaType type, HttpCharset charset) {
		return new ContentType(type, charset);
	}

	public static ContentType of(MediaType type) {
		return new ContentType(type);
	}

	public ContentType setCharset(Charset charset) {
		this.charset = HttpCharset.toHttpCharset(charset);
		return this;
	}

	public Charset getCharset() {
		return charset.toJavaCharset();
	}

	public MediaType getMediaType() {
		return type;
	}

	static ContentType parse(byte[] bytes, int pos, int length) {
		// parsing media type
		pos = skipSpaces(bytes, pos, length);
		int start = pos;
		int lowerCaseHashCode = 1;
		int end = pos + length;
		while (pos < end && bytes[pos] != ';') {
			byte b = bytes[pos];
			if (b >= 'A' && b <= 'Z') {
				b += 'a' - 'A';
			}
			lowerCaseHashCode = lowerCaseHashCode * 31 + b;
			pos++;
		}
		MediaType type = MediaType.parse(bytes, start, pos - start, lowerCaseHashCode);
		pos++;

		// parsing parameters if any (interested in 'charset' only)
		HttpCharset charset = null;
		if (pos < end) {
			pos = skipSpaces(bytes, pos, length);
			start = pos;
			while (pos < end) {
				if (bytes[pos] == '=' && ByteBufStrings.equalsLowerCaseAscii(CHARSET_KEY, bytes, start, pos - start)) {
					pos++;
					start = pos;
					while (pos < end && bytes[pos] != ';') {
						pos++;
					}
					charset = HttpCharset.parse(bytes, start, pos - start);
				} else if (bytes[pos] == ';' && pos + 1 < end) {
					start = skipSpaces(bytes, pos + 1, length);
				}
				pos++;
			}
		}
		return ContentType.of(type, charset);
	}

	int render(byte[] container, int pos) {
		pos += type.render(container, pos);
		if (charset != null) {
			container[pos++] = ';';
			container[pos++] = ' ';
			System.arraycopy(CHARSET_KEY, 0, container, pos, CHARSET_KEY.length);
			pos += CHARSET_KEY.length;
			container[pos++] = '=';
			pos += charset.render(container, pos);
		}
		return pos;
	}

	void render(ByteBuf buf) {
		int pos = render(buf.array(), buf.position());
		buf.position(pos);
	}

	int estimateSize() {
		int size = type.estimateSize();
		if (charset != null) {
			size += charset.estimateSize();
		}
		return size + 10;
	}

	@Override
	public String toString() {
		return "ContentType{" +
				"type=" + type +
				", charset=" + charset +
				'}';
	}
}
