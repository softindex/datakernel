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
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Utils.parseQ;
import static io.datakernel.util.Utils.skipSpaces;
import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static io.datakernel.util.ByteBufStrings.equalsLowerCaseAscii;

public final class AcceptCharset {
	public static final int DEFAULT_Q = 100;
	private static final byte[] Q_KEY = encodeAscii("q");

	private HttpCharset charset;
	private int q;

	private AcceptCharset(HttpCharset charset, int q) {
		this.charset = charset;
		this.q = q;
	}

	private AcceptCharset(HttpCharset charset) {
		this(charset, DEFAULT_Q);
	}

	public static AcceptCharset of(Charset charset) {
		return new AcceptCharset(HttpCharset.of(charset));
	}

	public static AcceptCharset of(Charset charset, int q) {
		return new AcceptCharset(HttpCharset.of(charset), q);
	}

	private static AcceptCharset of(HttpCharset charset) {
		return new AcceptCharset(charset);
	}

	private static AcceptCharset of(HttpCharset charset, int q) {
		return new AcceptCharset(charset, q);
	}

	public Charset getCharset() {
		return charset.toJavaCharset();
	}

	public int getQ() {
		return q;
	}

	static List<AcceptCharset> parse(byte[] bytes, int pos, int len) {
		List<AcceptCharset> chs = new ArrayList<>();
		parse(bytes, pos, len, chs);
		return chs;
	}

	static void parse(byte[] bytes, int pos, int len, List<AcceptCharset> list) {
		int end = pos + len;

		while (pos < end) {
			// parsing charset
			pos = skipSpaces(bytes, pos, end);
			int start = pos;
			while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
				pos++;
			}
			HttpCharset charset = HttpCharset.parse(bytes, start, pos - start);

			if (pos < end) {
				if (bytes[pos++] == ',') {
					list.add(AcceptCharset.of(charset));
				} else {
					int q = DEFAULT_Q;
					pos = skipSpaces(bytes, pos, end);
					start = pos;
					while (pos < end && bytes[pos] != ',') {
						if (bytes[pos] == '=' && equalsLowerCaseAscii(Q_KEY, bytes, start, pos - start)) {
							start = ++pos;
							while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
								pos++;
							}
							q = parseQ(bytes, start, pos - start);
							pos--;
						} else if (bytes[pos] == ';') {
							pos = skipSpaces(bytes, pos + 1, end);
							start = pos;
						}
						pos++;
					}
					list.add(AcceptCharset.of(charset, q));
					pos++;
				}
			} else {
				list.add(AcceptCharset.of(charset));
			}
		}
	}

	static void render(List<AcceptCharset> charsets, ByteBuf buf) {
		int pos = render(charsets, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(List<AcceptCharset> charsets, byte[] bytes, int pos) {
		for (int i = 0; i < charsets.size(); i++) {
			AcceptCharset charset = charsets.get(i);
			pos += HttpCharset.render(charset.charset, bytes, pos);
			if (charset.q != DEFAULT_Q) {
				bytes[pos++] = ';';
				bytes[pos++] = ' ';
				bytes[pos++] = 'q';
				bytes[pos++] = '=';
				bytes[pos++] = '0';
				bytes[pos++] = '.';
				int q = charset.q;
				if (q % 10 == 0) q /= 10;
				pos += ByteBufStrings.encodeDecimal(bytes, pos, q);
			}
			if (i < charsets.size() - 1) {
				bytes[pos++] = ',';
				bytes[pos++] = ' ';
			}
		}
		return pos;
	}

	int estimateSize() {
		return charset.size() + 10;
	}

	@Override
	public String toString() {
		return "AcceptCharset{" +
				"charset=" + charset +
				", q=" + q +
				'}';
	}
}
