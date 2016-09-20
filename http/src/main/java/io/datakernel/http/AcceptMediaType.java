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
import io.datakernel.bytebuf.ByteBufStrings;
import io.datakernel.exception.ParseException;

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.bytebuf.ByteBufStrings.equalsLowerCaseAscii;
import static io.datakernel.http.HttpUtils.parseQ;
import static io.datakernel.http.HttpUtils.skipSpaces;

public final class AcceptMediaType {
	public static final int DEFAULT_Q = 100;
	private static final byte[] Q_KEY = new byte[]{'q'};

	private MediaType mime;
	private int q;

	private AcceptMediaType(MediaType mime, int q) {
		this.mime = mime;
		this.q = q;
	}

	private AcceptMediaType(MediaType mime) {
		this(mime, DEFAULT_Q);
	}

	public static AcceptMediaType of(MediaType mime) {
		return new AcceptMediaType(mime);
	}

	public static AcceptMediaType of(MediaType mime, int q) {
		return new AcceptMediaType(mime, q);
	}

	static List<AcceptMediaType> parse(byte[] bytes, int pos, int length) throws ParseException {
		List<AcceptMediaType> cts = new ArrayList<>();
		parse(bytes, pos, length, cts);
		return cts;
	}

	static void parse(byte[] bytes, int pos, int length, List<AcceptMediaType> list) throws ParseException {
		int end = pos + length;

		while (pos < end) {
			// parsing media type
			pos = skipSpaces(bytes, pos, end);
			int start = pos;
			int lowerCaseHashCode = 1;
			while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
				byte b = bytes[pos];
				if (b >= 'A' && b <= 'Z') {
					b += 'a' - 'A';
				}
				lowerCaseHashCode = lowerCaseHashCode * 31 + b;
				pos++;
			}
			MediaType mime = MediaTypes.of(bytes, start, pos - start, lowerCaseHashCode);

			if (pos < end) {
				if (bytes[pos++] == ',') {
					list.add(AcceptMediaType.of(mime));
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
					list.add(AcceptMediaType.of(mime, q));
					pos++;
				}
			} else {
				list.add(AcceptMediaType.of(mime));
			}

		}
	}

	static void render(List<AcceptMediaType> types, ByteBuf buf) {
		int pos = render(types, buf.array(), buf.tail());
		buf.tail(pos);
	}

	static int render(List<AcceptMediaType> types, byte[] container, int pos) {
		for (int i = 0; i < types.size(); i++) {
			AcceptMediaType type = types.get(i);
			pos += MediaTypes.render(type.mime, container, pos);
			if (type.q != DEFAULT_Q) {
				container[pos++] = ';';
				container[pos++] = ' ';
				container[pos++] = 'q';
				container[pos++] = '=';
				container[pos++] = '0';
				container[pos++] = '.';
				int q = type.q;
				if (q % 10 == 0) q /= 10;
				pos += ByteBufStrings.encodeDecimal(container, pos, q);
			}
			if (i < types.size() - 1) {
				container[pos++] = ',';
				container[pos++] = ' ';
			}
		}
		return pos;
	}

	int estimateSize() {
		return mime.size() + 10;
	}

	public MediaType getMediaType() {
		return mime;
	}

	public int getQ() {
		return q;
	}

	@Override
	public String toString() {
		return "AcceptContentType{" +
				"mime=" + mime +
				", q=" + q +
				'}';
	}
}
