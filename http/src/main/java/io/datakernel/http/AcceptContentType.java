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

import java.util.ArrayList;
import java.util.List;

import static io.datakernel.http.HttpUtils.parseQ;
import static io.datakernel.http.HttpUtils.skipSpaces;
import static io.datakernel.util.ByteBufStrings.encodeAscii;

public class AcceptContentType {
	private static final byte[] Q_KEY = encodeAscii("q");
	public static final int DEFAULT_Q = 100;

	private MediaType mime;
	private int q;

	private AcceptContentType(MediaType mime, int q) {
		this.mime = mime;
		this.q = q;
	}

	private AcceptContentType(MediaType mime) {
		this(mime, DEFAULT_Q);
	}

	public static AcceptContentType of(MediaType mime) {
		return new AcceptContentType(mime);
	}

	public static AcceptContentType of(MediaType mime, int q) {
		return new AcceptContentType(mime, q);
	}

	static List<AcceptContentType> parse(byte[] bytes, int pos, int length) {
		List<AcceptContentType> cts = new ArrayList<>();
		parse(bytes, pos, length, cts);
		return cts;
	}

	static void parse(byte[] bytes, int pos, int length, List<AcceptContentType> list) {
		int end = pos + length;

		while (pos < end) {
			// parsing media type
			pos = skipSpaces(bytes, pos, length);
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
			MediaType mime = MediaType.parse(bytes, start, pos - start, lowerCaseHashCode);

			if (pos < end && bytes[pos] == ',') {
				pos++;
				pos = skipSpaces(bytes, pos, length);
				list.add(AcceptContentType.of(mime));
				continue;
			}

			// parsing parameters if any (interested in 'q' only)
			pos++;
			int q = DEFAULT_Q;
			if (pos < end) {
				pos = skipSpaces(bytes, pos, length);
				start = pos;
				while (pos < end && bytes[pos] != ',') {
					if (bytes[pos] == '=' && ByteBufStrings.equalsLowerCaseAscii(Q_KEY, bytes, start, pos - start)) {
						pos++;
						start = pos;
						while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
							pos++;
						}
						q = parseQ(bytes, start, pos - start);
					} else if ((bytes[pos] == ';' || bytes[pos] == ',') && pos + 1 < end) {
						start = skipSpaces(bytes, pos + 1, length);
					}
					pos++;
				}
			}
			list.add(AcceptContentType.of(mime, q));
		}
	}

	static void render(List<AcceptContentType> types, ByteBuf buf) {
		int pos = render(types, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(List<AcceptContentType> types, byte[] bytes, int pos) {
		for (int i = 0; i < types.size(); i++) {
			AcceptContentType type = types.get(i);
			pos += type.mime.render(bytes, pos);
			if (type.q != DEFAULT_Q) {
				bytes[pos++] = ';';
				bytes[pos++] = ' ';
				bytes[pos++] = 'q';
				bytes[pos++] = '=';
				bytes[pos++] = '0';
				bytes[pos++] = '.';
				int q = type.q;
				if (q % 10 == 0) q /= 10;
				pos += ByteBufStrings.encodeDecimal(bytes, pos, q);
			}
			if (i < types.size() - 1) {
				bytes[pos++] = ',';
				bytes[pos++] = ' ';
			}
		}
		return pos;
	}

	int estimateSize() {
		return mime.estimateSize() + 10;
	}

	public int getQ() {
		return q;
	}

	public MediaType getMime() {
		return mime;
	}

	@Override
	public String toString() {
		return "AcceptContentType{" +
				"mime=" + mime +
				", q=" + q +
				'}';
	}
}
