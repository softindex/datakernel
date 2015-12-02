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
import io.datakernel.http.HttpUtils.Pair;
import io.datakernel.util.ByteBufStrings;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.ByteBufStrings.*;

final class CharsetUtils {
	private CharsetUtils() {}

	static List<Charset> parse(String string) {
		List<Pair<Charset>> ch = new ArrayList<>();
		parse(string, ch);
		List<Charset> charsets = new ArrayList<>();
		for (Pair<Charset> pair : ch) {
			charsets.add(pair.getValue());
		}
		return charsets;
	}

	static void parse(String string, List<Pair<Charset>> charsets) {
		parse(encodeAscii(string), 0, string.length(), charsets);
	}

	static void parse(ByteBuf buf, List<Pair<Charset>> charsets) {
		int pos = parse(buf.array(), buf.position(), buf.limit(), charsets);
		buf.position(pos);
	}

	static int parse(byte[] bytes, int pos, int end, List<Pair<Charset>> charsets) {
		while (pos < end) {
			int start = pos;
			while (pos < end && !(bytes[pos] == ';' || bytes[pos] == ',')) {
				pos++;
			}
			int keyEnd = pos;

			Charset value = null;
			try {
				value = getCharset(bytes, start, keyEnd);
			} catch (Exception ignored) {

			}
			if (value != null && pos < end && bytes[pos] == ';') {
				pos += 3;
				int qStart = pos;
				while (bytes[pos] != '.') {
					pos++;
				}
				int integer = ByteBufStrings.decodeDecimal(bytes, qStart, pos - qStart);
				pos += 1;
				qStart = pos;
				while (pos < end && bytes[pos] != ',') {
					pos++;
				}
				double fraction = ByteBufStrings.decodeDecimal(bytes, qStart, pos - qStart);
				while (fraction > 1.0) {
					fraction /= 10.0;
				}
				charsets.add(new Pair<>(value, integer + fraction));
			} else if (value != null) {
				charsets.add(new Pair<>(value));
			}
			pos += 2;
		}
		return pos;
	}

	static void render(ByteBuf buf, List<Charset> charsets) {
		for (int i = 0; i < charsets.size(); i++) {
			Charset charset = charsets.get(i);
			putAscii(buf, charset.name());
			if (i < charsets.size() - 1) {
				putAscii(buf, ", ");
			}
		}
	}

	static void render(List<Pair<Charset>> charsets, ByteBuf buf) {
		int pos = render(charsets, buf.array(), buf.position());
		buf.position(pos);
	}

	static int render(List<Pair<Charset>> charsets, byte[] bytes, int pos) {
		for (int i = 0; i < charsets.size(); i++) {
			Pair<Charset> charset = charsets.get(i);

			byte[] chName = encodeAscii(charset.getValue().name());
			toLowerCaseAscii(chName);
			System.arraycopy(chName, 0, bytes, pos, chName.length);
			pos += chName.length;

			if (charset.getQuot() > 0.0) {
				encodeAscii(bytes, pos, ";q=");
				pos += 3;
				double q = charset.getQuot();
				int n = (int) q;
				pos += encodeDecimal(bytes, pos, n);
				bytes[pos++] = '.';
				while (q - (int) q > 0.000001) {
					q *= 10;
				}
				pos += encodeDecimal(bytes, pos, (int) q);
			}

			if (i < charsets.size() - 1) {
				encodeAscii(bytes, pos, ", ");
				pos += 2;
			}
		}
		return pos;
	}

	static Charset getCharset(byte[] bytes, int start, int end) {
		return Charset.forName(decodeAscii(bytes, start, end - start));
	}
}