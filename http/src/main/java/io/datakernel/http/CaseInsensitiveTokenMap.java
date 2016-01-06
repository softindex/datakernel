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

import java.lang.reflect.Array;

import static io.datakernel.util.ByteBufStrings.encodeAscii;
import static io.datakernel.util.ByteBufStrings.equalsLowerCaseAscii;

abstract class CaseInsensitiveTokenMap<T extends CaseInsensitiveTokenMap.Token> {
	static abstract class Token {
		protected byte[] lowerCaseBytes;
		protected int lowerCaseHashCode;
	}

	protected final T[] TOKENS;
	protected final int maxProbings;

	protected CaseInsensitiveTokenMap(int slotsNumber, int maxProbings, Class<T> elementsType) {
		this.maxProbings = maxProbings;
		@SuppressWarnings("unchecked")
		final T[] ts = (T[]) Array.newInstance(elementsType, slotsNumber);
		TOKENS = ts;
	}

	protected final T register(String name) {
		byte[] bytes = encodeAscii(name);

		byte[] lowerCaseBytes = new byte[bytes.length];
		int lowerCaseHashCode = 1;
		for (int i = 0; i < bytes.length; i++) {
			byte b = bytes[i];
			if (b >= 'A' && b <= 'Z')
				b += 'a' - 'A';
			lowerCaseBytes[i] = b;
			lowerCaseHashCode = lowerCaseHashCode * 31 + b;
		}

		T token = create(bytes, 0, bytes.length, lowerCaseBytes, lowerCaseHashCode);

		assert Integer.bitCount(TOKENS.length) == 1;
		for (int p = 0; p < maxProbings; p++) {
			int slot = (lowerCaseHashCode + p) & (TOKENS.length - 1);
			if (TOKENS[slot] == null) {
				TOKENS[slot] = token;
				return token;
			}
		}
		throw new IllegalArgumentException("CaseInsensitiveTokenMap hash collision, try to increase size");
	}

	protected final T get(byte[] bytes, int offset, int length, int lowerCaseHashCode) {
		for (int p = 0; p < maxProbings; p++) {
			int slot = (lowerCaseHashCode + p) & (TOKENS.length - 1);
			T t = TOKENS[slot];
			if (t == null)
				break;
			if (t.lowerCaseHashCode == lowerCaseHashCode && equalsLowerCaseAscii(t.lowerCaseBytes, bytes, offset, length)) {
				return t;
			}
		}
		return null;
	}

	protected abstract T create(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode);
}
