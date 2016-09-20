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

import static io.datakernel.bytebuf.ByteBufStrings.decodeAscii;

public class HttpHeader extends CaseInsensitiveTokenMap.Token {
	protected final byte[] bytes;
	protected final int offset;
	protected final int length;

	HttpHeader(byte[] bytes, int offset, int length,
	           byte[] lowerCaseBytes, int lowerCaseHashCode) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		this.lowerCaseBytes = lowerCaseBytes;
		this.lowerCaseHashCode = lowerCaseHashCode;
	}

	int size() {
		return length;
	}

	void writeTo(ByteBuf buf) {
		buf.put(bytes, offset, length);
	}

	@Override
	public int hashCode() {
		return lowerCaseHashCode;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || !(o instanceof HttpHeader))
			return false;
		HttpHeader that = (HttpHeader) o;

		if (length != that.length) return false;
		for (int i = 0; i < length; i++) {
			byte thisChar = this.bytes[offset + i];
			byte thatChar = that.bytes[that.offset + i];
			if (thisChar >= 'A' && thisChar <= 'Z')
				thisChar += 'a' - 'A';
			if (thatChar >= 'A' && thatChar <= 'Z')
				thatChar += 'a' - 'A';
			if (thisChar != thatChar)
				return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}
