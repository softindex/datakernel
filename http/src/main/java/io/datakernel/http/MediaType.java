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

import java.util.Arrays;

import static io.datakernel.bytebuf.ByteBufStrings.*;

// All media type values, subtype values, and parameter names as defined are case-insensitive RFC2045 section 2
public final class MediaType extends CaseInsensitiveTokenMap.Token {
	final byte[] bytes;
	final int offset;
	final int length;

	MediaType(byte[] bytes, int offset, int length, byte[] lowerCaseBytes, int lowerCaseHashCode) {
		this.bytes = bytes;
		this.offset = offset;
		this.length = length;
		this.lowerCaseBytes = lowerCaseBytes;
		this.lowerCaseHashCode = lowerCaseHashCode;
	}

	public static MediaType of(String mime) {
		byte[] bytes = encodeAscii(mime);
		return MediaTypes.of(bytes, 0, bytes.length, hashCodeLowerCaseAscii(bytes));
	}

	int size() {
		return bytes.length;
	}

	boolean isTextType() {
		return bytes.length > 5
				&& bytes[0] == 't'
				&& bytes[1] == 'e'
				&& bytes[2] == 'x'
				&& bytes[3] == 't'
				&& bytes[4] == '/';
	}

	@Override
	public int hashCode() {
		int result = bytes != null ? Arrays.hashCode(bytes) : 0;
		result = 31 * result + offset;
		result = 31 * result + length;
		return result;
	}

	@Override
	public String toString() {
		return decodeAscii(bytes, offset, length);
	}
}
