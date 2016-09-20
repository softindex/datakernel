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

import static io.datakernel.bytebuf.ByteBufStrings.encodeAscii;

/**
 * HttpMethod enum represents a request to be sent via a {@link HttpClientConnection} and a corresponding response.
 */
public enum HttpMethod {
	GET, PUT, POST, HEAD, DELETE, CONNECT, OPTIONS, TRACE, PATCH,
	SEARCH, COPY, MOVE, LOCK, UNLOCK, MKCOL, PROPFIND, PROPPATCH;

	protected final byte[] bytes;

	HttpMethod(String string) {
		this.bytes = encodeAscii(string);
	}

	HttpMethod() {
		this.bytes = encodeAscii(this.name());
	}

	public void write(ByteBuf buf) {
		buf.put(bytes);
	}

	public boolean compareTo(byte[] array, int offset, int size) {
		if (bytes.length != size)
			return false;
		for (int i = 0; i < bytes.length; i++) {
			if (bytes[i] != array[offset + i])
				return false;
		}
		return true;
	}
}