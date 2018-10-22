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

package io.global.common;

import java.util.Arrays;
import java.util.Base64;

public final class Hash implements Base64Identity {
	private final byte[] bytes;

	// region creators
	private Hash(byte[] bytes) {
		this.bytes = bytes;
	}

	public static Hash of(ByteArrayIdentity object) {
		return new Hash(CryptoUtils.sha256(object.toBytes()));
	}

	public static Hash ofBytes(byte[] bytes) {
		return new Hash(bytes);
	}

	public static Hash fromString(String s) {
		return new Hash(Base64.getUrlDecoder().decode(s));
	}
	// endregion

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Hash that = (Hash) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}

	@Override
	public String toString() {
		return "Hash@" + Integer.toHexString(hashCode());
	}
}
