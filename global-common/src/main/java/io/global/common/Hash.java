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

import io.datakernel.exception.ParseException;

import java.util.Arrays;

/**
 * Thin holder/wrapper around hash of some object.
 */
public final class Hash {
	private final byte[] bytes;

	// region creators
	private Hash(byte[] bytes) {
		this.bytes = bytes;
	}

	public static Hash sha256(byte[] data) {
		return new Hash(CryptoUtils.sha256(data));
	}

	public static Hash sha1(byte[] data) {
		return new Hash(CryptoUtils.sha1(data));
	}

	public static Hash of(byte[] bytes) {
		return new Hash(bytes);
	}

	public static Hash parse(byte[] bytes) throws ParseException {
		return new Hash(bytes);
	}

	public static Hash fromString(String s) throws ParseException {
		try {
			return parse(CryptoUtils.fromHexString(s));
		} catch (IllegalArgumentException e) {
			throw new ParseException(e);
		}
	}
	// endregion

	public byte[] getBytes() {
		return bytes;
	}

	public String asString() {
		return CryptoUtils.toHexString(bytes);
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
		return "Hash(" + CryptoUtils.toHexString(bytes) + ')';
	}
}
