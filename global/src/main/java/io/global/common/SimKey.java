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

import org.spongycastle.crypto.params.KeyParameter;

import java.util.Arrays;

public final class SimKey implements Base64Identity {
	private final byte[] key;
	private final KeyParameter keyParameter;

	private SimKey(KeyParameter keyParameter) {
		this.keyParameter = keyParameter;
		this.key = keyParameter.getKey();
	}

	private SimKey(byte[] key) {
		this.key = key;
		this.keyParameter = new KeyParameter(key);
	}

	public static SimKey generate() {
		return new SimKey(CryptoUtils.generateCipherKey(16));
	}

	public static SimKey of(KeyParameter keyParameter) {
		return new SimKey(keyParameter);
	}

	public static SimKey ofBytes(byte[] bytes) {
		return new SimKey(bytes);
	}

	public static SimKey fromString(String string) {
		return new SimKey(decoder.decode(string));
	}

	@Override
	public byte[] toBytes() {
		return key;
	}

	public KeyParameter getAesKey() {
		return keyParameter;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(key);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SimKey simKey = (SimKey) o;

		return Arrays.equals(key, simKey.key);
	}

	@Override
	public String toString() {
		return "SimKey@" + Integer.toHexString(Arrays.hashCode(key));
	}
}
