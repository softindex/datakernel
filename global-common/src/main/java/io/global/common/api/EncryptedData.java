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

package io.global.common.api;

import io.datakernel.common.parse.ParseException;
import io.global.common.CryptoUtils;
import io.global.common.SimKey;

import java.util.Arrays;

public final class EncryptedData {
	public final byte[] nonce;
	public final byte[] encryptedBytes;

	public EncryptedData(byte[] nonce, byte[] encryptedBytes) {
		this.nonce = nonce;
		this.encryptedBytes = encryptedBytes;
	}

	public static EncryptedData parse(byte[] nonce, byte[] encryptedBytes) throws ParseException {
		return new EncryptedData(nonce, encryptedBytes); // TODO
	}

	public static EncryptedData encrypt(byte[] bytes, SimKey simKey) {
		return CryptoUtils.encryptAES(bytes, simKey.getAesKey());
	}

	public byte[] decrypt(SimKey simKey) {
		return CryptoUtils.decryptAES(this, simKey.getAesKey());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		EncryptedData that = (EncryptedData) o;
		return Arrays.equals(nonce, that.nonce) &&
				Arrays.equals(encryptedBytes, that.encryptedBytes);
	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(nonce) + Arrays.hashCode(encryptedBytes);
	}

	public byte[] getNonce() {
		return nonce;
	}

	public byte[] getEncryptedBytes() {
		return encryptedBytes;
	}
}
