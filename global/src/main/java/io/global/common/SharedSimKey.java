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
import org.spongycastle.crypto.CryptoException;

import java.util.Arrays;

public final class SharedSimKey {
	private final Hash hash;
	private final byte[] encrypted;

	private SharedSimKey(Hash hash, byte[] encrypted) {
		this.hash = hash;
		this.encrypted = encrypted;
	}

	public static SharedSimKey of(Hash hash, byte[] encryptedSimKey) {
		return new SharedSimKey(hash, encryptedSimKey);
	}

	public static SharedSimKey of(SimKey simKey, PubKey receiver) {
		return of(Hash.sha1(simKey.getBytes()), receiver.encrypt(simKey.getBytes()));
	}

	public static SharedSimKey parse(Hash hash, byte[] encryptedSimKey) throws ParseException {
		return new SharedSimKey(hash, encryptedSimKey);
	}

	public Hash getHash() {
		return hash;
	}

	public byte[] getEncrypted() {
		return encrypted;
	}

	public SimKey decryptSimKey(PrivKey privKey) throws CryptoException {
		return SimKey.of(privKey.decrypt(encrypted));
	}

	@Override
	public int hashCode() {
		return 31 * hash.hashCode() + Arrays.hashCode(encrypted);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		SharedSimKey that = (SharedSimKey) o;

		return hash.equals(that.hash) && Arrays.equals(encrypted, that.encrypted);
	}

	@Override
	public String toString() {
		return "SharedSimKey{hash=" + hash + ", encrypted=@" + Integer.toHexString(Arrays.hashCode(encrypted)) + '}';
	}
}
