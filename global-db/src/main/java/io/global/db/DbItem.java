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

package io.global.db;

import io.datakernel.annotation.Nullable;
import io.global.common.CTRAESCipher;
import io.global.common.CryptoUtils;
import io.global.common.Hash;
import io.global.common.SimKey;

import java.util.Arrays;

public final class DbItem {
	private final byte[] key;
	private final Blob value;

	@Nullable
	private final Hash simKeyHash;

	private DbItem(byte[] key, Blob value, @Nullable Hash simKeyHash) {
		this.key = key;
		this.value = value;
		this.simKeyHash = simKeyHash;
	}

	private static DbItem crypt(DbItem item, @Nullable SimKey simKey, boolean storeHash) {
		if (simKey == null) {
			return item;
		}
		byte[] key = item.key;
		Blob value = item.value;

		byte[] data = value.getData();
		data = Arrays.copyOf(data, data.length);
		CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(data);
		return new DbItem(key, Blob.of(value.getTimestamp(), data), storeHash ? Hash.sha1(simKey.getBytes()) : null);

		// TODO anton: replace above 4 lines with below 2 when tests will use file storage instead of runtime stubs
		// CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(value.getData());
		// return new DbItem(key, value, storeHash ? Hash.sha1(simKey.getBytes()) : null);
	}

	public static DbItem encrypt(DbItem plainItem, @Nullable SimKey simKey) {
		return crypt(plainItem, simKey, true);
	}

	public static DbItem decrypt(DbItem encryptedItem, @Nullable SimKey simKey) {
		return crypt(encryptedItem, simKey, false);
	}

	public static DbItem of(byte[] key, Blob value) {
		return new DbItem(key, value, null);
	}

	public static DbItem parse(byte[] key, Blob value, @Nullable Hash simKeyHash) {
		return new DbItem(key, value, simKeyHash);
	}

	public byte[] getKey() {
		return key;
	}

	public Blob getValue() {
		return value;
	}

	@Nullable
	public Hash getSimKeyHash() {
		return simKeyHash;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DbItem dbItem = (DbItem) o;

		return Arrays.equals(key, dbItem.key) && value.equals(dbItem.value);
	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(key) + value.hashCode();
	}

	@Override
	public String toString() {
		return "DbItem{key=@" + Integer.toHexString(Arrays.hashCode(key)) + ", value=" + value + '}';
	}
}
