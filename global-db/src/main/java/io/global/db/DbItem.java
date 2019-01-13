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

import io.global.common.CTRAESCipher;
import io.global.common.CryptoUtils;
import io.global.common.Hash;
import io.global.common.SimKey;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

public final class DbItem {
	private final byte[] key;
	private final long timestamp;
	@Nullable
	private final byte[] value;

	@Nullable
	private final Hash simKeyHash;

	private DbItem(byte[] key, @Nullable byte[] value, long timestamp, @Nullable Hash simKeyHash) {
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
		this.simKeyHash = simKeyHash;
	}

	private static DbItem crypt(DbItem item, @Nullable SimKey simKey, boolean storeHash) {
		if (simKey == null) {
			return item;
		}
		byte[] key = item.key;
		byte[] value = item.value;

		assert value != null : "trying to crypt a tombstone";

		value = Arrays.copyOf(value, value.length);
		CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(value);
		return new DbItem(key, value, item.timestamp, storeHash ? Hash.sha1(simKey.getBytes()) : null);

		// TODO anton: replace above 4 lines with below 2 when tests will use file storage instead of runtime stubs
		// CTRAESCipher.create(simKey.getAesKey(), CryptoUtils.nonceFromBytes(key)).apply(value);
		// return new DbItem(key, value, item.timestamp, storeHash ? Hash.sha1(simKey.getBytes()) : null);
	}

	public static DbItem encrypt(DbItem plainItem, @Nullable SimKey simKey) {
		return crypt(plainItem, simKey, true);
	}

	public static DbItem decrypt(DbItem encryptedItem, @Nullable SimKey simKey) {
		return crypt(encryptedItem, simKey, false);
	}

	public static DbItem of(byte[] key, byte[] value, long timestamp) {
		return new DbItem(key, value, timestamp, null);
	}

	public static DbItem ofRemoved(byte[] key, long timestamp) {
		return new DbItem(key, null, timestamp, null);
	}

	public static DbItem parse(byte[] key, byte[] value, long timestamp, @Nullable Hash simKeyHash) {
		return new DbItem(key, value, timestamp, simKeyHash);
	}

	public byte[] getKey() {
		return key;
	}

	public byte[] getValue() {
		assert value != null : "Calling .getValue() on a tombstone key-value pair";
		return value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isRemoved() {
		return value == null;
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

		if (timestamp != dbItem.timestamp) return false;
		if (!Arrays.equals(key, dbItem.key)) return false;
		if (!Arrays.equals(value, dbItem.value)) return false;
		return Objects.equals(simKeyHash, dbItem.simKeyHash);
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(key);
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + Arrays.hashCode(value);
		result = 31 * result + (simKeyHash != null ? simKeyHash.hashCode() : 0);
		return result;
	}

	@Override
	public String toString() {
		return "DbItem{key=@" + Integer.toHexString(Arrays.hashCode(key)) +
				", value=@" + Integer.toHexString(Arrays.hashCode(value)) +
				", timestamp=" + timestamp +
				(simKeyHash != null ? ", endcrypted=true" : "") + '}';
	}
}
