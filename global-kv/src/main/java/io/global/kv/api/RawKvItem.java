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

package io.global.kv.api;

import io.global.common.Hash;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

public final class RawKvItem {
	private final long timestamp;

	private final byte[] key;
	@Nullable
	private final byte[] value;

	@Nullable
	private final Hash simKeyHash;

	public RawKvItem(byte[] key, @Nullable byte[] value, long timestamp, @Nullable Hash simKeyHash) {
		this.key = key;
		this.value = value;
		this.timestamp = timestamp;
		this.simKeyHash = simKeyHash;
	}

	public static RawKvItem of(byte[] key, byte[] value, long timestamp) {
		return new RawKvItem(key, value, timestamp, null);
	}

	public static RawKvItem ofRemoved(byte[] key, long timestamp) {
		return new RawKvItem(key, null, timestamp, null);
	}

	public static RawKvItem parse(byte[] key, byte[] value, long timestamp, @Nullable Hash simKeyHash) {
		return new RawKvItem(key, value, timestamp, simKeyHash);
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

		RawKvItem rawKvItem = (RawKvItem) o;

		if (timestamp != rawKvItem.timestamp) return false;
		if (!Arrays.equals(key, rawKvItem.key)) return false;
		if (!Arrays.equals(value, rawKvItem.value)) return false;
		return Objects.equals(simKeyHash, rawKvItem.simKeyHash);
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
		return "RawKvItem{key=@" + Integer.toHexString(Arrays.hashCode(key)) +
				", value=@" + Integer.toHexString(Arrays.hashCode(value)) +
				", timestamp=" + timestamp +
				(simKeyHash != null ? ", endcrypted=true" : "") + '}';
	}
}
