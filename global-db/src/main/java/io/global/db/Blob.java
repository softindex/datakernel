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

import java.util.Arrays;

public final class Blob {
	private final long timestamp;
	@Nullable
	private final byte[] data;

	private Blob(long timestamp, @Nullable byte[] data) {
		this.timestamp = timestamp;
		this.data = data;
	}

	public static Blob of(long timestamp, byte[] data) {
		assert data != null;
		return new Blob(timestamp, data);
	}

	public static Blob parse(long timestamp, @Nullable byte[] data) {
		return new Blob(timestamp, data);
	}

	public static Blob ofRemoved(long timestamp) {
		return new Blob(timestamp, null);
	}

	public long getTimestamp() {
		return timestamp;
	}

	public byte[] getData() {
		assert data != null : "Calling .getData() on a tombstone blob";
		return data;
	}

	public boolean isRemoved() {
		return data == null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Blob blob = (Blob) o;

		return timestamp == blob.timestamp && Arrays.equals(data, blob.data);
	}

	@Override
	public int hashCode() {
		return 31 * (int) (timestamp ^ (timestamp >>> 32)) + Arrays.hashCode(data);
	}

	@Override
	public String toString() {
		return "Blob{timestamp=" + timestamp + (data != null ? ", data=@" + Integer.toHexString(Arrays.hashCode(data)) : "") + '}';
	}
}
