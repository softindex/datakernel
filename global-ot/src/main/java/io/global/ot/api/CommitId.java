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

package io.global.ot.api;

import io.datakernel.common.parse.ParseException;
import io.global.common.CryptoUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.global.common.CryptoUtils.sha256;

public final class CommitId implements Comparable<CommitId> {
	private static final CommitId ROOT = new CommitId(1, new byte[]{});

	private final long level;
	private final byte[] bytes;

	private CommitId(long level, byte[] bytes) {
		this.level = level;
		this.bytes = bytes;
	}

	public static CommitId ofRoot() {
		return ROOT;
	}

	public static CommitId of(long level, byte[] bytes) {
		return new CommitId(level, bytes);
	}

	public static CommitId parse(byte[] bytes) throws ParseException {
		if (bytes.length < 8) {
			throw new ParseException(CommitId.class, "Length of byte array should not be less than 8, was" + bytes.length);
		}
		long level = ((long) bytes[7] << 56)
				| ((long) bytes[6] & 0xff) << 48
				| ((long) bytes[5] & 0xff) << 40
				| ((long) bytes[4] & 0xff) << 32
				| ((long) bytes[3] & 0xff) << 24
				| ((long) bytes[2] & 0xff) << 16
				| ((long) bytes[1] & 0xff) << 8
				| ((long) bytes[0] & 0xff);
		return new CommitId(level, Arrays.copyOfRange(bytes, 8, bytes.length));
	}

	public static CommitId ofCommitData(long level, byte[] bytes) {
		return new CommitId(level, sha256(bytes));
	}

	public boolean isRoot() {
		return this.bytes.length == 0;
	}

	public byte[] toBytes() {
		byte[] result = new byte[bytes.length + 8];
		for (int i = 0; i < 8; i++) {
			result[i] = (byte) (level >> 8 * i);
		}
		System.arraycopy(bytes, 0, result, 8, bytes.length);
		return result;
	}

	public byte[] getBytes() {
		return bytes;
	}

	public long getLevel() {
		return level;
	}

	@Override
	public int compareTo(@NotNull CommitId other) {
		int result;
		result = -Long.compare(this.level, other.level);
		if (result != 0) return result;
		result = Integer.compare(this.bytes.length, other.bytes.length);
		if (result != 0) return result;
		for (int i = 0; i < this.bytes.length; i++) {
			result = Byte.compare(this.bytes[i], other.bytes[i]);
			if (result != 0) return result;
		}
		return 0;

	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		CommitId commitId = (CommitId) o;

		if (level != commitId.level) return false;
		if (!Arrays.equals(bytes, commitId.bytes)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = Arrays.hashCode(bytes);
		result = 31 * result + (int) (level ^ (level >>> 32));
		return result;
	}

	@Override
	public String toString() {
		if (isRoot()) {
			return "[1]ROOT";
		}
		return "[" + level + "]" + CryptoUtils.toHexString(bytes);
	}
}
