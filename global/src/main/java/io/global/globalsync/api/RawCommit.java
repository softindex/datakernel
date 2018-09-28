/*
 * Copyright (C) 2015-2018  SoftIndex LLC.
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
 *
 */

package io.global.globalsync.api;

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.SimKeyHash;
import io.global.globalsync.util.BinaryDataFormats;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.global.globalsync.util.BinaryDataFormats.*;

public final class RawCommit {
	private final byte[] bytes;

	private final List<CommitId> parents;
	private final EncryptedData encryptedDiffs;
	private final SimKeyHash simKeyHash;
	private final long level;
	private final long timestamp;

	// region creators
	private RawCommit(byte[] bytes,
			List<CommitId> parents, EncryptedData encryptedDiffs, SimKeyHash simKeyHash,
			long level, long timestamp) {
		this.bytes = bytes;
		this.parents = parents;
		this.encryptedDiffs = encryptedDiffs;
		this.simKeyHash = simKeyHash;
		this.level = level;
		this.timestamp = timestamp;
	}

	public static RawCommit ofBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		List<CommitId> parents = readList(buf, BinaryDataFormats::readCommitId);
		EncryptedData encryptedData = readEncryptedData(buf);
		long level = buf.readLong();
		long timestamp = buf.readLong();
		SimKeyHash simKeyHash = readSimKeyHash(buf);
		return new RawCommit(bytes,
				parents, encryptedData, simKeyHash, level, timestamp);
	}

	public static RawCommit of(List<CommitId> parents, EncryptedData encryptedDiffs, SimKeyHash simKeyHash, long level, long timestamp) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(parents, BinaryDataFormats::sizeof) + sizeof(encryptedDiffs) + sizeof(simKeyHash) + 8 + 8);
		writeCollection(buf, parents, BinaryDataFormats::writeCommitId);
		writeEncryptedData(buf, encryptedDiffs);
		buf.writeLong(level);
		buf.writeLong(timestamp);
		writeSimKeyHash(buf, simKeyHash);
		return new RawCommit(buf.asArray(),
				parents, encryptedDiffs, simKeyHash, level, timestamp);
	}
	// endregion

	public Set<CommitId> getParents() {
		return new HashSet<>(parents);
	}

	public byte[] toBytes() {
		return bytes;
	}

	public EncryptedData getEncryptedDiffs() {
		return encryptedDiffs;
	}

	public SimKeyHash getSimKeyHash() {
		return simKeyHash;
	}

	public long getLevel() {
		return level;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		RawCommit that = (RawCommit) o;
		return Arrays.equals(bytes, that.bytes);
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(bytes);
	}
}
