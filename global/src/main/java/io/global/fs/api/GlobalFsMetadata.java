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

package io.global.fs.api;

import io.datakernel.annotation.Nullable;
import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.ByteArrayIdentity;
import io.global.common.Hash;

import java.util.Objects;

import static io.global.ot.util.BinaryDataFormats.*;

public final class GlobalFsMetadata implements ByteArrayIdentity {
	private byte[] bytes;

	private final LocalPath localPath;
	private final long size;
	private final long revision;

	@Nullable
	private final Hash simKeyHash;

	// region creators
	private GlobalFsMetadata(byte[] bytes, LocalPath localPath, long size, long revision, @Nullable Hash simKeyHash) {
		this.bytes = bytes;
		this.localPath = localPath;
		this.size = size;
		this.revision = revision;
		this.simKeyHash = simKeyHash;
	}

	public static GlobalFsMetadata of(LocalPath localPath, long size, long revision) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(localPath) + 9 + 8);
		writeLocalPath(buf, localPath);
		buf.writeVarLong(size);
		buf.writeLong(revision);
		return new GlobalFsMetadata(buf.asArray(), localPath, size, revision, null);
	}

	public static GlobalFsMetadata ofRemoved(LocalPath localPath, long revision) {
		return of(localPath, -1, revision);
	}

	public static GlobalFsMetadata fromBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		LocalPath localPath = readLocalPath(buf);
		long size = buf.readVarLong();
		long revision = buf.readLong();
		return new GlobalFsMetadata(bytes, localPath, size, revision, null);
	}
	// endregion

	public static GlobalFsMetadata getBetter(GlobalFsMetadata first, GlobalFsMetadata second) {
		if (first == null) {
			return second;
		}
		if (second == null) {
			return first;
		}
		assert Objects.equals(first.simKeyHash, second.simKeyHash);
		if (first.revision > second.revision) {
			return first;
		}
		if (second.revision > first.revision) {
			return second;
		}
		return first.size < second.size ? second : first;
	}

	public GlobalFsMetadata toRemoved() {
		return ofRemoved(localPath, revision);
	}

	public LocalPath getLocalPath() {
		return localPath;
	}

	public long getSize() {
		return size;
	}

	public long getRevision() {
		return revision;
	}

	@Nullable
	public Hash getSimKeyHash() {
		return simKeyHash;
	}

	public boolean isRemoved() {
		return size == -1;
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public int hashCode() {
		return 31 * (31 * (31 * localPath.hashCode() + (int) (size ^ (size >>> 32))) + (int) (revision ^ (revision >>> 32))) + (simKeyHash != null ? simKeyHash.hashCode() : 0);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		GlobalFsMetadata metadata = (GlobalFsMetadata) o;

		//noinspection ConstantConditions
		return size == metadata.size &&
				revision == metadata.revision &&
				localPath.equals(metadata.localPath) &&
				(simKeyHash != null ? simKeyHash.equals(metadata.simKeyHash) : metadata.simKeyHash == null);
	}

	@Override
	public String toString() {
		return "GlobalFsMetadata{localPath=" + localPath + ", size=" + size + ", revision=" + revision + ", simKeyHash=" + simKeyHash + '}';
	}
}
