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
import io.datakernel.remotefs.FileMetadata;
import io.global.common.ByteArrayIdentity;
import io.global.common.Hash;

import static io.global.ot.util.BinaryDataFormats.*;

public final class GlobalFsMetadata implements Comparable<GlobalFsMetadata>, ByteArrayIdentity {
	private byte[] bytes;

	private final String filename;
	private final long size;
	private final long revision;

	@Nullable
	private final Hash simKeyHash;

	// region creators
	private GlobalFsMetadata(byte[] bytes, String filename, long size, long revision, @Nullable Hash simKeyHash) {
		this.bytes = bytes;
		this.filename = filename;
		this.size = size;
		this.revision = revision;
		this.simKeyHash = simKeyHash;
	}

	public static GlobalFsMetadata of(String filename, long size, long revision, @Nullable Hash simKeyHash) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(filename) + 9 + 8 + 1 + (simKeyHash != null ? sizeof(simKeyHash) : 0));
		writeString(buf, filename);
		buf.writeVarLong(size);
		buf.writeLong(revision);
		if (simKeyHash != null) {
			buf.writeByte((byte) 1);
			write(buf, simKeyHash);
		} else {
			buf.writeByte((byte) 0);
		}
		return new GlobalFsMetadata(buf.asArray(), filename, size, revision, simKeyHash);
	}

	public static GlobalFsMetadata of(String filename, long size, long revision) {
		return of(filename, size, revision, null);
	}

	public static GlobalFsMetadata ofRemoved(String filename, long revision) {
		return of(filename, -1, revision);
	}

	public static GlobalFsMetadata fromBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		String filename = readString(buf);
		long size = buf.readVarLong();
		long revision = buf.readLong();
		Hash simKeyHash = buf.readByte() == 1 ? read(buf, Hash::ofBytes) : null;
		return new GlobalFsMetadata(bytes, filename, size, revision, simKeyHash);
	}
	// endregion

	@Override
	public int compareTo(@Nullable GlobalFsMetadata other) {
		// existing file is better than non-existing
		if (other == null) {
			return 1;
		}
		// revisions are straight precedence
		int res = Long.compare(revision, other.revision);
		return res != 0 ?
				res :
				// we are obviously biased towards bigger files here
				// (also tombstones have size == -1 so even empty
				// files are considered better than their absense)
				Long.compare(size, other.size);
	}

	public GlobalFsMetadata toRemoved(long revision) {
		// no key hash because tombstones do not need any encryption
		return ofRemoved(filename, revision);
	}

	public String getFilename() {
		return filename;
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

	public FileMetadata toFileMetadata() {
		return new FileMetadata(filename, size, revision);
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		GlobalFsMetadata metadata = (GlobalFsMetadata) o;

		return size == metadata.size &&
				revision == metadata.revision &&
				filename.equals(metadata.filename) &&
				(simKeyHash != null ? simKeyHash.equals(metadata.simKeyHash) : metadata.simKeyHash == null);
	}

	@Override
	public int hashCode() {
		return 29791 * filename.hashCode() +
				961 * (int) (size ^ (size >>> 32)) +
				31 * (int) (revision ^ (revision >>> 32)) +
				(simKeyHash != null ? simKeyHash.hashCode() : 0);
	}

	@Override
	public String toString() {
		return "GlobalFsMetadata{filename=" + filename + ", size=" + size + ", revision=" + revision + ", simKeyHash=" + simKeyHash + '}';
	}
}
