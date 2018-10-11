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

import io.datakernel.bytebuf.ByteBuf;
import io.datakernel.bytebuf.ByteBufPool;
import io.datakernel.exception.ParseException;
import io.global.common.Signable;

import java.util.Arrays;

import static io.global.ot.util.BinaryDataFormats.*;

public final class GlobalFsMetadata implements Signable {
	private byte[] bytes;

	private final String fs;
	private final String path;
	private final long size;
	private final long revision;

	// region creators
	private GlobalFsMetadata(byte[] bytes, String fs, String path, long size, long revision) {
		this.bytes = bytes;
		this.fs = fs;
		this.path = path;
		this.size = size;
		this.revision = revision;
	}

	public static GlobalFsMetadata of(String fs, String path, long size, long revision) {
		ByteBuf buf = ByteBufPool.allocate(sizeof(fs) + sizeof(path) + 9 + 8);
		writeString(buf, fs);
		writeString(buf, path);
		buf.writeVarLong(size);
		buf.writeLong(revision);
		return new GlobalFsMetadata(buf.asArray(), fs, path, size, revision);
	}

	public static GlobalFsMetadata ofRemoved(String fs, String path, long revision) {
		return of(fs, path, -1, revision);
	}

	public static GlobalFsMetadata fromBytes(byte[] bytes) throws ParseException {
		ByteBuf buf = ByteBuf.wrapForReading(bytes);
		String fs = readString(buf);
		String path = readString(buf);
		long size = buf.readVarLong();
		long revision = buf.readLong();
		return new GlobalFsMetadata(bytes, fs, path, size, revision);
	}
	// endregion

	public static GlobalFsMetadata getBetter(GlobalFsMetadata first, GlobalFsMetadata second) {
		if (first == null) {
			return second;
		}
		if (second == null) {
			return first;
		}
		if (first.revision > second.revision) {
			return first;
		}
		if (second.revision > first.revision) {
			return second;
		}
		return first.size > second.size ? first : second;
	}

	public String getFs() {
		return fs;
	}

	public String getPath() {
		return path;
	}

	public String getFullPath() {
		return fs + "::" + path;
	}

	public boolean isRemoved() {
		return size == -1;
	}

	public long getSize() {
		return size;
	}

	public long getRevision() {
		return revision;
	}

	@Override
	public byte[] toBytes() {
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		GlobalFsMetadata that = (GlobalFsMetadata) o;

		return size == that.size && revision == that.revision && Arrays.equals(bytes, that.bytes) && fs.equals(that.fs) && path.equals(that.path);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * (31 * (31 * Arrays.hashCode(bytes) + fs.hashCode()) + path.hashCode()) + (int) (size ^ (size >>> 32))) + (int) (revision ^ (revision >>> 32));
	}

	@Override
	public String toString() {
		return "GlobalFsMetadata{fs='" + fs + "', path='" + path + "', size=" + size + ", revision=" + revision + '}';
	}
}
