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

package io.global.globalfs.api;

/**
 * Similar to FileMetadata from RemoteFs, but over {@link GlobalFsPath} instead of String.
 */
public final class GlobalFsMetadata {
	private final GlobalFsPath path;
	private final long size;
	private final long revision;

	// region creators
	private GlobalFsMetadata(GlobalFsPath path, long size, long revision) {
		this.path = path;
		this.size = size;
		this.revision = revision;
	}

	public static GlobalFsMetadata of(GlobalFsPath path, long size, long revision) {
		return new GlobalFsMetadata(path, size, revision);
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

	public GlobalFsPath getPath() {
		return path;
	}

	public boolean isDeleted() {
		return size == -1;
	}

	public long getSize() {
		return size;
	}

	public long getRevision() {
		return revision;
	}

	@Override
	public int hashCode() {
		return 31 * (31 * path.hashCode() + (int) (size ^ (size >>> 32))) + (int) (revision ^ (revision >>> 32));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsMetadata that = (GlobalFsMetadata) o;
		return size == that.size && revision == that.revision && path.equals(that.path);
	}

	@Override
	public String toString() {
		return "GlobalFsMetadata{path=" + path + ", size=" + size + ", revision=" + revision + '}';
	}
}
