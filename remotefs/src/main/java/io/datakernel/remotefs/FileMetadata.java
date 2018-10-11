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

package io.datakernel.remotefs;

import io.datakernel.annotation.Nullable;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * This is a POJO for holding name, size and timestamp of some file
 */
public class FileMetadata {
	private final String name;
	private final long size;
	private final long timestamp;

	public FileMetadata(String name, long size, long timestamp) {
		this.name = checkNotNull(name, "name");
		this.size = size;
		this.timestamp = timestamp;
	}

	public String getName() {
		return name;
	}

	public long getSize() {
		return size;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean equalsIgnoringTimestamp(FileMetadata other) {
		return name.equals(other.name) && size == other.size;
	}

	@Override
	public String toString() {
		return name + "(size=" + size + ", timestamp=" + timestamp + ')';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FileMetadata that = (FileMetadata) o;

		return size == that.size && timestamp == that.timestamp && name.equals(that.name);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * name.hashCode() + (int) (size ^ (size >>> 32))) + (int) (timestamp ^ (timestamp >>> 32));
	}

	@Nullable
	public static FileMetadata getMoreCompleteFile(@Nullable FileMetadata first, @Nullable FileMetadata second) {
		if (first == null) {
			return second;
		}
		if (second == null) {
			return first;
		}
		if (first.getSize() > second.getSize()) {
			return first;
		}
		if (second.getSize() > first.getSize()) {
			return second;
		}
		return second.getTimestamp() > first.getTimestamp() ? second : first;
	}
}
