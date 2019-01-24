/*
 * Copyright (C) 2015-2019 SoftIndex LLC.
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

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * This is a POJO for holding name, size and timestamp of some file
 */
public final class FileMetadata {
	private final String filename;
	private final long size;
	private final long timestamp;

	public FileMetadata(String filename, long size, long timestamp) {
		this.filename = checkNotNull(filename, "name");
		this.size = size;
		this.timestamp = timestamp;
	}

	public String getFilename() {
		return filename;
	}

	public long getSize() {
		return size;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean equalsIgnoringTimestamp(FileMetadata other) {
		return filename.equals(other.filename) && size == other.size;
	}

	@Override
	public String toString() {
		return filename + "(size=" + size + ", timestamp=" + timestamp + ')';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		FileMetadata that = (FileMetadata) o;

		return size == that.size && timestamp == that.timestamp && filename.equals(that.filename);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * filename.hashCode() + (int) (size ^ (size >>> 32))) + (int) (timestamp ^ (timestamp >>> 32));
	}

	@Nullable
	public static FileMetadata getMoreCompleteFile(@Nullable FileMetadata first, @Nullable FileMetadata second) {
		if (first == null) {
			return second;
		}
		if (second == null) {
			return first;
		}
		if (first.size > second.size) {
			return first;
		}
		if (second.size > first.size) {
			return second;
		}
		return second.timestamp > first.timestamp ? second : first;
	}

	public static List<FileMetadata> flatten(Stream<List<FileMetadata>> streamOfLists) {
		Map<String, FileMetadata> map = new HashMap<>();
		streamOfLists
				.flatMap(List::stream)
				.forEach(meta -> map.compute(meta.getFilename(), ($, existing) -> getMoreCompleteFile(existing, meta)));
		return new ArrayList<>(map.values());
	}
}
