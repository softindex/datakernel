package io.datakernel.remotefs;

import static io.datakernel.util.Preconditions.checkNotNull;

/**
 * This is a POJO for holding both name and size of some file
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
}
