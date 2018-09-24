package io.global.globalfs.api;

public final class GlobalFsMetadata {
	private final GlobalFsAddress address;
	private final long size;
	private final long revision;

	public GlobalFsMetadata(GlobalFsAddress address, long size, long revision) {
		this.address = address;
		this.size = size;
		this.revision = revision;
	}

	public GlobalFsAddress getAddress() {
		return address;
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
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		GlobalFsMetadata that = (GlobalFsMetadata) o;
		return size == that.size && revision == that.revision && address.equals(that.address);
	}

	@Override
	public int hashCode() {
		return 31 * (31 * address.hashCode() + (int) (size ^ (size >>> 32))) + (int) (revision ^ (revision >>> 32));
	}

	@Override
	public String toString() {
		return "GlobalFsMetadata{address=" + address + ", size=" + size + ", revision=" + revision + '}';
	}

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
}
