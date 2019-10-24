package io.global.comm.pojo;

public final class ThreadMetadata {
	private final String title;
	private final long lastUpdate;

	private ThreadMetadata(String title, long lastUpdate) {
		this.title = title;
		this.lastUpdate = lastUpdate;
	}

	public static ThreadMetadata of(String title, long lastUpdate) {
		return new ThreadMetadata(title, lastUpdate);
	}

	public ThreadMetadata updated(long timestamp) {
		return new ThreadMetadata(title, timestamp);
	}

	public static ThreadMetadata parse(String title, long lastUpdate) {
		return new ThreadMetadata(title, lastUpdate);
	}

	public String getTitle() {
		return title;
	}

	public long getLastUpdate() {
		return lastUpdate;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ThreadMetadata metadata = (ThreadMetadata) o;

		return lastUpdate == metadata.lastUpdate && title.equals(metadata.title);
	}

	@Override
	public int hashCode() {
		return 31 * title.hashCode() + (int) (lastUpdate ^ (lastUpdate >>> 32));
	}
}
