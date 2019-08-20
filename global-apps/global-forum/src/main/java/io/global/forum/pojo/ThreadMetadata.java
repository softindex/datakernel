package io.global.forum.pojo;

public final class ThreadMetadata {
	private final String title;
	private final long threadId;

	public ThreadMetadata(String title, long threadId) {
		this.title = title;
		this.threadId = threadId;
	}

	public String getTitle() {
		return title;
	}

	public long getThreadId() {
		return threadId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ThreadMetadata that = (ThreadMetadata) o;

		if (threadId != that.threadId) return false;
		if (!title.equals(that.title)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = title.hashCode();
		result = 31 * result + (int) (threadId ^ (threadId >>> 32));
		return result;
	}
}
