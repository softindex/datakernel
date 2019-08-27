package io.global.forum.pojo;

public final class ThreadMetadata {
	private final String title;

	public ThreadMetadata(String title) {
		this.title = title;
	}

	public static ThreadMetadata of(String title) {
		return new ThreadMetadata(title);
	}

	public String getTitle() {
		return title;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		ThreadMetadata that = (ThreadMetadata) o;

		return title.equals(that.title);
	}

	@Override
	public int hashCode() {
		return title.hashCode();
	}
}
