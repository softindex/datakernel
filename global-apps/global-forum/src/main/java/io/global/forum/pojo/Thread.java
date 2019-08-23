package io.global.forum.pojo;

public final class Thread {
	private final String title;
	private final String threadRepoId;

	public Thread(String title, String threadRepoId) {
		this.title = title;
		this.threadRepoId = threadRepoId;
	}

	public String getTitle() {
		return title;
	}

	public String getRootPostId() {
		return threadRepoId;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Thread that = (Thread) o;

		if (!title.equals(that.title)) return false;
		if (!threadRepoId.equals(that.threadRepoId)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = title.hashCode();
		result = 31 * result + threadRepoId.hashCode();
		return result;
	}
}
