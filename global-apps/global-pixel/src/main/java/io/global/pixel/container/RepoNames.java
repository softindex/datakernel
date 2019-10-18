package io.global.pixel.container;

public final class RepoNames {
	private final String metadata;
	private final String threads;
	private final String session;
	private final String threadRepoPrefix;

	private RepoNames(String metadata, String threads, String session, String threadRepoPrefix) {
		this.metadata = metadata;
		this.threads = threads;
		this.session = session;
		this.threadRepoPrefix = threadRepoPrefix;
	}

	public static RepoNames of(String metadata, String threadIndices, String session, String threadRepoPrefix) {
		return new RepoNames(metadata, threadIndices, session, threadRepoPrefix);
	}

	public static RepoNames ofDefault(String forumPrefix) {
		return new RepoNames(
				forumPrefix + "/metadata",
				forumPrefix + "/threads",
				forumPrefix + "/session",
				forumPrefix + "/thread"
		);
	}

	public String getMetadata() {
		return metadata;
	}

	public String getAlbums() {
		return threads;
	}

	public String getThreadRepoPrefix() {
		return threadRepoPrefix;
	}

	public String getSession() {
		return session;
	}

	@Override
	public String toString() {
		return "ForumRepoNames{metadata='" + metadata
			 + "', threads='" + threads + "', session='" + session +
				"', threadRepoPrefix='" + threadRepoPrefix + "'}";
	}
}
