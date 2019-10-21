package io.global.comm.container;

public final class CommRepoNames {
	private final String metadata;
	private final String users;
	private final String bans;
	private final String threads;
	private final String session;
	private final String threadRepoPrefix;

	private CommRepoNames(String metadata, String users, String bans, String threads, String session, String threadRepoPrefix) {
		this.metadata = metadata;
		this.users = users;
		this.bans = bans;
		this.threads = threads;
		this.session = session;
		this.threadRepoPrefix = threadRepoPrefix;
	}

	public static CommRepoNames of(String metadata, String userStates, String ipBans, String threadIndices, String session, String threadRepoPrefix) {
		return new CommRepoNames(metadata, userStates, ipBans, threadIndices, session, threadRepoPrefix);
	}

	public static CommRepoNames ofDefault(String forumPrefix) {
		return new CommRepoNames(
				forumPrefix + "/metadata",
				forumPrefix + "/users",
				forumPrefix + "/bans",
				forumPrefix + "/threads",
				forumPrefix + "/session",
				forumPrefix + "/thread"
		);
	}

	public String getMetadata() {
		return metadata;
	}

	public String getUsers() {
		return users;
	}

	public String getBans() {
		return bans;
	}

	public String getThreads() {
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
		return "ForumRepoNames{metadata='" + metadata + "', user='" + users
				+ "', bans='" + bans + "', threads='" + threads + "', session='" + session +
				"', threadRepoPrefix='" + threadRepoPrefix + "'}";
	}
}