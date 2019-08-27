package io.global.forum.container;

public final class ForumRepoNames {
	private final String metadata;
	private final String users;
	private final String bans;
	private final String threads;
	private final String threadRepoPrefix;

	private ForumRepoNames(String metadata, String users, String bans, String threads, String threadRepoPrefix) {
		this.metadata = metadata;
		this.users = users;
		this.bans = bans;
		this.threads = threads;
		this.threadRepoPrefix = threadRepoPrefix;
	}

	public static ForumRepoNames of(String metadata, String userStates, String ipBans, String threadIndices, String threadRepoPrefix) {
		return new ForumRepoNames(metadata, userStates, ipBans, threadIndices, threadRepoPrefix);
	}

	public static ForumRepoNames ofDefault(String forumPrefix) {
		return new ForumRepoNames(
				forumPrefix + "/metadata",
				forumPrefix + "/users",
				forumPrefix + "/bans",
				forumPrefix + "/threads",
				forumPrefix + "/thread");
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

	@Override
	public String toString() {
		return "ForumRepoNames{metadata='" + metadata + "', user='" + users
				+ "', bans='" + bans + "', threads='" + threads + "', threadRepoPrefix='" + threadRepoPrefix + "'}";
	}
}
