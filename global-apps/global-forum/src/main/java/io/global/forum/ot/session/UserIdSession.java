package io.global.forum.ot.session;

import io.global.forum.pojo.UserId;

public final class UserIdSession {
	private final UserId userId;
	private long lastAccessTimestamp;

	public UserIdSession(UserId userId, long lastAccessTimestamp) {
		this.userId = userId;
		this.lastAccessTimestamp = lastAccessTimestamp;
	}

	public UserId getUserId() {
		return userId;
	}

	public long getLastAccessTimestamp() {
		return lastAccessTimestamp;
	}

	public void setLastAccessTimestamp(long lastAccessTimestamp) {
		this.lastAccessTimestamp = lastAccessTimestamp;
	}
}
