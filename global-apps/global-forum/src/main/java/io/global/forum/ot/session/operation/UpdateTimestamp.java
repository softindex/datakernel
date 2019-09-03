package io.global.forum.ot.session.operation;

import io.global.forum.ot.session.UserIdSession;

import java.util.Map;

public final class UpdateTimestamp implements SessionOperation {
	private final String sessionId;
	private final long previous;
	private final long next;

	private UpdateTimestamp(String sessionId, long previous, long next) {
		this.sessionId = sessionId;
		this.previous = previous;
		this.next = next;
	}

	public static UpdateTimestamp update(String sessionId, long previous, long next) {
		return new UpdateTimestamp(sessionId, previous, next);
	}

	@Override
	public void apply(Map<String, UserIdSession> sessions) {
		sessions.get(sessionId).setLastAccessTimestamp(next);
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	public long getPrevious() {
		return previous;
	}

	public long getNext() {
		return next;
	}
}
