package io.global.forum.ot.session.operation;

import io.global.forum.ot.session.UserIdSession;
import io.global.forum.pojo.UserId;

import java.util.Map;

import static io.global.forum.pojo.AuthService.DK_APP_STORE;

public final class AddOrRemoveSession implements SessionOperation {
	public static final AddOrRemoveSession EMPTY = of("", new UserId(DK_APP_STORE, ""), -1L, true);

	private final String sessionId;
	private final UserId userId;
	private final long timestamp;
	private final boolean remove;

	private AddOrRemoveSession(String sessionId, UserId userId, long timestamp, boolean remove) {
		this.sessionId = sessionId;
		this.userId = userId;
		this.timestamp = timestamp;
		this.remove = remove;
	}

	public static AddOrRemoveSession of(String sessionId, UserId userId, long timestamp, boolean remove) {
		return new AddOrRemoveSession(sessionId, userId, timestamp, remove);
	}

	public static AddOrRemoveSession add(String sessionId, UserId userId, long timestamp) {
		return of(sessionId, userId, timestamp, false);
	}

	public static AddOrRemoveSession remove(String sessionId, UserId userId, long timestamp) {
		return of(sessionId, userId, timestamp, false);
	}

	@Override
	public void apply(Map<String, UserIdSession> sessions) {
		if (remove) {
			sessions.remove(sessionId);
		} else {
			sessions.put(sessionId, new UserIdSession(userId, timestamp));
		}
	}

	@Override
	public String getSessionId() {
		return sessionId;
	}

	public UserId getUserId() {
		return userId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isRemove() {
		return remove;
	}

	public AddOrRemoveSession invert() {
		return of(sessionId, userId, timestamp, !remove);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		AddOrRemoveSession that = (AddOrRemoveSession) o;

		if (timestamp != that.timestamp) return false;
		if (remove != that.remove) return false;
		if (!sessionId.equals(that.sessionId)) return false;
		if (!userId.equals(that.userId)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = sessionId.hashCode();
		result = 31 * result + userId.hashCode();
		result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
		result = 31 * result + (remove ? 1 : 0);
		return result;
	}
}
