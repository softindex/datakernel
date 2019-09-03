package io.global.forum.ot.session.operation;

import io.global.forum.ot.session.UserIdSession;

import java.util.Map;

public interface SessionOperation {
	void apply(Map<String, UserIdSession> sessions);

	String getSessionId();
}
