package io.global.comm.ot.session.operation;

import io.global.comm.ot.session.UserIdSession;

import java.util.Map;

public interface SessionOperation {
	void apply(Map<String, UserIdSession> sessions);

	String getSessionId();
}
