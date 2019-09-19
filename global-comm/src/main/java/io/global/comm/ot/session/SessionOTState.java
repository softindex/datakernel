package io.global.comm.ot.session;

import io.datakernel.ot.OTState;
import io.global.comm.ot.session.operation.SessionOperation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class SessionOTState implements OTState<SessionOperation> {
	private final Map<String, UserIdSession> sessions = new HashMap<>();

	@Override
	public void init() {
		sessions.clear();
	}

	@Override
	public void apply(SessionOperation op) {
		op.apply(sessions);
	}

	public Map<String, UserIdSession> getSessions() {
		return Collections.unmodifiableMap(sessions);
	}
}
