package io.datakernel.http.session;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Extremely simple reference implementation of the session storage over a hash map.
 */
public final class SessionStoreInMemory<T> implements SessionStore<T> {
	private final Map<String, T> store = new HashMap<>();
	private final Duration sessionLifetime;

	public SessionStoreInMemory(Duration lifetime) {
		sessionLifetime = lifetime;
	}

	@Override
	public Promise<Void> save(String sessionId, T sessionObject) {
		store.put(sessionId, sessionObject);
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable T> get(String sessionId) {
		return Promise.of(store.get(sessionId));
	}

	@Override
	public Promise<Void> remove(String sessionId) {
		store.remove(sessionId);
		return Promise.complete();
	}

	@Override
	public Duration getSessionLifetime() {
		return sessionLifetime;
	}
}
