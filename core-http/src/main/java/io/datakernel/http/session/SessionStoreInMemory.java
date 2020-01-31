package io.datakernel.http.session;

import io.datakernel.common.time.CurrentTimeProvider;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * A simple reference implementation of the session storage over a hash map.
 */
public final class SessionStoreInMemory<T> implements SessionStore<T> {
	private final Map<String, TWithTimestamp> store = new HashMap<>();

	@Nullable
	private Duration sessionLifetime;

	CurrentTimeProvider now = CurrentTimeProvider.ofSystem();

	private SessionStoreInMemory() {
	}

	public static <T> SessionStoreInMemory<T> create() {
		return new SessionStoreInMemory<>();
	}

	public SessionStoreInMemory<T> withLifetime(Duration sessionLifetime) {
		this.sessionLifetime = sessionLifetime;
		return this;
	}

	@Override
	public Promise<Void> save(String sessionId, T sessionObject) {
		store.put(sessionId, new TWithTimestamp(sessionObject, now.currentTimeMillis()));
		return Promise.complete();
	}

	@Override
	public Promise<@Nullable T> get(String sessionId) {
		long timestamp = now.currentTimeMillis();
		TWithTimestamp tWithTimestamp = store.get(sessionId);
		if (tWithTimestamp == null) {
			return Promise.of(null);
		}
		if (sessionLifetime != null && tWithTimestamp.timestamp + sessionLifetime.toMillis() < timestamp) {
			store.remove(sessionId);
			return Promise.of(null);
		}
		tWithTimestamp.timestamp = timestamp;
		return Promise.of(tWithTimestamp.value);
	}

	@Override
	public Promise<Void> remove(String sessionId) {
		store.remove(sessionId);
		return Promise.complete();
	}

	@Override
	@Nullable
	public Duration getSessionLifetimeHint() {
		return sessionLifetime;
	}

	private class TWithTimestamp {
		T value;
		long timestamp;

		public TWithTimestamp(T value, long timestamp) {
			this.value = value;
			this.timestamp = timestamp;
		}
	}
}
