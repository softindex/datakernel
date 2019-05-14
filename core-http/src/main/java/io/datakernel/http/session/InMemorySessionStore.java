package io.datakernel.http.session;

import io.datakernel.async.Promise;

import java.util.HashMap;
import java.util.Map;

public class InMemorySessionStore<T> implements SessionStore<T> {
	private final Map<String, T> store = new HashMap<>();

	@Override
	public Promise<Void> save(String sessionId, T sessionObject) {
		store.put(sessionId, sessionObject);
		return Promise.complete();
	}

	@Override
	public Promise<T> get(String sessionId) {
		T sessionObject = store.get(sessionId);
		if (sessionObject == null) {
			return Promise.ofException(new IllegalArgumentException());
		}
		return Promise.of(sessionObject);
	}

	@Override
	public Promise<Long> size() {
		return Promise.of((long) store.size());
	}
}
