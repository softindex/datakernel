package io.datakernel.http.session;

import io.datakernel.async.Promise;

public interface SessionStore<T> {
	Promise<Void> save(String sessionId, T sessionObject);

	Promise<T> get(String sessionId);
}
