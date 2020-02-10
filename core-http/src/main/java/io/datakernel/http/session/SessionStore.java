package io.datakernel.http.session;

import io.datakernel.promise.Promise;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;

/**
 * Basic session storage for the {@link SessionServlet}.
 */
public interface SessionStore<T> {
	Promise<Void> save(String sessionId, T sessionObject);

	Promise<@Nullable T> get(String sessionId);

	Promise<Void> remove(String sessionId);

	@Nullable
	default Duration getSessionLifetimeHint() {
		return null;
	}
}
