package io.datakernel.http.session;

import io.datakernel.async.Promise;
import org.jetbrains.annotations.Nullable;

/**
 * Basic session storage for the {@link SessionServlet}.
 */
public interface SessionStore<T> {
	Promise<Void> save(String sessionId, T sessionObject);

	Promise<@Nullable T> get(String sessionId);
}
