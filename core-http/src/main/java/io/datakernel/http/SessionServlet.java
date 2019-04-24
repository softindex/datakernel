package io.datakernel.http;

import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.session.SessionStore;
import org.jetbrains.annotations.NotNull;

public class SessionServlet<T> implements AsyncServlet {
	public final String sessionId;

	private final SessionStore<T> store;
	private final AsyncServlet withSessionServlet;
	private final AsyncServlet noSessionServlet;

	private SessionServlet(SessionStore<T> store, String sessionId, AsyncServlet noSessionServlet, AsyncServlet withSessionServlet) {
		this.store = store;
		this.sessionId = sessionId;
		this.noSessionServlet = noSessionServlet;
		this.withSessionServlet = withSessionServlet;
	}

	public static <T> SessionServlet<T> create(SessionStore<T> store,
											   String sessionId,
											   AsyncServlet noSessionServlet,
											   AsyncServlet withSessionServlet) {
		return new SessionServlet<>(store, sessionId, noSessionServlet, withSessionServlet);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(HttpRequest request) throws UncheckedException {
		String id = request.getCookieOrNull(sessionId);

		if (id == null) {
			return noSessionServlet.serve(request);
		}
		return store.get(id)
				.thenEx((sessionObj, e) -> {
					if (e == null) {
						request.attach(sessionObj);
						return withSessionServlet.serve(request);
					}
					return noSessionServlet.serve(request);
				});
	}
}
