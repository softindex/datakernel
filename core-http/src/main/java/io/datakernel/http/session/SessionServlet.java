package io.datakernel.http.session;

import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

public final class SessionServlet<T> implements AsyncServlet {
	private final SessionStore<T> store;
	private final Function<HttpRequest, String> sessionIdExtractor;
	private final AsyncServlet noSessionServlet;
	private final AsyncServlet withSessionServlet;

	private SessionServlet(SessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor, AsyncServlet noSessionServlet, AsyncServlet withSessionServlet) {
		this.store = store;
		this.sessionIdExtractor = sessionIdExtractor;
		this.noSessionServlet = noSessionServlet;
		this.withSessionServlet = withSessionServlet;
	}

	public static <T> SessionServlet<T> create(SessionStore<T> store, String sessionIdCookie,
			AsyncServlet noSessionServlet,
			AsyncServlet withSessionServlet) {
		return new SessionServlet<>(store, request -> request.getCookie(sessionIdCookie), noSessionServlet, withSessionServlet);
	}

	public static <T> SessionServlet<T> create(SessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor,
			AsyncServlet noSessionServlet,
			AsyncServlet withSessionServlet) {
		return new SessionServlet<>(store, sessionIdExtractor, noSessionServlet, withSessionServlet);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		String id = sessionIdExtractor.apply(request);

		if (id == null) {
			return noSessionServlet.serve(request);
		}

		return store.get(id)
				.then(sessionObject -> {
					if (sessionObject != null) {
						request.attach(sessionObject);
						return withSessionServlet.serve(request);
					} else {
						return noSessionServlet.serve(request);
					}
				});
	}
}
