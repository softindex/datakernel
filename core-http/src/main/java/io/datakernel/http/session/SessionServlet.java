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
	private final AsyncServlet publicServlet;
	private final AsyncServlet privateServlet;

	private SessionServlet(SessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor, AsyncServlet publicServlet, AsyncServlet privateServlet) {
		this.store = store;
		this.sessionIdExtractor = sessionIdExtractor;
		this.publicServlet = publicServlet;
		this.privateServlet = privateServlet;
	}

	public static <T> SessionServlet<T> create(SessionStore<T> store, String sessionIdCookie,
			AsyncServlet publicServlet,
			AsyncServlet privateServlet) {
		return new SessionServlet<>(store, request -> request.getCookie(sessionIdCookie), publicServlet, privateServlet);
	}

	public static <T> SessionServlet<T> create(SessionStore<T> store, Function<HttpRequest, String> sessionIdExtractor,
			AsyncServlet publicServlet,
			AsyncServlet privateServlet) {
		return new SessionServlet<>(store, sessionIdExtractor, publicServlet, privateServlet);
	}

	@Override
	public @NotNull Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		String id = sessionIdExtractor.apply(request);

		if (id == null) {
			return publicServlet.serve(request);
		}

		return store.get(id)
				.then(sessionObject -> {
					if (sessionObject != null) {
						request.attach(sessionObject);
						return privateServlet.serve(request);
					} else {
						return publicServlet.serve(request);
					}
				});
	}
}
