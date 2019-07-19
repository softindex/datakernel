package io.datakernel.http.session;

import io.datakernel.async.Promise;
import io.datakernel.exception.UncheckedException;
import io.datakernel.http.AsyncServlet;
import io.datakernel.http.HttpRequest;
import io.datakernel.http.HttpResponse;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;

/**
 * This is a simple abstract reference implementation of a concept known as HTTP sessions.
 * It operates over some session storage, session ids that are somehow (usually through cookies)
 * encoded in the requests and two other servlets one for when the session object is present
 * and one when its not - the latter one usually redirects to the main or login pages or something.
 * <p>
 * The session object is {@link HttpRequest#attach attached} to the request so that the first servlet
 * could then receive and use it.
 */
public final class SessionServlet<T> implements AsyncServlet {
	public static final String ATTACHMENT_KEY = "session";

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
						request.attach(ATTACHMENT_KEY, sessionObject);
						return privateServlet.serve(request);
					} else {
						return publicServlet.serve(request);
					}
				});
	}
}
