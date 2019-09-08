package io.datakernel.http;

import io.datakernel.common.exception.UncheckedException;
import io.datakernel.promise.Promise;
import org.jetbrains.annotations.NotNull;

import java.util.Base64;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static io.datakernel.http.ContentTypes.PLAIN_TEXT_UTF_8;
import static io.datakernel.http.HttpHeaders.AUTHORIZATION;
import static io.datakernel.http.HttpHeaders.CONTENT_TYPE;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This is a simple reference implementation of the HTTP Basic Auth protocol.
 * <p>
 * It operates over some servlet that it restricts access to and the async predicate for the credentials.
 * <p>
 * Also the credentials are {@link HttpRequest#attach attached} to the request so that the private servlet
 * could then receive and use it.
 */
public final class BasicAuth implements AsyncServlet {

	public static final BiPredicate<String, String> SILLY = (login, pass) -> true;

	private static final String PREFIX = "Basic ";
	private static final Base64.Decoder DECODER = Base64.getDecoder();

	private final AsyncServlet next;
	private final String challenge;
	private final BiFunction<String, String, Promise<Boolean>> credentialsLookup;

	private Function<HttpResponse, HttpResponse> failureResponse =
			request -> request
					.withHeader(CONTENT_TYPE, HttpHeaderValue.ofContentType(PLAIN_TEXT_UTF_8))
					.withBody("Authentification is required".getBytes(UTF_8));

	public BasicAuth(AsyncServlet next, String realm, BiFunction<String, String, Promise<Boolean>> credentialsLookup) {
		this.next = next;
		this.credentialsLookup = credentialsLookup;

		challenge = PREFIX + "realm=\"" + realm + "\", charset=\"UTF-8\"";
	}

	public BasicAuth withFailureResponse(Function<HttpResponse, HttpResponse> failureResponse) {
		this.failureResponse = failureResponse;
		return this;
	}

	public static AsyncServletDecorator decorator(String realm, BiFunction<String, String, Promise<Boolean>> credentialsLookup) {
		return next -> new BasicAuth(next, realm, credentialsLookup);
	}

	public static Function<AsyncServlet, AsyncServlet> decorator(String realm,
			BiFunction<String, String, Promise<Boolean>> credentialsLookup,
			Function<HttpResponse, HttpResponse> failureResponse) {
		return next -> new BasicAuth(next, realm, credentialsLookup)
				.withFailureResponse(failureResponse);
	}

	@NotNull
	@Override
	public Promise<HttpResponse> serve(@NotNull HttpRequest request) throws UncheckedException {
		String header = request.getHeader(AUTHORIZATION);
		if (header == null || !header.startsWith(PREFIX)) {
			return Promise.of(failureResponse.apply(HttpResponse.unauthorized401(challenge)));
		}
		byte[] raw;
		try {
			raw = DECODER.decode(header.substring(PREFIX.length()));
		} catch (IllegalArgumentException e) {
			// all the messages in decode method's illegal argument exception are informative enough
			return Promise.ofException(HttpException.ofCode(400,"Base64: " + e.getMessage()));
		}
		String[] authData = new String(raw, UTF_8).split(":", 2);
		if (authData.length != 2) {
			return Promise.ofException(HttpException.ofCode(400, "No ':' separator"));
		}
		return credentialsLookup.apply(authData[0], authData[1])
				.then(result -> {
					if (result) {
						request.attach(new BasicAuthCredentials(authData[0], authData[1]));
						return next.serve(request);
					}
					return Promise.of(failureResponse.apply(HttpResponse.unauthorized401(challenge)));
				});
	}

	public static final class BasicAuthCredentials {
		private String username;
		private String password;

		public BasicAuthCredentials(String username, String password) {
			this.username = username;
			this.password = password;
		}

		public String getUsername() {
			return username;
		}

		public String getPassword() {
			return password;
		}
	}

	public static BiPredicate<String, String> lookupFrom(Map<String, String> credentials) {
		return (login, pass) -> pass.equals(credentials.get(login));
	}
}
