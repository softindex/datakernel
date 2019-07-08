package io.datakernel.http;

import org.jetbrains.annotations.NotNull;

/**
 * A blocking version of {@link AsyncServlet}.
 */
public interface BlockingServlet {
	@NotNull
	HttpResponse serve(@NotNull HttpRequest request) throws Exception;
}
