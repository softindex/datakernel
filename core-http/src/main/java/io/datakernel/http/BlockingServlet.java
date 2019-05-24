package io.datakernel.http;

import org.jetbrains.annotations.NotNull;

public interface BlockingServlet {
	@NotNull
	HttpResponse serve(@NotNull HttpRequest request);
}
