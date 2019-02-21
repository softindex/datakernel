package io.datakernel.http;

import org.jetbrains.annotations.NotNull;

@FunctionalInterface
public interface HttpRendered {

	@NotNull
	HttpResponse render();
}
