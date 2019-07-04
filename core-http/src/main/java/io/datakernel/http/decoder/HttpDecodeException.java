package io.datakernel.http.decoder;

import io.datakernel.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

public class HttpDecodeException extends StacklessException {
	@NotNull
	private final HttpDecodeErrors errors;

	public HttpDecodeException(@NotNull HttpDecodeErrors errors) {this.errors = errors;}

	@NotNull
	public HttpDecodeErrors getErrors() {
		return errors;
	}
}
