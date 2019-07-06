package io.datakernel.http.decoder;

import io.datakernel.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

/**
 * An exception that occurs when an invalid HTTP request is received and the decoder fails
 * to map it on needed type.
 */
public class HttpDecodeException extends StacklessException {
	@NotNull
	private final HttpDecodeErrors errors;

	public HttpDecodeException(@NotNull HttpDecodeErrors errors) {this.errors = errors;}

	@NotNull
	public HttpDecodeErrors getErrors() {
		return errors;
	}
}
