package io.datakernel.http.decoder;

import io.datakernel.common.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

/**
 * An exception that occurs when an invalid HTTP request is received and the decoder fails
 * to map it on needed type.
 */
public class DecodeException extends StacklessException {
	@NotNull
	private final DecodeErrors errors;

	public DecodeException(@NotNull DecodeErrors errors) {this.errors = errors;}

	@NotNull
	public DecodeErrors getErrors() {
		return errors;
	}
}
