package io.datakernel.http.parser;

import io.datakernel.exception.StacklessException;
import org.jetbrains.annotations.NotNull;

public class HttpParamParseException extends StacklessException {
	@NotNull
	private final HttpParamParseErrorsTree errors;

	public HttpParamParseException(@NotNull HttpParamParseErrorsTree errors) {this.errors = errors;}

	@NotNull
	public HttpParamParseErrorsTree getErrors() {
		return errors;
	}
}
