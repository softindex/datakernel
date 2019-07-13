package io.datakernel.di.core;

/**
 * A runtime exception that is thrown on startup when some of the static conditions fail
 * (missing or cyclic dependencies, incorrect annotations etc.) or in runtime when
 * you ask an {@link Injector} for an instance it does not have a {@link Binding binding} for.
 */
public final class DIException extends RuntimeException {
	public DIException(String message) {
		super(message);
	}

	public DIException(String message, Throwable cause) {
		super(message, cause);
	}
}
