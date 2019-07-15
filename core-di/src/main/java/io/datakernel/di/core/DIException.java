package io.datakernel.di.core;

import org.jetbrains.annotations.Nullable;

/**
 * A runtime exception that is thrown on startup when some of the static conditions fail
 * (missing or cyclic dependencies, incorrect annotations etc.) or in runtime when
 * you ask an {@link Injector} for an instance it does not have a {@link Binding binding} for.
 */
public final class DIException extends RuntimeException {
	public static DIException cannotConstruct(Key<?> key, @Nullable Binding<?> binding) {
		return new DIException((binding != null ? "Binding refused to" : "No binding to") + " construct an instance for key " +
				key.getDisplayString() + (binding != null && binding.getLocation() != null ? ("\n\t at" + binding.getLocation()) : ""));
	}

	public DIException(String message) {
		super(message);
	}

	public DIException(String message, Throwable cause) {
		super(message, cause);
	}
}
