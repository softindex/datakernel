package io.datakernel.di.core;

import org.jetbrains.annotations.Nullable;

import static io.datakernel.di.util.Utils.getScopeDisplayString;

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

	public static DIException noCachedBidning(Key<?> key, Scope[] scope) {
		throw new DIException("No cached binding was bound for key " + key.getDisplayString() + " in scope " + getScopeDisplayString(scope) + ". " +
				"Either bind it or check if a binding for such key exists with hasBinding() call.");
	}

	public DIException(String message) {
		super(message);
	}

	public DIException(String message, Throwable cause) {
		super(message, cause);
	}
}
