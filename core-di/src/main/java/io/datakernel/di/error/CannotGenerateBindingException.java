package io.datakernel.di.error;

import io.datakernel.di.core.Key;

public final class CannotGenerateBindingException extends IllegalStateException {

	private final Key<?> key;

	public CannotGenerateBindingException(Key<?> key, String message) {
		super(message + " for key " + key.getDisplayString());
		this.key = key;
	}

	public Key<?> getKey() {
		return key;
	}
}
