package io.datakernel.di.error;

import io.datakernel.di.core.Key;

public final class InvalidImplicitBindingException extends IllegalStateException {

	private final Key<?> key;

	public InvalidImplicitBindingException(Key<?> key, String message) {
		super(key.getDisplayString() + ", " + message);
		this.key = key;
	}

	public Key<?> getKey() {
		return key;
	}
}
