package io.datakernel.di.error;

import io.datakernel.di.Key;

public final class NoBindingsForKey extends IllegalStateException {
	private final Key<?> key;

	public NoBindingsForKey(Key<?> key) {
		super("provided key " + key + " with no associated bindings");
		this.key = key;
	}

	public Key<?> getKey() {
		return key;
	}
}
