package io.datakernel.di.core;

import java.util.Objects;

/**
 * A simple POJO that combines a {@link Key} with a boolean of is it required or not.
 * @see Binding
 */
public final class Dependency {
	private final Key<?> key;
	private final boolean required;

	private Dependency(Key<?> key, boolean required) {
		this.key = key;
		this.required = required;
	}

	public static Dependency toKey(Key<?> key) {
		return new Dependency(key, true);
	}

	public static Dependency toKey(Key<?> key, boolean required) {
		return new Dependency(key, required);
	}

	public static Dependency toOptionalKey(Key<?> key) {
		return new Dependency(key, false);
	}

	public Key<?> getKey() {
		return key;
	}

	public boolean isRequired() {
		return required;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Dependency that = (Dependency) o;

		return required == that.required && Objects.equals(key, that.key);
	}

	@Override
	public int hashCode() {
		return 31 * (key != null ? key.hashCode() : 0) + (required ? 1 : 0);
	}

	public String getDisplayString() {
		return (required ? "" : "optional ") + key.getDisplayString();
	}

	@Override
	public String toString() {
		return "{" + (required ? "" : "optional ") + key + "}";
	}
}
