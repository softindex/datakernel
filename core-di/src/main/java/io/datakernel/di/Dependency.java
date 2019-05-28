package io.datakernel.di;

import java.util.Objects;

public final class Dependency {
	private final Key<?> key;
	private final boolean required;

	public Dependency(Key<?> key, boolean required) {
		this.key = key;
		this.required = required;
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
