package io.datakernel.di.binding;

import io.datakernel.di.Key;

import java.util.Objects;

/**
 * A simple POJO that combines a {@link Key} with a boolean of is it required or not.
 *
 * @see Binding
 */
public final class Dependency {
	private final Key<?> key;
	private final boolean required;
	private final boolean implicit;

	private Dependency(Key<?> key, boolean required, boolean implicit) {
		this.key = key;
		this.required = required;
		this.implicit = implicit;
	}

	public static Dependency toKey(Key<?> key) {
		return new Dependency(key, true, false);
	}

	public static Dependency toKey(Key<?> key, boolean required) {
		return new Dependency(key, required, false);
	}

	public static Dependency toOptionalKey(Key<?> key) {
		return new Dependency(key, false, false);
	}

	/**
	 * Implicit dependencies do not cause cycle-check errors and are drawn in gray in debug graphviz output.
	 * Such dependencies <b>SHOULD NOT</b> be instantiated since they may cause various cycle-related errors,
	 * such infinite recursion.
	 * <p>
	 * They are used to describe some logical dependency that may or may not be cyclic
	 */
	public static Dependency implicit(Key<?> key, boolean required) {
		return new Dependency(key, required, true);
	}

	public Key<?> getKey() {
		return key;
	}

	public boolean isRequired() {
		return required;
	}

	public boolean isImplicit() {
		return implicit;
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
		return (required ? "" : "optional ") + key;
	}
}
