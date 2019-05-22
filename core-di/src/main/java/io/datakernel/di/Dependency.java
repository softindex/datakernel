package io.datakernel.di;

import java.util.Objects;

public final class Dependency {
	private final Key<?> key;
	private final boolean required;
	private final boolean postponed;

	public Dependency(Key<?> key, boolean required, boolean postponed) {
		this.key = key;
		this.required = required;
		this.postponed = postponed;
	}

	public Key<?> getKey() {
		return key;
	}

	public boolean isRequired() {
		return required;
	}

	public boolean isPostponed() {
		return postponed;
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
		return key.getDisplayString() + (required ? "" : "(required=false)") + (postponed ? "(postponed=true)" : "");
	}

	@Override
	public String toString() {
		return "Dependency{key=" + key + ", required=" + required + ", postponed=" + postponed + '}';
	}
}
