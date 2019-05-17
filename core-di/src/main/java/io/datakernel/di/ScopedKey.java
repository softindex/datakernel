package io.datakernel.di;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class ScopedKey<T> {
	@Nullable
	private final Scope scope;
	@NotNull
	private final Key<T> key;

	private ScopedKey(@Nullable Scope scope, @NotNull Key<T> key) {
		this.scope = scope;
		this.key = key;
	}

	public static <T> ScopedKey<T> ofUnscoped(@NotNull Key<T> key) {
		return new ScopedKey<>(null, key);
	}

	public static <T> ScopedKey<T> ofScoped(@NotNull Scope scope, @NotNull Key<T> key) {
		return new ScopedKey<>(scope, key);
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	@NotNull
	public Key<T> getKey() {
		return key;
	}

	public boolean isScoped() {
		return scope != null;
	}

	public boolean isUnscoped() {
		return scope == null;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ScopedKey<?> other = (ScopedKey<?>) o;
		return Objects.equals(scope, other.scope) && key.equals(other.key);

	}

	@Override
	public int hashCode() {
		int result = scope != null ? scope.hashCode() : 0;
		result = 31 * result + key.hashCode();
		return result;
	}
}
