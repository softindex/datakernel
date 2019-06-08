package io.datakernel.di.util;

import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

public final class ScopedValue<T> {
	public static final Scope[] UNSCOPED = new Scope[0];

	@NotNull
	private final Scope[] scope;
	private final T value;

	private ScopedValue(@NotNull Scope[] scope, T value) {
		this.scope = scope;
		this.value = value;
	}

	public static <T> ScopedValue<T> of(@NotNull T value) {
		return new ScopedValue<>(UNSCOPED, value);
	}

	public static <T> ScopedValue<T> of(@NotNull Scope scope, @NotNull T value) {
		return new ScopedValue<>(new Scope[]{scope}, value);
	}

	public static <T> ScopedValue<T> of(@NotNull Scope[] scope, @NotNull T value) {
		return new ScopedValue<>(scope.length != 0 ? scope : UNSCOPED, value);
	}

	@NotNull
	public Scope[] getScope() {

		return scope;
	}

	@NotNull
	public T get() {
		return value;
	}

	public boolean isScoped() {
		return scope != UNSCOPED;
	}

	public boolean isUnscoped() {
		return scope == UNSCOPED;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ScopedValue other = (ScopedValue) o;
		return Arrays.equals(scope, other.scope) && value.equals(other.value);

	}

	@Override
	public int hashCode() {
		return 31 * Arrays.hashCode(scope) + value.hashCode();
	}

	@Override
	public String toString() {
		return Stream.of(scope)
				.map(s -> "->" + s)
				.collect(joining("", "()", " " + value.toString()));
	}
}
