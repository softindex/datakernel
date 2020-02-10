package io.datakernel.di.util;

import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.datakernel.di.core.Scope.UNSCOPED;
import static io.datakernel.di.util.Utils.getScopeDisplayString;

/**
 * This is a simple generic POJO (or POGJO) for some object with associated scope path.
 * <p>
 * It is generic only because it is used as both ScopedValue&lt;Key&lt;?&gt;&gt;, and ScopedValue&lt;Dependency&gt;.
 */
public final class ScopedValue<T> {
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
		return scope.length != 0;
	}

	public boolean isUnscoped() {
		return scope.length == 0;
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
		return getScopeDisplayString(scope) + " " + value.toString();
	}
}
