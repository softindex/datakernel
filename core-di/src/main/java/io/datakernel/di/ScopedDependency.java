package io.datakernel.di;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;

public final class ScopedDependency {
	@Nullable
	private final Scope scope;
	@NotNull
	private final Dependency dependency;

	private ScopedDependency(@Nullable Scope scope, @NotNull Dependency dependency) {
		this.scope = scope;
		this.dependency = dependency;
	}

	public static ScopedDependency ofUnscoped(@NotNull Dependency key) {
		return new ScopedDependency(null, key);
	}

	public static ScopedDependency ofScoped(@NotNull Scope scope, @NotNull Dependency key) {
		return new ScopedDependency(scope, key);
	}

	@Nullable
	public Scope getScope() {
		return scope;
	}

	@NotNull
	public Dependency getDependency() {
		return dependency;
	}

	public boolean isScoped() {
		return scope != null;
	}

	public boolean isUnscoped() {
		return scope == null;
	}

	public String getDisplayString() {
		return (scope != null ? scope.getDisplayString() + " " : "") + dependency.getDisplayString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ScopedDependency other = (ScopedDependency) o;
		return Objects.equals(scope, other.scope) && dependency.equals(other.dependency);

	}

	@Override
	public int hashCode() {
		return 31 * (scope != null ? scope.hashCode() : 0) + dependency.hashCode();
	}

	@Override
	public String toString() {
		return "ScopedDependency{scope=" + scope + ", dependency=" + dependency + '}';
	}
}
