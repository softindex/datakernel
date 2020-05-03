package io.datakernel.di;

import io.datakernel.di.annotation.ScopeAnnotation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.Utils.checkArgument;
import static io.datakernel.di.util.Utils.isMarker;

public final class Scope {
	public static final Scope[] UNSCOPED = new Scope[0];

	@NotNull
	private final Class<? extends Annotation> annotationType;
	private final boolean threadsafe;

	private Scope(@NotNull Class<? extends Annotation> annotationType, boolean threadsafe) {
		this.annotationType = annotationType;
		this.threadsafe = threadsafe;
	}

	/**
	 * Creates a Scope from a marker (or stateless) annotation, only identified by its class.
	 */
	public static Scope of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Scope by annotation type only accepts marker annotations with no arguments");
		ScopeAnnotation scopeAnnotation = annotationType.getAnnotation(ScopeAnnotation.class);
		checkArgument(scopeAnnotation != null, "Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return new Scope(annotationType, scopeAnnotation.threadsafe());
	}

	/**
	 * Creates a Scope from a real (or its custom surrogate impl) annotation instance.
	 */
	public static Scope of(Annotation annotation) {
		return of(annotation.annotationType());
	}

	@NotNull
	public Class<? extends Annotation> getAnnotationType() {
		return annotationType;
	}

	public boolean isThreadsafe() {
		return threadsafe;
	}

	public String getDisplayString() {
		return annotationType.getSimpleName();
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Scope other = (Scope) o;
		return annotationType == other.annotationType;
	}

	@Override
	public int hashCode() {
		return 31 * annotationType.hashCode();
	}

	@Override
	public String toString() {
		return "@" + annotationType.getName() + "()";
	}
}
