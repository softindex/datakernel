package io.datakernel.di.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.Objects;

public abstract class AbstractAnnotation {
	@NotNull
	private final Class<? extends Annotation> annotationType;

	@Nullable
	private final Annotation annotation;

	protected AbstractAnnotation(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		this.annotationType = annotationType;
		this.annotation = annotation == null || isMarker(annotationType) ? null : annotation;
	}

	public static boolean isMarker(Class<? extends Annotation> annotationType) {
		return annotationType.getDeclaredMethods().length == 0;
	}

	@NotNull
	public Class<? extends Annotation> getAnnotationType() {
		return annotationType;
	}

	@Nullable
	public Annotation getAnnotation() {
		return annotation;
	}

	public String getDisplayString() {
		if (annotation != null) {
			return ReflectionUtils.getShortName(annotation.toString()).replace("value=", "");
		}
		return "@" + ReflectionUtils.getShortName(annotationType.getName()) + "()";
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AbstractAnnotation other = (AbstractAnnotation) o;
		return annotationType == other.annotationType &&
				Objects.equals(this.annotation, other.annotation);
	}

	@Override
	public int hashCode() {
		return Objects.hash(annotationType, annotation);
	}

	@Override
	public String toString() {
		return annotation != null ? annotation.toString() : "@" + annotationType.getName() + "()";
	}
}
