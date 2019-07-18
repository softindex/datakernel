package io.datakernel.di.util;

import io.datakernel.di.core.Name;
import io.datakernel.di.core.Scope;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.util.Objects;

import static io.datakernel.di.util.Utils.checkArgument;

/**
 * This is a helper class for making tag-like objects that are identified by stateless or stateful annotations.
 * <p>
 * You don't need to extend it yourself.
 *
 * @see Name
 * @see Scope
 */
public abstract class AbstractAnnotation {
	@NotNull
	private final Class<? extends Annotation> annotationType;
	@NotNull
	private final Annotation annotation;

	protected AbstractAnnotation(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		this.annotationType = annotationType;
		this.annotation = annotation == null || isMarker(annotationType) ? new MarkerAnnotationImpl(annotationType) : annotation;
	}

	public static boolean isMarker(Class<? extends Annotation> annotationType) {
		return annotationType.getDeclaredMethods().length == 0;
	}

	@NotNull
	public Class<? extends Annotation> getAnnotationType() {
		return annotationType;
	}

	@NotNull
	public Annotation getAnnotation() {
		return annotation;
	}

	public boolean isMarkedBy(Class<? extends Annotation> metaAnnotation) {
		return annotationType.isAnnotationPresent(metaAnnotation);
	}

	public String getDisplayString() {
		return ReflectionUtils.getShortName(annotation.toString());
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
		return annotation.toString();
	}

	public static Annotation createMarkerAnnotationInstance(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Given annotation type does not correspond to a marker annotation");
		return new MarkerAnnotationImpl(annotationType);
	}

	private static class MarkerAnnotationImpl implements Annotation {
		private final Class<? extends Annotation> annotationType;

		private MarkerAnnotationImpl(Class<? extends Annotation> annotationType) {
			this.annotationType = annotationType;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Annotation)) return false;

			return ((Annotation) o).annotationType().equals(annotationType);
		}

		@NotNull
		@Override
		public String toString() {
			return "@" + annotationType.getName() + "()";
		}

		@NotNull
		@Override
		public Class<? extends Annotation> annotationType() {
			return annotationType;
		}
	}
}
