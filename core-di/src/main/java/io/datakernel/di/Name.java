package io.datakernel.di;

import io.datakernel.di.util.AbstractAnnotation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.util.Preconditions.checkArgument;

public final class Name extends AbstractAnnotation {
	protected Name(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		super(annotationType, annotation);
	}

	public static Name of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType));
		checkArgument(annotationType.getAnnotation(NameAnnotation.class) != null);
		return new Name(annotationType, null);
	}

	public static Name of(Annotation annotation) {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		checkArgument(annotationType.getAnnotation(NameAnnotation.class) != null);
		return isMarker(annotationType) ?
				new Name(annotationType, null) :
				new Name(annotationType, annotation);
	}

	public static Name of(String name) {
		return new Name(Named.class, new NamedImpl(name));
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static class NamedImpl implements Named {
		@NotNull
		private final String value;

		NamedImpl(@NotNull String value) {
			this.value = value;
		}

		@NotNull
		@Override
		public String value() {
			return this.value;
		}

		@Override
		public int hashCode() {
			// This is specified in java.lang.Annotation.
			return (127 * "value".hashCode()) ^ value.hashCode();
		}

		@Override
		public boolean equals(Object o) {
			if (!(o instanceof Named)) return false;

			Named other = (Named) o;
			return value.equals(other.value());
		}

		@NotNull
		@Override
		public String toString() {
			return "@" + Named.class.getName() + "(" + value + ")";
		}

		@NotNull
		@Override
		public Class<? extends Annotation> annotationType() {
			return Named.class;
		}
	}
}
