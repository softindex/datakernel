package io.datakernel.di;

import io.datakernel.di.util.AbstractAnnotation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

public final class Scope extends AbstractAnnotation {
	protected Scope(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		super(annotationType, annotation);
	}

	public static Scope of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType));
		checkArgument(annotationType.getAnnotation(ScopeAnnotation.class) != null);
		return new Scope(annotationType, null);
	}

	public static Scope of(Annotation annotation) {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		checkArgument(annotationType.getAnnotation(ScopeAnnotation.class) != null);
		return isMarker(annotationType) ?
				new Scope(annotationType, null) :
				new Scope(annotationType, annotation);
	}

	public static Scope of(String scope) {
		return new Scope(Scoped.class, new Scope.ScopedImpl(scope));
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static class ScopedImpl implements Scoped {
		@NotNull
		private final String value;

		ScopedImpl(@NotNull String value) {
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
			if (!(o instanceof Scoped)) return false;

			Scoped other = (Scoped) o;
			return value.equals(other.value());
		}

		@NotNull
		@Override
		public String toString() {
			return "@" + Scoped.class.getName() + "(" + value + ")";
		}

		@NotNull
		@Override
		public Class<? extends Annotation> annotationType() {
			return Scoped.class;
		}
	}

}
