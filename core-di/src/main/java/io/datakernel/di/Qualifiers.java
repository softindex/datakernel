package io.datakernel.di;

import io.datakernel.di.annotation.Named;
import io.datakernel.di.module.UniqueQualifierImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

/**
 * This class holds utility methods used for validating and creating objects used as qualifiers.
 * Qualifiers serve as additional tags to distinguish different {@link Key keys} that have same type.
 * <p>
 */
public final class Qualifiers {
	/**
	 * A shortcut for creating qualifiers based on {@link Named} built-in annotation.
	 */
	public static Object named(String name) {
		return new NamedImpl(name);
	}

	public static Object uniqueQualifier() {
		return new UniqueQualifierImpl();
	}

	public static Object uniqueQualifier(@Nullable Object qualifier) {
		return qualifier instanceof UniqueQualifierImpl ? qualifier : new UniqueQualifierImpl(qualifier);
	}

	@SuppressWarnings("ClassExplicitlyAnnotation")
	private static final class NamedImpl implements Named {
		private static final int VALUE_HASHCODE = 127 * "value".hashCode();

		@NotNull
		private final String value;

		NamedImpl(@NotNull String value) {
			this.value = value;
		}

		@NotNull
		@Override
		public String value() {
			return value;
		}

		@Override
		public int hashCode() {
			// This is specified in java.lang.Annotation.
			return VALUE_HASHCODE ^ value.hashCode();
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
