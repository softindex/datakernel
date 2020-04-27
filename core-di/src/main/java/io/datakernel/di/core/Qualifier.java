package io.datakernel.di.core;

import io.datakernel.di.annotation.Named;
import io.datakernel.di.annotation.QualifierAnnotation;
import io.datakernel.di.module.UniqueQualifierImpl;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.AbstractAnnotation.isMarker;
import static io.datakernel.di.util.Utils.checkArgument;

/**
 * This class is used as an additional tag in to distinguish different {@link Key keys} that have same type.
 * <p>
 * Since annotations could be placed almost in every place a type could occur in Java language, they are used as names
 * and this class is merely a wrapper around them.
 * <p>
 * Creating a custom stateless name annotation is as easy as creating your own annotation with no parameters and then using the
 * {@link #named Qualifier.of} constructor on it.
 * <p>
 * If you want to create a stateful annotation, you need to get an instance of it with compatible equals method
 * (you can use {@link NamedImpl} as an example) and then call {@link #named Qualifier.of} constructor on it too.
 * After that, you can use your annotation in our DSL's and then make keys programmaticaly with created Name instances.
 */
public final class Qualifier {

	/**
	 * Validates a marker (or stateless) annotation, only identified by its class.
	 */
	public static Object validate(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Name by annotation type only accepts marker annotations with no arguments");
		checkArgument(annotationType.isAnnotationPresent(QualifierAnnotation.class),
				"Only annotations annotated with @QualifierAnnotation meta-annotation are allowed");
		return annotationType;
	}

	/**
	 * Validates a qualifier from a real (or its custom surrogate impl) annotation instance and returns a valid qualifier
	 */
	public static Object validate(Annotation annotation) {
		//noinspection unchecked
		Class<Annotation> annotationType = (Class<Annotation>) annotation.annotationType();
		checkArgument(annotationType.isAnnotationPresent(QualifierAnnotation.class),
				"Only annotations annotated with @QualifierAnnotation meta-annotation are allowed");
		return isMarker(annotationType) ?
				annotationType :
				annotation;
	}

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
