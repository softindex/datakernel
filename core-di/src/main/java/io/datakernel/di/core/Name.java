package io.datakernel.di.core;

import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.util.AbstractAnnotation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.Utils.checkArgument;

/**
 * This class is used as an additional tag in to distinguish different {@link Key keys} that have same type.
 * <p>
 * Since annotations could be placed almost in every place a type could occur in Java language, they are used as names
 * and this class is merely a wrapper around them.
 * <p>
 * Creating a custom stateless name annotation is as easy as creating your own annotation with no parameters and then using the
 * {@link #of Name.of} constructor on it.
 * <p>
 * If you want to create a stateful annotation, you need to get an instance of it with compatible equals method
 * (you can use {@link NamedImpl} as an example) and then call {@link #of Name.of} constructor on it too.
 * After that, you can use your annotation in our DSL's and then make keys programmaticaly with created Name instances.
 */
public final class Name extends AbstractAnnotation {
	protected Name(@NotNull Class<? extends Annotation> annotationType, @Nullable Annotation annotation) {
		super(annotationType, annotation);
	}

	/**
	 * Creates a Name from a marker (or stateless) annotation, only identified by its class.
	 */
	public static Name of(Class<? extends Annotation> annotationType) {
		checkArgument(isMarker(annotationType), "Name by annotation type only accepts marker annotations with no arguments");
		checkArgument(annotationType.isAnnotationPresent(NameAnnotation.class) || annotationType.isAnnotationPresent(KeySetAnnotation.class),
				"Only annotations annotated with @NameAnnotation or @KeySetAnnotation meta-annotations are allowed");
		return new Name(annotationType, null);
	}

	/**
	 * Creates a Name from a real (or its custom surrogate impl) annotation instance.
	 */
	public static Name of(Annotation annotation) {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		checkArgument(annotationType.isAnnotationPresent(NameAnnotation.class) || annotationType.isAnnotationPresent(KeySetAnnotation.class),
				"Only annotations annotated with @NameAnnotation or @KeySetAnnotation meta-annotations are allowed");
		return isMarker(annotationType) ?
				new Name(annotationType, null) :
				new Name(annotationType, annotation);
	}

	/**
	 * A shortcut for creating names based on {@link Named} built-in annotation.
	 */
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
			return value;
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
