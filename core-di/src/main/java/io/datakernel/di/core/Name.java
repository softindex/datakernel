package io.datakernel.di.core;

import io.datakernel.di.annotation.KeySetAnnotation;
import io.datakernel.di.annotation.NameAnnotation;
import io.datakernel.di.annotation.Named;
import io.datakernel.di.util.AnnotationTag;
import io.datakernel.di.util.ReflectionUtils;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.Utils.checkArgument;
import static java.util.Collections.singletonMap;

/**
 * This class is used as an additional tag in to distinguish different {@link Key keys} that have same type.
 * <p>
 * Since annotations could be placed almost in every place a type could occur in Java language, they are used as names
 * and this class is merely a wrapper around them.
 * <p>
 * Creating a custom stateless name annotation is as easy as creating your own annotation with no parameters and then using the
 * {@link #of Name.of} constructor on it.
 * <p>
 * If you want to create a stateful annotation, you need to get an instance of it with compatible equals method.
 * You can either implement it yourself or create one using our {@link ReflectionUtils#createAnnotationInstance proxying} mechanism.
 * Then you can call {@link #of Name.of} constructor on it too.
 * After that, you can use your annotation in our DSL's and then make keys programmatically with created Name instances.
 */
public final class Name extends AnnotationTag {
	private Name(Class<? extends Annotation> annotationType) {
		super(annotationType);
	}

	private Name(Annotation annotation) {
		super(annotation);
	}

	/**
	 * Creates a Name from a marker (or stateless) annotation, only identified by its class.
	 */
	public static Name of(Class<? extends Annotation> annotationType) {
		checkArgument(annotationType.isAnnotationPresent(NameAnnotation.class) || annotationType.isAnnotationPresent(KeySetAnnotation.class),
				"Only annotations annotated with @NameAnnotation or @KeySetAnnotation meta-annotations are allowed");
		return new Name(annotationType);
	}

	/**
	 * Creates a Name from a real (or its custom surrogate impl) annotation instance.
	 */
	public static Name of(Annotation annotation) {
		Class<? extends Annotation> annotationType = annotation.annotationType();
		checkArgument(annotationType.isAnnotationPresent(NameAnnotation.class) || annotationType.isAnnotationPresent(KeySetAnnotation.class),
				"Only annotations annotated with @NameAnnotation or @KeySetAnnotation meta-annotations are allowed");
		return new Name(annotation);
	}

	/**
	 * A shortcut for creating names based on {@link Named} built-in annotation.
	 */
	public static Name of(String name) {
		return new Name(ReflectionUtils.createAnnotationInstance(Named.class, singletonMap("value", name)));
	}
}
