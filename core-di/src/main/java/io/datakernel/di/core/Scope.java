package io.datakernel.di.core;

import io.datakernel.di.annotation.ScopeAnnotation;
import io.datakernel.di.util.AnnotationTag;
import org.jetbrains.annotations.NotNull;

import java.lang.annotation.Annotation;

import static io.datakernel.di.util.Utils.checkArgument;

/**
 * Scopes work with annotations in just the same way {@link Name Names} work with them.
 * @see Name
 */
public final class Scope extends AnnotationTag {
	public static final Scope[] UNSCOPED = new Scope[0];

	public Scope(@NotNull Class<? extends Annotation> annotationType) {
		super(annotationType);
	}

	public Scope(@NotNull Annotation annotation) {
		super(annotation);
	}

	/**
	 * Creates a Scope from a marker (or stateless) annotation, only identified by its class.
	 */
	public static Scope of(Class<? extends Annotation> annotationType) {
		checkArgument(annotationType.isAnnotationPresent(ScopeAnnotation.class), "Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return new Scope(annotationType);
	}

	/**
	 * Creates a Scope from a real (or its custom surrogate impl) annotation instance.
	 */
	public static Scope of(Annotation annotation) {
		checkArgument(annotation.annotationType().isAnnotationPresent(ScopeAnnotation.class),
				"Only annotations annotated with @ScopeAnnotation meta-annotation are allowed");
		return new Scope(annotation);
	}
}
