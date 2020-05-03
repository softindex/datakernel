package io.datakernel.di.annotation;

import io.datakernel.di.Scope;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a marker meta-annotation that declares an annotation as the one that
 * can be used as a {@link Scope scope}.
 */
@Retention(RUNTIME)
@Target(ANNOTATION_TYPE)
public @interface ScopeAnnotation {
	boolean threadsafe() default true;
}
