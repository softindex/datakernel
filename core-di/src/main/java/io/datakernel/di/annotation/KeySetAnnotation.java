package io.datakernel.di.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a marker meta-annotation that declares an annotation as the one that
 * can be used as a key set {@link io.datakernel.di.core.Name name}.
 */
@Target(ANNOTATION_TYPE)
@Retention(RUNTIME)
public @interface KeySetAnnotation {
}
