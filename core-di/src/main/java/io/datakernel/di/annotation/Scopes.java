package io.datakernel.di.annotation;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a special annotation that allows you to declare bindings in nested scopes
 * using {@link Provides provider methods}.
 * <p>
 * Note that it does not allow you to use stateful scope annotations.
 */
@Retention(RUNTIME)
@Target(ElementType.METHOD)
public @interface Scopes {
	Class<? extends Annotation>[] value();
}
