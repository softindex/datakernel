package io.datakernel.di.annotation;

import io.datakernel.di.core.Injector;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a builtin {@link KeySetAnnotation} that groups together keys
 * that will be requested (and their singletons created) on {@link Injector#createEagerSingletons()} call.
 */
@Retention(RUNTIME)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
@KeySetAnnotation
public @interface EagerSingleton {
}
