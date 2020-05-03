package io.datakernel.di.annotation;

import io.datakernel.di.Injector;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A binding which is transient has no slot in object cache. That is - it works more like traditional DI's,
 * creating new instances upon each {@link Injector#getInstance getInstance} call.
 * <p>
 * Bindings cannot be both transient and {@link Eager eager} at the same time.
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Transient {
}
