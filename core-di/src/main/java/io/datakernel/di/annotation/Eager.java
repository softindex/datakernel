package io.datakernel.di.annotation;

import io.datakernel.di.Injector;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * A binding marked as eager would be called for an instance of its object immediately upon injector creation.
 * Next calls if {@link Injector#getInstance getInstance} would only retrieve the cached instance.
 * <p>
 * Bindings cannot be both eager and {@link Transient transient} at the same time.
 */
@Target({FIELD, PARAMETER, METHOD})
@Retention(RUNTIME)
public @interface Eager {
}
