package io.datakernel.di.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is used along with {@link Provides provider method} DSL and {@link Inject injection} DSL
 * for marking binding dependencies as optional.
 * <p>
 * The values of {@link Inject inject fields} will be not touched and parameters of {@link Provides provider methods}
 * are set to null.
 */
@Target({FIELD, PARAMETER})
@Retention(RUNTIME)
public @interface Optional {
}
