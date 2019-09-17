package io.datakernel.di.annotation;

import io.datakernel.di.core.Key;
import io.datakernel.di.util.Utils;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a helper annotation that can be used to override how this type is {@link Key#getDisplayString() displayed}
 * in error messages and {@link Utils#makeGraphVizGraph debug graphs}.
 * <p>
 * Packages and enclosing classes are stripped off for readability,
 * but if you have multiple types with the same name in different packages
 * you can override their display name by applying this annotation.
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface ShortTypeName {
	String value();
}

