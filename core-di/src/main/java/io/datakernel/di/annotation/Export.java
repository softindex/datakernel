package io.datakernel.di.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * If module provides at least one binding marked as exported then all non-exported bindings
 * will be visible only from bindings inside of the module.
 * Injector would only be able to retrieve the exported bindings.
 * <p>
 * If no bindings are exported then all of them are - this is made for backwards-compatibility and simplicity.
 * Module that has bindings but exports none of them is useless - binding generators only work with exported (or 'public') bindings.
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Export {
}
