package io.datakernel.di.annotation;

import io.datakernel.di.core.Multibinder;
import io.datakernel.di.module.AbstractModule;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This is a built-in shortcut for {@link Provides provider method} that provides its result as a singleton set
 * and adds a {@link Multibinder#toSet() set multibinder} for the provided set key to the module.
 *
 * @see AbstractModule
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface ProvidesIntoSet {
}
