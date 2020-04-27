package io.datakernel.di.annotation;

import io.datakernel.di.core.Qualifier;
import io.datakernel.di.module.AbstractModule;
import io.datakernel.di.module.ModuleBuilder;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is part of the provider method DSL, it allows you to build bindings and even a subset of
 * {@link io.datakernel.di.core.BindingGenerator generators} using methods declared in your modules.
 * <p>
 * Method return type and method {@link Qualifier qualifier annotation} form a {@link io.datakernel.di.core.Key key}
 * that the resulting binding is bound to, its parameter types and their {@link Qualifier qualifier annotations} form
 * binding dependencies and its body forms the factory for the binding.
 * <p>
 * Note that provider methods are called using reflection, so if you need the best performance
 * for some frequently-entered scopes consider using less declarative but reflection-free
 * {@link ModuleBuilder#bind(io.datakernel.di.core.Key)}  binding DSL}
 *
 * @see AbstractModule
 */
@Target(METHOD)
@Retention(RUNTIME)
public @interface Provides {
}
