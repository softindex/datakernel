package io.datakernel.di.annotation;

import io.datakernel.di.util.ReflectionUtils;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * This annotation is part of the injection DSL, it allows you to generate bindings
 * using object constructors or static factory methods.
 * By {@link io.datakernel.di.module.DefaultModule default} there is a {@link io.datakernel.di.core.BindingGenerator generator}
 * present, that can generate missing bindings for injectable classes.
 * <p>
 * This annotation can be put on the class itself - its default constructor is used for binding generation and must exist,
 * on class constructor that will be used, or on factory method (static method with return type of that class).
 * <p>
 * When a binding is generated, class methods and fields are scanned for the inject annotation and added as the binding dependencies -
 * on instance creation fields will be <i>injected</i> and methods will be called with their parameters <i>injected</i> and return ignored.
 * Name annotations on fields and method parameters will be considered.
 *
 * @see ReflectionUtils#generateImplicitBinding
 */
@Target({FIELD, CONSTRUCTOR, METHOD, TYPE})
@Retention(RUNTIME)
public @interface Inject {
}
