package io.datakernel.di.core;

/**
 * A provider, unlike other DI frameworks, is just a version of {@link Injector#getInstance} with a baked in key.
 * If you need a function that returns a new object each time then you are looking for {@link InstanceFactory}.
 * <p>
 * The main reason for its existence is that a binding for it is that it has a {@link io.datakernel.di.module.DefaultModule default generator}
 * for its binding so it can be fluently requested by {@link io.datakernel.di.annotation.Provides provider methods} etc.
 */
public interface InstanceProvider<T> {
	Key<T> key();

	T get();
}
