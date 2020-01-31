package io.datakernel.di.core;

/**
 * A provider, unlike other DI frameworks, is just a version of {@link Injector#getInstance} with a baked in key.
 * If you need a function that returns a new object each time then you need to make your binding {@link Binding#transiently transient}.
 * <p>
 * The main reason for its existence is that it has a {@link io.datakernel.di.module.DefaultModule default generator}
 * for its binding, so it can be fluently requested by {@link io.datakernel.di.annotation.Provides provider methods} etc.
 * <p>
 * Also it can be used for lazy dependency cycle resolution.
 */
public interface InstanceProvider<T> {
	Key<T> key();

	T get();
}
