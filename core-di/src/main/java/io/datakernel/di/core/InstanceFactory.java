package io.datakernel.di.core;

import io.datakernel.di.util.Constructors;

import java.util.function.Supplier;

/**
 * Similar to other frameworks, a provider is a function that creates a new object each time it is called.
 * However, any of its dependencies are still fetched with {@link Injector#getInstance}, so if you want
 * to get a deep copy your bindings need to depend on instance factories themselved.
 * This is not a very good practice and alsmost out of scope of DataKernel DI which is highly singleton-centric.
 * <p>
 * You can use {@link Injector#getBinding} and then {@link Binding#getFactory()} for doing so youself, but
 * the main reason for its existence is that a binding for it is that it has a {@link io.datakernel.di.module.DefaultModule default generator}
 * for its binding so it can be fluently requested by {@link io.datakernel.di.annotation.Provides provider methods} etc.
 */
public interface InstanceFactory<T> extends Supplier<T>, Constructors.Constructor0<T> {
	Key<T> key();

	T create();

	@Override
	default T get() {
		return create();
	}
}
