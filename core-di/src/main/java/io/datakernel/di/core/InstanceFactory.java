package io.datakernel.di.core;

/**
 * Similar to other frameworks, a provider is a function that creates a new object each time it is called.
 * However, any of its dependencies are still fetched with {@link Injector#getInstance}, so if you want
 * to get a deep copy your bindings need to depend on instance factories themselves.
 * This is not a very good practice and almost out of scope of DataKernel DI which is highly singleton-centric.
 * <p>
 * You can use {@link Injector#getBinding} and then {@link Binding#getCompiler()} for doing so yourself, but
 * the main reason for its existence is that a binding for it is that it has a {@link io.datakernel.di.module.DefaultModule default generator}
 * for its binding so it can be fluently requested by {@link io.datakernel.di.annotation.Provides provider methods} etc.
 */
public interface InstanceFactory<T> {
	Key<T> key();

	T create();
}
