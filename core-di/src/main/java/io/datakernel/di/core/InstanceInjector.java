package io.datakernel.di.core;

/**
 * This is a function which can inject instances into {@link io.datakernel.di.annotation.Inject}
 * fields and methods of some <b>already existing object</b>.
 * This is so-called 'post-injections' since such injections are not part of object creation.
 * <p>
 * It has a {@link io.datakernel.di.module.DefaultModule default generator} and
 * can only be obtained by depending on it and then requesting it from the {@link Injector injector}.
 */
public interface InstanceInjector<T> {
	Key<T> key();

	void injectInto(T existingInstance);
}
