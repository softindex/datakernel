package io.datakernel.di.impl;

import io.datakernel.di.core.Binding;
import io.datakernel.di.core.Key;
import org.jetbrains.annotations.Nullable;

/**
 * Only reason for this not to be an anonymous class as any other in {@link Binding}
 * is that Injector does not allocate a slot for this binding
 * despite the binding being cached (so that wrappers such as mapInstance are not non-cached
 * as it would've been if the plain binding was make non-cached)
 */
public final class PlainCompiler<T> implements BindingCompiler<T> {
	private final Key<? extends T> key;

	public PlainCompiler(Key<? extends T> key) {
		this.key = key;
	}

	public Key<? extends T> getKey() {
		return key;
	}

	@SuppressWarnings("unchecked")
	@Override
	public CompiledBinding<T> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, @Nullable Integer slot) {
		return (CompiledBinding<T>) compiledBindings.get(key);
	}
}
