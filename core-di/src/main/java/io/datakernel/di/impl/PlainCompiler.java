package io.datakernel.di.impl;

import io.datakernel.di.core.Key;

import java.util.concurrent.atomic.AtomicReferenceArray;

public final class PlainCompiler<R> implements BindingCompiler<R> {
	private final Key<? extends R> to;

	public PlainCompiler(Key<? extends R> to) {
		this.to = to;
	}

	public Key<? extends R> getDestination() {
		return to;
	}

	@Override
	public CompiledBinding<R> compile(CompiledBindingLocator compiledBindings, boolean threadsafe, int scope, int index) {
		return new CompiledBinding<R>() {
			final CompiledBinding<? extends R> compiledBinding = compiledBindings.get(to);

			@SuppressWarnings("unchecked")
			@Override
			public R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
				R instance = compiledBinding.getInstance(scopedInstances, synchronizedScope);
				scopedInstances[scope].lazySet(index, instance);
				return instance;
			}

			@Override
			public R createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
				return compiledBinding.createInstance(scopedInstances, synchronizedScope);
			}
		};
	}
}
