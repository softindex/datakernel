package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractUnsyncCompiledBinding<R> implements CompiledBinding<R> {
	protected final int scope;
	protected final int index;

	protected AbstractUnsyncCompiledBinding(int scope, int index) {
		this.scope = scope;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		AtomicReferenceArray array = scopedInstances[scope];
		R instance = (R) array.get(index);
		if (instance != null) return instance;
		instance = doCreateInstance(scopedInstances, synchronizedScope);
		array.lazySet(index, instance);
		return instance;
	}

	@Override
	public final R createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		return doCreateInstance(scopedInstances, synchronizedScope);
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);
}
