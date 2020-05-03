package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

@SuppressWarnings("rawtypes")
public abstract class AbstractRootCompiledBinding<R> implements CompiledBinding<R> {
	private volatile R instance;
	protected final int index;

	protected AbstractRootCompiledBinding(int index) {
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		R localInstance = instance;
		if (localInstance != null) return localInstance;
		synchronized (this) {
			localInstance = instance;
			if (localInstance != null) return localInstance;
			localInstance = (R) scopedInstances[0].get(index);
			if (localInstance != null) return instance = localInstance;
			instance = doCreateInstance(scopedInstances, synchronizedScope);
		}
		scopedInstances[0].lazySet(index, instance);
		return instance;
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);
}
