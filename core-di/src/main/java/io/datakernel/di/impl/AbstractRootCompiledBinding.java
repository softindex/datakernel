package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractRootCompiledBinding<R> implements CompiledBinding<R> {
	private volatile R instance;
	protected final int scope;
	protected final int index;

	protected AbstractRootCompiledBinding(int scope, int index) {
		this.scope = scope;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		if (instance != null) return instance;
		synchronized (this) {
			if (instance != null) return instance;
			instance = doCreateInstance(scopedInstances, synchronizedScope);
		}
		scopedInstances[scope].lazySet(index, instance);
		return this.instance;
	}

	@Override
	public final R createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		return doCreateInstance(scopedInstances, synchronizedScope);
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

}
