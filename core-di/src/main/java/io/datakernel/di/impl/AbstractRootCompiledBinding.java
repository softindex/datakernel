package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractRootCompiledBinding<R> implements CompiledBinding<R> {
	private volatile R instance;
	protected final int level;
	protected final int index;

	protected AbstractRootCompiledBinding(int level, int index) {
		this.level = level;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] instances, int lockedLevel) {
		if (instance != null) return instance;
		synchronized (this) {
			if (instance != null) return instance;
			instance = doCreateInstance(instances, lockedLevel);
		}
		instances[level].lazySet(index, instance);
		return this.instance;
	}

	@Override
	public final R createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
		return doCreateInstance(instances, lockedLevel);
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel);

}
