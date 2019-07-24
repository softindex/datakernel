package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractCompiledBinding<R> implements CompiledBinding<R> {
	protected final int level;
	protected final int index;

	protected AbstractCompiledBinding(int level, int index) {
		this.level = level;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] instances, int lockedLevel) {
		AtomicReferenceArray array = instances[level];
		R instance = (R) array.get(index);
		if (instance != null) return instance;
		if (lockedLevel == level) {
			instance = doCreateInstance(instances, level);
			array.set(index, instance);
			return instance;
		}
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			instance = (R) array.get(index);
			if (instance != null) return instance;
			instance = doCreateInstance(instances, level);
			array.set(index, instance);
			return instance;
		}
	}

	@Override
	public R createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
		if (lockedLevel == level) return doCreateInstance(instances, level);
		AtomicReferenceArray array = instances[level];
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			return doCreateInstance(instances, level);
		}
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] instances, int lockedLevel);

}
