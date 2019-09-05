package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractCompiledBinding<R> implements CompiledBinding<R> {
	protected final int scope;
	protected final int index;

	protected AbstractCompiledBinding(int scope, int index) {
		this.scope = scope;
		this.index = index;
	}

	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		AtomicReferenceArray array = scopedInstances[scope];
		R instance = (R) array.get(index);
		if (instance != null) return instance;
		if (synchronizedScope == scope) {
			instance = doCreateInstance(scopedInstances, synchronizedScope);
			array.set(index, instance);
			return instance;
		}
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			instance = (R) array.get(index);
			if (instance != null) return instance;
			instance = doCreateInstance(scopedInstances, scope);
			array.set(index, instance);
			return instance;
		}
	}

	@Override
	public R createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
		if (synchronizedScope == scope) return doCreateInstance(scopedInstances, synchronizedScope);
		AtomicReferenceArray array = scopedInstances[scope];
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			return doCreateInstance(scopedInstances, scope);
		}
	}

	protected abstract R doCreateInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

}
