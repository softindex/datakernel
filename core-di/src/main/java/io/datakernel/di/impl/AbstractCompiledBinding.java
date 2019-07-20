package io.datakernel.di.impl;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReferenceArray;

public abstract class AbstractCompiledBinding<R> implements CompiledBinding<R> {
	protected final int level;
	protected final int index;

	protected AbstractCompiledBinding(int level, int index) {
		this.level = level;
		this.index = index;
	}

	@NotNull
	@SuppressWarnings("unchecked")
	@Override
	public final R getInstance(AtomicReferenceArray[] instances) {
		AtomicReferenceArray array = instances[level];
		R instance = (R) array.get(index);
		if (instance != null) return instance;
		//noinspection SynchronizationOnLocalVariableOrMethodParameter
		synchronized (array) {
			instance = (R) array.get(index);
			if (instance != null) return instance;
			instance = createInstance(instances);
			array.set(index, instance);
			return instance;
		}
	}
}
