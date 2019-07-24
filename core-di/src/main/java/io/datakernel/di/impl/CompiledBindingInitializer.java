package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public interface CompiledBindingInitializer<R> {
	void initInstance(R instance, AtomicReferenceArray[] instances, int synchronizedScope);
}
