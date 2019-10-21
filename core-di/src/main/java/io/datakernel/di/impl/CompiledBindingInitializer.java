package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * An abstract class instead of an interface for the same reason as {@link CompiledBinding}
 */
public abstract class CompiledBindingInitializer<R> {
	public abstract void initInstance(R instance, AtomicReferenceArray[] instances, int synchronizedScope);
}
