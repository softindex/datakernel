package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * An abstract class instead of an interface for the same reason as {@link CompiledBinding}
 */
@SuppressWarnings("rawtypes")
public interface CompiledBindingInitializer<R> {
	void initInstance(R instance, AtomicReferenceArray[] instances, int synchronizedScope);
}
