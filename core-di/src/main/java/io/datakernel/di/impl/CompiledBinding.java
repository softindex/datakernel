package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This is defined as an abstract class and not a functional interface so that
 * any anonymous class usage is compiled as a inner class and not a method with invokedynamic instruction.
 * This is needed for compiled bindings to be applicable for later specialization.
 */
public interface CompiledBinding<R> {
	@SuppressWarnings("Convert2Lambda")
	CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			return null;
		}
	};

	R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

	@SuppressWarnings("unchecked")
	static <R> CompiledBinding<R> missingOptionalBinding() {
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}
}
