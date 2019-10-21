package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * This is defined as an abstract class and not a functional interface so that
 * any anonymous class usage is compiled as a inner class and not a method with invokedynamic instruction.
 * This is needed for compiled bindings to be applicable for later specialization.
 */
public abstract class CompiledBinding<R> {
	private static final CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			return null;
		}
	};

	public abstract R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

	@SuppressWarnings("unchecked")
	public static <R> CompiledBinding<R> missingOptionalBinding() {
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}
}
