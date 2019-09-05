package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public interface CompiledBinding<R> {
	R getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

	R createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope);

	static <R> CompiledBinding<R> missingOptionalBinding() {
		//noinspection unchecked
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}

	CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			return null;
		}

		@Override
		public Object createInstance(AtomicReferenceArray[] scopedInstances, int synchronizedScope) {
			return null;
		}
	};
}
