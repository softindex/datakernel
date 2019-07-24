package io.datakernel.di.impl;

import java.util.concurrent.atomic.AtomicReferenceArray;

public interface CompiledBinding<R> {
	R getInstance(AtomicReferenceArray[] instances, int lockedLevel);

	R createInstance(AtomicReferenceArray[] instances, int lockedLevel);

	static <R> CompiledBinding<R> missingOptionalBinding() {
		//noinspection unchecked
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}

	CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] instances, int lockedLevel) {
			return null;
		}

		@Override
		public Object createInstance(AtomicReferenceArray[] instances, int lockedLevel) {
			return null;
		}
	};
}
