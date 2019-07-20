package io.datakernel.di.core;

import java.util.concurrent.atomic.AtomicReferenceArray;

public interface CompiledBinding<R> {
	R getInstance(AtomicReferenceArray[] instances);

	R createInstance(AtomicReferenceArray[] instances);

	static <R> CompiledBinding<R> missingOptionalBinding() {
		//noinspection unchecked
		return (CompiledBinding<R>) MISSING_OPTIONAL_BINDING;
	}

	CompiledBinding<?> MISSING_OPTIONAL_BINDING = new CompiledBinding<Object>() {
		@Override
		public Object getInstance(AtomicReferenceArray[] instances) {
			return null;
		}

		@Override
		public Object createInstance(AtomicReferenceArray[] instances) {
			return null;
		}
	};
}
