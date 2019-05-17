package io.datakernel.util.guice;

import io.datakernel.di.Inject;

public final class RequiredDependency<T> {
	@Inject
	private T value;

	public T get() {
		return value;
	}
}
