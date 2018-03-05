package io.datakernel.util.guice;

import com.google.inject.Inject;

public final class RequiredDependency<T> {
	@Inject
	private T value;

	public T get() {
		return value;
	}
}
