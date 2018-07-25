package io.datakernel.async;

public interface AsyncExecutor {
	<T> Stage<T> execute(AsyncSupplier<T> supplier);
}
